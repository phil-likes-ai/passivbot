from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_REPEAT_SECONDS = 90
DEFAULT_WATCH_POLL_SECONDS = 20
DEFAULT_TELEGRAM_POLL_SECONDS = 5
DEFAULT_TELEGRAM_TIMEOUT_SECONDS = 20
MAX_OUTPUT_PREVIEW_CHARS = 420
MAX_MESSAGE_CHARS = 3500
MAX_OUTPUT_PREVIEW_LINES = 3
MAX_SENT_MESSAGE_HISTORY = 40
MAX_OPERATOR_COMMAND_HISTORY = 40
DEFAULT_CODEX_CONSUMER_LIMIT = 5


@dataclass(frozen=True)
class TelegramNotificationConfig:
    bot_token: str
    chat_id: str
    allowed_chat_ids: tuple[str, ...] = ()
    allowed_user_ids: tuple[str, ...] = ()
    allowed_usernames: tuple[str, ...] = ()
    replies_enabled: bool = True
    poll_updates_enabled: bool = True
    poll_limit: int = 20
    poll_timeout_seconds: int = DEFAULT_TELEGRAM_TIMEOUT_SECONDS
    poll_interval_seconds: int = DEFAULT_TELEGRAM_POLL_SECONDS
    dispatch_inbox_enabled: bool = False
    dispatch_limit: int = 1
    codex_consumer_enabled: bool = False
    codex_consumer_limit: int = DEFAULT_CODEX_CONSUMER_LIMIT
    codex_input_path: str = ""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _read_latest_turn_preview(logs_dir: Path) -> str:
    if not logs_dir.exists():
        return ""
    turn_logs = sorted(logs_dir.glob("turns-*.jsonl"), key=lambda path: path.stat().st_mtime)
    for log_path in reversed(turn_logs):
        lines = log_path.read_text(encoding="utf-8").splitlines()
        for raw_line in reversed(lines):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            preview = str(payload.get("output_preview") or "").strip()
            if preview:
                return preview
    return ""


def _summarize_text(
    text: str, limit: int = MAX_OUTPUT_PREVIEW_CHARS, max_lines: int = MAX_OUTPUT_PREVIEW_LINES
) -> str:
    raw_lines = [line.strip() for line in (text or "").strip().splitlines() if line.strip()]
    if not raw_lines:
        return ""
    clipped_lines = raw_lines[:max_lines]
    summary = "\n".join(clipped_lines)
    if len(raw_lines) > max_lines:
        summary += "\n..."
    if len(summary) <= limit:
        return summary
    return summary[: limit - 3].rstrip() + "..."


def _display_value(value: Any) -> str:
    if value in (None, "", False):
        return ""
    return str(value)


def _session_state_dir(project_path: Path, session_id: str) -> Path:
    return project_path / ".omx" / "state" / "sessions" / session_id


def _bridge_state_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-progress-bridge-state.json"


def _telegram_inbox_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-inbox.jsonl"


def _telegram_outbox_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-outbox.jsonl"


def _telegram_dispatch_queue_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-dispatch-queue.jsonl"


def _telegram_codex_handoff_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-codex-handoff.jsonl"


def _telegram_latest_codex_handoff_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-latest-codex-handoff.json"


def _telegram_operator_inbox_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-operator-inbox.jsonl"


def _telegram_latest_command_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-latest-operator-command.json"


def _load_bridge_inputs(project_path: Path, session_id: str) -> dict[str, Any]:
    session_dir = _session_state_dir(project_path, session_id)
    logs_dir = project_path / ".omx" / "logs"
    ralph_state = _read_json(session_dir / "ralph-state.json")
    hud_state = _read_json(session_dir / "hud-state.json")
    latest_turn_preview = _read_latest_turn_preview(logs_dir)
    changed_files = _read_changed_files(project_path)
    return {
        "ralph_state": ralph_state,
        "hud_state": hud_state,
        "latest_turn_preview": latest_turn_preview,
        "changed_files": changed_files,
    }


def _read_changed_files(project_path: Path, limit: int = 6) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(project_path), "status", "--short", "--untracked-files=all"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    changed: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        path_text = line[3:].strip()
        if not path_text or path_text.startswith((".omx/", ".pytest_cache/", "tests/.tmp_omx_bridge/")):
            continue
        changed.append(path_text)
        if len(changed) >= limit:
            break
    return changed


def _extract_command_hint(*texts: str) -> str:
    patterns = [
        re.compile(r"(python\b[^\n`]{0,220})", re.IGNORECASE),
        re.compile(r"(py\s+(?:-[^\s]+\s+)?[^\n`]{1,220})", re.IGNORECASE),
        re.compile(r"(pytest\b[^\n`]{0,220})", re.IGNORECASE),
        re.compile(r"(cargo\b[^\n`]{0,220})", re.IGNORECASE),
        re.compile(r"(git\b[^\n`]{0,220})", re.IGNORECASE),
        re.compile(r"(maturin\b[^\n`]{0,220})", re.IGNORECASE),
        re.compile(r"(passivbot\b[^\n`]{0,220})", re.IGNORECASE),
    ]
    command_tokens = {
        "python": ("-m", ".py", "pytest", "unittest", "py_compile"),
        "py": ("-m", ".py", "pytest", "unittest", "py_compile"),
        "pytest": ("-q", "tests/", "::", "-k", "--"),
        "cargo": ("test", "check", "run", "fmt", "clippy"),
        "git": ("status", "diff", "log", "show", "grep"),
        "maturin": ("develop", "build"),
        "passivbot": ("live", "backtest", "optimize"),
    }
    for text in texts:
        if not text:
            continue
        for raw_line in text.splitlines():
            line = " ".join(raw_line.strip().strip("`").split())
            if not line:
                continue
            for pattern in patterns:
                match = pattern.search(line)
                if not match:
                    continue
                cmd = " ".join(match.group(1).split())[:200]
                first = cmd.split()[0].lower()
                required_markers = command_tokens.get(first, ())
                if required_markers and not any(marker in cmd for marker in required_markers):
                    continue
                return cmd
    return ""


def _compose_message(event: str, session_id: str, project_path: Path, payload: dict[str, Any]) -> str:
    ralph_state = payload.get("ralph_state") or {}
    hud_state = payload.get("hud_state") or {}
    latest_turn_preview = payload.get("latest_turn_preview") or ""
    changed_files = payload.get("changed_files") or []

    mode = ralph_state.get("mode") or "omx"
    current_phase = ralph_state.get("current_phase") or "unknown"
    iteration = ralph_state.get("iteration")
    max_iterations = ralph_state.get("max_iterations")
    current_slice = ralph_state.get("current_slice") or "unspecified"
    completed_slice = ralph_state.get("completed_slice") or ""
    blocking_issue = ralph_state.get("blocking_issue") or ""
    updated_at = ralph_state.get("updated_at") or hud_state.get("last_progress_at") or ""
    task_description = ralph_state.get("task_description") or ""
    started_at = ralph_state.get("started_at") or ""
    completed_at = ralph_state.get("completed_at") or ""
    active = bool(ralph_state.get("active"))
    architect_verdict = _display_value(ralph_state.get("architect_verdict"))
    previous_architect_verdict = _display_value(ralph_state.get("previous_architect_verdict"))
    deslop_phase = _display_value(ralph_state.get("deslop_phase"))
    inventory_gate_file = _display_value(ralph_state.get("inventory_gate_file"))
    turn_count = hud_state.get("turn_count")
    status = "active" if active else "inactive"

    latest_output = (
        hud_state.get("last_agent_output")
        or latest_turn_preview
        or payload.get("instruction")
        or ""
    )
    latest_output = _summarize_text(str(latest_output))
    command_hint = _extract_command_hint(
        str(hud_state.get("last_agent_output") or ""),
        str(latest_turn_preview),
        str(payload.get("instruction") or ""),
    )

    iteration_text = ""
    if iteration is not None and max_iterations is not None:
        iteration_text = f"{iteration}/{max_iterations}"
    elif iteration is not None:
        iteration_text = str(iteration)

    if event == "session-start":
        header = "Passivbot OMX started"
    elif event == "session-idle":
        header = "Passivbot OMX progress"
    elif event in {"session-end", "session-stop"}:
        header = "Passivbot OMX finished" if not active else "Passivbot OMX stopped"
    else:
        header = f"Passivbot OMX {event}"

    lines = [header, f"Session: {session_id}", f"Project: {project_path.name}"]
    lines.append(f"State: {status} | Mode: {mode} | Phase: {current_phase}")
    if iteration_text:
        iteration_line = f"Iteration: {iteration_text}"
        if turn_count is not None:
            iteration_line += f" | Turns: {turn_count}"
        lines.append(iteration_line)
    elif turn_count is not None:
        lines.append(f"Turns: {turn_count}")
    if current_slice:
        lines.append(f"Current slice: {current_slice}")
    if completed_slice and event in {"session-idle", "session-end", "session-stop"}:
        lines.append(f"Last completed slice: {completed_slice}")
    if architect_verdict:
        architect_line = f"Architect verdict: {architect_verdict}"
        if previous_architect_verdict:
            architect_line += f" (prev {previous_architect_verdict})"
        lines.append(architect_line)
    if deslop_phase and event in {"session-idle", "session-end", "session-stop"}:
        lines.append(f"Deslop: {deslop_phase}")
    if inventory_gate_file:
        lines.append(f"Inventory gate: {Path(inventory_gate_file).name}")
    if blocking_issue and event not in {"session-end", "session-stop"}:
        lines.append(f"Blocker: {blocking_issue}")
    if task_description:
        lines.append(f"Task: {task_description}")
    if changed_files:
        lines.append("Changed files:")
        lines.extend(f"- {path}" for path in changed_files)
    if command_hint:
        lines.append(f"Current command: {command_hint}")
    elif current_slice:
        lines.append(f"Current focus: {current_slice}")
    if latest_output:
        lines.append("Latest output:")
        lines.extend(f"- {line}" for line in latest_output.splitlines())
    if started_at:
        lines.append(f"Started: {started_at}")
    if updated_at:
        lines.append(f"Updated: {updated_at}")
    if completed_at and event in {"session-end", "session-stop"}:
        lines.append(f"Completed: {completed_at}")
    lines.append("Telegram: reply, /status, /continue, /pause, /stop, /queue, /files, /latest, /mute")

    message = "\n".join(lines).strip()
    if len(message) <= MAX_MESSAGE_CHARS:
        return message
    return message[: MAX_MESSAGE_CHARS - 3].rstrip() + "..."


def _signature_for(event: str, message: str, payload: dict[str, Any]) -> str:
    ralph_state = payload.get("ralph_state") or {}
    hud_state = payload.get("hud_state") or {}
    signature_parts = [
        event,
        str(ralph_state.get("updated_at") or ""),
        str(ralph_state.get("current_phase") or ""),
        str(ralph_state.get("current_slice") or ""),
        str(ralph_state.get("completed_slice") or ""),
        str(ralph_state.get("blocking_issue") or ""),
        str(hud_state.get("last_progress_at") or ""),
        message,
    ]
    return "|".join(signature_parts)


def _load_bridge_state(project_path: Path, session_id: str) -> dict[str, Any]:
    return _read_json(_bridge_state_path(project_path, session_id))


def _save_bridge_state(project_path: Path, session_id: str, state: dict[str, Any]) -> None:
    _write_json(_bridge_state_path(project_path, session_id), state)


def _truncate_history(entries: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return entries[-limit:] if len(entries) > limit else entries


def _should_send(
    event: str,
    signature: str,
    now: datetime,
    bridge_state: dict[str, Any],
    repeat_seconds: int = DEFAULT_REPEAT_SECONDS,
) -> bool:
    if event == "session-idle" and bridge_state.get("telegram_muted"):
        return False
    if event in {"session-end", "session-stop", "session-start"}:
        return bridge_state.get(f"last_signature:{event}") != signature

    if event != "session-idle":
        return True

    last_signature = bridge_state.get("last_signature:session-idle")
    last_sent_at = _parse_iso8601(bridge_state.get("last_sent_at:session-idle"))
    if last_signature != signature:
        return True
    if last_sent_at is None:
        return True
    return (now - last_sent_at).total_seconds() >= repeat_seconds


def _write_bridge_state(project_path: Path, session_id: str, event: str, signature: str, now: datetime) -> None:
    existing = _load_bridge_state(project_path, session_id)
    existing[f"last_signature:{event}"] = signature
    existing[f"last_sent_at:{event}"] = now.isoformat()
    _save_bridge_state(project_path, session_id, existing)


def _normalize_str_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _telegram_inbound_controls_enabled(config: dict[str, Any]) -> bool:
    return any(
        bool(config.get(key))
        for key in (
            "replies_enabled",
            "poll_updates_enabled",
            "dispatch_inbox_enabled",
            "codex_consumer_enabled",
        )
    )


def _validate_telegram_inbound_auth(config_path: Path, config: dict[str, Any]) -> None:
    if not _telegram_inbound_controls_enabled(config):
        return
    if (config.get("allowed_user_ids") or []) or (config.get("allowed_usernames") or []):
        return
    raise RuntimeError(
        "Telegram inbound controls in "
        f"{config_path} require at least one of allowedUserIds or allowedUsernames "
        "when pollUpdatesEnabled, repliesEnabled, dispatchInboxEnabled, or "
        "codexConsumerEnabled are enabled"
    )


def _load_telegram_config(config_path: Path | None = None) -> dict[str, Any]:
    if config_path is None:
        config_path = Path.home() / ".codex" / ".omx-config.json"
    config = _read_json(config_path)
    telegram = ((config.get("notifications") or {}).get("telegram")) or {}
    bot_token = str(telegram.get("botToken") or "").strip()
    chat_id = str(telegram.get("chatId") or "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError(f"Missing Telegram bot token or chat id in {config_path}")
    allowed_chat_ids = _normalize_str_list(telegram.get("allowedChatIds"))
    if chat_id:
        allowed_chat_ids = [chat_id, *[item for item in allowed_chat_ids if item != chat_id]]
    parsed = {
        "bot_token": bot_token,
        "chat_id": chat_id,
        "allowed_chat_ids": allowed_chat_ids,
        "allowed_user_ids": _normalize_str_list(telegram.get("allowedUserIds")),
        "allowed_usernames": [name.lstrip("@").lower() for name in _normalize_str_list(telegram.get("allowedUsernames"))],
        "replies_enabled": bool(telegram.get("repliesEnabled", True)),
        "poll_updates_enabled": bool(telegram.get("pollUpdatesEnabled", True)),
        "poll_limit": int(telegram.get("pollLimit") or 20),
        "poll_timeout_seconds": int(telegram.get("pollTimeoutSeconds") or DEFAULT_TELEGRAM_TIMEOUT_SECONDS),
        "poll_interval_seconds": int(telegram.get("pollIntervalSeconds") or DEFAULT_TELEGRAM_POLL_SECONDS),
        "dispatch_inbox_enabled": bool(telegram.get("dispatchInboxEnabled", False)),
        "dispatch_limit": int(telegram.get("dispatchLimit") or 1),
        "codex_consumer_enabled": bool(telegram.get("codexConsumerEnabled", False)),
        "codex_consumer_limit": int(telegram.get("codexConsumerLimit") or DEFAULT_CODEX_CONSUMER_LIMIT),
        "codex_input_path": str(telegram.get("codexInputPath") or "").strip(),
        "config_path": str(config_path),
    }
    _validate_telegram_inbound_auth(config_path, parsed)
    return parsed


def _load_notification_config(config_path: Path | None = None) -> tuple[str, str]:
    telegram_config = _load_telegram_config(config_path)
    return telegram_config["bot_token"], telegram_config["chat_id"]


def _load_notification_config_details(config_path: Path | None = None) -> TelegramNotificationConfig:
    telegram_config = _load_telegram_config(config_path)
    return TelegramNotificationConfig(
        bot_token=str(telegram_config["bot_token"]),
        chat_id=str(telegram_config["chat_id"]),
        allowed_chat_ids=tuple(str(item) for item in telegram_config.get("allowed_chat_ids") or []),
        allowed_user_ids=tuple(str(item) for item in telegram_config.get("allowed_user_ids") or []),
        allowed_usernames=tuple(str(item) for item in telegram_config.get("allowed_usernames") or []),
        replies_enabled=bool(telegram_config.get("replies_enabled", True)),
        poll_updates_enabled=bool(telegram_config.get("poll_updates_enabled", True)),
        poll_limit=int(telegram_config.get("poll_limit") or 20),
        poll_timeout_seconds=int(telegram_config.get("poll_timeout_seconds") or DEFAULT_TELEGRAM_TIMEOUT_SECONDS),
        poll_interval_seconds=int(telegram_config.get("poll_interval_seconds") or DEFAULT_TELEGRAM_POLL_SECONDS),
        dispatch_inbox_enabled=bool(telegram_config.get("dispatch_inbox_enabled", False)),
        dispatch_limit=int(telegram_config.get("dispatch_limit") or 1),
        codex_consumer_enabled=bool(telegram_config.get("codex_consumer_enabled", False)),
        codex_consumer_limit=int(telegram_config.get("codex_consumer_limit") or DEFAULT_CODEX_CONSUMER_LIMIT),
        codex_input_path=str(telegram_config.get("codex_input_path") or ""),
    )


def _default_codex_input_path(project_path: Path, session_id: str) -> str:
    return str((_session_state_dir(project_path, session_id) / "telegram-codex-input.json").resolve())


def _watcher_pid_path(project_path: Path, session_id: str) -> Path:
    return _session_state_dir(project_path, session_id) / "telegram-progress-watcher.pid"


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _watcher_running(project_path: Path, session_id: str) -> bool:
    pid_path = _watcher_pid_path(project_path, session_id)
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (TypeError, ValueError):
        return False
    return _is_pid_running(pid)


def _write_watcher_pid(project_path: Path, session_id: str) -> None:
    pid_path = _watcher_pid_path(project_path, session_id)
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")


def _clear_watcher_pid(project_path: Path, session_id: str) -> None:
    pid_path = _watcher_pid_path(project_path, session_id)
    try:
        pid_path.unlink()
    except FileNotFoundError:
        pass


def _spawn_watcher(
    *,
    session_id: str,
    project_path: Path,
    config_path: str | None,
    repeat_seconds: int,
    poll_seconds: int,
    telegram_poll_seconds: int,
    telegram_timeout_seconds: int,
) -> dict[str, Any]:
    if _watcher_running(project_path, session_id):
        return {"spawned": False, "reason": "already-running"}

    python_exe = sys.executable
    command = [
        python_exe,
        str((project_path / "src" / "omx_telegram_progress_bridge.py").resolve()),
        "--watch",
        "--session-id",
        session_id,
        "--project-path",
        str(project_path),
        "--repeat-seconds",
        str(repeat_seconds),
        "--poll-seconds",
        str(poll_seconds),
        "--telegram-poll-seconds",
        str(telegram_poll_seconds),
        "--telegram-timeout-seconds",
        str(telegram_timeout_seconds),
    ]
    if config_path:
        command.extend(["--config-path", config_path])

    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NO_WINDOW", 0
        )

    subprocess.Popen(
        command,
        cwd=str(project_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        creationflags=creationflags,
        close_fds=False if os.name == "nt" else True,
    )
    return {"spawned": True, "reason": "started"}


def _telegram_api_request(bot_token: str, method: str, payload: dict[str, Any], timeout: int = 45) -> Any:
    req = request.Request(
        url=f"https://api.telegram.org/bot{bot_token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except error.URLError as exc:
        raise RuntimeError(f"Telegram {method} failed: {exc}") from exc
    result = json.loads(body)
    if not result.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {result}")
    return result.get("result")


def _default_reply_markup() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "Continue", "callback_data": "cmd:continue"},
                {"text": "Pause", "callback_data": "cmd:pause"},
                {"text": "Stop", "callback_data": "cmd:stop"},
            ],
            [
                {"text": "Status", "callback_data": "cmd:status"},
                {"text": "Help", "callback_data": "cmd:help"},
            ],
        ]
    }


def _send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    reply_to_message_id: int | None = None,
    reply_markup: dict[str, Any] | None = None,
    disable_notification: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return _telegram_api_request(bot_token, "sendMessage", payload)


def _send_telegram_reply(
    bot_token: str,
    chat_id: str,
    text: str,
    reply_to_message_id: int | None = None,
) -> dict[str, Any]:
    return _send_telegram_message(
        bot_token,
        chat_id,
        text,
        reply_to_message_id=reply_to_message_id,
    )


def _telegram_get_updates(bot_token: str, offset: int, timeout_seconds: int) -> list[dict[str, Any]]:
    payload = {
        "offset": offset,
        "timeout": timeout_seconds,
        "allowed_updates": ["message", "callback_query"],
    }
    result = _telegram_api_request(bot_token, "getUpdates", payload, timeout=timeout_seconds + 10)
    return list(result or [])


def _telegram_answer_callback(bot_token: str, callback_query_id: str, text: str = "") -> None:
    payload: dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text[:180]
    _telegram_api_request(bot_token, "answerCallbackQuery", payload)


def _record_sent_message(project_path: Path, session_id: str, event: str, message_result: dict[str, Any]) -> None:
    bridge_state = _load_bridge_state(project_path, session_id)
    sent_messages = list(bridge_state.get("telegram_sent_messages") or [])
    sent_messages.append(
        {
            "message_id": int(message_result.get("message_id") or 0),
            "chat_id": str(((message_result.get("chat") or {}).get("id") or "")),
            "session_id": session_id,
            "event": event,
            "sent_at": _utc_now().isoformat(),
        }
    )
    bridge_state["telegram_last_message_id"] = int(message_result.get("message_id") or 0)
    bridge_state["telegram_sent_messages"] = [
        entry for entry in sent_messages if int(entry.get("message_id") or 0) > 0
    ][-MAX_SENT_MESSAGE_HISTORY:]
    _save_bridge_state(project_path, session_id, bridge_state)


def _queue_incoming_message(project_path: Path, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    _append_jsonl(_telegram_inbox_path(project_path, session_id), payload)
    return payload


def _get_pending_inbox_entries(project_path: Path, session_id: str, limit: int) -> list[dict[str, Any]]:
    path = _telegram_inbox_path(project_path, session_id)
    if not path.exists():
        return []
    bridge_state = _load_bridge_state(project_path, session_id)
    consumed_update_id = int(bridge_state.get("telegram:last_consumed_update_id") or 0)
    pending: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        payload = json.loads(raw_line)
        if int(payload.get("update_id") or 0) <= consumed_update_id:
            continue
        pending.append(payload)
        if len(pending) >= limit:
            break
    return pending


def _mark_inbox_consumed(project_path: Path, session_id: str, update_id: int) -> dict[str, Any]:
    bridge_state = _load_bridge_state(project_path, session_id)
    bridge_state["telegram:last_consumed_update_id"] = int(update_id)
    _save_bridge_state(project_path, session_id, bridge_state)
    return bridge_state


def _queue_operator_command(
    *,
    project_path: Path,
    session_id: str,
    command: str,
    text: str,
    source_update: dict[str, Any],
    kind: str,
) -> dict[str, Any]:
    bridge_state = _load_bridge_state(project_path, session_id)
    counter = int(bridge_state.get("telegram_command_counter") or 0) + 1
    bridge_state["telegram_command_counter"] = counter
    entry = {
        "id": f"{session_id}-{counter}",
        "session_id": session_id,
        "command": command,
        "text": text,
        "kind": kind,
        "status": "pending",
        "created_at": _utc_now().isoformat(),
        "source": "telegram",
        "chat_id": str(((source_update.get("chat") or {}).get("id") or "")),
        "user_id": str(((source_update.get("from") or {}).get("id") or "")),
        "username": str(((source_update.get("from") or {}).get("username") or "")),
        "message_id": int(source_update.get("message_id") or 0),
        "reply_to_message_id": int(((source_update.get("reply_to_message") or {}).get("message_id") or 0)),
    }
    history = list(bridge_state.get("telegram_operator_commands") or [])
    history.append(entry)
    bridge_state["telegram_operator_commands"] = history[-MAX_OPERATOR_COMMAND_HISTORY:]
    bridge_state["telegram_last_command_at"] = entry["created_at"]
    _save_bridge_state(project_path, session_id, bridge_state)
    _append_jsonl(_telegram_operator_inbox_path(project_path, session_id), entry)
    _write_json(_telegram_latest_command_path(project_path, session_id), entry)
    return entry


def _queue_codex_handoff(project_path: Path, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    bridge_state = _load_bridge_state(project_path, session_id)
    history = list(bridge_state.get("telegram_codex_handoffs") or [])
    history.append(payload)
    bridge_state["telegram_codex_handoffs"] = _truncate_history(history, MAX_OPERATOR_COMMAND_HISTORY)
    bridge_state["telegram_last_codex_handoff_at"] = str(payload.get("created_at") or _utc_now().isoformat())
    _save_bridge_state(project_path, session_id, bridge_state)
    _append_jsonl(_telegram_codex_handoff_path(project_path, session_id), payload)
    _write_json(_telegram_latest_codex_handoff_path(project_path, session_id), payload)
    return payload


def _summarize_pending_commands(project_path: Path, session_id: str) -> str:
    bridge_state = _load_bridge_state(project_path, session_id)
    commands = list(bridge_state.get("telegram_operator_commands") or [])
    if not commands:
        return "No queued Telegram operator commands."
    lines = ["Queued Telegram operator commands:"]
    for entry in commands[-8:]:
        text = _summarize_text(str(entry.get("text") or ""), limit=120, max_lines=1)
        suffix = f" | {text}" if text else ""
        lines.append(
            f"- {entry.get('id')} | {entry.get('status')} | {entry.get('command')}{suffix}"
        )
    return "\n".join(lines)


def _compose_help_message(session_id: str) -> str:
    return "\n".join(
        [
            "Passivbot OMX Telegram bridge",
            f"Session: {session_id}",
            "Commands:",
            "- reply to a bridge message: queue a free-form instruction for Codex/OMX",
            "- /status: force a fresh status snapshot",
            "- /continue or /resume: queue continue command",
            "- /pause: queue pause command",
            "- /stop: queue stop command",
            "- /latest: show latest output preview",
            "- /files: show changed files",
            "- /queue or /orders: show queued Telegram operator commands",
            "- /mute or /unmute: mute/unmute automatic idle updates",
            "Behavior:",
            "- This bridge queues operator intent to session state files.",
            "- It can also emit Windows-safe Codex handoff payloads for an external Codex runner.",
            "- It does not directly place exchange orders or mutate live trading internals.",
        ]
    )


def _voice_acknowledgement(session_id: str, message: dict[str, Any]) -> str:
    voice = message.get("voice") or {}
    return (
        f"Voice message received for OMX. Session: {session_id}. "
        f"file_id={voice.get('file_id') or 'unknown'} duration={voice.get('duration') or 'unknown'}s"
    )


def _compose_codex_handoff(session_id: str, project_path: Path, entry: dict[str, Any]) -> dict[str, Any]:
    bridge_inputs = _load_bridge_inputs(project_path, session_id)
    ralph_state = bridge_inputs.get("ralph_state") or {}
    hud_state = bridge_inputs.get("hud_state") or {}
    operator_text = str(entry.get("body") or entry.get("text") or "").strip()
    kind = str(entry.get("kind") or "text")
    instruction_lines = [
        f"Telegram operator message for OMX session {session_id}.",
        f"Sender: {entry.get('from_username') or 'unknown'}",
        f"Message kind: {kind}",
        f"Current phase: {ralph_state.get('current_phase') or 'unknown'}",
        f"Current slice: {ralph_state.get('current_slice') or 'unspecified'}",
    ]
    if operator_text:
        instruction_lines.extend(["Operator message:", operator_text])
    elif kind == "voice":
        instruction_lines.extend(["Voice metadata:", json.dumps(entry.get("voice") or {}, sort_keys=True)])
    latest_output = str(hud_state.get("last_agent_output") or bridge_inputs.get("latest_turn_preview") or "").strip()
    if latest_output:
        instruction_lines.extend(["Latest OMX output:", _summarize_text(latest_output, limit=1200, max_lines=12)])
    changed_files = list(bridge_inputs.get("changed_files") or [])
    if changed_files:
        instruction_lines.extend(["Changed files:", *[f"- {path}" for path in changed_files]])
    created_at = _utc_now().isoformat()
    return {
        "session_id": session_id,
        "source": "telegram",
        "created_at": created_at,
        "update_id": int(entry.get("update_id") or 0),
        "message_id": int(entry.get("message_id") or 0),
        "from_username": str(entry.get("from_username") or ""),
        "kind": kind,
        "operator_text": operator_text,
        "voice": entry.get("voice") or {},
        "instruction": "\n".join(instruction_lines).strip(),
        "project_path": str(project_path),
        "consumed_from": "telegram-inbox.jsonl",
    }


def _write_codex_input_file(codex_input_path: str, payload: dict[str, Any]) -> str:
    target_path = Path(codex_input_path).expanduser().resolve()
    _write_json(target_path, payload)
    return str(target_path)


def _sample_telegram_config_block() -> dict[str, Any]:
    return {
        "notifications": {
            "telegram": {
                "botToken": "123456:replace-me",
                "chatId": "123456789",
                "allowedChatIds": ["123456789"],
                "allowedUserIds": ["123456789"],
                "allowedUsernames": ["your_telegram_username"],
                "repliesEnabled": True,
                "pollUpdatesEnabled": True,
                "pollLimit": 20,
                "pollTimeoutSeconds": 20,
                "pollIntervalSeconds": 5,
                "dispatchInboxEnabled": False,
                "dispatchLimit": 1,
                "codexConsumerEnabled": True,
                "codexConsumerLimit": 5,
                "codexInputPath": ""
            }
        }
    }


def _send_bridge_message(
    *,
    project_dir: Path,
    session_id: str,
    bot_token: str,
    chat_id: str,
    text: str,
    event: str,
    reply_to_message_id: int | None = None,
    include_controls: bool = False,
    disable_notification: bool = False,
) -> dict[str, Any]:
    result = _send_telegram_message(
        bot_token,
        chat_id,
        text,
        reply_to_message_id=reply_to_message_id,
        reply_markup=_default_reply_markup() if include_controls else None,
        disable_notification=disable_notification,
    )
    if isinstance(result, dict):
        _record_sent_message(project_dir, session_id, event, result)
    return result


def _authorized_message(
    telegram_config: dict[str, Any],
    message_like: dict[str, Any],
) -> bool:
    chat = message_like.get("chat") or ((message_like.get("message") or {}).get("chat") or {})
    user = message_like.get("from") or ((message_like.get("message") or {}).get("from") or {})
    allowed_chat_ids = set(telegram_config.get("allowed_chat_ids") or [])
    allowed_user_ids = set(telegram_config.get("allowed_user_ids") or [])
    allowed_usernames = set(telegram_config.get("allowed_usernames") or [])
    chat_id = str(chat.get("id") or "")
    user_id = str(user.get("id") or "")
    username = str(user.get("username") or "").lower()
    if allowed_chat_ids and chat_id not in allowed_chat_ids:
        return False
    if not allowed_user_ids and not allowed_usernames:
        return False
    if allowed_user_ids and user_id not in allowed_user_ids:
        return False
    if allowed_usernames and username not in allowed_usernames:
        return False
    return True


def _authorized_from_notification_config(
    config: TelegramNotificationConfig,
    message_like: dict[str, Any],
) -> bool:
    return _authorized_message(
        {
            "allowed_chat_ids": list(config.allowed_chat_ids),
            "allowed_user_ids": list(config.allowed_user_ids),
            "allowed_usernames": list(config.allowed_usernames),
        },
        message_like,
    )


def _message_targets_session(bridge_state: dict[str, Any], message: dict[str, Any]) -> bool:
    reply_to_message = message.get("reply_to_message") or {}
    reply_to_message_id = int(reply_to_message.get("message_id") or 0)
    if reply_to_message_id <= 0:
        return False
    for entry in bridge_state.get("telegram_sent_messages") or []:
        if int(entry.get("message_id") or 0) == reply_to_message_id:
            return True
    return False


def _parse_command_text(text: str) -> tuple[str, str]:
    normalized = " ".join((text or "").strip().split())
    if not normalized.startswith("/"):
        return "", normalized
    command_text = normalized.split(maxsplit=1)[0][1:]
    command = command_text.split("@", maxsplit=1)[0].lower()
    args = normalized[len(normalized.split(maxsplit=1)[0]) :].strip()
    return command, args


def _process_inbound_command(
    *,
    project_dir: Path,
    session_id: str,
    message: dict[str, Any],
    command: str,
    args: str,
    bot_token: str,
    chat_id: str,
    config_path: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    del config_path
    reply_to_message_id = int(message.get("message_id") or 0)
    if command in {"help", "start"}:
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=_compose_help_message(session_id),
                event="telegram-help",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": "help"}

    if command == "status":
        result = run(
            event="session-idle",
            session_id=session_id,
            project_path=str(project_dir),
            instruction="telegram-/status",
            repeat_seconds=DEFAULT_REPEAT_SECONDS,
            config_path=None,
            dry_run=dry_run,
            force_send=True,
        )
        return {"action": "status", "result": result}

    if command == "latest":
        payload = _load_bridge_inputs(project_dir, session_id)
        latest_text = (
            payload.get("hud_state", {}).get("last_agent_output")
            or payload.get("latest_turn_preview")
            or "No latest output available."
        )
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=_summarize_text(str(latest_text), limit=1200, max_lines=12) or "No latest output available.",
                event="telegram-latest",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": "latest"}

    if command == "files":
        files = _read_changed_files(project_dir, limit=20)
        text = "Changed files:\n" + "\n".join(f"- {item}" for item in files) if files else "No changed files."
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=text,
                event="telegram-files",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": "files"}

    if command in {"mute", "unmute"}:
        bridge_state = _load_bridge_state(project_dir, session_id)
        bridge_state["telegram_muted"] = command == "mute"
        _save_bridge_state(project_dir, session_id, bridge_state)
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=(
                    "Automatic idle Telegram updates muted. Terminal events still send."
                    if command == "mute"
                    else "Automatic idle Telegram updates unmuted."
                ),
                event=f"telegram-{command}",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": command}

    if command in {"queue", "orders"}:
        summary = _summarize_pending_commands(project_dir, session_id)
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=summary,
                event="telegram-queue",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": "queue"}

    queueable_commands = {
        "continue": "continue",
        "resume": "continue",
        "pause": "pause",
        "stop": "stop",
        "reply": "reply",
        "note": "reply",
    }
    if command in queueable_commands:
        entry = _queue_operator_command(
            project_path=project_dir,
            session_id=session_id,
            command=queueable_commands[command],
            text=args,
            source_update=message,
            kind="command",
        )
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=f"Queued {entry['command']} for {session_id} as {entry['id']}",
                event="telegram-queued",
                reply_to_message_id=reply_to_message_id,
            )
        return {"action": "queued", "entry": entry}

    if not dry_run:
        _send_bridge_message(
            project_dir=project_dir,
            session_id=session_id,
            bot_token=bot_token,
            chat_id=chat_id,
            text=_compose_help_message(session_id),
            event="telegram-help",
            reply_to_message_id=reply_to_message_id,
        )
    return {"action": "unknown"}


def process_telegram_updates_once(
    *,
    session_id: str,
    project_path: str,
    config_path: str | None = None,
    dry_run: bool = False,
    timeout_seconds: int = DEFAULT_TELEGRAM_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    telegram_config = _load_telegram_config(Path(config_path) if config_path else None)
    bot_token = str(telegram_config["bot_token"])
    chat_id = str(telegram_config["chat_id"])
    bridge_state = _load_bridge_state(project_dir, session_id)
    offset = int(bridge_state.get("telegram_update_offset") or 0)
    updates = _telegram_get_updates(bot_token, offset, timeout_seconds)
    processed = 0
    queued = 0
    ignored = 0
    for update in updates:
        update_id = int(update.get("update_id") or 0)
        if update_id > 0:
            bridge_state["telegram_update_offset"] = update_id + 1
        message = update.get("message")
        callback_query = update.get("callback_query")
        message_like = callback_query or message or {}
        if not message_like:
            ignored += 1
            continue
        if not _authorized_message(telegram_config, message_like):
            ignored += 1
            continue
        if callback_query:
            _telegram_answer_callback(bot_token, str(callback_query.get("id") or ""), text="Received")
            callback_message = callback_query.get("message") or {}
            callback_data = str(callback_query.get("data") or "")
            command = callback_data.replace("cmd:", "", 1).strip().lower()
            if command == "status":
                _process_inbound_command(
                    project_dir=project_dir,
                    session_id=session_id,
                    message=callback_message,
                    command="status",
                    args="",
                    bot_token=bot_token,
                    chat_id=chat_id,
                    config_path=config_path,
                    dry_run=dry_run,
                )
            elif command == "help":
                _process_inbound_command(
                    project_dir=project_dir,
                    session_id=session_id,
                    message=callback_message,
                    command="help",
                    args="",
                    bot_token=bot_token,
                    chat_id=chat_id,
                    config_path=config_path,
                    dry_run=dry_run,
                )
            elif command in {"continue", "pause", "stop"}:
                entry = _queue_operator_command(
                    project_path=project_dir,
                    session_id=session_id,
                    command=command,
                    text="",
                    source_update={
                        **callback_message,
                        "from": callback_query.get("from") or {},
                    },
                    kind="callback",
                )
                queued += 1
                if not dry_run:
                    _send_bridge_message(
                        project_dir=project_dir,
                        session_id=session_id,
                        bot_token=bot_token,
                        chat_id=chat_id,
                        text=f"Queued {command} for {session_id} as {entry['id']}",
                        event="telegram-queued",
                        reply_to_message_id=int(callback_message.get("message_id") or 0),
                    )
            else:
                ignored += 1
                continue
            processed += 1
            continue

        assert message is not None
        text = str(message.get("text") or "").strip()
        if not text:
            ignored += 1
            continue
        command, args = _parse_command_text(text)
        if command:
            result = _process_inbound_command(
                project_dir=project_dir,
                session_id=session_id,
                message=message,
                command=command,
                args=args,
                bot_token=bot_token,
                chat_id=chat_id,
                config_path=config_path,
                dry_run=dry_run,
            )
            if result.get("action") == "queued":
                queued += 1
            processed += 1
            continue
        if not _message_targets_session(bridge_state, message):
            ignored += 1
            continue
        entry = _queue_operator_command(
            project_path=project_dir,
            session_id=session_id,
            command="reply",
            text=text,
            source_update=message,
            kind="reply",
        )
        queued += 1
        processed += 1
        if not dry_run:
            _send_bridge_message(
                project_dir=project_dir,
                session_id=session_id,
                bot_token=bot_token,
                chat_id=chat_id,
                text=f"Queued reply for {session_id} as {entry['id']}",
                event="telegram-queued",
                reply_to_message_id=int(message.get("message_id") or 0),
            )
        bridge_state = _load_bridge_state(project_dir, session_id)
    _save_bridge_state(project_dir, session_id, bridge_state)
    return {
        "session_id": session_id,
        "project_path": str(project_dir),
        "processed": processed,
        "queued": queued,
        "ignored": ignored,
        "telegram_update_offset": int(bridge_state.get("telegram_update_offset") or 0),
    }


def _process_incoming_updates(
    *,
    session_id: str,
    project_path: Path,
    config: TelegramNotificationConfig,
    bridge_state: dict[str, Any],
) -> dict[str, Any]:
    raw_result = _telegram_api_request(
        config.bot_token,
        "getUpdates",
        {
            "offset": int(bridge_state.get("telegram:last_update_id") or 0) + 1,
            "timeout": config.poll_timeout_seconds,
            "limit": config.poll_limit,
            "allowed_updates": ["message", "callback_query"],
        },
    )
    if isinstance(raw_result, dict) and "result" in raw_result:
        updates = list(raw_result.get("result") or [])
    else:
        updates = list(raw_result or [])

    processed = 0
    queued = 0
    replied = 0
    last_update_id = int(bridge_state.get("telegram:last_update_id") or 0)
    for update in updates:
        processed += 1
        update_id = int(update.get("update_id") or 0)
        if update_id > last_update_id:
            last_update_id = update_id
        message = update.get("message") or {}
        callback_query = update.get("callback_query") or {}
        message_like = callback_query or message
        if not message_like:
            continue
        if not _authorized_from_notification_config(config, message_like):
            continue
        if callback_query:
            callback_id = str(callback_query.get("id") or "")
            if callback_id:
                _telegram_answer_callback(config.bot_token, callback_id, text="Received")
            callback_message = callback_query.get("message") or {}
            callback_data = str(callback_query.get("data") or "")
            command = callback_data.replace("cmd:", "", 1).strip().lower()
            if command in {"status", "help", "latest", "files", "queue", "orders", "mute", "unmute"}:
                _process_inbound_command(
                    project_dir=project_path,
                    session_id=session_id,
                    message=callback_message,
                    command=command,
                    args="",
                    bot_token=config.bot_token,
                    chat_id=str((callback_message.get("chat") or {}).get("id") or config.chat_id),
                    config_path=None,
                    dry_run=False,
                )
                replied += 1
                continue
            if command in {"continue", "pause", "stop"}:
                _queue_operator_command(
                    project_path=project_path,
                    session_id=session_id,
                    command=command,
                    text="",
                    source_update={
                        **callback_message,
                        "from": callback_query.get("from") or {},
                    },
                    kind="callback",
                )
                queued += 1
                _send_bridge_message(
                    project_dir=project_path,
                    session_id=session_id,
                    bot_token=config.bot_token,
                    chat_id=str((callback_message.get("chat") or {}).get("id") or config.chat_id),
                    text=f"Queued {command} for {session_id}",
                    event="telegram-queued",
                    reply_to_message_id=int(callback_message.get("message_id") or 0),
                )
                replied += 1
            continue

        assert message
        chat_id = str((message.get("chat") or {}).get("id") or config.chat_id)
        from_user = message.get("from") or {}
        if message.get("voice"):
            payload = {
                "update_id": update_id,
                "message_id": int(message.get("message_id") or 0),
                "chat_id": chat_id,
                "from_username": str(from_user.get("username") or ""),
                "text": "",
                "body": "",
                "kind": "voice",
                "has_voice": True,
                "voice": message.get("voice") or {},
            }
            _queue_incoming_message(project_path, session_id, payload)
            queued += 1
            if config.replies_enabled:
                _append_jsonl(
                    _telegram_outbox_path(project_path, session_id),
                    {"event": "ack-reply", "text": _voice_acknowledgement(session_id, message)},
                )
                _send_telegram_reply(
                    config.bot_token,
                    chat_id,
                    _voice_acknowledgement(session_id, message),
                    reply_to_message_id=int(message.get("message_id") or 0),
                )
                replied += 1
            continue

        text = str(message.get("text") or "").strip()
        if not text:
            continue
        command, args = _parse_command_text(text)
        if command == "reply":
            payload = {
                "update_id": update_id,
                "message_id": int(message.get("message_id") or 0),
                "chat_id": chat_id,
                "from_username": str(from_user.get("username") or ""),
                "text": text,
                "body": args,
                "kind": "text",
                "command": "/reply",
            }
            _queue_incoming_message(project_path, session_id, payload)
            queued += 1
            if config.replies_enabled:
                _append_jsonl(_telegram_outbox_path(project_path, session_id), {"event": "ack-reply", "text": "Queued for OMX."})
                _send_telegram_reply(
                    config.bot_token,
                    chat_id,
                    "Queued for OMX.",
                    reply_to_message_id=int(message.get("message_id") or 0),
                )
                replied += 1
            continue
        if command:
            result = _process_inbound_command(
                project_dir=project_path,
                session_id=session_id,
                message=message,
                command=command,
                args=args,
                bot_token=config.bot_token,
                chat_id=chat_id,
                config_path=None,
                dry_run=False,
            )
            if result.get("action") == "queued":
                queued += 1
            replied += 1
            continue
        if _message_targets_session(_load_bridge_state(project_path, session_id), message):
            payload = {
                "update_id": update_id,
                "message_id": int(message.get("message_id") or 0),
                "chat_id": chat_id,
                "from_username": str(from_user.get("username") or ""),
                "text": text,
                "body": text,
                "kind": "text",
                "command": "reply-target",
            }
            _queue_incoming_message(project_path, session_id, payload)
            queued += 1
            if config.replies_enabled:
                _append_jsonl(_telegram_outbox_path(project_path, session_id), {"event": "ack-reply", "text": "Queued for OMX."})
                _send_telegram_reply(
                    config.bot_token,
                    chat_id,
                    "Queued for OMX.",
                    reply_to_message_id=int(message.get("message_id") or 0),
                )
                replied += 1
            continue
    return {
        "processed": processed,
        "queued": queued,
        "replied": replied,
        "last_update_id": last_update_id,
    }


def _load_tmux_hook_config(project_path: Path) -> dict[str, Any]:
    path = project_path / ".omx" / "tmux-hook.json"
    return _read_json(path)


def _build_dispatch_prompt(session_id: str, entry: dict[str, Any], tmux_config: dict[str, Any]) -> str:
    marker = str(tmux_config.get("marker") or "[OMX_TMUX_INJECT]")
    prompt_template = str(tmux_config.get("prompt_template") or marker)
    if entry.get("kind") == "voice":
        body = (
            f"Telegram message for OMX session {session_id}. "
            f"Sender={entry.get('from_username') or 'unknown'}. Voice metadata: "
            f"{json.dumps(entry.get('voice') or {}, sort_keys=True)}"
        )
    else:
        body = (
            f"Telegram message for OMX session {session_id}. "
            f"Sender={entry.get('from_username') or 'unknown'}. Body: {entry.get('body') or entry.get('text') or ''}"
        )
    return prompt_template.replace(marker, body)


def dispatch_next_inbox_entry(
    *,
    session_id: str,
    project_path: str,
    limit: int = 1,
    mark_consumed: bool = False,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    pending = _get_pending_inbox_entries(project_dir, session_id, limit=limit)
    tmux_config = _load_tmux_hook_config(project_dir)
    dispatched: list[dict[str, Any]] = []
    for entry in pending:
        payload = {
            **entry,
            "session_id": session_id,
            "tmux_enabled": bool(tmux_config.get("enabled")),
            "tmux_target": ((tmux_config.get("target") or {}).get("value") or ""),
            "marker": str(tmux_config.get("marker") or "[OMX_TMUX_INJECT]"),
            "prompt": _build_dispatch_prompt(session_id, entry, tmux_config),
        }
        _append_jsonl(_telegram_dispatch_queue_path(project_dir, session_id), payload)
        dispatched.append(payload)
        if mark_consumed:
            _mark_inbox_consumed(project_dir, session_id, int(entry.get("update_id") or 0))
    return {"count": len(dispatched), "dispatched": dispatched}


def consume_telegram_inbox_to_codex(
    *,
    session_id: str,
    project_path: str,
    limit: int = DEFAULT_CODEX_CONSUMER_LIMIT,
    codex_input_path: str = "",
    mark_consumed: bool = False,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    pending = _get_pending_inbox_entries(project_dir, session_id, limit=limit)
    consumed: list[dict[str, Any]] = []
    for entry in pending:
        payload = _compose_codex_handoff(session_id, project_dir, entry)
        if codex_input_path:
            payload["codex_input_path"] = _write_codex_input_file(codex_input_path, payload)
        _queue_codex_handoff(project_dir, session_id, payload)
        consumed.append(payload)
        if mark_consumed:
            _mark_inbox_consumed(project_dir, session_id, int(entry.get("update_id") or 0))
    return {"count": len(consumed), "consumed": consumed}


def reset_connector_state(*, session_id: str, project_path: str) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    session_dir = _session_state_dir(project_dir, session_id)
    removed_files: list[str] = []
    for path in [
        _bridge_state_path(project_dir, session_id),
        _telegram_inbox_path(project_dir, session_id),
        _telegram_outbox_path(project_dir, session_id),
        _telegram_dispatch_queue_path(project_dir, session_id),
        _telegram_operator_inbox_path(project_dir, session_id),
        _telegram_latest_command_path(project_dir, session_id),
        _telegram_codex_handoff_path(project_dir, session_id),
        _telegram_latest_codex_handoff_path(project_dir, session_id),
        _watcher_pid_path(project_dir, session_id),
    ]:
        try:
            path.unlink()
            removed_files.append(str(path))
        except FileNotFoundError:
            continue
    session_dir.mkdir(parents=True, exist_ok=True)
    reset_state = {"reset_at": _utc_now().isoformat(), "telegram_muted": False}
    _save_bridge_state(project_dir, session_id, reset_state)
    return {
        "session_id": session_id,
        "project_path": str(project_dir),
        "removed_files": removed_files,
        "bridge_state": reset_state,
    }


def send_connector_message(
    *,
    session_id: str,
    project_path: str,
    text: str,
    event: str,
    reply_to_message_id: int | None = None,
    config_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    config = _load_notification_config_details(Path(config_path) if config_path else None)
    payload = {
        "event": event,
        "text": text,
        "reply_to_message_id": reply_to_message_id,
        "created_at": _utc_now().isoformat(),
    }
    _append_jsonl(_telegram_outbox_path(project_dir, session_id), payload)
    if not dry_run:
        _send_telegram_reply(config.bot_token, config.chat_id, text, reply_to_message_id=reply_to_message_id)
    return {
        "event": event,
        "chat_id": config.chat_id,
        "reply_to_message_id": reply_to_message_id,
        "sent": not dry_run,
    }


def _audit_connector_state(project_path: Path, session_id: str) -> dict[str, Any]:
    bridge_state = _load_bridge_state(project_path, session_id)
    findings: list[str] = []
    if bridge_state.get("telegram_sent_messages") and not isinstance(bridge_state.get("telegram_sent_messages"), list):
        findings.append("telegram_sent_messages should be a list")
    if bridge_state.get("telegram_operator_commands") and not isinstance(
        bridge_state.get("telegram_operator_commands"), list
    ):
        findings.append("telegram_operator_commands should be a list")
    for path in [
        _telegram_inbox_path(project_path, session_id),
        _telegram_outbox_path(project_path, session_id),
        _telegram_dispatch_queue_path(project_path, session_id),
        _telegram_operator_inbox_path(project_path, session_id),
        _telegram_codex_handoff_path(project_path, session_id),
    ]:
        if not path.exists():
            continue
        for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not raw_line.strip():
                findings.append(f"{path.name}: blank line at {line_no}")
                continue
            try:
                json.loads(raw_line)
            except json.JSONDecodeError as exc:
                findings.append(f"{path.name}: invalid JSON at line {line_no}: {exc}")
    return {
        "session_id": session_id,
        "project_path": str(project_path),
        "ok": not findings,
        "findings": findings,
        "bridge_state_keys": sorted(bridge_state.keys()),
    }


def run(
    *,
    event: str,
    session_id: str,
    project_path: str,
    instruction: str = "",
    repeat_seconds: int = DEFAULT_REPEAT_SECONDS,
    config_path: str | None = None,
    dry_run: bool = False,
    force_send: bool = False,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    payload = _load_bridge_inputs(project_dir, session_id)
    payload["instruction"] = instruction
    ralph_state = payload.get("ralph_state") or {}
    message = _compose_message(event, session_id, project_dir, payload)
    signature = _signature_for(event, message, payload)
    now = _utc_now()
    bridge_state = _load_bridge_state(project_dir, session_id)
    should_send = force_send or _should_send(event, signature, now, bridge_state, repeat_seconds=repeat_seconds)
    if event in {"session-end", "session-stop"}:
        if ralph_state.get("active") and not ralph_state.get("completed_at"):
            should_send = False

    result = {
        "event": event,
        "session_id": session_id,
        "project_path": str(project_dir),
        "message": message,
        "should_send": should_send,
    }
    if not should_send:
        return result

    if not dry_run:
        bot_token, chat_id = _load_notification_config(Path(config_path) if config_path else None)
        send_result = _send_bridge_message(
            project_dir=project_dir,
            session_id=session_id,
            bot_token=bot_token,
            chat_id=chat_id,
            text=message,
            event=event,
            include_controls=True,
            disable_notification=event == "session-idle",
        )
        result["telegram_result"] = send_result
    _write_bridge_state(project_dir, session_id, event, signature, now)
    return result


def watch_session(
    *,
    session_id: str,
    project_path: str,
    repeat_seconds: int = DEFAULT_REPEAT_SECONDS,
    poll_seconds: int = DEFAULT_WATCH_POLL_SECONDS,
    telegram_poll_seconds: int = DEFAULT_TELEGRAM_POLL_SECONDS,
    telegram_timeout_seconds: int = DEFAULT_TELEGRAM_TIMEOUT_SECONDS,
    config_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    project_dir = Path(project_path).resolve()
    _write_watcher_pid(project_dir, session_id)
    notification_config: TelegramNotificationConfig | None = None
    last_telegram_poll_at = 0.0
    if not dry_run:
        notification_config = _load_notification_config_details(Path(config_path) if config_path else None)
    try:
        start_result = run(
            event="session-start",
            session_id=session_id,
            project_path=str(project_dir),
            instruction="watcher-start",
            repeat_seconds=repeat_seconds,
            config_path=config_path,
            dry_run=dry_run,
        )
        last_result = start_result
        while True:
            if notification_config is not None and notification_config.poll_updates_enabled:
                now_monotonic = time.monotonic()
                poll_interval_seconds = max(1, notification_config.poll_interval_seconds or telegram_poll_seconds)
                if now_monotonic - last_telegram_poll_at >= poll_interval_seconds:
                    bridge_state = _load_bridge_state(project_dir, session_id)
                    incoming_result = _process_incoming_updates(
                        session_id=session_id,
                        project_path=project_dir,
                        config=notification_config,
                        bridge_state=bridge_state,
                    )
                    bridge_state["telegram:last_update_id"] = int(incoming_result.get("last_update_id") or 0)
                    _save_bridge_state(project_dir, session_id, bridge_state)
                    if notification_config.dispatch_inbox_enabled:
                        dispatch_next_inbox_entry(
                            session_id=session_id,
                            project_path=str(project_dir),
                            limit=notification_config.dispatch_limit,
                            mark_consumed=True,
                        )
                    if notification_config.codex_consumer_enabled:
                        consume_telegram_inbox_to_codex(
                            session_id=session_id,
                            project_path=str(project_dir),
                            limit=notification_config.codex_consumer_limit,
                            codex_input_path=notification_config.codex_input_path or _default_codex_input_path(project_dir, session_id),
                            mark_consumed=True,
                        )
                    last_telegram_poll_at = now_monotonic
            time.sleep(max(3, poll_seconds))
            payload = _load_bridge_inputs(project_dir, session_id)
            ralph_state = payload.get("ralph_state") or {}
            if ralph_state:
                event = "session-end" if not ralph_state.get("active") else "session-idle"
                last_result = run(
                    event=event,
                    session_id=session_id,
                    project_path=str(project_dir),
                    instruction="watcher-poll",
                    repeat_seconds=repeat_seconds,
                    config_path=config_path,
                    dry_run=dry_run,
                )
                if event == "session-end":
                    break
        return last_result
    finally:
        _clear_watcher_pid(project_dir, session_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send and receive OMX progress updates through Telegram.")
    parser.add_argument("--event")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--project-path", required=True)
    parser.add_argument("--instruction", default="")
    parser.add_argument("--repeat-seconds", type=int, default=DEFAULT_REPEAT_SECONDS)
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_WATCH_POLL_SECONDS)
    parser.add_argument("--telegram-poll-seconds", type=int, default=DEFAULT_TELEGRAM_POLL_SECONDS)
    parser.add_argument("--telegram-timeout-seconds", type=int, default=DEFAULT_TELEGRAM_TIMEOUT_SECONDS)
    parser.add_argument("--config-path")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--process-telegram-updates", action="store_true")
    parser.add_argument("--consume-telegram-inbox", action="store_true")
    parser.add_argument("--consume-limit", type=int, default=DEFAULT_CODEX_CONSUMER_LIMIT)
    parser.add_argument("--codex-input-path", default="")
    parser.add_argument("--reset-connector-state", action="store_true")
    parser.add_argument("--audit-connector", action="store_true")
    parser.add_argument("--print-sample-config", action="store_true")
    args = parser.parse_args(argv)

    if args.print_sample_config:
        result = _sample_telegram_config_block()
    elif args.reset_connector_state:
        result = reset_connector_state(session_id=args.session_id, project_path=args.project_path)
    elif args.audit_connector:
        result = _audit_connector_state(Path(args.project_path).resolve(), args.session_id)
    elif args.watch:
        result = watch_session(
            session_id=args.session_id,
            project_path=args.project_path,
            repeat_seconds=args.repeat_seconds,
            poll_seconds=args.poll_seconds,
            telegram_poll_seconds=args.telegram_poll_seconds,
            telegram_timeout_seconds=args.telegram_timeout_seconds,
            config_path=args.config_path,
            dry_run=args.dry_run,
        )
    elif args.process_telegram_updates:
        result = process_telegram_updates_once(
            session_id=args.session_id,
            project_path=args.project_path,
            config_path=args.config_path,
            dry_run=args.dry_run,
            timeout_seconds=args.telegram_timeout_seconds,
        )
    elif args.consume_telegram_inbox:
        result = consume_telegram_inbox_to_codex(
            session_id=args.session_id,
            project_path=args.project_path,
            limit=args.consume_limit,
            codex_input_path=args.codex_input_path,
            mark_consumed=True,
        )
    else:
        if not args.event:
            parser.error(
                "--event is required unless --watch, --process-telegram-updates, --consume-telegram-inbox, --reset-connector-state, --audit-connector, or --print-sample-config is used"
            )
        result = run(
            event=args.event,
            session_id=args.session_id,
            project_path=args.project_path,
            instruction=args.instruction,
            repeat_seconds=args.repeat_seconds,
            config_path=args.config_path,
            dry_run=args.dry_run,
        )
        if args.event == "session-start" and not args.dry_run:
            spawn_result = _spawn_watcher(
                session_id=args.session_id,
                project_path=Path(args.project_path).resolve(),
                config_path=args.config_path,
                repeat_seconds=args.repeat_seconds,
                poll_seconds=args.poll_seconds,
                telegram_poll_seconds=args.telegram_poll_seconds,
                telegram_timeout_seconds=args.telegram_timeout_seconds,
            )
            result["watcher"] = spawn_result
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
