import json
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

import omx_telegram_progress_bridge as bridge


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


@contextmanager
def _workspace_tmp_dir() -> Iterator[Path]:
    path = Path("tests/.tmp_omx_bridge") / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_idle_message_summarizes_current_ralph_state():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        logs_dir = project_path / ".omx" / "logs"
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "executing",
                "iteration": 3,
                "max_iterations": 20,
                "current_slice": "warmup hardening",
                "completed_slice": "slice 1 execution prep hardening",
                "architect_verdict": "APPROVE",
                "previous_architect_verdict": "ITERATE",
                "deslop_phase": "no-op complete",
                "inventory_gate_file": ".omx/state/reliability-first-slice-inventory.md",
                "task_description": "Eliminate silent failures",
                "started_at": "2026-04-11T08:30:00Z",
                "updated_at": "2026-04-11T09:00:00Z",
                "active": True,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "Patched warmup_candles_staggered and running tests.\nTargeted pytest is next.\nThen architect verification.",
                "last_progress_at": "2026-04-11T09:00:01Z",
                "turn_count": 17,
            },
        )
        logs_dir.mkdir(parents=True, exist_ok=True)
        (logs_dir / "turns-2026-04-11.jsonl").write_text("", encoding="utf-8")

        payload = bridge._load_bridge_inputs(project_path, session_id)
        payload["changed_files"] = ["src/passivbot.py", "tests/test_passivbot_execution.py"]
        message = bridge._compose_message("session-idle", session_id, project_path, payload)

        assert "Passivbot OMX progress" in message
        assert "State: active | Mode: ralph | Phase: executing" in message
        assert "Iteration: 3/20 | Turns: 17" in message
        assert "Current slice: warmup hardening" in message
        assert "Last completed slice: slice 1 execution prep hardening" in message
        assert "Architect verdict: APPROVE (prev ITERATE)" in message
        assert "Deslop: no-op complete" in message
        assert "Inventory gate: reliability-first-slice-inventory.md" in message
        assert "Task: Eliminate silent failures" in message
        assert "Changed files:" in message
        assert "- src/passivbot.py" in message
        assert "- tests/test_passivbot_execution.py" in message
        assert "Current focus: warmup hardening" in message
        assert "Latest output:" in message
        assert "- Patched warmup_candles_staggered and running tests." in message
        assert "- Targeted pytest is next." in message
        assert "Started: 2026-04-11T08:30:00Z" in message


def test_idle_event_dedupes_identical_updates_inside_repeat_window(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "executing",
                "iteration": 4,
                "max_iterations": 20,
                "current_slice": "slice 3",
                "updated_at": "2026-04-11T09:05:00Z",
                "active": True,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "Running targeted pytest.",
                "last_progress_at": "2026-04-11T09:05:01Z",
                "turn_count": 22,
            },
        )
        sent_messages = []

        monkeypatch.setattr(
            bridge, "_load_notification_config", lambda config_path=None: ("token", "chat")
        )
        monkeypatch.setattr(
            bridge,
            "_send_telegram_message",
            lambda bot_token, chat_id, text, **kwargs: sent_messages.append((bot_token, chat_id, text, kwargs)),
        )

        first = bridge.run(
            event="session-idle",
            session_id=session_id,
            project_path=str(project_path),
            repeat_seconds=90,
        )
        second = bridge.run(
            event="session-idle",
            session_id=session_id,
            project_path=str(project_path),
            repeat_seconds=90,
        )

        assert first["should_send"] is True
        assert second["should_send"] is False
        assert len(sent_messages) == 1


def test_session_end_message_reports_finished_without_waiting_for_input():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "verifying",
                "current_slice": "slice 3 verification",
                "completed_slice": "warmup hardening",
                "architect_verdict": "APPROVE",
                "updated_at": "2026-04-11T09:10:00Z",
                "completed_at": "2026-04-11T09:11:00Z",
                "active": False,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "12 tests passed; slice 3 is complete.",
                "last_progress_at": "2026-04-11T09:10:30Z",
                "turn_count": 29,
            },
        )

        payload = bridge._load_bridge_inputs(project_path, session_id)
        payload["changed_files"] = ["src/omx_telegram_progress_bridge.py"]
        message = bridge._compose_message("session-end", session_id, project_path, payload)

        assert "Passivbot OMX finished" in message
        assert "waiting for input" not in message.lower()
        assert "State: inactive | Mode: ralph | Phase: verifying" in message
        assert "Iteration:" not in message
        assert "Architect verdict: APPROVE" in message
        assert "Completed: 2026-04-11T09:11:00Z" in message
        assert "- src/omx_telegram_progress_bridge.py" in message
        assert "- 12 tests passed; slice 3 is complete." in message


def test_stop_event_skips_false_terminal_alert_while_ralph_is_still_active(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "executing",
                "current_slice": "slice 3 verification",
                "updated_at": "2026-04-11T09:10:00Z",
                "active": True,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "Waiting on architect verifier.",
                "last_progress_at": "2026-04-11T09:10:30Z",
            },
        )

        monkeypatch.setattr(
            bridge, "_load_notification_config", lambda config_path=None: ("token", "chat")
        )
        sent_messages = []
        monkeypatch.setattr(
            bridge,
            "_send_telegram_message",
            lambda bot_token, chat_id, text, **kwargs: sent_messages.append((bot_token, chat_id, text, kwargs)),
        )

        result = bridge.run(
            event="session-stop",
            session_id=session_id,
            project_path=str(project_path),
        )

        assert result["should_send"] is False
        assert sent_messages == []


def test_command_hint_is_included_when_latest_output_contains_a_command():
    payload = {
        "ralph_state": {
            "mode": "ralph",
            "current_phase": "verifying",
            "iteration": 5,
            "max_iterations": 20,
            "current_slice": "verification",
            "updated_at": "2026-04-11T09:20:00Z",
            "active": True,
        },
        "hud_state": {
            "last_agent_output": "Running python -m pytest -q tests/test_passivbot_execution.py now.",
            "turn_count": 31,
            "last_progress_at": "2026-04-11T09:20:01Z",
        },
        "latest_turn_preview": "",
        "changed_files": ["tests/test_passivbot_execution.py"],
        "instruction": "",
    }

    message = bridge._compose_message("session-idle", "omx-test-session", Path("C:/passivbot"), payload)

    assert "Current command: python -m pytest -q tests/test_passivbot_execution.py now." in message


def test_watch_session_polls_until_ralph_becomes_inactive(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        events = []
        states = iter(
            [
                {"ralph_state": {"active": True}},
                {"ralph_state": {"active": False}},
            ]
        )

        monkeypatch.setattr(bridge.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(bridge, "_load_bridge_inputs", lambda *_args, **_kwargs: next(states))

        def fake_run(**kwargs):
            events.append(kwargs["event"])
            return {"event": kwargs["event"], "should_send": True}

        monkeypatch.setattr(bridge, "run", fake_run)

        result = bridge.watch_session(
            session_id=session_id,
            project_path=str(project_path),
            repeat_seconds=30,
            poll_seconds=1,
            dry_run=True,
        )

        assert result["event"] == "session-end"
        assert events == ["session-start", "session-idle", "session-end"]
        assert not bridge._watcher_pid_path(project_path, session_id).exists()


def test_process_telegram_updates_queues_reply_for_session(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(
            session_dir / "telegram-progress-bridge-state.json",
            {
                "telegram_sent_messages": [
                    {
                        "message_id": 700,
                        "chat_id": "chat",
                        "session_id": session_id,
                        "event": "session-idle",
                    }
                ]
            },
        )

        monkeypatch.setattr(
            bridge,
            "_load_telegram_config",
            lambda config_path=None: {
                "bot_token": "token",
                "chat_id": "chat",
                "allowed_chat_ids": ["chat"],
                "allowed_user_ids": ["42"],
                "allowed_usernames": [],
                "poll_timeout_seconds": 1,
                "poll_interval_seconds": 1,
            },
        )
        monkeypatch.setattr(
            bridge,
            "_telegram_get_updates",
            lambda bot_token, offset, timeout_seconds: [
                {
                    "update_id": 10,
                    "message": {
                        "message_id": 701,
                        "text": "please continue with the refactor",
                        "chat": {"id": "chat"},
                        "from": {"id": 42, "username": "operator"},
                        "reply_to_message": {"message_id": 700},
                    },
                }
            ],
        )
        sent_messages = []
        monkeypatch.setattr(
            bridge,
            "_send_telegram_message",
            lambda bot_token, chat_id, text, **kwargs: sent_messages.append((bot_token, chat_id, text, kwargs))
            or {"message_id": 900, "chat": {"id": chat_id}},
        )

        result = bridge.process_telegram_updates_once(
            session_id=session_id,
            project_path=str(project_path),
        )

        latest = bridge._read_json(bridge._telegram_latest_command_path(project_path, session_id))
        assert result["processed"] == 1
        assert result["queued"] == 1
        assert latest["command"] == "reply"
        assert latest["text"] == "please continue with the refactor"
        assert sent_messages


def test_process_telegram_updates_queues_callback_command(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(session_dir / "telegram-progress-bridge-state.json", {})

        monkeypatch.setattr(
            bridge,
            "_load_telegram_config",
            lambda config_path=None: {
                "bot_token": "token",
                "chat_id": "chat",
                "allowed_chat_ids": ["chat"],
                "allowed_user_ids": ["42"],
                "allowed_usernames": [],
                "poll_timeout_seconds": 1,
                "poll_interval_seconds": 1,
            },
        )
        monkeypatch.setattr(
            bridge,
            "_telegram_get_updates",
            lambda bot_token, offset, timeout_seconds: [
                {
                    "update_id": 11,
                    "callback_query": {
                        "id": "cb-1",
                        "data": "cmd:continue",
                        "from": {"id": 42, "username": "operator"},
                        "message": {
                            "message_id": 777,
                            "chat": {"id": "chat"},
                        },
                    },
                }
            ],
        )
        answered = []
        monkeypatch.setattr(
            bridge,
            "_telegram_answer_callback",
            lambda bot_token, callback_query_id, text="": answered.append((bot_token, callback_query_id, text)),
        )
        monkeypatch.setattr(
            bridge,
            "_send_telegram_message",
            lambda bot_token, chat_id, text, **kwargs: {"message_id": 901, "chat": {"id": chat_id}},
        )

        result = bridge.process_telegram_updates_once(
            session_id=session_id,
            project_path=str(project_path),
        )

        latest = bridge._read_json(bridge._telegram_latest_command_path(project_path, session_id))
        assert result["processed"] == 1
        assert result["queued"] == 1
        assert latest["command"] == "continue"
        assert latest["kind"] == "callback"
        assert answered == [("token", "cb-1", "Received")]


def test_process_telegram_status_command_forces_status_snapshot(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "executing",
                "current_slice": "slice 1",
                "updated_at": "2026-04-11T09:00:00Z",
                "active": True,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "Running tests.",
                "turn_count": 3,
                "last_progress_at": "2026-04-11T09:00:01Z",
            },
        )

        monkeypatch.setattr(
            bridge,
            "_load_telegram_config",
            lambda config_path=None: {
                "bot_token": "token",
                "chat_id": "chat",
                "allowed_chat_ids": ["chat"],
                "allowed_user_ids": ["42"],
                "allowed_usernames": [],
                "poll_timeout_seconds": 1,
                "poll_interval_seconds": 1,
            },
        )
        monkeypatch.setattr(
            bridge,
            "_telegram_get_updates",
            lambda bot_token, offset, timeout_seconds: [
                {
                    "update_id": 12,
                    "message": {
                        "message_id": 710,
                        "text": "/status",
                        "chat": {"id": "chat"},
                        "from": {"id": 42, "username": "operator"},
                    },
                }
            ],
        )
        sent_messages = []
        monkeypatch.setattr(
            bridge,
            "_send_telegram_message",
            lambda bot_token, chat_id, text, **kwargs: sent_messages.append(text)
            or {"message_id": 902, "chat": {"id": chat_id}},
        )

        result = bridge.process_telegram_updates_once(
            session_id=session_id,
            project_path=str(project_path),
        )

        assert result["processed"] == 1
        assert result["queued"] == 0
        assert any("Passivbot OMX progress" in text for text in sent_messages)


def test_load_notification_config_details_supports_bidirectional_settings(tmp_path):
    config_path = tmp_path / ".codex" / ".omx-config.json"
    _write_json(
        config_path,
        {
            "notifications": {
                "telegram": {
                    "botToken": "bot-token",
                    "chatId": "123",
                    "allowedChatIds": ["456"],
                    "allowedUserIds": ["456"],
                    "allowedUsernames": ["@Operator"],
                    "repliesEnabled": True,
                    "pollUpdatesEnabled": True,
                    "pollLimit": 7,
                    "pollTimeoutSeconds": 9,
                    "dispatchInboxEnabled": True,
                    "dispatchLimit": 3,
                }
            }
        },
    )

    config = bridge._load_notification_config_details(config_path)

    assert config.bot_token == "bot-token"
    assert config.chat_id == "123"
    assert config.allowed_chat_ids == ("123", "456")
    assert config.allowed_user_ids == ("456",)
    assert config.allowed_usernames == ("operator",)
    assert config.replies_enabled is True
    assert config.poll_updates_enabled is True
    assert config.poll_limit == 7
    assert config.poll_timeout_seconds == 9
    assert config.dispatch_inbox_enabled is True
    assert config.dispatch_limit == 3


@pytest.mark.parametrize(
    ("overrides"),
    [
        {"repliesEnabled": True},
        {"pollUpdatesEnabled": True},
        {"dispatchInboxEnabled": True},
        {"codexConsumerEnabled": True},
    ],
)
def test_load_notification_config_details_rejects_inbound_without_user_allowlist(
    tmp_path,
    overrides,
):
    config_path = tmp_path / ".codex" / ".omx-config.json"
    _write_json(
        config_path,
        {
            "notifications": {
                "telegram": {
                    "botToken": "bot-token",
                    "chatId": "123",
                    "allowedChatIds": ["123"],
                    **overrides,
                }
            }
        },
    )

    with pytest.raises(RuntimeError, match="allowedUserIds or allowedUsernames"):
        bridge._load_notification_config_details(config_path)


def test_load_notification_config_details_rejects_default_inbound_without_user_allowlist(tmp_path):
    config_path = tmp_path / ".codex" / ".omx-config.json"
    _write_json(
        config_path,
        {
            "notifications": {
                "telegram": {
                    "botToken": "bot-token",
                    "chatId": "123",
                    "allowedChatIds": ["123"],
                }
            }
        },
    )

    with pytest.raises(RuntimeError, match="allowedUserIds or allowedUsernames"):
        bridge._load_notification_config_details(config_path)


def test_authorized_message_rejects_chat_only_allowlist():
    assert (
        bridge._authorized_message(
            {"allowed_chat_ids": ["123"], "allowed_user_ids": [], "allowed_usernames": []},
            {"chat": {"id": 123}, "from": {"id": 456, "username": "owner"}},
        )
        is False
    )


def test_process_incoming_updates_replies_and_queues_messages(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(session_dir / "ralph-state.json", {"mode": "ralph", "current_phase": "executing", "active": True})
        _write_json(session_dir / "hud-state.json", {"last_agent_output": "running", "turn_count": 2})

        sent_payloads = []
        processed_commands = []

        def fake_api(bot_token: str, method: str, payload: dict | None = None) -> dict:
            if method == "getUpdates":
                return {
                    "ok": True,
                    "result": [
                        {"update_id": 100, "message": {"message_id": 1, "chat": {"id": 123, "type": "private"}, "from": {"id": 123, "username": "owner"}, "text": "/help"}},
                        {"update_id": 101, "message": {"message_id": 2, "chat": {"id": 123, "type": "private"}, "from": {"id": 123, "username": "owner"}, "text": "/status"}},
                        {"update_id": 102, "message": {"message_id": 3, "chat": {"id": 123, "type": "private"}, "from": {"id": 123, "username": "owner"}, "text": "/reply investigate this"}},
                        {"update_id": 103, "message": {"message_id": 4, "chat": {"id": 123, "type": "private"}, "from": {"id": 123, "username": "owner"}, "voice": {"file_id": "voice-file", "duration": 5, "mime_type": "audio/ogg"}}},
                        {"update_id": 104, "message": {"message_id": 5, "chat": {"id": 123, "type": "private"}, "from": {"id": 999, "username": "intruder"}, "text": "/reply ignore me"}},
                    ],
                }
            if method == "sendMessage":
                sent_payloads.append(payload)
                return {"ok": True, "result": {"message_id": 9000}}
            raise AssertionError(f"Unexpected method {method}")

        monkeypatch.setattr(bridge, "_telegram_api_request", fake_api)
        monkeypatch.setattr(
            bridge,
            "_process_inbound_command",
            lambda **kwargs: processed_commands.append(kwargs["command"]) or {"action": "replied"},
        )

        result = bridge._process_incoming_updates(
            session_id=session_id,
            project_path=project_path,
            config=bridge.TelegramNotificationConfig(
                bot_token="bot-token",
                chat_id="123",
                allowed_chat_ids=("123",),
                allowed_user_ids=("123",),
                replies_enabled=True,
                poll_updates_enabled=True,
            ),
            bridge_state={"telegram:last_update_id": 99},
        )

        inbox_lines = bridge._telegram_inbox_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        outbox_lines = bridge._telegram_outbox_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        inbox_entries = [json.loads(line) for line in inbox_lines]
        outbox_entries = [json.loads(line) for line in outbox_lines]

        assert result == {"processed": 5, "queued": 2, "replied": 4, "last_update_id": 104}
        assert len(sent_payloads) == 2
        assert processed_commands == ["help", "status"]
        assert sent_payloads[0]["text"] == "Queued for OMX."
        assert sent_payloads[1]["text"].startswith("Voice message received for OMX.")
        assert [entry["kind"] for entry in inbox_entries] == ["text", "voice"]
        assert inbox_entries[0]["body"] == "investigate this"
        assert inbox_entries[0]["command"] == "/reply"
        assert inbox_entries[1]["voice"]["file_id"] == "voice-file"
        assert [entry["event"] for entry in outbox_entries] == ["ack-reply", "ack-reply"]


def test_process_incoming_updates_handles_callback_commands_and_direct_replies(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            session_dir / "telegram-progress-bridge-state.json",
            {
                "telegram_sent_messages": [
                    {"message_id": 777, "chat_id": "123", "session_id": session_id, "event": "session-idle"}
                ]
            },
        )

        api_calls = []

        def fake_api(bot_token: str, method: str, payload: dict | None = None) -> dict:
            api_calls.append((method, payload))
            if method == "getUpdates":
                return {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 200,
                            "callback_query": {
                                "id": "cb-1",
                                "data": "cmd:continue",
                                "from": {"id": 123, "username": "owner"},
                                "message": {"message_id": 777, "chat": {"id": 123, "type": "private"}},
                            },
                        },
                        {
                            "update_id": 201,
                            "message": {
                                "message_id": 9,
                                "chat": {"id": 123, "type": "private"},
                                "from": {"id": 123, "username": "owner"},
                                "text": "please continue from here",
                                "reply_to_message": {"message_id": 777},
                            },
                        },
                    ],
                }
            if method == "answerCallbackQuery":
                return {"ok": True, "result": True}
            if method == "sendMessage":
                return {"ok": True, "result": {"message_id": 9001}}
            raise AssertionError(f"Unexpected method {method}")

        monkeypatch.setattr(bridge, "_telegram_api_request", fake_api)

        result = bridge._process_incoming_updates(
            session_id=session_id,
            project_path=project_path,
            config=bridge.TelegramNotificationConfig(
                bot_token="bot-token",
                chat_id="123",
                allowed_chat_ids=("123",),
                allowed_user_ids=("123",),
                replies_enabled=True,
                poll_updates_enabled=True,
            ),
            bridge_state={"telegram:last_update_id": 199},
        )

        command_entries = [
            json.loads(line)
            for line in bridge._telegram_operator_inbox_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        ]
        inbox_entries = [
            json.loads(line)
            for line in bridge._telegram_inbox_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        ]

        assert result == {"processed": 2, "queued": 2, "replied": 2, "last_update_id": 201}
        assert any(method == "answerCallbackQuery" for method, _payload in api_calls)
        assert command_entries[0]["command"] == "continue"
        assert command_entries[0]["kind"] == "callback"
        assert inbox_entries[0]["command"] == "reply-target"
        assert inbox_entries[0]["body"] == "please continue from here"


def test_get_pending_inbox_entries_and_mark_consumed():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"

        bridge._queue_incoming_message(project_path, session_id, {"update_id": 10, "text": "first"})
        bridge._queue_incoming_message(project_path, session_id, {"update_id": 11, "text": "second"})
        bridge._queue_incoming_message(project_path, session_id, {"update_id": 12, "text": "third"})

        pending = bridge._get_pending_inbox_entries(project_path, session_id, limit=10)
        assert [entry["update_id"] for entry in pending] == [10, 11, 12]

        state = bridge._mark_inbox_consumed(project_path, session_id, 11)
        assert state["telegram:last_consumed_update_id"] == 11

        pending_after_ack = bridge._get_pending_inbox_entries(project_path, session_id, limit=10)
        assert [entry["update_id"] for entry in pending_after_ack] == [12]


def test_send_connector_message_sends_and_logs(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        sent_payloads = []

        monkeypatch.setattr(
            bridge,
            "_load_notification_config_details",
            lambda _config_path=None: bridge.TelegramNotificationConfig(bot_token="bot-token", chat_id="123"),
        )
        monkeypatch.setattr(
            bridge,
            "_send_telegram_reply",
            lambda bot_token, chat_id, text, reply_to_message_id=None: sent_payloads.append(
                {
                    "bot_token": bot_token,
                    "chat_id": chat_id,
                    "text": text,
                    "reply_to_message_id": reply_to_message_id,
                }
            ),
        )

        result = bridge.send_connector_message(
            session_id=session_id,
            project_path=str(project_path),
            text="OMX answer ready",
            event="omx-answer",
            reply_to_message_id=77,
        )

        outbox_entries = [
            json.loads(line)
            for line in bridge._telegram_outbox_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        ]

        assert result["event"] == "omx-answer"
        assert result["chat_id"] == "123"
        assert result["reply_to_message_id"] == 77
        assert result["sent"] is True
        assert sent_payloads == [
            {
                "bot_token": "bot-token",
                "chat_id": "123",
                "text": "OMX answer ready",
                "reply_to_message_id": 77,
            }
        ]
        assert outbox_entries[0]["event"] == "omx-answer"
        assert outbox_entries[0]["text"] == "OMX answer ready"
        assert outbox_entries[0]["reply_to_message_id"] == 77


def test_dispatch_next_inbox_entry_creates_tmux_ready_request_and_marks_consumed():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"

        _write_json(
            project_path / ".omx" / "tmux-hook.json",
            {
                "enabled": True,
                "target": {"type": "pane", "value": "%42"},
                "prompt_template": "Continue from current mode state. [OMX_TMUX_INJECT]",
                "marker": "[OMX_TMUX_INJECT]",
            },
        )
        bridge._queue_incoming_message(
            project_path,
            session_id,
            {
                "update_id": 501,
                "message_id": 99,
                "chat_id": "123",
                "from_username": "owner",
                "text": "please summarize progress",
                "body": "please summarize progress",
                "kind": "text",
            },
        )

        result = bridge.dispatch_next_inbox_entry(
            session_id=session_id,
            project_path=str(project_path),
            limit=1,
            mark_consumed=True,
        )

        queue_entries = [
            json.loads(line)
            for line in bridge._telegram_dispatch_queue_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        ]
        bridge_state = bridge._read_json(bridge._bridge_state_path(project_path, session_id))

        assert result["count"] == 1
        assert result["dispatched"][0]["tmux_target"] == "%42"
        assert "please summarize progress" in result["dispatched"][0]["prompt"]
        assert queue_entries[0]["tmux_enabled"] is True
        assert queue_entries[0]["marker"] == "[OMX_TMUX_INJECT]"
        assert bridge_state["telegram:last_consumed_update_id"] == 501


def test_build_dispatch_prompt_includes_voice_metadata():
    prompt = bridge._build_dispatch_prompt(
        "omx-test-session",
        {
            "from_username": "owner",
            "kind": "voice",
            "has_voice": True,
            "voice": {"file_id": "voice-file", "duration": 3},
            "text": "",
            "body": "",
        },
        {"prompt_template": "Dispatch: [OMX_TMUX_INJECT]", "marker": "[OMX_TMUX_INJECT]"},
    )

    assert prompt.startswith("Dispatch: Telegram message for OMX session omx-test-session.")
    assert '"file_id": "voice-file"' in prompt


def test_watch_session_auto_dispatches_pending_inbox_when_enabled(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        events = []
        states = iter([
            {"ralph_state": {"active": True}},
            {"ralph_state": {"active": False}},
        ])

        monkeypatch.setattr(bridge.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(bridge, "_load_bridge_inputs", lambda *_args, **_kwargs: next(states))
        monkeypatch.setattr(bridge, "_load_notification_config_details", lambda _config_path=None: bridge.TelegramNotificationConfig(bot_token="bot", chat_id="123", poll_updates_enabled=True, dispatch_inbox_enabled=True, dispatch_limit=2))
        monkeypatch.setattr(bridge, "_process_incoming_updates", lambda **_kwargs: {"processed": 1, "queued": 1, "replied": 0, "last_update_id": 55})

        dispatch_calls = []

        monkeypatch.setattr(
            bridge,
            "dispatch_next_inbox_entry",
            lambda **kwargs: dispatch_calls.append(kwargs) or {"count": 1, "dispatched": [{"update_id": 55}]},
        )

        def fake_run(**kwargs):
            events.append(kwargs["event"])
            return {"event": kwargs["event"], "should_send": True}

        monkeypatch.setattr(bridge, "run", fake_run)

        result = bridge.watch_session(
            session_id=session_id,
            project_path=str(project_path),
            repeat_seconds=30,
            poll_seconds=1,
            dry_run=False,
        )

        assert result["event"] == "session-end"
        assert events == ["session-start", "session-idle", "session-end"]
        assert len(dispatch_calls) == 1
        assert dispatch_calls[0]["mark_consumed"] is True
        assert dispatch_calls[0]["limit"] == 2


def test_watch_session_uses_default_codex_input_path_when_enabled(monkeypatch):
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        events = []
        states = iter([
            {"ralph_state": {"active": True}},
            {"ralph_state": {"active": False}},
        ])

        monkeypatch.setattr(bridge.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(bridge.time, "monotonic", iter([10.0, 10.0]).__next__)
        monkeypatch.setattr(bridge, "_load_bridge_inputs", lambda *_args, **_kwargs: next(states))
        monkeypatch.setattr(
            bridge,
            "_load_notification_config_details",
            lambda _config_path=None: bridge.TelegramNotificationConfig(
                bot_token="bot",
                chat_id="123",
                poll_updates_enabled=True,
                poll_interval_seconds=1,
                codex_consumer_enabled=True,
                codex_consumer_limit=2,
                codex_input_path="",
            ),
        )
        monkeypatch.setattr(bridge, "_process_incoming_updates", lambda **_kwargs: {"processed": 1, "queued": 1, "replied": 0, "last_update_id": 55})

        consume_calls = []

        monkeypatch.setattr(
            bridge,
            "consume_telegram_inbox_to_codex",
            lambda **kwargs: consume_calls.append(kwargs) or {"count": 1, "consumed": [{"update_id": 55}]},
        )

        def fake_run(**kwargs):
            events.append(kwargs["event"])
            return {"event": kwargs["event"], "should_send": True}

        monkeypatch.setattr(bridge, "run", fake_run)

        result = bridge.watch_session(
            session_id=session_id,
            project_path=str(project_path),
            repeat_seconds=30,
            poll_seconds=1,
            dry_run=False,
        )

        expected_codex_input = str((project_path / ".omx" / "state" / "sessions" / session_id / "telegram-codex-input.json").resolve())

        assert result["event"] == "session-end"
        assert events == ["session-start", "session-idle", "session-end"]
        assert len(consume_calls) == 1
        assert consume_calls[0]["mark_consumed"] is True
        assert consume_calls[0]["limit"] == 2
        assert consume_calls[0]["codex_input_path"] == expected_codex_input


def test_consume_telegram_inbox_to_codex_writes_handoff_and_marks_consumed():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        codex_input_path = session_dir / "codex-input.json"
        _write_json(
            session_dir / "ralph-state.json",
            {
                "mode": "ralph",
                "current_phase": "executing",
                "current_slice": "slice 1",
                "active": True,
            },
        )
        _write_json(
            session_dir / "hud-state.json",
            {
                "last_agent_output": "Current OMX output.",
                "turn_count": 1,
            },
        )
        bridge._queue_incoming_message(
            project_path,
            session_id,
            {
                "update_id": 50,
                "message_id": 10,
                "from_username": "operator",
                "text": "/reply continue",
                "body": "continue",
                "kind": "text",
            },
        )

        result = bridge.consume_telegram_inbox_to_codex(
            session_id=session_id,
            project_path=str(project_path),
            limit=1,
            codex_input_path=str(codex_input_path),
            mark_consumed=True,
        )

        handoff_entries = [
            json.loads(line)
            for line in bridge._telegram_codex_handoff_path(project_path, session_id).read_text(encoding="utf-8").splitlines()
        ]
        latest_handoff = bridge._read_json(bridge._telegram_latest_codex_handoff_path(project_path, session_id))
        bridge_state = bridge._read_json(bridge._bridge_state_path(project_path, session_id))
        codex_input = bridge._read_json(codex_input_path)

        assert result["count"] == 1
        assert handoff_entries[0]["operator_text"] == "continue"
        assert "Telegram operator message for OMX session omx-test-session." in handoff_entries[0]["instruction"]
        assert latest_handoff["message_id"] == 10
        assert bridge_state["telegram:last_consumed_update_id"] == 50
        assert codex_input["message_id"] == 10


def test_reset_connector_state_clears_connector_files():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        _write_json(session_dir / "telegram-progress-bridge-state.json", {"telegram_muted": True})
        (session_dir / "telegram-inbox.jsonl").write_text('{"update_id": 1}\n', encoding="utf-8")
        (session_dir / "telegram-codex-handoff.jsonl").write_text('{"message_id": 1}\n', encoding="utf-8")

        result = bridge.reset_connector_state(session_id=session_id, project_path=str(project_path))

        assert result["session_id"] == session_id
        assert bridge._read_json(bridge._bridge_state_path(project_path, session_id))["telegram_muted"] is False
        assert not bridge._telegram_inbox_path(project_path, session_id).exists()
        assert not bridge._telegram_codex_handoff_path(project_path, session_id).exists()


def test_audit_connector_state_reports_invalid_jsonl_line():
    with _workspace_tmp_dir() as tmp_path:
        project_path = tmp_path / "passivbot"
        session_id = "omx-test-session"
        session_dir = project_path / ".omx" / "state" / "sessions" / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        (session_dir / "telegram-outbox.jsonl").write_text('{"ok": true}\nnot-json\n', encoding="utf-8")

        result = bridge._audit_connector_state(project_path, session_id)

        assert result["ok"] is False
        assert any("telegram-outbox.jsonl: invalid JSON" in finding for finding in result["findings"])


def test_sample_telegram_config_block_includes_codex_consumer_settings():
    sample = bridge._sample_telegram_config_block()

    telegram = sample["notifications"]["telegram"]
    assert telegram["codexConsumerEnabled"] is True
    assert telegram["codexConsumerLimit"] == 5
    assert telegram["codexInputPath"] == ""
