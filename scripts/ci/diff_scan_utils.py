from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence


ZERO_SHA = "0" * 40


@dataclass(frozen=True)
class AddedLine:
    file_path: str
    line_no: int
    text: str


@dataclass(frozen=True)
class AddedHunk:
    file_path: str
    added_lines: List[AddedLine]


def _run_git(args: Sequence[str]) -> str:
    return subprocess.check_output(["git", *args], text=True, encoding="utf-8")


def _is_valid_commitish(value: Optional[str]) -> bool:
    if not value or value == ZERO_SHA:
        return False
    try:
        subprocess.check_output(
            ["git", "rev-parse", "--verify", f"{value}^{{commit}}"],
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
        )
    except subprocess.CalledProcessError:
        return False
    return True


def build_unified_diff(
    *,
    base: Optional[str],
    head: Optional[str],
    paths: Optional[Sequence[str]] = None,
    diff_filter: Optional[str] = None,
) -> str:
    cmd = ["diff", "--unified=0", "--no-color"]
    if diff_filter:
        cmd.append(f"--diff-filter={diff_filter}")
    if _is_valid_commitish(base) and _is_valid_commitish(head):
        cmd.extend([base, head])
    elif _is_valid_commitish(head):
        cmd.append(head)
    else:
        cmd.extend(["HEAD~1", "HEAD"])
    if paths:
        cmd.append("--")
        cmd.extend(paths)
    return _run_git(cmd)


def parse_added_hunks(diff_text: str) -> List[AddedHunk]:
    hunks: List[AddedHunk] = []
    current_file: Optional[str] = None
    current_lines: List[AddedLine] = []
    new_line_no: Optional[int] = None

    def flush() -> None:
        nonlocal current_lines
        if current_file is not None and current_lines:
            hunks.append(AddedHunk(file_path=current_file, added_lines=current_lines))
        current_lines = []

    for raw_line in diff_text.splitlines():
        if raw_line.startswith("+++ "):
            flush()
            current_file = raw_line[4:].strip()
            if current_file.startswith("b/"):
                current_file = current_file[2:]
            elif current_file == "/dev/null":
                current_file = None
            new_line_no = None
            continue
        if raw_line.startswith("@@ "):
            flush()
            if current_file is None:
                continue
            header = raw_line.split(" ", 3)[2]
            plus_part = header[1:]
            if "," in plus_part:
                start_text = plus_part.split(",", 1)[0]
            else:
                start_text = plus_part
            new_line_no = int(start_text)
            continue
        if current_file is None or new_line_no is None:
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            current_lines.append(AddedLine(current_file, new_line_no, raw_line[1:]))
            new_line_no += 1
        elif raw_line.startswith(" "):
            new_line_no += 1
        elif raw_line.startswith("-"):
            continue

    flush()
    return hunks
def read_diff_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")
