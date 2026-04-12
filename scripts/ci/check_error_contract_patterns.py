from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ci.diff_scan_utils import AddedHunk, build_unified_diff, parse_added_hunks, read_diff_file


ALLOW_MARKER = "error-contract: allow"
DEFAULT_PREFIXES = ("src/",)
DEFAULT_SUFFIXES = (".py",)
GET_DEFAULT_RE = re.compile(r"\.get\([^\n]*,\s*(0|0\.0|None|False|\{\}|\[\])\)")
RETURN_EXC_RE = re.compile(r"return_exceptions\s*=\s*True")
EXCEPT_RE = re.compile(r"^\s*(except\s*:|except\s+Exception(?:\s+as\s+\w+)?\s*:)\s*(?P<trailing>.*)$")
PASS_CONTINUE_RE = re.compile(r"^\s*(pass|continue)\b")


@dataclass(frozen=True)
class Finding:
    file_path: str
    line_no: int
    rule: str
    evidence: str


def _iter_scan_lines(hunks: Sequence[AddedHunk], prefixes: Iterable[str], suffixes: Iterable[str]) -> Iterable[AddedHunk]:
    for hunk in hunks:
        if hunk.file_path.startswith(tuple(prefixes)) and hunk.file_path.endswith(tuple(suffixes)):
            yield hunk


def scan_hunks(hunks: Sequence[AddedHunk]) -> List[Finding]:
    findings: List[Finding] = []
    for hunk in _iter_scan_lines(hunks, DEFAULT_PREFIXES, DEFAULT_SUFFIXES):
        lines = hunk.added_lines
        for idx, line in enumerate(lines):
            if ALLOW_MARKER in line.text:
                continue
            if RETURN_EXC_RE.search(line.text):
                findings.append(Finding(line.file_path, line.line_no, "return_exceptions_true", line.text.strip()))
            if GET_DEFAULT_RE.search(line.text):
                findings.append(Finding(line.file_path, line.line_no, "dict_get_default", line.text.strip()))

            match = EXCEPT_RE.match(line.text)
            if not match:
                continue
            trailing = match.group("trailing").strip()
            if PASS_CONTINUE_RE.match(trailing):
                findings.append(Finding(line.file_path, line.line_no, "except_swallow", line.text.strip()))
                continue
            for next_line in lines[idx + 1 : idx + 4]:
                if ALLOW_MARKER in next_line.text:
                    break
                stripped = next_line.text.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if PASS_CONTINUE_RE.match(next_line.text):
                    findings.append(
                        Finding(
                            line.file_path,
                            line.line_no,
                            "except_swallow",
                            f"{line.text.strip()} -> {next_line.text.strip()}",
                        )
                    )
                break
    return findings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Block forbidden error-contract regressions in added lines.")
    parser.add_argument("--base", help="Base commit SHA/ref for diff scanning.")
    parser.add_argument("--head", help="Head commit SHA/ref for diff scanning.")
    parser.add_argument("--diff-file", help="Read a unified diff from a file instead of invoking git diff.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    diff_text = (
        read_diff_file(args.diff_file)
        if args.diff_file
        else build_unified_diff(base=args.base, head=args.head, paths=list(DEFAULT_PREFIXES))
    )
    hunks = parse_added_hunks(diff_text)
    findings = scan_hunks(hunks)
    if not findings:
        print("No forbidden error-contract regressions found in added Python lines.")
        return 0

    print("Forbidden error-contract regressions detected:")
    for finding in findings:
        print(f"{finding.file_path}:{finding.line_no}: [{finding.rule}] {finding.evidence}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
