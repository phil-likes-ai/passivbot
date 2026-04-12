from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ci.diff_scan_utils import AddedHunk, build_unified_diff, parse_added_hunks, read_diff_file


ALLOW_MARKER = "secret-scan: allow"
EXCLUDED_PREFIXES = ("tests/", ".omx/", ".sisyphus/", "venv/", ".pytest_cache/", ".pytest_tmp/")
PLACEHOLDER_RE = re.compile(
    r"(?i)(example|dummy|sample|placeholder|changeme|test[_-]?secret|secret123|super-secret-token|token123|your[_-])"
)
HIGH_SIGNAL_RULES = {
    "telegram_bot_token": re.compile(r"\b\d{7,10}:[A-Za-z0-9_-]{35,}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "openai_key": re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "generic_secret_assignment": re.compile(
        r"(?i)\b(api[_-]?key|secret|token|password)\b.{0,24}[:=].{0,6}[\"'][A-Za-z0-9_./+=-]{16,}[\"']"
    ),
    "bearer_token": re.compile(r"Bearer\s+[A-Za-z0-9._=-]{20,}"),
}


@dataclass(frozen=True)
class Finding:
    file_path: str
    line_no: int
    rule: str
    evidence: str


def _is_placeholder(text: str) -> bool:
    return bool(PLACEHOLDER_RE.search(text))


def scan_hunks(hunks: Sequence[AddedHunk]) -> List[Finding]:
    findings: List[Finding] = []
    for hunk in hunks:
        if hunk.file_path.startswith(EXCLUDED_PREFIXES):
            continue
        for line in hunk.added_lines:
            if ALLOW_MARKER in line.text or _is_placeholder(line.text):
                continue
            for rule, pattern in HIGH_SIGNAL_RULES.items():
                if pattern.search(line.text):
                    findings.append(Finding(line.file_path, line.line_no, rule, line.text.strip()))
                    break
    return findings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Block high-signal secret regressions in added lines.")
    parser.add_argument("--base", help="Base commit SHA/ref for diff scanning.")
    parser.add_argument("--head", help="Head commit SHA/ref for diff scanning.")
    parser.add_argument("--diff-file", help="Read a unified diff from a file instead of invoking git diff.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    diff_text = read_diff_file(args.diff_file) if args.diff_file else build_unified_diff(base=args.base, head=args.head)
    hunks = parse_added_hunks(diff_text)
    findings = scan_hunks(hunks)
    if not findings:
        print("No high-signal secret regressions found in added lines.")
        return 0

    print("Potential secrets detected in added lines:")
    for finding in findings:
        print(f"{finding.file_path}:{finding.line_no}: [{finding.rule}] {finding.evidence}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
