from __future__ import annotations

from pathlib import Path

from scripts.ci import check_error_contract_patterns as error_gate
from scripts.ci import check_secret_patterns as secret_gate
from scripts.ci.diff_scan_utils import parse_added_hunks


def test_parse_added_hunks_tracks_added_line_numbers():
    diff = """diff --git a/src/example.py b/src/example.py
--- a/src/example.py
+++ b/src/example.py
@@ -1,2 +1,4 @@
 line1
+added_one = 1
 line2
+added_two = 2
"""

    hunks = parse_added_hunks(diff)
    assert len(hunks) == 1
    assert [line.line_no for line in hunks[0].added_lines] == [2, 4]
    assert [line.text for line in hunks[0].added_lines] == ["added_one = 1", "added_two = 2"]


def test_error_contract_gate_flags_swallowed_exception_and_return_exceptions():
    diff = """diff --git a/src/example.py b/src/example.py
--- a/src/example.py
+++ b/src/example.py
@@ -10,0 +11,5 @@
+try:
+    do_work()
+except Exception:
+    continue
+results = await asyncio.gather(*tasks, return_exceptions=True)
"""

    findings = error_gate.scan_hunks(parse_added_hunks(diff))
    rules = {(finding.rule, finding.line_no) for finding in findings}
    assert ("except_swallow", 13) in rules
    assert ("return_exceptions_true", 15) in rules


def test_error_contract_gate_allows_inline_suppression():
    diff = """diff --git a/src/example.py b/src/example.py
--- a/src/example.py
+++ b/src/example.py
@@ -10,0 +11,2 @@
+except Exception:  # error-contract: allow
+    continue
"""

    findings = error_gate.scan_hunks(parse_added_hunks(diff))
    assert findings == []


def test_secret_gate_flags_high_signal_token_and_ignores_placeholder():
    diff = """diff --git a/src/example.py b/src/example.py
--- a/src/example.py
+++ b/src/example.py
@@ -1,0 +1,3 @@
+TELEGRAM_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
+TEST_SECRET = "super-secret-token"
+OPENAI_KEY = "sk-abcdefghijklmnopqrstuvwxyzABCDEF"
"""

    findings = secret_gate.scan_hunks(parse_added_hunks(diff))
    rules = [finding.rule for finding in findings]
    assert "telegram_bot_token" in rules
    assert "openai_key" in rules
    assert all("super-secret-token" not in finding.evidence for finding in findings)


def test_cli_entrypoints_accept_diff_files(tmp_path: Path):
    diff_path = tmp_path / "sample.diff"
    diff_path.write_text(
        """diff --git a/src/example.py b/src/example.py
--- a/src/example.py
+++ b/src/example.py
@@ -1,0 +1,2 @@
+except Exception:
+    pass
""",
        encoding="utf-8",
    )

    assert error_gate.main(["--diff-file", str(diff_path)]) == 1
    assert secret_gate.main(["--diff-file", str(diff_path)]) == 0
