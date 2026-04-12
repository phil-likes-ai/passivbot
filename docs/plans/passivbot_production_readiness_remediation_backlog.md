# Passivbot Production Readiness Remediation Backlog

_Status: active working backlog_

## Purpose

Capture the concrete remediation work required to move Passivbot from "tests pass" to
"production-ready with explicit safety controls, fail-loud behavior, and hardened operator
surfaces." This file is intended to be the implementation-facing task list so the work is not
lost across sessions.

## Canonical companion docs

Use these together when executing this backlog:

- `.omx/plans/prd-reliability-first-modernization-wave.md`
- `.omx/plans/test-spec-reliability-first-modernization-wave.md`
- `docs/plans/passivbot_production_readiness_pr_slices.md`
- `.sisyphus/drafts/passivbot-remaining-work-breakdown.md`
- `.sisyphus/drafts/passivbot-final-wave-readiness-matrix.md`

Treat this plan as stale until reconciled with the backlog below:

- `.sisyphus/plans/passivbot-unified-modernization-overhaul-plan.md`

## Execution rules

1. Trading-critical paths must follow `docs/ai/error_contract.md`.
2. Rust remains source of truth for order behavior, risk, and unstuck logic.
3. Remove silent downgrade behavior before broad refactors.
4. Keep diffs small and slice-based.
5. Every completed slice must include focused verification evidence.

## Canonical execution contract

- This file is the canonical backlog.
- `docs/plans/passivbot_production_readiness_pr_slices.md` is the canonical PR/slice queue.
- Historical `.sisyphus` plans may provide context, but they must not be used as the source of
  completion truth if they conflict with the backlog or slice queue.

---

## P0 - Immediate containment and access control

### P0.1 Rotate leaked Telegram credential and purge runtime artifacts
**Priority:** Critical  
**Target:** Same day  
**Depends on:** None

**Files / areas**
- `.omx/logs/turns-2026-04-11.jsonl`
- `.omx/**`
- `monitor/**`
- `.gitignore`

**Tasks**
- Rotate the leaked Telegram bot token immediately.
- Remove runtime logs/state artifacts containing live credentials or operator data.
- Add ignore rules for `.omx/` and monitor runtime output.
- Remove any tracked runtime-only `.omx/*` state/log files from git.
- Review monitor fixture paths and keep only intentional test fixtures tracked.

**Acceptance criteria**
- The leaked token is invalidated.
- `git ls-files .omx monitor` contains no accidental runtime artifacts.
- Secret scan finds no live credentials in tracked files.

**Verification**
- Secret scan / grep pass.
- Clean `git status` after a normal run except for intended source changes.

---

### P0.2 Make Telegram operator auth fail-closed
**Priority:** Critical  
**Target:** Same day  
**Depends on:** P0.1

**Files / areas**
- `src/omx_telegram_progress_bridge.py`
- `README.md`
- `configs/examples/omx.telegram.sample.omx-config.json`

**Tasks**
- Require `allowedChatIds` and at least one of `allowedUserIds` or `allowedUsernames` when polling/replies are enabled.
- Fail startup if inbound command consumption is enabled without a per-user allowlist.
- Remove any implicit trust model where an allowed chat alone authorizes all users in that chat.
- Document exact production-safe configuration in README and sample config.

**Acceptance criteria**
- Non-allowlisted users in an allowed group cannot issue bridge commands.
- Misconfigured auth fails at startup with actionable messaging.
- Docs match runtime enforcement.

**Verification**
- Add targeted tests for authorized/unauthorized direct chat, group chat, and reply flows.

---

### P0.3 Lock monitor relay and dashboards to safe defaults
**Priority:** High  
**Target:** Same day to 2 days  
**Depends on:** None

**Files / areas**
- `src/tools/monitor_relay.py`
- `src/monitor_relay.py`
- `src/monitor_dev.py`
- `src/monitor_web.py`
- `src/tools/fill_events_dash.py`

**Tasks**
- Make all monitor/dashboard services default to `127.0.0.1` only.
- Refuse `0.0.0.0` unless an explicit insecure/public flag is passed.
- Add authentication if remote relay/dashboard access is supported at all.
- Reduce raw payload exposure in snapshot/history responses where feasible.
- Add loud warnings when an insecure bind is explicitly enabled.

**Acceptance criteria**
- Operator surfaces are local-only by default.
- Public exposure requires deliberate opt-in.
- Supported production mode does not allow unauthenticated relay/dashboard access.

**Verification**
- Targeted tests for bind policy.
- Manual curl/websocket checks confirming auth/local-only behavior.

---

### P0.4 Stop logging sensitive headers and stop failing open on endpoint override config
**Priority:** High  
**Target:** 1 to 2 days  
**Depends on:** None

**Files / areas**
- `src/custom_endpoint_overrides.py`
- related tests

**Tasks**
- Mask `Authorization`, API keys, passphrases, proxy credentials, and similar headers in logs.
- Replace raw header logging with redacted summaries.
- Decide live-mode behavior for invalid custom endpoint override config:
  - hard-fail by default for live mode, or
  - require explicit opt-in for fail-open mode.
- Add regression tests for redaction and invalid-config handling.

**Acceptance criteria**
- Logs never print sensitive header values.
- Invalid critical config cannot silently degrade to an empty config in live mode.

**Verification**
- Targeted tests plus grep/log assertion checks.

---

## P1 - Trading-critical fail-loud reliability wave

### P1.1 Harden `src/passivbot.py` critical runtime/update paths
**Priority:** High  
**Target:** 2 to 5 days  
**Depends on:** P0 items recommended first

**Files / areas**
- `src/passivbot.py`
- targeted tests under `tests/`

**Known hotspots**
- update prep / orchestration around `update_pos_oos_pnls_ohlcvs()`
- warmup / mode-prep broad catches

**Tasks**
- Remove `return_exceptions=True` from trading-critical prep paths.
- Replace broad exception fallback with hard-fail or explicitly bounded, logged, tested fallback only where allowed.
- Add restart-safe smoke coverage for a failed prep cycle followed by healthy rerun.

**Acceptance criteria**
- Failures surface to caller/test harness instead of creating false-success cycles.
- No forbidden silent-downgrade pattern remains in touched critical paths.

**Verification**
- Targeted pytest.
- Grep audit on touched files.
- Restart/replay smoke test.

---

### P1.2 Harden `src/candlestick_manager.py` critical fetch/freshness paths
**Priority:** High  
**Target:** 3 to 7 days  
**Depends on:** P1.1 recommended

**Files / areas**
- `src/candlestick_manager.py`
- focused candlestick-manager tests

**Tasks**
- Classify remaining broad catches into `critical-path` vs `best-effort`.
- Remove or narrow broad catches in candle freshness, pagination, gap handling, and orchestrator-fed paths.
- Keep best-effort catches only with justification and tests.

**Acceptance criteria**
- Touched critical paths fail loudly and preserve actionable context.
- Retry exhaustion / fallback behavior is explicit and tested.

**Verification**
- Manager-level tests for freshness, pagination, retry exhaustion, fallback behavior.
- Grep audit for touched paths.

---

### P1.3 Harden `src/fill_events_manager.py` normalize/fetch paths
**Priority:** High  
**Target:** 3 to 7 days  
**Depends on:** P1.1 recommended

**Files / areas**
- `src/fill_events_manager.py`
- `tests/test_fill_events_manager.py`
- parse/model tests

**Tasks**
- Remove broad catches that convert malformed exchange payloads into neutral defaults.
- Raise on missing required trade price/size/IDs in trading-critical normalization paths.
- Preserve deterministic parse/coalesce behavior.

**Acceptance criteria**
- Missing required fill fields hard-fail where required by contract.
- Exchange-specific malformed payload cases are covered by tests.

**Verification**
- Targeted pytest for malformed payloads and exchange-specific edge cases.
- Grep audit for touched paths.

---

### P1.4 Add exchange adapter parity tests for critical behavior
**Priority:** High  
**Target:** 3 to 7 days  
**Depends on:** P1.1 to P1.3 as surfaces are touched

**Files / areas**
- `src/passivbot_exchange_config.py`
- exchange adapter modules
- exchange adapter test files for Bybit / KuCoin / Hyperliquid

**Tasks**
- Add parity tests for config updates, failure propagation, pagination, and no silent fallback on required values.
- Verify websocket freshness / REST fallback behavior where live correctness depends on it.

**Acceptance criteria**
- Priority adapters obey the same fail-loud contract for required live inputs.

**Verification**
- Targeted adapter pytest bundle.

---

## P2 - Repo hygiene and implementation support

### P2.1 Separate generated runtime output from source tree
**Priority:** Medium  
**Target:** 2 to 5 days  
**Depends on:** P0.1

**Files / areas**
- `.gitignore`
- runtime docs / monitor docs
- any scripts that emit into tracked paths by default

**Tasks**
- Move generated logs/state/checkpoints/caches into ignored runtime paths.
- Keep only intentional fixtures under dedicated test-fixture directories.
- Document the supported runtime directory layout.

**Acceptance criteria**
- Fresh local execution does not dirty git with runtime files.

**Verification**
- Manual run + `git status` check.

---

### P2.2 Fix unsafe tar extraction helper
**Priority:** Medium  
**Target:** 1 day  
**Depends on:** None

**Files / areas**
- `sync_tar.py`
- `tests/test_sync_tar.py`

**Tasks**
- Validate archive member paths before extraction.
- Reject path traversal and absolute-path members.

**Acceptance criteria**
- Malicious archive entries cannot escape destination.

**Verification**
- Traversal regression test.

---

### P2.3 Reconcile plan drift and establish one current implementation plan
**Priority:** Medium  
**Target:** 1 day  
**Depends on:** Initial backlog adoption

**Files / areas**
- `.sisyphus/plans/passivbot-unified-modernization-overhaul-plan.md`
- this file
- `.sisyphus/drafts/passivbot-remaining-work-breakdown.md`

**Tasks**
- Mark stale/optimistic plan artifacts clearly.
- Point active work to one current backlog.
- Keep phase completion status aligned with actual code/test evidence.

**Acceptance criteria**
- No doc claims Phases 1 to 5 are complete unless evidence exists.

**Verification**
- Human review of plan consistency.

---

## P3 - Architecture and production hardening

### P3.1 Continue breaking up Python god classes in small verified slices
**Priority:** Medium  
**Target:** ongoing  
**Depends on:** P1 slices should lead where reliability debt exists

**Files / areas**
- `src/passivbot.py`
- `src/candlestick_manager.py`
- `src/fill_events_manager.py`

**Tasks**
- Extract one cohesive cluster at a time only after adjacent silent-failure debt is addressed.
- Add or refresh tests at each new boundary.

**Acceptance criteria**
- Smaller files, clearer boundaries, no behavior drift.

**Verification**
- Focused tests and diagnostics per extracted slice.

---

### P3.2 Push more risk and unstuck safety into Rust
**Priority:** Medium  
**Target:** ongoing  
**Depends on:** Reliability groundwork complete enough to safely continue

**Files / areas**
- `passivbot-rust/src/risk.rs`
- `passivbot-rust/src/entries.rs`
- `passivbot-rust/src/closes.rs`
- Rust tests

**Tasks**
- Add deterministic tests for exposure/risk/unstuck helpers.
- Implement prioritized risk-of-ruin / regime / TWEL / unstuck upgrades in Rust.

**Acceptance criteria**
- Critical risk behavior is Rust-owned, deterministic, and test-covered.

**Verification**
- `cargo check --tests`
- targeted Rust tests
- targeted backtests where applicable

---

### P3.3 Add CI security and release gates
**Priority:** Medium  
**Target:** 2 to 5 days  
**Depends on:** P0.1, P1 slices useful first

**Files / areas**
- CI workflow files
- lint/type/test config
- release docs

**Tasks**
- Add secret scanning.
- Add dependency audit.
- Add grep gate for forbidden error-contract anti-patterns.
- Add targeted Python test bundle and Rust verification to CI.
- Add touched-module lint/type checks.

**Acceptance criteria**
- Regressions in secrets, silent-failure patterns, or critical tests block merges.

**Verification**
- CI green on intended baseline.

---

## P4 - Production deployment readiness

### P4.1 Define a hardened supported production profile
**Priority:** Medium  
**Target:** before real-money rollout  
**Depends on:** P0 through P3 substantially complete

**Files / areas**
- deployment docs
- container/runtime docs
- runbooks

**Tasks**
- Define supported modes: dev, staging/fake-live, production live.
- Require for production:
  - secure secret injection
  - local-only or authenticated operator surfaces
  - structured logs and alerts
  - restart-safe behavior
  - documented rollback/runbooks
- Document one canonical production path.

**Acceptance criteria**
- There is a single documented production deployment path with explicit controls and prerequisites.

**Verification**
- Staging signoff checklist complete.
- Dry-run operational exercise completed.

---

# Production-ready checklist

## Security
- [ ] All leaked/reused secrets rotated
- [ ] No secrets in repo, logs, runtime artifacts, or tracked fixtures
- [ ] `.omx/` and runtime monitor outputs ignored
- [ ] Telegram bridge requires per-user allowlist
- [ ] No unauthenticated operator or dashboard endpoints exposed
- [ ] Sensitive headers/credentials are masked in logs
- [ ] Dependency audit passes
- [ ] Secret scan passes

## Trading correctness
- [ ] No forbidden silent-failure patterns remain in touched critical paths
- [ ] Exchange fetches hard-fail on required input failure
- [ ] Required EMA/risk/order inputs never downgrade to neutral defaults
- [ ] Restart/replay smoke tests pass
- [ ] Live/backtest behavior stays aligned where Rust is source of truth

## Reliability
- [ ] `src/passivbot.py` critical runtime/update paths hardened
- [ ] `src/candlestick_manager.py` critical fetch/freshness paths hardened
- [ ] `src/fill_events_manager.py` critical normalize paths hardened
- [ ] Priority exchange adapters have parity tests
- [ ] Runtime surfaces fail closed on invalid critical config

## Observability
- [ ] Structured logging used consistently
- [ ] No raw secrets in logs
- [ ] Health/monitor telemetry is available without public sensitive payload exposure
- [ ] Operator actions are auditable

## Testing and verification
- [ ] Full `python -m pytest -q` passes
- [ ] Targeted regression bundle defined and passing
- [ ] `cargo check --tests` passes for touched Rust paths
- [ ] Critical smoke tests pass on staging/fake-live
- [ ] Grep audit passes for forbidden error-contract patterns

## Deployment
- [ ] Supported production deployment path is documented
- [ ] Secrets come from secure runtime injection, not source files
- [ ] Operator dashboards are local-only or reverse-proxied with auth/TLS
- [ ] Rollback/runbook exists
- [ ] Staging validation completed before real capital

---

## Recommended execution order

1. P0.1 Rotate token + purge artifacts
2. P0.2 Fail-closed Telegram auth
3. P0.3 Lock down relay/dashboard exposure
4. P0.4 Mask headers + fix endpoint-override fail-open behavior
5. P1.1 Harden `src/passivbot.py`
6. P1.2 Harden `src/candlestick_manager.py`
7. P1.3 Harden `src/fill_events_manager.py`
8. P1.4 Add adapter parity tests
9. P2.1 Clean runtime artifact hygiene fully
10. P2.3 Reconcile stale plans
11. P3.1 to P3.3 Continue decomposition, Rust hardening, and CI gates
12. P4.1 Complete production-profile signoff
