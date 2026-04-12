# Passivbot Production Readiness PR Slice Queue

_Status: canonical implementation queue_

## Purpose

Turn the production-readiness backlog into concrete, PR-sized execution slices with stable task IDs,
explicit scope boundaries, verification steps, and dependency order.

## Canonical planning set

Execute from these documents in this order:

1. `docs/plans/passivbot_production_readiness_remediation_backlog.md` — canonical backlog
2. `docs/plans/passivbot_production_readiness_pr_slices.md` — canonical PR/slice queue
3. `.omx/plans/prd-reliability-first-modernization-wave.md` — reliability execution rules
4. `.omx/plans/test-spec-reliability-first-modernization-wave.md` — verification contract
5. `.sisyphus/drafts/passivbot-final-wave-readiness-matrix.md` — final signoff criteria

Historical/supporting docs may add context, but they do not override this queue.

## Status legend

- `queued` — ready to implement now
- `complete` — implemented and verified
- `blocked` — depends on an earlier slice landing first
- `deferred` — valid work, but not on the immediate critical path
- `archived` — historical reference only; do not execute from it

## Slice rules

1. One slice = one reviewable PR.
2. Do not mix security containment and broad refactors in the same PR.
3. Reliability slices must include grep audit + targeted tests.
4. If a slice touches trading-critical behavior, follow `docs/ai/error_contract.md`.
5. Do not mark a slice complete without verification evidence.

---

# Immediate execution queue

| ID | Title | Status | Size | Depends on |
|---|---|---|---|---|
| PB-REL-004 | Harden fill-events malformed payload handling | complete | M | PB-REL-001 |
| PB-XCH-001 | Add exchange adapter parity coverage for required-input behavior | complete | M | PB-REL-003, PB-REL-004 |
| PB-CI-001 | Add secret/dependency/grep gates to CI | complete | M | PB-SEC-001, PB-SEC-003 |
| PB-DOC-001 | Define hardened production deployment profile and signoff checklist | deferred | M | PB-SEC-002, PB-OPS-001, PB-CI-001 |

---

# Slice cards

## PB-SEC-001 — Purge runtime artifacts and harden ignore rules
- **Status:** complete
- **Suggested branch:** `chore/pb-sec-001-ignore-runtime-artifacts`
- **Target PR size:** small
- **In scope files:** `.gitignore`, tracked `.omx/*` runtime files, tracked runtime `monitor/*` artifacts, related docs if needed
- **Out of scope:** behavior changes to trading logic
- **Tasks:**
  - add `.omx/` ignore coverage
  - add ignore coverage for runtime-generated monitor events/checkpoints/history
  - remove accidentally tracked runtime-only files from git
  - verify kept monitor files are intentional fixtures only
- **Acceptance criteria:**
  - normal local execution no longer dirties git with runtime artifacts
  - no tracked `.omx` runtime logs/state remain
- **Verification:**
  - `git status --short --untracked-files=all .omx monitor`
  - `git ls-files .omx monitor`
  - `git check-ignore -v .omx monitor monitor/fake .omx/logs/omx-2026-04-11.jsonl`

## PB-SEC-002 — Make Telegram bridge auth fail-closed
- **Status:** complete
- **Suggested branch:** `fix/pb-sec-002-telegram-auth`
- **Target PR size:** medium
- **Depends on:** `PB-SEC-001`
- **In scope files:** `src/omx_telegram_progress_bridge.py`, `tests/test_omx_telegram_progress_bridge.py`, `README.md`, `configs/examples/omx.telegram.sample.omx-config.json`
- **Out of scope:** new Telegram features
- **Tasks:**
  - require per-user allowlist when inbound commands/replies are enabled
  - reject configs that only allow a chat but not users
  - ensure group-chat unauthorized users cannot inject operator commands
  - align docs/sample config with runtime behavior
- **Acceptance criteria:**
  - non-allowlisted users in allowed chats are rejected
  - startup fails on incomplete auth config
- **Verification:**
  - `python -m pytest tests/test_omx_telegram_progress_bridge.py -q`

## PB-OPS-001 — Lock monitor relay to safe local-only defaults
- **Status:** complete
- **Suggested branch:** `fix/pb-ops-001-monitor-relay-local-only`
- **Target PR size:** medium
- **In scope files:** `src/tools/monitor_relay.py`, `src/monitor_relay.py`, `src/monitor_dev.py`, `src/monitor_web.py`, `tests/test_monitor_relay.py`, `tests/test_monitor_dev.py`, `tests/test_monitor_web.py`
- **Out of scope:** full dashboard redesign
- **Tasks:**
  - require localhost bind by default
  - require explicit insecure/public flag for `0.0.0.0`
  - add warning path for insecure mode
  - if remote support remains, require auth in supported production mode
- **Acceptance criteria:**
  - unauthenticated public bind is not the default supported path
  - helper tools do not silently auto-launch public relay instances
- **Verification:**
  - `python -m pytest tests/test_monitor_relay.py tests/test_monitor_dev.py tests/test_monitor_web.py -q`

## PB-OPS-002 — Lock fill-events dashboard to safe local-only defaults
- **Status:** complete
- **Suggested branch:** `fix/pb-ops-002-fill-events-dash-bind`
- **Target PR size:** small
- **In scope files:** `src/tools/fill_events_dash.py`, dashboard tests if present or add focused test coverage/docs
- **Out of scope:** dashboard feature work
- **Tasks:**
  - change default bind to localhost
  - require explicit insecure/public flag for `0.0.0.0`
  - emit loud warning if public mode is enabled
- **Acceptance criteria:**
  - default dashboard launch is local-only
- **Verification:**
  - targeted tests if added, otherwise manual arg parsing / startup verification

## PB-SEC-003 — Redact custom endpoint headers and fail closed on invalid live config
- **Status:** complete
- **Suggested branch:** `fix/pb-sec-003-endpoint-override-redaction`
- **Target PR size:** medium
- **In scope files:** `src/custom_endpoint_overrides.py`, `tests/test_custom_endpoints.py`
- **Out of scope:** redesign of custom endpoint feature
- **Tasks:**
  - mask sensitive header values in all logs
  - avoid logging raw `rest_extra_headers`
  - define and enforce live-mode behavior for invalid endpoint config
  - add regression tests for redaction and invalid-config policy
- **Acceptance criteria:**
  - logs never expose sensitive headers
  - invalid live config does not silently become empty config unless explicitly allowed
- **Verification:**
  - `python -m pytest tests/test_custom_endpoints.py -q`

## PB-REL-001 — Remove false-success behavior from update prep cycle
- **Status:** complete
- **Suggested branch:** `fix/pb-rel-001-update-prep-fail-loud`
- **Target PR size:** medium
- **Depends on:** `PB-SEC-001`
- **In scope files:** `src/passivbot.py`, relevant passivbot tests (likely `tests/test_passivbot_execution.py` and/or new targeted slice tests)
- **Out of scope:** adjacent refactors unrelated to prep-cycle failure behavior
- **Tasks:**
  - replace `return_exceptions=True` handling in the critical prep/update cycle
  - surface actionable context when required collaborator calls fail
  - prove failed cycle does not advance as success and healthy rerun remains restart-safe
- **Acceptance criteria:**
  - no false-success path remains in this slice
  - test covers failure then healthy retry
- **Verification:**
  - targeted pytest for the slice
  - grep audit on touched paths: `rg -n "except Exception|except:|return_exceptions=True|\.get\([^\n]*,\s*(0|0\.0|None|False|\{\}|\[\])\)" src/passivbot.py tests`

## PB-REL-002 — Harden warmup/mode-prep broad catches in `src/passivbot.py`
- **Status:** complete
- **Suggested branch:** `fix/pb-rel-002-passivbot-warmup-hardening`
- **Target PR size:** medium
- **Depends on:** `PB-REL-001`
- **In scope files:** `src/passivbot.py`, `tests/test_passivbot_startup_utils.py`, `tests/test_passivbot_warmup_utils.py`, or directly adjacent targeted tests
- **Out of scope:** unrelated extraction work
- **Tasks:**
  - inventory broad catches in warmup/mode-prep
  - classify each touched site as critical-path vs best-effort
  - hard-fail or explicitly bound/test any remaining fallback
- **Acceptance criteria:**
  - touched critical paths no longer swallow broad exceptions
- **Verification:**
  - targeted pytest bundle for touched warmup/startup behavior
  - grep audit on touched files

## PB-REL-003 — Harden candlestick critical fetch/freshness paths
- **Status:** complete
- **Suggested branch:** `fix/pb-rel-003-candlestick-fail-loud`
- **Target PR size:** large
- **Depends on:** `PB-REL-001`
- **In scope files:** `src/candlestick_manager.py`, targeted candlestick-manager tests
- **Out of scope:** broad decomposition not directly supporting reliability
- **Tasks:**
  - classify critical catches in fetch/freshness/pagination/gap paths
  - remove or narrow broad catches for required-input flows
  - add retry exhaustion / backoff / pagination tests where missing
- **Acceptance criteria:**
  - touched required-input paths fail loudly with actionable context
- **Verification:**
  - `python -m pytest tests/test_candlestick_manager.py tests/test_live_candlestick_manager.py -q`
  - plus any newly added focused suites

## PB-REL-004 — Harden fill-events malformed payload handling
- **Status:** complete
- **Suggested branch:** `fix/pb-rel-004-fill-events-fail-loud`
- **Target PR size:** medium
- **Depends on:** `PB-REL-001`
- **In scope files:** `src/fill_events_manager.py`, fill-events parse/model/manager tests
- **Out of scope:** unrelated fill-events decomposition
- **Tasks:**
  - remove neutral-default downgrades for required payload fields
  - fail loudly on malformed critical payloads
  - add exchange-specific malformed payload coverage
- **Acceptance criteria:**
  - missing required fill fields no longer normalize silently to zeros/empties
- **Verification:**
  - targeted fill-events pytest bundle
  - grep audit on touched files

## PB-XCH-001 — Add exchange adapter parity coverage for required-input behavior
- **Status:** complete
- **Suggested branch:** `test/pb-xch-001-adapter-parity`
- **Target PR size:** medium
- **Depends on:** `PB-REL-003`, `PB-REL-004`
- **In scope files:** exchange adapter tests for Bybit/KuCoin/Hyperliquid, `src/passivbot_exchange_config.py` if needed
- **Out of scope:** new exchange features
- **Tasks:**
  - verify config update sequencing and failure propagation
  - verify tolerated no-op/not-modified paths remain explicit
  - verify no silent fallback on required values
- **Acceptance criteria:**
  - priority adapters share one tested contract for required-input behavior
- **Verification:**
  - targeted exchange pytest bundle

## PB-OPS-003 — Make `sync_tar.py` extraction traversal-safe
- **Status:** complete
- **Suggested branch:** `fix/pb-ops-003-sync-tar-safe-extract`
- **Target PR size:** small
- **In scope files:** `sync_tar.py`, `tests/test_sync_tar.py`
- **Out of scope:** remote transfer workflow redesign
- **Tasks:**
  - validate archive members before extraction
  - reject absolute paths and `..` traversal members
- **Acceptance criteria:**
  - malicious archive cannot escape destination
- **Verification:**
  - `python -m pytest tests/test_sync_tar.py -q`

## PB-CI-001 — Add secret/dependency/grep gates to CI
- **Status:** complete
- **Suggested branch:** `ci/pb-ci-001-security-gates`
- **Target PR size:** medium
- **Depends on:** `PB-SEC-001`, `PB-SEC-003`
- **In scope files:** CI workflow files, lint/type/test config, developer docs
- **Out of scope:** broad CI platform migration
- **Tasks:**
  - add secret scanning
  - add dependency audit
  - add grep gate for forbidden error-contract patterns
  - add targeted Python and Rust verification steps
- **Acceptance criteria:**
  - merges fail on secret leakage, dependency audit failures, or forbidden silent-failure regressions
- **Verification:**
  - CI dry run / workflow validation

## PB-ARCH-001 — Reconcile stale planning docs and mark archived plans
- **Status:** complete
- **Suggested branch:** `docs/pb-arch-001-plan-reconciliation`
- **Target PR size:** small
- **In scope files:** `docs/plans/passivbot_production_readiness_remediation_backlog.md`, this queue, `.sisyphus/plans/passivbot-unified-modernization-overhaul-plan.md`, `.sisyphus/plans/passivbot-modernization-plan.md`, `.sisyphus/drafts/passivbot-remaining-work-breakdown.md`, `.sisyphus/drafts/passivbot-final-wave-readiness-matrix.md`, `.sisyphus/drafts/passivbot-final-completion-evidence.md`
- **Out of scope:** implementation behavior changes
- **Tasks:**
  - mark stale plan artifacts as archived/historical
  - point all active planning docs to canonical backlog + queue
  - remove outdated “everything complete” status claims
- **Acceptance criteria:**
  - future sessions cannot mistake the archived plan for the active queue
- **Verification:**
  - doc review
  - grep for canonical path references

## PB-DOC-001 — Define hardened production deployment profile and signoff checklist
- **Status:** deferred
- **Suggested branch:** `docs/pb-doc-001-production-profile`
- **Target PR size:** medium
- **Depends on:** `PB-SEC-002`, `PB-OPS-001`, `PB-CI-001`
- **In scope files:** deployment docs, container docs, runbooks, `CHANGELOG.md` if operator-facing behavior changes
- **Out of scope:** implementation changes better handled in earlier security/reliability slices
- **Tasks:**
  - define supported dev/staging/prod modes
  - define secure secrets path, operator surface policy, and signoff checklist
  - document canonical production deployment path
- **Acceptance criteria:**
  - one supported hardened production profile exists on paper before real-money rollout
- **Verification:**
  - doc review and staging checklist walkthrough

---

# Not currently in the immediate queue

These remain valid backlog themes but should be reopened only after the immediate queue is under control:

- deeper `src/passivbot.py` extraction beyond reliability-adjacent slices
- broader `src/candlestick_manager.py` decomposition not tied to critical fail-loud work
- `src/fill_events_manager.py` data-model extraction beyond malformed-payload hardening
- Rust risk/strategy upgrade wave after Python-side fail-loud foundation is cleaner
- additional coverage-ratchet work not directly tied to critical production readiness blockers
