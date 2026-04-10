# Passivbot Massive Overhaul Plan

## 1. Goal and Success Criteria
**Goal**: Transform Passivbot from a monolithic prototype into a highly maintainable, thoroughly tested, securely typed, and strategically safer enterprise-grade trading bot.

**Success Criteria**:
- Test coverage (`pytest`) on `src/` exceeds 80%.
- `ruff` (linting) and `mypy` (strict typing) pass completely on the codebase.
- "God classes" (`passivbot.py`, `candlestick_manager.py`, `backtest.rs`, `orchestrator.rs`) are modularized following Single Responsibility Principles.
- The `unstuck` math and `martingale` routines in the Rust core handle "trapped capital" scenarios more effectively, verified via the backtester.
- No `except Exception: pass` or default silencing in critical trading paths (strict adherence to `docs/ai/error_contract.md`).

## 2. Scope Boundaries
**IN SCOPE**:
- Modifying `pyproject.toml` to introduce modern linting.
- Extensive test writing.
- Breaking apart large files into modular directories (e.g., separating exchange routing, state tracking, and UI within `passivbot.py`).
- Tuning mathematical logic in `passivbot-rust/src/closes.rs` and `entries.rs`.

**OUT OF SCOPE**:
- Dropping support for existing exchanges.
- Changing the foundational Rust architecture paradigm (Rust will remain the source of truth).
- Moving from a Martingale core to an entirely unrelated paradigm (e.g. machine learning prediction).

## 3. Constraints and Guardrails
- **Statelessness**: Refactors must not introduce local persistent state. Behavior must remain re-derivable from exchange data + config.
- **Python-Rust Boundary**: Business logic dictates entries/closes must remain in Rust. Python handles IO and plumbing.
- **Fallback safety**: Adhere strictly to the fallback matrix. No silent failures on exchange fetches or Risk inputs.
- **Evolutionary changes**: Deconstruct large files incrementally, running tests continually.

## 4. Execution Tasks

### Phase 1: Tooling & Coverage Foundation (Python)
- [ ] Task 1: Update `pyproject.toml` and `tox.ini` (or create `pytest.ini`/`ruff.toml`) to enforce strict `ruff` formatting/linting and `mypy` typing.
- [ ] Task 2: Fix all immediate `ruff` violations in `src/` (auto-fixes where possible, manual interventions where necessary).
- [ ] Task 3: Apply basic typing `mypy` stubs across the main utility files in `src/` to get a passing baseline.
- [ ] Task 4: Write missing `pytest` coverage for foundational utilities (`pure_funcs.py`, config managers) to establish a baseline before breaking apart complex logic.

### Phase 2: Python God Class Deconstruction
- [ ] Task 5: Deconstruct `candlestick_manager.py`. Extract cache management, websocket listening, and data validation into separate modules.
- [ ] Task 6: Deconstruct `passivbot.py`. Extract the event loop orchestrator, the user-interface/logging setups, and the exchange configuration handlers into distinct cleanly typed classes.
- [ ] Task 7: Apply rigorous unit tests (`pytest`) against the newly separated modules from `passivbot.py` and `candlestick_manager.py`.

### Phase 3: Rust Architecture & Safety Audit
- [ ] Task 8: Audit `passivbot-rust/src/risk.rs` and `utils.rs` for silent failure conditions or floating point arithmetic vulnerabilities. Add Rust unit tests to verify bounds.
- [ ] Task 9: Deconstruct `orchestrator.rs` and `backtest.rs`. Abstract the state-machine execution flow from the raw mathematical calculations.

### Phase 4: Strategy Tuning (Unstuck Logic)
- [ ] Task 10: Modify `closes.rs` and `entries.rs` to implement an enhanced "unstuck" mechanism. Introduce dynamic take-profit shrinking or progressive grid clearing to prevent capital lockup during deep trends.
- [ ] Task 11: Run the `optimizer` and `backtest.rs` heavily against these new strategies using historical OHLCV data to verify that the enhanced unstuck logic actively prevents deep drawdowns.

## Final Verification Wave
- [ ] Verify test coverage report exceeds expectations.
- [ ] Run live fake-bot to ensure exchange integrations hold up with modularized `passivbot.py`.
- [ ] Confirm compliance with `principles.yaml` and `error_contract.md`.
