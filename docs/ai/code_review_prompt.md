# Code Review Checklist (Lean)

Use this checklist when reviewing Passivbot changes.

## Critical Checks (Block Merge)

1. Rust boundary respected for behavior changes.
2. Stateless requirement preserved.
3. Error handling follows `error_contract.md`.
4. `position_side` vs `side` usage is correct.
5. Signed qty conventions preserved.
6. EMA span derivations remain float.
7. No security regressions or secret leakage.

## Important Checks

1. Config placement follows hierarchy (`config.live` default).
2. Exchange-specific pagination/quirks handled where relevant.
3. Logging level/tag usage is sane for operators.
4. Tests cover normal path, edge cases, and regression behavior.
5. User-facing changes have `CHANGELOG.md` entry.

## Test Execution Minimum

1. Run existing relevant tests.
2. Add/run targeted tests for changed paths.
3. If Rust changed, rebuild extension before Python tests.

```bash
cd passivbot-rust && maturin develop --release && cd ..
pytest
```

## Critical Modernization Regression Bundle

Use this targeted bundle before signing off modernization work in the repeatedly touched critical modules:

```bash
python -m pytest tests/test_fill_events_parse_utils.py
python -m pytest tests/test_passivbot_exchange_config.py tests/test_exchange_config_updates.py
python -m pytest tests/test_candlestick_manager_ccxt_utils.py tests/test_candlestick_manager_coverage_utils.py
python -m pytest tests/test_fill_events_parse_utils.py tests/test_fill_events_model_utils.py tests/test_fill_events_coalesce_utils.py
cd passivbot-rust && cargo check --tests && cd ..
```

Expand the targeted Python subset further when the touched slice lands in other `test_passivbot_*`, `test_candlestick_manager_*`, or `test_fill_events_*` modules.

## Targeted Smoke Backtest (When Relevant)

Use a short window + one coin for fast review checks on order/risk/EMA/config-flow changes.

```bash
python3 - <<'PY'
import copy, json
cfg = copy.deepcopy(json.load(open("configs/examples/default_trailing_grid_long_npos10.json")))
cfg.setdefault("backtest", {})
cfg["backtest"]["start_date"] = "2025-01-01"
cfg["backtest"]["end_date"] = "2025-01-08"
cfg.setdefault("live", {})
cfg["live"]["approved_coins"] = {"long": ["BTC/USDT:USDT"], "short": []}
json.dump(cfg, open("/tmp/review_backtest.json", "w"), indent=2)
PY
passivbot backtest /tmp/review_backtest.json --suite n -dp
```

## Review Report Template

```markdown
## Summary

## Critical Issues

## Important Issues

## Test Results
- Existing tests:
- New tests:
- Backtest smoke:

## Open Questions / Assumptions

## Change Summary
```

## Report Format Rules

1. Findings first, ordered by severity, with `file:line`.
2. Distinguish confirmed issues vs assumptions.
3. Include tests run and outcomes.
