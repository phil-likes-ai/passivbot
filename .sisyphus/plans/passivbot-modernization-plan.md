# Passivbot Modernization Plan

## Goal
Modernize and harden Passivbot end-to-end: remove silent-failure patterns, reduce god classes, improve exchange reliability for KuCoin/Bybit/Hyperliquid, modernize the grid/risk engine for 2026, and raise test coverage on trading-critical paths.

## What has already been done
- Fixed `or_default()` in `src/passivbot.py` to avoid swallowing `BaseException` subclasses.
- Fixed bare `except:` usage in `did_create_order()` and `did_cancel_order()`.
- Improved symbol conversion helpers in `src/passivbot.py` by replacing bare `except:` with `except Exception`.
- Hardened `src/procedures.py`:
  - replaced leaked `json.load(open(...))` / `json.dump(..., open(...))` patterns with context managers;
  - replaced several bare `except:` blocks with `except Exception`;
  - made optional imports (`hjson`, `pandas`) explicit with warnings;
  - fixed local typing/import issues (`utc_ms`, tuple return annotation, recursive exception printer flow).
- Hardened exchange adapters:
  - `src/exchanges/ccxt_bot.py` batch execution/cancellation now raises after logging instead of returning exceptions as data;
  - base CCXT balance extraction now fails loudly when quote balance payload is missing;
  - `src/exchanges/bybit.py` no longer truncates PnL lookbacks at one year, has safer unified-account balance parsing, and uses shared cursor pagination helpers;
  - `src/exchanges/kucoin.py` now paces exchange-config writes, exposes a real 1m candle watch/REST fallback path, prefers explicit close intent in `determine_pos_side()`, and fails loudly on config-write failures;
  - `src/exchanges/hyperliquid.py` now uses a shared CCXT rate-limit backoff helper.
- Tightened live config validation in `src/config/validate.py` for high-risk operator inputs such as leverage, max realized loss %, concurrency/batch limits, warmup ratio, recv window, margin mode preference, and time-in-force.
- Began deconstructing `src/passivbot.py` with low-risk extractions:
  - extracted order execution helpers to `src/passivbot_execution.py`;
  - extracted runtime/signal/shutdown helpers to `src/passivbot_runtime.py` and removed a restart-loop `except: pass` cleanup swallow;
  - extracted custom-order-id, order-type token, order tuple/unstuck detection, and trailing-bundle utilities to `src/passivbot_order_utils.py`.
  - extracted general pure helpers (`calc_pnl`, timestamp clipping, safe caller/function names, RSS lookup, etc.) to `src/passivbot_utils.py`.
  - extracted order-matching helpers into `src/passivbot_utils.py`.
  - extracted live candle warmup window calculation to `src/passivbot_warmup_utils.py`.
  - extracted balance/min-cost helpers to `src/passivbot_balance_utils.py` and added balance-update / UPNL / effective-min-cost-refresh helpers.
  - extracted exchange-config retry/backoff helpers to `src/passivbot_exchange_config.py`.
  - extracted symbol-id / symbol-conversion helpers to `src/passivbot_symbol_utils.py` and added `coin_to_symbol()`.
  - extended `src/passivbot_symbol_utils.py` with market-metadata initialization.
  - extracted approval / market-age gate helpers to `src/passivbot_approval_utils.py`.
  - extracted ticker normalization/update helpers to `src/passivbot_ticker_utils.py`.
  - extracted CCXT client option / endpoint override helpers to `src/passivbot_client_utils.py`.
  - extracted formatting helpers (`pad_sym`, duration formatting) to `src/passivbot_format_utils.py`.
  - extracted fetch-budget TTL helper to `src/passivbot_fetch_budget_utils.py`.
  - extracted runtime ops helpers (`_get_fetch_delay_seconds`, `stop_data_maintainers`) to `src/passivbot_runtime_ops.py`.
  - extended `src/passivbot_runtime_ops.py` with health-summary scheduling helper.
  - extended `src/passivbot_runtime_ops.py` with health-summary logging helper.
  - extended `src/passivbot_runtime_ops.py` with memory snapshot logging helper.
  - extended `src/passivbot_runtime_ops.py` with exchange-time helper.
  - extended `src/passivbot_runtime_ops.py` with log-silence watchdog context helper.
  - extended `src/passivbot_runtime_ops.py` with silence-watchdog runtime helpers.
  - extracted unstuck logging/scheduling helpers to `src/passivbot_unstuck_utils.py` and added live allowance and allowance-calculation helpers.
  - extracted position/trailing predicate helpers to `src/passivbot_position_utils.py`.
  - extracted position-change logging helper to `src/passivbot_position_logging_utils.py`.
  - extracted trailing position-change helper to `src/passivbot_trailing_utils.py`.
  - extracted startup banner helper to `src/passivbot_startup_utils.py`.
  - extended `src/passivbot_startup_utils.py` with boot-stagger helper.
  - extended `src/passivbot_startup_utils.py` with startup-ready finalization helper.
  - extended `src/passivbot_startup_utils.py` with startup-error reporting helper.
  - extended `src/passivbot_startup_utils.py` with startup pre-loop staging helper.
  - extracted wallet-exposure helpers to `src/passivbot_exposure_utils.py` and added WEL propagation helper.
  - extracted order-update placeholder helpers and recent-order tracking helpers to `src/passivbot_order_update_utils.py`.
  - extracted default live-state/approved-symbol hooks to `src/passivbot_hook_utils.py`, including `symbol_is_eligible()`.
  - extracted mode/position-count helpers to `src/passivbot_mode_utils.py` and added max-position-cap, forager-mode, orchestrator mode-conversion, fallback-override, and live-symbol-universe helpers.
  - extracted coin-override initialization, config-lookup, live/bot accessors, and bot-param shorthand helpers to `src/passivbot_override_utils.py`.
  - extracted PnL manager initialization helper to `src/passivbot_pnls_utils.py`.
  - began deconstructing `src/fill_events_manager.py` with fee, time, position-state, realized-PnL reconstruction, parsing, pagination, Bybit normalization/closed-pnl, cache, fetcher-construction, and CLI helper modules.
  - began deconstructing `src/candlestick_manager.py` by extracting query and EMA helper modules.
  - extracted init-markets exchange-config retry helper to `src/passivbot_market_init_utils.py`.
  - extended `src/passivbot_market_init_utils.py` with post-market-load setup helper.
  - extended `src/passivbot_market_init_utils.py` with loaded-market application helper.
  - extracted EMA-gating logging helper to `src/passivbot_logging_utils.py` and removed silent swallow in that path.
  - extracted first-timestamp refresh and lookup helpers to `src/passivbot_timestamp_utils.py`.
  - extracted EMA debug logging helper to `src/passivbot_logging_utils.py`.
  - extracted debug/log-once helpers to `src/passivbot_debug_utils.py`.
  - extracted candlestick-manager logging helper cluster to `src/candlestick_manager_logging_utils.py` and added strict-gap + persistent-gap summary helpers.
  - extracted candlestick-manager index maintenance helper to `src/candlestick_manager_index_utils.py` and added persist-batch observer setter, path helpers, and symbol-index loader.
  - extracted candlestick-manager legacy completeness helper to `src/candlestick_manager_legacy_utils.py`.
  - extracted candlestick-manager shard loading helper to `src/candlestick_manager_shard_utils.py`.
  - extended `src/candlestick_manager_logging_utils.py` with remote-fetch callback helper.

## Confirmed audit findings

### 1. Biggest god classes / oversized modules
- `src/passivbot.py` — 8486 lines
- `src/candlestick_manager.py` — 6734 lines
- `src/fill_events_manager.py` — 4706 lines
- `src/downloader.py` — 2183 lines
- `src/optimize.py` — 2064 lines
- `src/backtest.py` — 1966 lines
- Rust hotspots:
  - `passivbot-rust/src/backtest.rs` — 6786 lines
  - `passivbot-rust/src/orchestrator.rs` — 4132 lines
  - `passivbot-rust/src/python.rs` — 2448 lines

### 2. Exchange adapter hotspots

#### KuCoin
- `update_exchange_config_by_symbols()` bursts margin/leverage calls without throttling.
- `watch_ohlcvs_1m` is effectively a no-op; candles rely on REST polling.
- `determine_pos_side()` may be risky when position state is stale.

#### Bybit
- `fetch_open_orders()` and `fetch_positions()` use manual pagination.
- `fetch_pnl()` has 7-day windows and a 52-page cap, risking silent truncation.
- Unified account balance logic is complex and needs edge-case testing.

#### Hyperliquid
- Strongest adapter of the three.
- Explicit backoff logic exists and should be generalized into shared helpers.
- HIP-3 / isolated-margin unsupported paths should fail earlier via config validation.

### 3. Structural/type-safety findings
- `passivbot.py` has a high volume of type and interface drift.
- Config/API dicts flow too deep without strong validation.
- Current mixin wiring around HSL/monitoring contributes to attribute-type drift.

### 4. Strategy modernization findings
- Current system is grid/martingale-adjacent and should be treated as high tail-risk.
- Highest-value 2026 improvements:
  - volatility regime gate (HV30/ATR);
  - volatility-scaled spacing and sizing;
  - dynamic TWEL denominator based on tradable symbols;
  - risk-of-ruin caps on recursive sizing;
  - regime-flip exits and time-decay exits;
  - websocket health fallback to REST.

## Active implementation order

### Phase 1 — correctness and silent-failure hardening
1. Fix `asyncio.gather(return_exceptions=True)` handling in `src/exchanges/ccxt_bot.py`.
2. Remove remaining bare `except:` / `except Exception: pass` patterns in trading-critical paths.
3. Fix `_get_balance`-style neutral defaults where trading-critical values should fail loudly.
4. Replace ad-hoc prints in `src/procedures.py` and similar critical modules with structured logging.
5. Make exchange-config mutations fail loudly when exchange state cannot be applied.

#### Phase 1 QA
- Tool: `lsp_diagnostics` on each touched Python file.
  - Expected: no new diagnostics introduced in edited regions.
- Tool: `grep` for `except:` and `except Exception: pass` in touched files.
  - Expected: targeted patterns removed from edited trading-critical paths.
- Tool: targeted `pytest` for exchange/error-handling tests.
  - Expected: batch/failure-path tests pass and exceptions surface where intended.

### Phase 2 — exchange reliability
1. Add throttling/semaphore around KuCoin config-update bursts. ✅
2. Add safer KuCoin candle fallback or websocket support. ✅
3. Rework Bybit PnL pagination to prevent silent truncation. ✅
4. Harden Bybit unified-account balance calculation with tests. ✅
5. Generalize Hyperliquid backoff/pacing into shared adapter utilities. ✅
6. Add shared exchange-config updater with per-exchange pacing hooks. ⏳ next likely exchange-layer abstraction

### Phase 2a — live config safety
1. Validate high-risk live numeric fields and ranges before runtime. ✅
2. Reject invalid `margin_mode_preference` and `time_in_force` values. ✅
3. Expand validation further for bot/risk cross-field invariants. ⏳ next config-layer step

#### Phase 2 QA
- Tool: exchange adapter unit tests / new parity tests under `tests/exchanges/`.
  - Expected: KuCoin/Bybit/Hyperliquid adapter tests pass for config mapping, balance, pagination, and failure modes.
- Tool: targeted mocks around rate-limit and websocket-fallback paths.
  - Expected: semaphore/backoff logic throttles request bursts and REST fallback activates when websocket freshness is lost.
- Tool: `lsp_diagnostics` on touched adapter files.
  - Expected: no new type errors in modified adapter logic.

### Phase 3 — god-class decomposition
1. Split `src/passivbot.py` into:
   - orchestrator/core bot,
   - order executor,
   - pnl/fill manager,
   - forager,
   - health/monitor helpers,
   - typed config access layer.
2. Split `src/candlestick_manager.py` into:
   - storage/cache,
   - gap repair,
   - interpolation/resampling,
   - exchange-fetch adapters.
3. Split `src/fill_events_manager.py` into:
   - fetchers,
   - normalization,
   - cache/persistence,
   - reconciliation.
4. Later: split Rust `backtest.rs`, `orchestrator.rs`, and `python.rs` by domain.

#### Phase 3 QA
- Tool: existing unit/integration tests that exercise orchestration, monitoring, candles, and fill events.
  - Expected: behavior remains unchanged after extraction/refactor.
- Tool: `pytest` for focused regression suites plus any new extraction-specific tests.
  - Expected: moved code preserves public interfaces and state transitions.
- Tool: `lsp_diagnostics` on extracted modules.
  - Expected: no broken imports, missing attributes, or new symbol-resolution issues.

#### Phase 3 Progress
- `order executor` extraction: ✅ `src/passivbot_execution.py`
- `runtime/signal/shutdown` extraction: ✅ `src/passivbot_runtime.py`
- `order-type / trailing helper` extraction: ✅ `src/passivbot_order_utils.py`
- `order tuple helper` extraction: ✅ `src/passivbot_order_utils.py`
- `unstuck-order detection helper` extraction: ✅ `src/passivbot_order_utils.py`
- `general pure helpers` extraction: ✅ `src/passivbot_utils.py`
- `order matching helpers` extraction: ✅ `src/passivbot_utils.py`
- `warmup window helper` extraction: ✅ `src/passivbot_warmup_utils.py`
- `balance/min-cost helper` extraction: ✅ `src/passivbot_balance_utils.py`
- `balance update handler` extraction: ✅ `src/passivbot_balance_utils.py`
- `UPNL aggregation helper` extraction: ✅ `src/passivbot_balance_utils.py`
- `effective-min-cost refresh helper` extraction: ✅ `src/passivbot_balance_utils.py`
- `exchange-config retry/backoff` extraction: ✅ `src/passivbot_exchange_config.py`
- `symbol-id / symbol-conversion helper` extraction: ✅ `src/passivbot_symbol_utils.py`
- `market-metadata initialization` extraction: ✅ `src/passivbot_symbol_utils.py`
- `coin-to-symbol mapping helper` extraction: ✅ `src/passivbot_symbol_utils.py`
- `approval / market-age gate helper` extraction: ✅ `src/passivbot_approval_utils.py`
- `ticker normalization/update helper` extraction: ✅ `src/passivbot_ticker_utils.py`
- `CCXT client option / endpoint override helper` extraction: ✅ `src/passivbot_client_utils.py`
- `formatting helper` extraction: ✅ `src/passivbot_format_utils.py`
- `fetch-budget TTL helper` extraction: ✅ `src/passivbot_fetch_budget_utils.py`
- `runtime ops helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `health-summary scheduling helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `health-summary logging helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `memory snapshot logging helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `exchange-time helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `watchdog context helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `silence-watchdog runtime helper` extraction: ✅ `src/passivbot_runtime_ops.py`
- `unstuck logging/scheduling helper` extraction: ✅ `src/passivbot_unstuck_utils.py`
- `unstuck live-allowance helper` extraction: ✅ `src/passivbot_unstuck_utils.py`
- `unstuck allowance calculation helper` extraction: ✅ `src/passivbot_unstuck_utils.py`
- `position/trailing predicate helper` extraction: ✅ `src/passivbot_position_utils.py`
- `position-change logging helper` extraction: ✅ `src/passivbot_position_logging_utils.py`
- `trailing position-change helper` extraction: ✅ `src/passivbot_trailing_utils.py`
- `startup banner helper` extraction: ✅ `src/passivbot_startup_utils.py`
- `boot-stagger helper` extraction: ✅ `src/passivbot_startup_utils.py`
- `startup-ready finalization helper` extraction: ✅ `src/passivbot_startup_utils.py`
- `startup-error reporting helper` extraction: ✅ `src/passivbot_startup_utils.py`
- `startup pre-loop staging helper` extraction: ✅ `src/passivbot_startup_utils.py`
- `wallet-exposure helper` extraction: ✅ `src/passivbot_exposure_utils.py`
- `wallet-exposure propagation helper` extraction: ✅ `src/passivbot_exposure_utils.py`
- `order-update placeholder helper` extraction: ✅ `src/passivbot_order_update_utils.py`
- `recent-order tracking helper` extraction: ✅ `src/passivbot_order_update_utils.py`
- `default hook helper` extraction: ✅ `src/passivbot_hook_utils.py`
- `default symbol eligibility hook` extraction: ✅ `src/passivbot_hook_utils.py`
- `mode/position-count helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `forager-mode helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `first-timestamp refresh helper` extraction: ✅ `src/passivbot_timestamp_utils.py`
- `max-position-cap helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `mode-conversion helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `fallback mode-override helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `live-symbol-universe helper` extraction: ✅ `src/passivbot_mode_utils.py`
- `coin-override init helper` extraction: ✅ `src/passivbot_override_utils.py`
- `config override lookup helper` extraction: ✅ `src/passivbot_override_utils.py`
- `live/bot accessor helper` extraction: ✅ `src/passivbot_override_utils.py`
- `bot-param shorthand helper` extraction: ✅ `src/passivbot_override_utils.py`
- `PnL init helper` extraction: ✅ `src/passivbot_pnls_utils.py`
- `fill-events fee helper` extraction: ✅ `src/fill_events_fee_utils.py`
- `fill-events Bybit fee helper` extraction: ✅ `src/fill_events_fee_utils.py`
- `fill-events time helper` extraction: ✅ `src/fill_events_time_utils.py`
- `fill-events position-state helper` extraction: ✅ `src/fill_events_position_utils.py`
- `fill-events realized-PnL helper` extraction: ✅ `src/fill_events_position_utils.py`
- `fill-events parsing helper` extraction: ✅ `src/fill_events_parse_utils.py`
- `fill-events pagination helper` extraction: ✅ `src/fill_events_pagination_utils.py`
- `fill-events KuCoin PnL matching helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events KuCoin trade normalization helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events KuCoin order-detail helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events KuCoin discrepancy helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events KuCoin trade-batch collection helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events KuCoin positions-history batch helper` extraction: ✅ `src/fill_events_kucoin_utils.py`
- `fill-events Binance enrichment helper` extraction: ✅ `src/fill_events_binance_utils.py`
- `fill-events Bitget normalization helper` extraction: ✅ `src/fill_events_bitget_utils.py`
- `fill-events Bitget detail-result helper` extraction: ✅ `src/fill_events_bitget_utils.py`
- `fill-events Bitget batch-processing helper` extraction: ✅ `src/fill_events_bitget_utils.py`
- `fill-events Bitget cursor-step helper` extraction: ✅ `src/fill_events_bitget_utils.py`
- `fill-events OKX normalization helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX order-detail cache helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX batch-processing helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX pagination-cursor helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX finalization helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX fetch-params helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events OKX batch-stop helper` extraction: ✅ `src/fill_events_okx_utils.py`
- `fill-events Bybit trade normalization helper` extraction: ✅ `src/fill_events_bybit_utils.py`
- `fill-events Bybit closed-pnl helper` extraction: ✅ `src/fill_events_bybit_utils.py`
- `fill-events cache metadata helper` extraction: ✅ `src/fill_events_cache_utils.py`
- `fill-events cache gap helper` extraction: ✅ `src/fill_events_cache_utils.py`
- `fill-events cache day-file IO helper` extraction: ✅ `src/fill_events_cache_utils.py`
- `fill-events fetcher-construction helper` extraction: ✅ `src/fill_events_fetcher_utils.py`
- `fill-events CLI helper` extraction: ✅ `src/fill_events_cli_utils.py`
- `init-markets exchange-config retry helper` extraction: ✅ `src/passivbot_market_init_utils.py`
- `init-markets post-load setup helper` extraction: ✅ `src/passivbot_market_init_utils.py`
- `loaded-market application helper` extraction: ✅ `src/passivbot_market_init_utils.py`
- `candlestick logging helper` extraction: ✅ `src/candlestick_manager_logging_utils.py`
- `candlestick index maintenance helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick persist-batch observer helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick path helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick symbol-index helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick legacy completeness helper` extraction: ✅ `src/candlestick_manager_legacy_utils.py`
- `candlestick shard loading helper` extraction: ✅ `src/candlestick_manager_shard_utils.py`
- `candlestick synthesis helper` extraction: ✅ `src/candlestick_manager_synthesis_utils.py`
- `candlestick core helper` extraction: ✅ `src/candlestick_manager_core_utils.py`
- `candlestick misc helper` extraction: ✅ `src/candlestick_manager_misc_utils.py`
- `candlestick caller/time helper` extraction: ✅ `src/candlestick_manager_misc_utils.py`
- `candlestick GateIO quarantine helper` extraction: ✅ `src/candlestick_manager_misc_utils.py`
- `candlestick date helper` extraction: ✅ `src/candlestick_manager_date_utils.py`
- `candlestick legacy-path helper` extraction: ✅ `src/candlestick_manager_legacy_path_utils.py`
- `candlestick legacy-scan helper` extraction: ✅ `src/candlestick_manager_legacy_path_utils.py`
- `candlestick disk-load planner` extraction: ✅ `src/candlestick_manager_disk_utils.py`
- `candlestick disk-load executor` extraction: ✅ `src/candlestick_manager_disk_utils.py`
- `candlestick save-range bucket helper` extraction: ✅ `src/candlestick_manager_disk_utils.py`
- `candlestick incremental-save helper` extraction: ✅ `src/candlestick_manager_disk_utils.py`
- `candlestick persist helper` extraction: ✅ `src/candlestick_manager_persist_utils.py`
- `candlestick synthetic-tracking helper` extraction: ✅ `src/candlestick_manager_persist_utils.py`
- `candlestick runtime-synthetic helper` extraction: ✅ `src/candlestick_manager_persist_utils.py`
- `candlestick EMA/runtime apply helper` extraction: ✅ `src/candlestick_manager_persist_utils.py`
- `candlestick gap helper` extraction: ✅ `src/candlestick_manager_gap_utils.py`
- `candlestick gap-mutation helper` extraction: ✅ `src/candlestick_manager_gap_utils.py`
- `candlestick coverage helper` extraction: ✅ `src/candlestick_manager_coverage_utils.py`
- `candlestick index-rebuild helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick refresh/inception helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick min-shard/probe helper` extraction: ✅ `src/candlestick_manager_index_utils.py`
- `candlestick CCXT helper` extraction: ✅ `src/candlestick_manager_ccxt_utils.py`
- `candlestick CCXT payload helper` extraction: ✅ `src/candlestick_manager_ccxt_utils.py`
- `candlestick CCXT call/error helper` extraction: ✅ `src/candlestick_manager_ccxt_utils.py`
- `candlestick pre-inception gap helper` extraction: ✅ `src/candlestick_manager_gap_utils.py`
- `candlestick remote-fetch callback helper` extraction: ✅ `src/candlestick_manager_logging_utils.py`
- `candlestick strict-gap summary helper` extraction: ✅ `src/candlestick_manager_logging_utils.py`
- `candlestick persistent-gap summary helper` extraction: ✅ `src/candlestick_manager_logging_utils.py`
- `EMA-gating logging helper` extraction: ✅ `src/passivbot_logging_utils.py`
- `EMA debug logging helper` extraction: ✅ `src/passivbot_logging_utils.py`
- `debug/log-once helper` extraction: ✅ `src/passivbot_debug_utils.py`
- `candlestick bulk query helper` extraction: ✅ `src/candlestick_manager_query_utils.py`
- `candlestick EMA helper` extraction: ✅ `src/candlestick_manager_ema_utils.py`
- `first-timestamp refresh helper` extraction: ✅ `src/passivbot_timestamp_utils.py`
- `first-timestamp lookup helper` extraction: ✅ `src/passivbot_timestamp_utils.py`
- next likely extraction: health/status snapshot helpers, forager helper cluster, or balance/equity history helpers

### Phase 4 — 2026 strategy/risk upgrades
1. Add regime gate (HV30/ATR) to entry planning.
2. Add volatility-scaled grid spacing and size throttles.
3. Add dynamic TWEL denominators based on tradable symbols.
4. Add risk-of-ruin guardrails for recursive/martingale sizing.
5. Add time-based decay exits for stale positions.
6. Add regime-flip exits on trend detection.

#### Phase 4 QA
- Tool: targeted backtests and orchestrator/risk tests.
  - Expected: new controls reduce exposure under high-volatility/trend regimes without breaking baseline planning.
- Tool: dedicated regression tests for TWEL, spacing, and sizing outputs.
  - Expected: deterministic outputs for known input fixtures and explicit behavior changes where intended.
- Tool: manual config-sanity review for new knobs.
  - Expected: every new risk/strategy parameter is validated, documented, and defaults safely.

### Phase 5 — coverage and verification
1. Add parity tests for KuCoin/Bybit/Hyperliquid adapters.
2. Expand tests for order placement, risk gates, exchange failures, and PnL history.
3. Run diagnostics on all changed files.
4. Run targeted pytest, then broader regression coverage.

#### Phase 5 QA
- Tool: full targeted `pytest` matrix for touched areas, then broader repo regression run.
  - Expected: no regressions in trading-critical paths and new coverage for previously untested branches.
- Tool: `lsp_diagnostics` on all changed directories.
  - Expected: zero new diagnostics caused by the modernization work.
- Tool: final grep audit for silent-failure anti-patterns in touched modules.
  - Expected: no remaining targeted silent-failure patterns in edited critical files.

## Final verification wave
- Re-run `lsp_diagnostics` on every changed file and modified directory.
- Run targeted `pytest` for each completed phase immediately after implementation.
- Run a broader regression suite before declaring the modernization slice done.
- Re-run grep-based audits for silent failure, unsafe defaults, and adapter pacing patterns in touched modules.
- Confirm changed behavior in `CHANGELOG.md` / docs when user-facing or operator-facing behavior changes.

## Current next actionable item
Continue `src/passivbot.py` deconstruction with the next low-risk cohesive cluster (likely health/status snapshot helpers or forager/balance helper paths) while continuing to eliminate remaining broad exception swallowing in active runtime paths.

## Resume notes for later sessions
- This file is the durable source of truth for the modernization effort.
- Highest-risk exchanges for user priorities: KuCoin, Bybit, Hyperliquid.
- Highest architectural priorities: `src/passivbot.py`, `src/candlestick_manager.py`, `src/fill_events_manager.py`.
- Highest near-term execution target: `src/exchanges/ccxt_bot.py` batch/error handling.
