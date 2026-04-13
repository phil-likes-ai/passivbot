# Production Deployment Profile

This document defines the supported production deployment profile for Passivbot live trading.

Use it together with:

- [`container_deployment.md`](container_deployment.md) for the canonical container/runtime contract
- [`hosting/railway.md`](hosting/railway.md) when Railway is the deployment target
- [`monitor.md`](monitor.md) for monitor relay/dashboard behavior
- [`fake_live.md`](fake_live.md) for staging and fake-live validation
- [`plans/passivbot_production_readiness_remediation_backlog.md`](plans/passivbot_production_readiness_remediation_backlog.md)
- [`plans/passivbot_production_readiness_pr_slices.md`](plans/passivbot_production_readiness_pr_slices.md)

## Supported Modes

| Mode | Purpose | Capital at risk | Allowed operator surface posture | Canonical validation |
|---|---|---:|---|---|
| Dev | Local implementation and debugging | None | Local-only tools, ad hoc configs | targeted tests, lint/diagnostics |
| Staging / fake-live | Rehearse live behavior without real exchange risk | None | Local-only or authenticated internal-only | fake-live replay + targeted regression bundle |
| Production live | Real-money trading | Real capital | Local-only by default; remote access only through authenticated/TLS-protected gateways | deployment checklist + rollout verification + rollback plan |

Production live is the only supported real-money mode. Anything else is preparation for production, not a substitute for it.

## Canonical Production Path

Supported production deployments should use the same runtime contract everywhere:

1. Build or publish the canonical live image from `Dockerfile_live`.
2. Start through `container/entrypoint.sh`, which executes the normal `passivbot live` CLI path.
3. Supply config through a mounted config file or managed inline config, following [`container_deployment.md`](container_deployment.md).
4. Supply secrets through secure runtime injection only:
   - mounted `api-keys.json` from a secret volume, or
   - managed environment variables consumed at container start and rendered into `/run/passivbot`.
5. Persist writable runtime outputs outside the repo tree, typically under `/data`:
   - `/data/configs`
   - `/data/logs`
   - `/data/monitor`
6. Treat Railway and similar hosts as thin consumers of the same image and env contract, not separate runtime architectures.

## Production Controls

### Secrets

- Do not place live credentials in the repo, tracked configs, runtime artifacts, or shell history snippets committed to docs.
- Prefer mounted secret files or managed secret stores over hand-edited repo-local `api-keys.json`.
- If env-generated credentials are used, inject them only at runtime and let the entrypoint render ephemeral files under `/run/passivbot`.
- Rotate any credential immediately if it appears in logs, runtime artifacts, or version control.

### Operator Surfaces

- Monitor relay, monitor dashboard, and fill-events dashboard are local-only by default.
- Remote operator access is supported only behind an authenticated reverse proxy with TLS.
- Do not expose relay/dashboard endpoints directly on `0.0.0.0` without an explicit, reviewed reason.
- Telegram bridge usage is optional. If enabled for production, it must use the fail-closed allowlist model:
  - `allowedChatIds`
  - plus `allowedUserIds` or `allowedUsernames`
- Treat Telegram as operator coordination only; it is not a direct order-placement surface.

### Logging, Evidence, and Alerting

- Keep `logging.persist_to_file = true` for production live runs.
- Persist logs to a mounted writable path such as `/data/logs`.
- Keep monitor output on a mounted writable path such as `/data/monitor` when monitor tooling is enabled.
- Retain enough logs/monitor evidence to support restart diagnosis, incident review, and rollback decisions.
- Use external alerting/supervision for process health, repeated restarts, and operator-visible failures.

### Runtime Safety

- Deploy only from a reviewed commit with passing targeted verification and CI gates.
- Keep custom endpoint overrides explicit and reviewed before live rollout.
- Prefer one bot per container/process so health state, logs, and rollback are easy to reason about.
- Use restart-safe deployment tooling; do not rely on mutable in-repo runtime state to preserve behavior.

## Unsupported Production Patterns

The following are out of profile for real-money production:

- Direct public relay/dashboard binds without auth/TLS
- Repo-local live secrets or committed runtime artifacts
- Dirty-tree deployments where the shipped code does not match a reviewed commit
- Platform-specific startup forks that bypass `passivbot live`
- Treating fake-live or monitor-only runs as sufficient production signoff

## Rollout Runbook

### 1. Preflight

- Confirm the target commit is reviewed and CI-green.
- Confirm the active queue and backlog do not show unresolved production blockers.
- Confirm secrets are present only through the supported runtime injection path.
- Confirm config path, log path, and monitor path point to writable mounted locations.
- Confirm operator surfaces remain local-only or are fronted by authenticated TLS.

### 2. Staging / Fake-Live Validation

Run a pre-production validation pass before real capital:

- targeted regression bundle for touched trading/runtime surfaces
- `cargo check --manifest-path passivbot-rust/Cargo.toml --tests` when Rust-adjacent changes are involved
- at least one relevant fake-live scenario from [`fake_live.md`](fake_live.md) for the change class
- monitor/relay attach test if production monitoring is enabled

Do not promote to production if staging/fake-live still hides missing data, broken auth, or unsafe operator exposure.

### 3. Deploy

- Start the canonical live container/runtime with the chosen mounted config and secrets path.
- Verify startup logs show the expected user, exchange, config source, and any intentional endpoint overrides.
- Verify monitor root and log root are writing to the mounted paths, not into the repo checkout.
- Verify no unexpected public listener is opened.

### 4. Post-Deploy Checks

- Confirm the bot reaches healthy startup without repeated restart loops.
- Confirm live logs are rotating/persisting as expected.
- Confirm monitor relay/dashboard access works only through the intended local or authenticated path.
- Confirm any Telegram bridge in use accepts only allowlisted operators.

## Rollback Runbook

Rollback must be possible without improvisation:

1. Stop the affected live process/container.
2. Preserve logs and monitor artifacts from the failed rollout window.
3. Restore the previous known-good image/config pair.
4. Re-run the startup verification checks.
5. Record the rollback reason and the exact failing commit/config pair before attempting another rollout.

## Production Signoff Checklist

Use this checklist before real-money rollout:

- [ ] Canonical container/runtime path is used (`Dockerfile_live` + `container/entrypoint.sh` + `passivbot live`)
- [ ] Secrets come from runtime injection only; no repo-local live secret material is required
- [ ] Logs and monitor output write to mounted writable paths outside the repo tree
- [ ] Monitor/dashboard surfaces are local-only or reverse-proxied with auth/TLS
- [ ] Telegram bridge, if enabled, uses fail-closed per-user allowlists
- [ ] Targeted regression bundle passes for the deployed change set
- [ ] Rust verification (`cargo check --tests`) passes for any Rust-adjacent change
- [ ] Relevant fake-live or staging rehearsal has been completed and reviewed
- [ ] Rollback steps are documented and the previous known-good revision is available
- [ ] Operators know which logs, monitor surfaces, and alerts prove the deployment is healthy

This checklist is the minimum bar for the supported production profile. If any item is not true, the deployment is outside the documented production path.
