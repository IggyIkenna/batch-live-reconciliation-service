# GCS Paths — batch-live-reconciliation-service

## Inputs (Read-Only)

```
gs://<execution-store-bucket>/
  configs/snapshots/{date}/config.json   ← frozen EOD config (written by execution-service at 00:30 UTC)

gs://<events-bucket>/
  live/events/{date}/ml-inference-service/    ← archived live ML events
  live/events/{date}/strategy-service/        ← archived live strategy events
  live/events/{date}/execution-service/       ← archived live execution events
  t1-recon/events/{date}/ml-inference-service/    ← batch replay ML events
  t1-recon/events/{date}/strategy-service/        ← batch replay strategy events
  t1-recon/events/{date}/execution-service/       ← batch replay execution events
```

## Outputs (Written by This Service)

```
gs://<recon-bucket>/
  t1-recon/recon/
    ml_recon_report_{date}.json        ← Stage 1 deviation report
    strategy_recon_report_{date}.json  ← Stage 2 deviation report
    execution_recon_report_{date}.json ← Stage 3 deviation report
    agent_report_{date}.md             ← Stage 4 agent analysis
    summary_{date}.json                ← Stage 5 consolidated summary
    index.json                         ← Cumulative index of all recon runs (appended)
```

## Namespace Rules

- This service NEVER writes to `batch/` or `live/` GCS prefixes.
- All outputs go to `t1-recon/recon/` only.
- Inputs are read from `live/events/` and `t1-recon/events/` (written by upstream batch services with `--run-tag t1-recon`).

## References

- `unified-trading-pm/codex/08-workflows/t1-batch-dag.md` — full pipeline DAG, schedules, deviation thresholds
