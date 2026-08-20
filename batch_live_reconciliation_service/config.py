"""
Service configuration for batch-live-reconciliation-service.

Uses UnifiedCloudConfig (no os.getenv() per workspace standards).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal, cast, override

from unified_trading_library import CloudProvider, UnifiedCloudConfig, get_cloud_provider, resolve_bucket_name


class ReconConfig(UnifiedCloudConfig):
    """Configuration for batch-live-reconciliation-service."""

    # GCS bucket for recon outputs (summaries, agent reports, index)
    recon_bucket: str = ""

    # GCS bucket for events (live events archived here; batch events written here)
    events_bucket: str = ""

    # GCS bucket for execution config snapshots
    execution_store_bucket: str = ""

    # GCS buckets for data pipeline reconciliation (per-category)
    instruments_bucket_cefi: str = ""
    instruments_bucket_tradfi: str = ""
    instruments_bucket_defi: str = ""
    market_data_tick_bucket_cefi: str = ""
    market_data_tick_bucket_tradfi: str = ""
    market_data_tick_bucket_defi: str = ""

    # Strategy-service base URL — canonical position/PnL baseline for Stage 2
    # (GET {url}/api/v1/accounts/{account_id}/pnl-series). When unset, Stage 2
    # falls back to the raw GCS strategy-service event archives.
    strategy_service_url: str = ""

    # Account id used to query the strategy-service pnl-series baseline in Stage 2.
    strategy_account_id: str = "odum-live"

    # Alerting-service base URL — POST target for the daily T+1 determinism recon
    # verdict (P6). The recon alert client posts a UAC AlertEvent JSON to
    # {url}/api/v1/alerts/rules/recent. When unset, the verdict is logged only
    # (no HTTP post). NEVER an import of alerting-service (service↔service ban).
    alerting_service_url: str = ""

    # Slack channel target for the daily T+1 recon verdict (P6).
    recon_alert_channel: str = "#uts-live-alerts"

    # Ledger roots for the daily-determinism stage (P7.1-B). The paper run's
    # ledger_root (run A, the reference) and the batch rerun's ledger_root
    # (run B); each holds its run_manifest.json + InstructionLedger JSONL. The
    # cron sets these from the prior day's paper run + its batch rerun. The
    # handler reads each RunManifest from these roots, runs reconcile_day, and
    # POSTs the DETERMINISM verdict. Empty → the stage logs "no run configured".
    paper_ledger_root: str = ""
    batch_ledger_root: str = ""

    # Cloud Run Job timeout per stage (seconds)
    stage_timeout_seconds: int = 1800  # 30 minutes

    # Whether to skip writing to GCS (dry-run mode)
    dry_run: bool = False

    # Soak mode — during a paper/live soak window, recon deviations are still
    # detected + reported but CRITICAL escalation is suppressed (downgraded to a
    # non-paging routing). The CRITICAL-suppression counterpart lives in
    # alerting-service; this flag is the producer-side signal carried on emitted
    # recon-drift events.
    soak_mode: bool = False

    # T+1 batch/live TTL (engine/live_ttl.py) — the two config knobs the plan's
    # "T+1 batch/live reconciliation + live TTL" tranche calls for. See
    # engine/live_ttl.LiveTtlConfig for the full contract; these mirror its
    # defaults so callers building a LiveTtlConfig from service config get the
    # same sensible, non-blocking defaults (exact-match tolerance, 30-day grace).
    live_ttl_reconciliation_tolerance: float = 0.0
    live_ttl_horizon_days: int = 30

    def _derive_cross_cutting_buckets(self, cloud: CloudProvider) -> None:
        """Derive recon/events/execution-store buckets (kept out of model_post_init for size).

        recon: env-tiered kind added 2026-07-13 per
        plans/active/issues/recon_bucket_missing_nightly_recon_failing_2026_07_13.md — the
        prior hand-rolled f"recon-{project_id}" bucket never existed (nightly Cloud Run job
        failed 55/56 runs). Resolves to recon-{env}-{pid} (e.g. recon-prd-...).
        events / execution-store: same resulting names as the prior f-strings, now
        SSOT-resolved via resolve_bucket_name (no inline gs://).
        Local/CI mode keeps the plain f-string fallback so local unit tests
        (test_config.py) stay deterministic without depending on cloud-providers.yaml.
        """
        project_id = self.gcp_project_id or ""
        if cloud == CloudProvider.LOCAL:
            if not self.recon_bucket:
                self.recon_bucket = f"recon-{project_id}"
            if not self.events_bucket:
                self.events_bucket = f"{project_id}-events"
            if not self.execution_store_bucket:
                self.execution_store_bucket = f"execution-store-cefi-{project_id}"
            return
        cloud_name = cast(Literal["gcp", "aws"], cloud)
        if not self.recon_bucket:
            self.recon_bucket = resolve_bucket_name(cloud=cloud_name, kind="recon")
        if not self.events_bucket:
            self.events_bucket = resolve_bucket_name(cloud=cloud_name, kind="events")
        if not self.execution_store_bucket:
            self.execution_store_bucket = resolve_bucket_name(
                cloud=cloud_name, kind="execution-store", asset_group="cefi"
            )

    @override
    def model_post_init(self, __context: object) -> None:
        """Derive bucket names from project_id if not set."""
        cloud = get_cloud_provider()
        self._derive_cross_cutting_buckets(cloud)
        # Data pipeline buckets — resolve via resolve_bucket_name (bucket-name SSOT).
        # Skip in local mode: resolve_bucket_name only accepts "gcp" / "aws".
        if cloud != CloudProvider.LOCAL:
            cloud_name = cast(Literal["gcp", "aws"], cloud)
            if not self.instruments_bucket_cefi:
                self.instruments_bucket_cefi = resolve_bucket_name(
                    cloud=cloud_name, kind="instruments-store", asset_group="cefi"
                )
            if not self.instruments_bucket_tradfi:
                self.instruments_bucket_tradfi = resolve_bucket_name(
                    cloud=cloud_name, kind="instruments-store", asset_group="tradfi"
                )
            if not self.instruments_bucket_defi:
                self.instruments_bucket_defi = resolve_bucket_name(
                    cloud=cloud_name, kind="instruments-store", asset_group="defi"
                )
            if not self.market_data_tick_bucket_cefi:
                self.market_data_tick_bucket_cefi = resolve_bucket_name(
                    cloud=cloud_name, kind="market-data", asset_group="cefi"
                )
            if not self.market_data_tick_bucket_tradfi:
                self.market_data_tick_bucket_tradfi = resolve_bucket_name(
                    cloud=cloud_name, kind="market-data", asset_group="tradfi"
                )
            if not self.market_data_tick_bucket_defi:
                self.market_data_tick_bucket_defi = resolve_bucket_name(
                    cloud=cloud_name, kind="market-data", asset_group="defi"
                )


@lru_cache(maxsize=1)
def get_recon_config() -> ReconConfig:
    """Return singleton recon config."""
    return ReconConfig()
