"""
CLI entry point for batch-live-reconciliation-service.

Uses ServiceBootstrap from unified-trading-library to handle all
infrastructure boilerplate. The ReconcileHandler runs the T+1
batch-live reconciliation pipeline.

Usage:
    python -m batch_live_reconciliation_service --operation reconcile --mode batch \
        --start-date 2026-03-09 --end-date 2026-03-09
    python -m batch_live_reconciliation_service --operation reconcile --mode batch \
        --start-date 2026-03-09 --end-date 2026-03-09 --dry-run
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import BaseModeHandler, ServiceBootstrap, UnifiedServiceHandler

from batch_live_reconciliation_service.cli.handlers.reconcile_handler import ReconcileHandler
from batch_live_reconciliation_service.engine.mock_data_provider import run_mock_pipeline

logger = logging.getLogger(__name__)


def _add_recon_args(parser: argparse.ArgumentParser) -> None:
    """Add reconciliation-specific CLI arguments."""
    _ = parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )


_OPERATIONS: dict[str, type[BaseModeHandler] | type[UnifiedServiceHandler]] = {
    "reconcile": ReconcileHandler,
}


def _get_mock_pipeline() -> int:
    return run_mock_pipeline()


def main() -> None:
    """Main CLI entry point -- ServiceBootstrap handles all infrastructure.

    SERVICE_EVENT: STARTED
    SERVICE_EVENT: STOPPED
    SERVICE_EVENT: FAILED
    """
    ServiceBootstrap(
        service_name="batch-live-reconciliation-service",
        operations=_OPERATIONS,
        config={},
        modes=["batch"],
        description=("T+1 Batch-Live Reconciliation -- nightly pipeline replay and deviation analysis"),
        add_asset_group_arg=False,
        extra_args_fn=_add_recon_args,
        mock_pipeline_fn=_get_mock_pipeline,
    ).run()


if __name__ == "__main__":
    main()
