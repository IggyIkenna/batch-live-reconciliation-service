"""
CLI entry point for batch-live-reconciliation-service.

Usage:
    python -m batch_live_reconciliation_service --date 2026-03-09
    python -m batch_live_reconciliation_service --date 2026-03-09 --dry-run
    python -m batch_live_reconciliation_service  # defaults to yesterday
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import cast

from unified_trading_library import LogLevel

from batch_live_reconciliation_service.orchestrator import run_reconciliation

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="batch-live-reconciliation-service",
        description=(
            "T+1 Batch-Live Reconciliation — nightly pipeline replay and deviation analysis"
        ),
    )
    _ = parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to reconcile (YYYY-MM-DD). Defaults to yesterday.",
    )
    _ = parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run pipeline without writing to GCS (read-only validation).",
    )
    _ = parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    # LOG_LEVEL env var validation (SSOT for log levels)
    _raw_log_level = os.environ.get("LOG_LEVEL", "INFO")  # config-bootstrap: before UCC init
    try:
        _log_level = LogLevel(_raw_log_level)
    except ValueError as err:
        valid = ", ".join(v.value for v in LogLevel)
        raise SystemExit(f"Invalid LOG_LEVEL={_raw_log_level!r}. Must be one of: {valid}") from err

    # --- MOCK MODE: use pre-generated seed data ---
    from batch_live_reconciliation_service.config import ReconConfig

    if ReconConfig().is_mock_mode():
        from batch_live_reconciliation_service.engine.mock_data_provider import run_mock_pipeline

        logger.info("MOCK MODE: redirecting to mock pipeline")
        return run_mock_pipeline()

    parser = _build_parser()
    args = parser.parse_args(argv)

    log_level: str = cast(str, args.log_level)
    logging.basicConfig(
        level=cast(int, getattr(logging, log_level)),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    date_arg: str | None = cast("str | None", args.date)
    dry_run: bool = cast(bool, args.dry_run)

    if date_arg:
        _ = datetime.strptime(date_arg, "%Y-%m-%d")  # validate format
        date = date_arg
    else:
        date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()

    logger.info("Starting reconciliation: date=%s dry_run=%s", date, dry_run)

    report = run_reconciliation(date=date, dry_run=dry_run)

    if report.status.value == "passed":
        logger.info("Reconciliation PASSED — %d total deviations", report.total_deviations)
        return 0
    else:
        logger.error(
            "Reconciliation FAILED — %d deviations, failed stages: %s",
            report.total_deviations,
            [s.value for s in report.failed_stages],
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
