# SCHEMA_PROVENANCE_EXEMPT: _ManifestSidePair is private stage-internal result — not a cross-repo contract.
"""Stage 0 — Manifest absence reason agreement check.

Compares the batch-mode vs live-mode manifest capture_status and error_reason
for each date x asset_group pair.

Agreement rules (from writegate Phase 2.E.3 SSOT):
- Both captured → OK
- Both empty_confirmed with same error_reason → OK (expected gap, agreed)
- Both empty_confirmed but different reasons → FLAG (reason disagreement)
- One captured, other attempted_failed → FLAG (asymmetric failure)
- One or both absent from manifest → skip (fail-open; no manifest row = not written)
- Unknown status combinations → log warning, no flag (fail-open)

Uses pipeline_mode column from the V8 manifest schema. Reads manifest via
read_availability_index() per bucket; the bucket is already scoped to one
asset_group, so no asset_group column filter is needed.

Fail-open on OSError/ValueError per workspace manifest-guard contract.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from unified_trading_library import CaptureStatus, read_availability_index

from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
)

logger = logging.getLogger(__name__)

_PIPELINE_MODE_BATCH = "batch"
_PIPELINE_MODE_LIVE = "live"


@dataclass
class _ManifestSidePair:
    """Batch vs live capture_status + error_reason for one date in one bucket."""

    date: str
    batch_status: str  # "captured" | "empty_confirmed" | "attempted_failed" | "absent"
    live_status: str
    batch_reason: str  # error_reason column; "" if not applicable
    live_reason: str


def _get_sides(manifest_df: pd.DataFrame, date_str: str) -> _ManifestSidePair:
    """Extract batch and live rows for one date from a manifest DataFrame."""
    date_rows = manifest_df[manifest_df["date"].astype(str) == date_str]

    def _extract(mode: str) -> tuple[str, str]:
        if "pipeline_mode" in manifest_df.columns:
            rows = date_rows[date_rows["pipeline_mode"] == mode]
        else:
            # Manifest pre-v8 or no pipeline_mode column — treat as absent.
            return "absent", ""
        if rows.empty:
            return "absent", ""
        status = str(rows["capture_status"].iloc[0])
        reason = str(rows["error_reason"].iloc[0]) if "error_reason" in rows.columns else ""
        return status, reason

    batch_status, batch_reason = _extract(_PIPELINE_MODE_BATCH)
    live_status, live_reason = _extract(_PIPELINE_MODE_LIVE)
    return _ManifestSidePair(
        date=date_str,
        batch_status=batch_status,
        live_status=live_status,
        batch_reason=batch_reason,
        live_reason=live_reason,
    )


def _flag_pair(pair: _ManifestSidePair) -> str | None:
    """Return a flag description string if the pair warrants flagging, else None."""
    # Either side absent from manifest — no data to compare, skip (fail-open).
    if pair.batch_status == "absent" or pair.live_status == "absent":
        return None

    # Both captured — agreement.
    if pair.batch_status == CaptureStatus.CAPTURED and pair.live_status == CaptureStatus.CAPTURED:
        return None

    # Both empty_confirmed — check reason agreement.
    if pair.batch_status == CaptureStatus.EMPTY_CONFIRMED and pair.live_status == CaptureStatus.EMPTY_CONFIRMED:
        if pair.batch_reason == pair.live_reason:
            return None  # Agreed on the same expected gap.
        return (
            f"date={pair.date}: both empty_confirmed but reasons differ "
            f"(batch={pair.batch_reason!r}, live={pair.live_reason!r})"
        )

    # One captured, other attempted_failed — asymmetric failure.
    captured = CaptureStatus.CAPTURED
    failed = CaptureStatus.ATTEMPTED_FAILED
    if (pair.batch_status == captured and pair.live_status == failed) or (
        pair.batch_status == failed and pair.live_status == captured
    ):
        return (
            f"date={pair.date}: batch={pair.batch_status!r}, live={pair.live_status!r} — "
            "one side captured while other failed"
        )

    # Either side attempted_failed (other status is not captured) — flag.
    if pair.batch_status == CaptureStatus.ATTEMPTED_FAILED or pair.live_status == CaptureStatus.ATTEMPTED_FAILED:
        return (
            f"date={pair.date}: batch={pair.batch_status!r}, live={pair.live_status!r} — "
            "attempted_failed on at least one side"
        )

    # One captured, one empty_confirmed — asymmetry but not necessarily a bug (flag as info).
    if pair.batch_status == CaptureStatus.CAPTURED and pair.live_status == CaptureStatus.EMPTY_CONFIRMED:
        return f"date={pair.date}: batch=captured but live=empty_confirmed (reason={pair.live_reason!r})"
    if pair.batch_status == CaptureStatus.EMPTY_CONFIRMED and pair.live_status == CaptureStatus.CAPTURED:
        return f"date={pair.date}: live=captured but batch=empty_confirmed (reason={pair.batch_reason!r})"

    # Unknown combinations — log but don't flag.
    logger.warning(
        "manifest_reason_check: unexpected status pair for %s — batch=%s live=%s (no flag, fail-open)",
        pair.date,
        pair.batch_status,
        pair.live_status,
    )
    return None


def check_manifest_reason_agreement(
    *,
    bucket: str,
    dates: list[str],
    asset_group: str,
) -> list[DeviationRecord]:
    """Compare batch vs live manifest capture_status and error_reason for a date range.

    Parameters
    ----------
    bucket:
        GCS bucket name for the upstream availability manifest.
    dates:
        ISO date strings (YYYY-MM-DD) to compare.
    asset_group:
        Asset group label for DeviationRecord descriptions.

    Returns
    -------
    list[DeviationRecord]
        One record per flagged date pair. Empty list = all dates agree.
        Returns empty list on manifest read errors (fail-open).
    """
    try:
        manifest = read_availability_index(bucket)
    except (OSError, ValueError) as exc:
        logger.warning(
            "manifest_reason_check: could not read %s — failing open: %s",
            bucket,
            exc,
        )
        return []

    if manifest.empty:
        return []

    deviations: list[DeviationRecord] = []
    for date_str in dates:
        try:
            pair = _get_sides(manifest, date_str)
            flag = _flag_pair(pair)
            if flag is not None:
                deviations.append(
                    DeviationRecord(
                        metric_name="manifest_reason_disagreement",
                        stage=ReconStage.DATA_PIPELINE_RECON,
                        actual_value=1.0,
                        threshold=0.0,
                        direction="above",
                        description=f"[{asset_group}] {flag}",
                    )
                )
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            logger.warning(
                "manifest_reason_check: error processing %s/%s — skipping: %s",
                asset_group,
                date_str,
                exc,
            )
    return deviations
