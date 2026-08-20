"""FastAPI endpoints for reconciliation break resolution.

Provides endpoints for the UI to:
1. List reconciliation breaks with filters
2. Resolve breaks (accept/reject/investigate)
3. Generate pre-filled manual booking requests for corrections

# SCHEMA_PROVENANCE_EXEMPT — API-layer response/request shapes (CORRECT-LOCAL).
# ReconciliationBreakResponse, ResolveResponse, BookCorrectionResponse, BookCorrectionRequest
# are UI-facing API contracts for this service only, not cross-service domain types.
"""

from __future__ import annotations

import json
import logging
from typing import cast

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal import (  # noqa: qg-deep-import
    ReconciliationAction,
    ReconciliationResolution,
)
from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.api.resolution_state import (
    DeltaExclusion,
    ExclusionScope,
    PauseRequiredError,
    ResolutionStateStore,
    TradingPause,
)
from batch_live_reconciliation_service.config import get_recon_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/t1-recon", tags=["reconciliation"])


# ---------------------------------------------------------------------------
# Response schemas (CORRECT-LOCAL — API-layer only)
# ---------------------------------------------------------------------------


class ReconciliationBreakResponse(BaseModel):  # CORRECT-LOCAL
    """A single reconciliation break for the UI."""

    break_id: str
    date: str
    venue: str
    break_type: str
    instrument_id: str
    live_value: float
    batch_value: float
    delta: float
    status: str
    detected_at: str


class ResolveResponse(BaseModel):  # CORRECT-LOCAL
    """Response after resolving a break."""

    break_id: str
    action: str
    status: str
    message: str


class BookCorrectionResponse(BaseModel):  # CORRECT-LOCAL
    """Pre-filled manual instruction request for correcting a break."""

    venue: str
    instrument_id: str
    side: str
    quantity: float
    execution_mode: str = "record_only"
    reason: str
    category: str = ""
    source_reference: str = ""


class BookCorrectionRequest(BaseModel):  # CORRECT-LOCAL
    """Request to generate a correction booking from a break."""

    break_id: str = Field(..., description="ID of the break to correct")


# ---------------------------------------------------------------------------
# Stage-5 GCS reads (G1 — wire the resolution surface to real recon output)
# ---------------------------------------------------------------------------
#
# BLRS deviations are stage/metric-level aggregate threshold breaches (e.g.
# "alpha_pnl_gap 3.2% > 2% of notional"), not per-venue/per-position breaks —
# there is no raw live-vs-batch value pair per deviation, only the observed
# metric (``actual_value``) vs its tolerance (``threshold``). We map those
# onto the UI-facing ``live_value``/``batch_value``/``delta`` fields as the
# closest honest fit; ``venue`` is not populated by any stage today (a T+1
# pipeline/strategy/execution audit is cross-venue), so it reads "ALL".


def _load_index(bucket: str) -> list[dict[str, object]]:
    """Load the recon run index (most-recent-first) from GCS, or [] if absent."""
    client = get_storage_client()
    try:
        raw = client.download_bytes(bucket=bucket, blob_path="t1-recon/recon/index.json")
        return cast(list[dict[str, object]], json.loads(raw.decode("utf-8")))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError):
        return []


def _load_summary(bucket: str, date: str) -> dict[str, object] | None:
    """Load one date's consolidated Stage-5 summary from GCS, or None if absent."""
    client = get_storage_client()
    try:
        raw = client.download_bytes(bucket=bucket, blob_path=f"t1-recon/recon/summary_{date}.json")
        return cast(dict[str, object], json.loads(raw.decode("utf-8")))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError):
        return None


def _break_id(date: str, stage: str, metric_name: str, instrument_id: str | None) -> str:
    return f"{date}:{stage}:{metric_name}:{instrument_id or 'AGGREGATE'}"


def _breaks_from_summary(summary: dict[str, object]) -> list[ReconciliationBreakResponse]:
    """Flatten every stage's deviations in a Stage-5 summary into UI-facing breaks."""
    date = str(summary.get("date", ""))  # noqa: qg-empty-fallback  # writer-populated; empty reads as unknown-date
    raw_stages = summary.get("stages", [])  # noqa: qg-empty-fallback  # a stage-less summary has zero breaks to surface
    stages = cast(list[dict[str, object]], raw_stages)
    breaks: list[ReconciliationBreakResponse] = []
    for stage_entry in stages:
        stage_name = str(stage_entry.get("stage", ""))  # noqa: qg-empty-fallback  # writer-populated; never absent
        raw_deviations = stage_entry.get("deviations", [])  # noqa: qg-empty-fallback  # a PASSED stage has zero
        deviations = cast(list[dict[str, object]], raw_deviations)
        for dev in deviations:
            metric_name = str(dev.get("metric_name", ""))  # noqa: qg-empty-fallback  # writer-populated; never absent
            instrument_id = cast("str | None", dev.get("instrument_id"))
            actual_value = float(cast(float, dev.get("actual_value", 0.0)))
            threshold = float(cast(float, dev.get("threshold", 0.0)))
            breaks.append(
                ReconciliationBreakResponse(
                    break_id=_break_id(date, stage_name, metric_name, instrument_id),
                    date=date,
                    venue="ALL",
                    break_type=metric_name,
                    instrument_id=instrument_id or "AGGREGATE",
                    live_value=actual_value,
                    batch_value=threshold,
                    delta=actual_value - threshold,
                    status="pending",
                    detected_at=str(dev.get("first_seen_at", "")),  # noqa: qg-empty-fallback  # writer-populated
                )
            )
    return breaks


def _current_breaks() -> list[ReconciliationBreakResponse]:
    """The breaks the resolution surface currently offers.

    Reads the latest Stage-5 GCS summary (real recon output) when one
    exists. Falls back to the illustrative mock set only when NO run has
    ever produced a summary — a genuine "recon ran, zero deviations today"
    result stays a real (possibly empty) list, it never masks into the mock.
    """
    config = get_recon_config()
    index = _load_index(config.recon_bucket)
    if not index:
        return list(_MOCK_BREAKS)
    latest_date = str(index[0].get("date", ""))  # noqa: qg-empty-fallback  # empty → treated as no-summary below
    summary = _load_summary(config.recon_bucket, latest_date) if latest_date else None
    if summary is None:
        return list(_MOCK_BREAKS)
    return _breaks_from_summary(summary)


# ---------------------------------------------------------------------------
# Mock break data — pre-activation fallback (see _current_breaks above)
# ---------------------------------------------------------------------------

_MOCK_BREAKS: list[ReconciliationBreakResponse] = [
    ReconciliationBreakResponse(
        break_id="BRK-001",
        date="2026-03-22",
        venue="Binance",
        break_type="position",
        instrument_id="BTC-USDT",
        live_value=1.2045,
        batch_value=1.2000,
        delta=0.0045,
        status="pending",
        detected_at="2026-03-22T14:32:00Z",
    ),
    ReconciliationBreakResponse(
        break_id="BRK-002",
        date="2026-03-22",
        venue="Deribit",
        break_type="pnl",
        instrument_id="ETH-USD-PERP",
        live_value=34521.80,
        batch_value=34200.00,
        delta=321.80,
        status="pending",
        detected_at="2026-03-22T13:18:00Z",
    ),
    ReconciliationBreakResponse(
        break_id="BRK-003",
        date="2026-03-21",
        venue="OKX",
        break_type="fee",
        instrument_id="SOL-USDT",
        live_value=12.45,
        batch_value=11.90,
        delta=0.55,
        status="resolved",
        detected_at="2026-03-21T22:00:00Z",
    ),
]

# In-memory resolution store (replaced by GCS persistence in production)
_resolutions: dict[str, ReconciliationResolution] = {}

# Operator state: pauses + delta exclusions + their soft-delete audit trail (W12).
# Pauses and VIRTUAL exclusions are process-local; PERSISTENT exclusions live in
# GCS — see resolution_state's module docstring for why that split IS the feature.
_state = ResolutionStateStore()


def get_state_store() -> ResolutionStateStore:
    """Return the module-level operator-state store."""
    return _state


class PauseRequest(BaseModel):  # CORRECT-LOCAL
    """Pause automated trading on a break's instrument before booking a correction."""

    break_id: str = Field(..., description="Break whose instrument should be paused")
    reason: str = Field(..., min_length=10, description="Why trading is being paused")
    actor: str = Field(..., description="Operator performing the pause")


class RevokeRequest(BaseModel):  # CORRECT-LOCAL
    """Revoke a pause or an exclusion. Soft-delete — the record is retained."""

    break_id: str = Field(..., description="Break the pause/exclusion was recorded against")
    reason: str = Field(..., min_length=10, description="Why it is being revoked")
    actor: str = Field(..., description="Operator performing the revoke")


class ExcludeRequest(BaseModel):  # CORRECT-LOCAL
    """Exclude a break's delta from reconciliation."""

    break_id: str = Field(..., description="Break to exclude")
    scope: ExclusionScope = Field(..., description="virtual = this run only; persistent = until revoked")
    reason: str = Field(..., min_length=10, description="Why the delta is excluded")
    actor: str = Field(..., description="Operator performing the exclusion")


class PauseView(BaseModel):  # CORRECT-LOCAL
    """One pause record, active or revoked (audit view)."""

    venue: str
    instrument_id: str
    break_id: str
    reason: str
    paused_by: str
    paused_at: str
    active: bool
    revoked_at: str | None = None
    revoked_by: str | None = None
    revoke_reason: str | None = None


class ExclusionView(BaseModel):  # CORRECT-LOCAL
    """One exclusion record, active or revoked (audit view)."""

    break_id: str
    scope: str
    reason: str
    excluded_by: str
    excluded_at: str
    run_date: str | None = None
    active: bool
    revoked_at: str | None = None
    revoked_by: str | None = None
    revoke_reason: str | None = None


def _pause_view(entry: TradingPause) -> PauseView:
    return PauseView(
        venue=entry.venue,
        instrument_id=entry.instrument_id,
        break_id=entry.break_id,
        reason=entry.reason,
        paused_by=entry.paused_by,
        paused_at=entry.paused_at.isoformat(),
        active=entry.is_active,
        revoked_at=entry.revoked_at.isoformat() if entry.revoked_at else None,
        revoked_by=entry.revoked_by,
        revoke_reason=entry.revoke_reason,
    )


def _exclusion_view(entry: DeltaExclusion) -> ExclusionView:
    return ExclusionView(
        break_id=entry.break_id,
        scope=entry.scope.value,
        reason=entry.reason,
        excluded_by=entry.excluded_by,
        excluded_at=entry.excluded_at.isoformat(),
        run_date=entry.run_date,
        active=entry.is_active,
        revoked_at=entry.revoked_at.isoformat() if entry.revoked_at else None,
        revoked_by=entry.revoked_by,
        revoke_reason=entry.revoke_reason,
    )


def _require_break(break_id: str) -> ReconciliationBreakResponse:
    brk = next((b for b in _current_breaks() if b.break_id == break_id), None)
    if brk is None:
        raise HTTPException(status_code=404, detail=f"Break {break_id} not found")
    return brk


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/breaks", response_model=list[ReconciliationBreakResponse])
async def list_breaks(
    venue: str | None = None,
    break_type: str | None = None,
    status: str | None = None,
    include_excluded: bool = False,
) -> list[ReconciliationBreakResponse]:
    """List reconciliation breaks with optional filters.

    Breaks with an active delta exclusion are hidden by default — that is what
    excluding one is for. ``include_excluded=true`` shows them anyway (marked
    ``status="excluded"``) so an operator can audit what is being suppressed
    without having to read the exclusion list separately.
    """
    result = _current_breaks()

    if venue:
        result = [b for b in result if b.venue.lower() == venue.lower()]
    if break_type:
        result = [b for b in result if b.break_type == break_type]
    if status:
        result = [b for b in result if b.status == status]

    # Apply resolutions
    for brk in result:
        if brk.break_id in _resolutions:
            brk.status = _resolutions[brk.break_id].action.value

    # W12 delta exclusion. Evaluated per break because each carries its own run
    # date, and a VIRTUAL exclusion is scoped to exactly one of them.
    bucket = get_recon_config().recon_bucket
    kept: list[ReconciliationBreakResponse] = []
    for brk in result:
        excluded = brk.break_id in _state.excluded_break_ids(run_date=brk.date, bucket=bucket)
        if not excluded:
            kept.append(brk)
        elif include_excluded:
            brk.status = "excluded"
            kept.append(brk)
    return kept


@router.post("/resolve", response_model=ResolveResponse)
async def resolve_break(resolution: ReconciliationResolution) -> ResolveResponse:
    """Resolve a reconciliation break (accept/reject/investigate).

    Persists resolution to audit log for FCA compliance.
    """
    _ = _require_break(resolution.break_id)
    _resolutions[resolution.break_id] = resolution

    log_event(
        "RECONCILIATION_BREAK_RESOLVED",
        details={
            "break_id": resolution.break_id,
            "action": resolution.action.value,
            "resolved_by": resolution.resolved_by,
            "note": resolution.note,
            "correcting_instruction_id": resolution.correcting_instruction_id or "",
        },
    )

    logger.info(
        "Break %s resolved: action=%s by=%s",
        resolution.break_id,
        resolution.action.value,
        resolution.resolved_by,
    )

    action_messages: dict[str, str] = {
        ReconciliationAction.ACCEPT: "Break accepted as expected divergence",
        ReconciliationAction.REJECT: "Break rejected — correction required",
        ReconciliationAction.INVESTIGATE: "Break flagged for investigation",
    }

    return ResolveResponse(
        break_id=resolution.break_id,
        action=resolution.action.value,
        status="resolved" if resolution.action != ReconciliationAction.INVESTIGATE else "investigating",
        message=action_messages.get(resolution.action, "Break resolved"),
    )


@router.post("/book-correction", response_model=BookCorrectionResponse)
async def book_correction(request: BookCorrectionRequest) -> BookCorrectionResponse:
    """Generate a pre-filled manual instruction request for correcting a break.

    Returns the parameters needed to navigate to the back-office booking page
    with the form pre-filled for the correction.
    """
    brk = _require_break(request.break_id)

    # W12 pause-before-manual-entry: refuse to hand out a booking while automation
    # is still trading this instrument. A correction booked against a position
    # automation also believes it owns can double-apply.
    try:
        _state.require_pause(brk.venue, brk.instrument_id)
    except PauseRequiredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # Determine side based on delta direction
    side = "BUY" if brk.delta > 0 else "SELL"

    return BookCorrectionResponse(
        venue=brk.venue,
        instrument_id=brk.instrument_id,
        side=side,
        quantity=abs(brk.delta),
        execution_mode="record_only",
        reason=(
            f"Correction for reconciliation break {request.break_id}: "
            f"{brk.break_type} delta {brk.delta} on {brk.venue} {brk.instrument_id}"
        ),
        source_reference=request.break_id,
    )


# ---------------------------------------------------------------------------
# W12 — pauses, delta exclusions, and their soft-delete audit trail
# ---------------------------------------------------------------------------


@router.post("/pause", response_model=PauseView)
async def pause_trading(request: PauseRequest) -> PauseView:
    """Pause automated trading on a break's instrument.

    Required before `/book-correction` will hand out a manual booking.
    """
    brk = _require_break(request.break_id)
    entry = _state.pause(
        venue=brk.venue,
        instrument_id=brk.instrument_id,
        break_id=brk.break_id,
        reason=request.reason,
        paused_by=request.actor,
    )
    return _pause_view(entry)


@router.post("/pause/revoke", response_model=PauseView)
async def revoke_pause(request: RevokeRequest) -> PauseView:
    """Lift a pause. Soft-delete — the record stays in the audit trail."""
    brk = _require_break(request.break_id)
    revoked = _state.revoke_pause(
        venue=brk.venue,
        instrument_id=brk.instrument_id,
        revoked_by=request.actor,
        reason=request.reason,
    )
    if revoked is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active pause for {brk.venue}/{brk.instrument_id} to revoke",
        )
    return _pause_view(revoked)


@router.get("/pauses", response_model=list[PauseView])
async def list_pauses(active_only: bool = False) -> list[PauseView]:
    """Every pause ever recorded. Defaults to the full audit view, revoked included."""
    entries = _state.all_pauses()
    if active_only:
        entries = tuple(e for e in entries if e.is_active)
    return [_pause_view(e) for e in entries]


@router.post("/exclusions", response_model=ExclusionView)
async def exclude_delta(request: ExcludeRequest) -> ExclusionView:
    """Exclude a break's delta from reconciliation.

    `virtual` applies to that break's own run date only — a later run re-raises
    it. `persistent` applies to every run until revoked and is written to GCS,
    so it survives a restart.
    """
    brk = _require_break(request.break_id)
    config = get_recon_config()
    try:
        entry = _state.exclude(
            break_id=brk.break_id,
            scope=request.scope,
            reason=request.reason,
            excluded_by=request.actor,
            run_date=brk.date,
            bucket=config.recon_bucket,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _exclusion_view(entry)


@router.post("/exclusions/revoke", response_model=ExclusionView)
async def revoke_exclusion(request: RevokeRequest) -> ExclusionView:
    """Revoke an exclusion so the break is raised again. Soft-delete."""
    config = get_recon_config()
    revoked = _state.revoke_exclusion(
        break_id=request.break_id,
        revoked_by=request.actor,
        reason=request.reason,
        bucket=config.recon_bucket,
    )
    if revoked is None:
        raise HTTPException(status_code=404, detail=f"No active exclusion for break {request.break_id}")
    return _exclusion_view(revoked)


@router.get("/exclusions", response_model=list[ExclusionView])
async def list_exclusions(active_only: bool = False) -> list[ExclusionView]:
    """Every exclusion ever recorded. Defaults to the full audit view."""
    config = get_recon_config()
    entries = _state.all_exclusions(config.recon_bucket)
    if active_only:
        entries = tuple(e for e in entries if e.is_active)
    return [_exclusion_view(e) for e in entries]
