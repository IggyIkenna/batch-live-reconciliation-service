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

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from unified_api_contracts.internal import (  # noqa: qg-deep-import
    ReconciliationAction,
    ReconciliationResolution,
)
from unified_trading_library import log_event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reconciliation", tags=["reconciliation"])


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
# Mock break data (replaced by GCS reads when stage5 results are available)
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/breaks", response_model=list[ReconciliationBreakResponse])
async def list_breaks(
    venue: str | None = None,
    break_type: str | None = None,
    status: str | None = None,
) -> list[ReconciliationBreakResponse]:
    """List reconciliation breaks with optional filters."""
    result = list(_MOCK_BREAKS)

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

    return result


@router.post("/resolve", response_model=ResolveResponse)
async def resolve_break(resolution: ReconciliationResolution) -> ResolveResponse:
    """Resolve a reconciliation break (accept/reject/investigate).

    Persists resolution to audit log for FCA compliance.
    """
    # Validate break exists
    brk = next((b for b in _MOCK_BREAKS if b.break_id == resolution.break_id), None)
    if brk is None:
        raise HTTPException(status_code=404, detail=f"Break {resolution.break_id} not found")

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
    brk = next((b for b in _MOCK_BREAKS if b.break_id == request.break_id), None)
    if brk is None:
        raise HTTPException(status_code=404, detail=f"Break {request.break_id} not found")

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
