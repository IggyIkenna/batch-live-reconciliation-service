"""Build + POST the daily T+1 recon verdict as a UAC ``AlertEvent`` (P6).

The determinism stage produces ONE ``DailyReconReport`` per T+1 trading day.
This module maps that report to a UAC ``AlertEvent`` and POSTs it to
alerting-service over HTTP — alerting-service routes it to the
``#uts-live-alerts`` Slack channel.

Severity policy (DETERMINISM verdict):
  - ε=0 (``is_deterministic=True``) → INFO (paper≡batch held; carries the
    execution-alpha summary rollup).
  - a determinism bug (``is_deterministic=False`` on a DETERMINISM verdict) →
    CRITICAL (paper≠batch is ALWAYS a bug — non-determinism / input-capture gap
    / fill-model drift).

This is a typed HTTP client (aiohttp) — there is NO cross-service Python import
of alerting-service (the T4 service↔service ban). The contract is the UAC
``AlertEvent`` schema; alerting-service is reached only over HTTP.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Literal

import aiohttp
from unified_api_contracts.alerting import AlertCode
from unified_api_contracts.internal import AlertEvent, DailyReconReport, ReconVerdictType

logger = logging.getLogger(__name__)

_ALERTS_INGRESS_PATH = "/api/v1/alerts/rules/recent"
_RULE_ID = "DAILY_T1_DETERMINISM_RECON"
_POST_TIMEOUT_SECONDS = 15

# Severity literal accepted by AlertEvent.severity.
_Severity = Literal["DEBUG", "INFO", "WARNING", "CRITICAL", "FATAL"]


def build_recon_alert_event(report: DailyReconReport, *, channel: str) -> AlertEvent:
    """Map a ``DailyReconReport`` to a UAC ``AlertEvent``.

    INFO on an ε=0 determinism pass (with the execution-alpha summary);
    CRITICAL on a determinism bug (``is_deterministic=False`` on a DETERMINISM
    verdict).

    Args:
        report: The daily T+1 reconciliation report.
        channel: Slack channel target (e.g. ``#uts-live-alerts``), surfaced in
            the message for alerting-service routing context.

    Returns:
        A populated ``AlertEvent`` ready to POST to alerting-service.
    """
    determinism_bug = report.verdict_type == ReconVerdictType.DETERMINISM and not report.is_deterministic
    severity: _Severity = "CRITICAL" if determinism_bug else "INFO"

    if determinism_bug:
        message = (
            f"[{channel}] DETERMINISM BUG ({report.determinism_bug_class.value}): "
            f"paper run {report.run_a_id} ≠ batch run {report.run_b_id} for "
            f"[{report.window_start.date()}]. matched={report.matched} "
            f"unmatched_a={report.unmatched_a} unmatched_b={report.unmatched_b} "
            f"mean_delta={report.mean_fill_price_delta_bps}bps p99={report.p99_fill_price_delta_bps}bps. "
            f"paper≡batch is a PROOF (ε=0) — any diff is a bug, not tolerance."
        )
    else:
        message = (
            f"[{channel}] T+1 recon GREEN ({report.verdict_type.value}): "
            f"runs {report.run_a_id} vs {report.run_b_id} for [{report.window_start.date()}]. "
            f"matched={report.matched} trades, ε=0 determinism held. "
            f"execution-alpha mean={report.mean_fill_price_delta_bps}bps p99={report.p99_fill_price_delta_bps}bps."
        )

    return AlertEvent(
        alert_id=str(uuid.uuid4()),
        rule_id=_RULE_ID,
        triggered_at=datetime.now(UTC),
        severity=severity,
        message=message,
        metric_value=float(report.mean_fill_price_delta_bps),
        threshold=0.0,  # DETERMINISM verdict: ε=0 is the only passing value.
        code=AlertCode.BATCH_VS_LIVE_RECON_DRIFTED,
    )


async def post_recon_alert(
    report: DailyReconReport,
    *,
    alerting_service_url: str,
    channel: str,
    session: aiohttp.ClientSession | None = None,
) -> AlertEvent | None:
    """Build + POST the daily recon verdict ``AlertEvent`` to alerting-service.

    Args:
        report: The daily T+1 reconciliation report.
        alerting_service_url: Alerting-service base URL. Empty string → no POST
            (the verdict is logged only).
        channel: Slack channel target for the verdict.
        session: Optional caller-owned aiohttp session (else one is created).

    Returns:
        The posted ``AlertEvent`` on success, else ``None`` (no URL configured).
    """
    event = build_recon_alert_event(report, channel=channel)

    if not alerting_service_url:
        logger.warning(
            "[recon-alert] alerting_service_url unset — verdict NOT posted (severity=%s): %s",
            event.severity,
            event.message,
        )
        return None

    url = f"{alerting_service_url.rstrip('/')}{_ALERTS_INGRESS_PATH}"
    payload = event.model_dump(mode="json")
    timeout = aiohttp.ClientTimeout(total=_POST_TIMEOUT_SECONDS)

    own_session = session is None
    http = session if session is not None else aiohttp.ClientSession(timeout=timeout)
    try:
        async with http.post(url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            logger.info("[recon-alert] posted %s verdict to %s (status=%d)", event.severity, url, resp.status)
    finally:
        if own_session:
            await http.close()

    return event
