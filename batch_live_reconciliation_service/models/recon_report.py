"""
Pydantic models for reconciliation reports.

No Any types per workspace standards.

# SCHEMA_PROVENANCE_EXEMPT — service-internal pipeline models (CORRECT-LOCAL).
# ReconReport, StageReport, DeviationRecord are not cross-service contracts;
# they are the private result types for this service's T+1 pipeline.
# Cross-service contract (resolution workflow) lives in unified-internal-contracts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field
from unified_api_contracts.internal.reconciliation import ReconciliationAgeFields, ReconciliationDimension


class ReconStage(StrEnum):
    """T+1 reconciliation pipeline stages."""

    CONFIG_PULL = "config_pull"
    DATA_PIPELINE_RECON = "data_pipeline_recon"
    ML_RECON = "ml_recon"
    STRATEGY_RECON = "strategy_recon"
    EXECUTION_RECON = "execution_recon"
    PAPER_LIVE_RECON = "paper_live_recon"
    BATCH_PAPER_RECON = "batch_paper_recon"
    AGENT_ANALYSIS = "agent_analysis"
    RESULTS_WRITER = "results_writer"


class FailureRoutingAction(StrEnum):
    """Closed-set policy for paper-vs-live / batch-vs-paper deviation failures.

    pvl-p21a: alert is always emitted; escalation determines automatic system response.
    """

    ALERT = "alert"
    AUTO_PAUSE_LIVE = "auto_pause_live"
    AUTO_DEMOTE_TO_PAPER = "auto_demote_to_paper"


class ReconStatus(StrEnum):
    """Status of a reconciliation run or stage."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class DeviationRecord(ReconciliationAgeFields):  # CORRECT-LOCAL — service-internal recon report, not a domain contract
    """A single metric deviation above threshold.

    Inherits ``ReconciliationAgeFields`` for age-tracking (P0.4) and carries
    ``dimension: ReconciliationDimension`` for 12-dimension tagging (P0.6).
    All age fields are populated at row-creation time, never at query time.
    """

    metric_name: str
    stage: ReconStage
    actual_value: float
    threshold: float
    direction: str  # "above" | "below"
    description: str
    instrument_id: str | None = None
    dimension: ReconciliationDimension

    @classmethod
    def new(
        cls,
        *,
        metric_name: str,
        stage: ReconStage,
        actual_value: float,
        threshold: float,
        direction: str,
        description: str,
        dimension: ReconciliationDimension,
        instrument_id: str | None = None,
        first_seen_at: datetime | None = None,
        last_seen_at: datetime | None = None,
        unreconciled_age_seconds: int | None = None,
    ) -> DeviationRecord:
        """Factory that auto-populates age fields at write-time (P0.4 compliance).

        Callers MUST supply ``dimension`` explicitly per the 12-dimension tagging
        requirement (P0.6). Age fields default to now / 0 seconds if not supplied.
        """
        now = datetime.now(UTC)
        return cls(
            metric_name=metric_name,
            stage=stage,
            actual_value=actual_value,
            threshold=threshold,
            direction=direction,
            description=description,
            dimension=dimension,
            instrument_id=instrument_id,
            first_seen_at=first_seen_at if first_seen_at is not None else now,
            last_seen_at=last_seen_at if last_seen_at is not None else now,
            unreconciled_age_seconds=unreconciled_age_seconds if unreconciled_age_seconds is not None else 0,
        )


class StageReport(BaseModel):  # CORRECT-LOCAL — service-internal recon report, not a domain contract
    """Results for a single reconciliation stage."""

    stage: ReconStage
    status: ReconStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    deviations: list[DeviationRecord] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    error_message: str | None = None
    output_gcs_path: str | None = None

    @property
    def deviation_count(self) -> int:
        return len(self.deviations)

    @property
    def passed(self) -> bool:
        return self.status == ReconStatus.PASSED


class ReconReport(BaseModel):  # CORRECT-LOCAL — service-internal recon report, not a domain contract
    """Full T+1 reconciliation run report."""

    date: str  # YYYY-MM-DD
    run_id: str
    started_at: datetime
    completed_at: datetime | None = None
    status: ReconStatus = ReconStatus.PENDING
    stages: list[StageReport] = Field(default_factory=list)
    agent_report_gcs_path: str | None = None
    summary_gcs_path: str | None = None

    @property
    def total_deviations(self) -> int:
        return sum(s.deviation_count for s in self.stages)

    @property
    def failed_stages(self) -> list[ReconStage]:
        return [s.stage for s in self.stages if s.status == ReconStatus.FAILED]
