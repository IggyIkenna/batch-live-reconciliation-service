"""
Pydantic models for reconciliation reports.

No Any types per workspace standards.

# SCHEMA_PROVENANCE_EXEMPT — service-internal pipeline models (CORRECT-LOCAL).
# ReconReport, StageReport, DeviationRecord are not cross-service contracts;
# they are the private result types for this service's T+1 pipeline.
# Cross-service contract (resolution workflow) lives in unified-internal-contracts.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


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


class DeviationRecord(BaseModel):  # CORRECT-LOCAL — service-internal recon report, not a domain contract
    """A single metric deviation above threshold."""

    metric_name: str
    stage: ReconStage
    actual_value: float
    threshold: float
    direction: str  # "above" | "below"
    description: str
    instrument_id: str | None = None


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
