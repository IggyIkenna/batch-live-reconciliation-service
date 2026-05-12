#!/usr/bin/env python3
"""Generate mock reconciliation data for local dev / CI mock mode.

batch-live-reconciliation-service is Layer 7 (monitoring/reconciliation) of the
mock data dependency chain. It consumes execution events, strategy outputs, and
ML inference results from upstream services to produce T+1 reconciliation
reports with deviation detection.

Upstream dependencies (Layer 6):
  - risk-and-exposure-service  (risk snapshots)
  - position-balance-monitor-service  (position snapshots)
  - pnl-attribution-service  (P&L attribution data)

Usage:
    python scripts/seed_mock_data.py --scenario normal --seed 42 --env local
    python scripts/seed_mock_data.py --scenario heavy --seed 42 --env local
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from unified_api_contracts.internal.modes import MockScenario
from unified_trading_library import SeedWriter, get_seed_writer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVICE_NAME: Final[str] = "batch-live-reconciliation-service"
LAYER: Final[int] = 7

# Instruments used in reconciliation (matching upstream seed data)
INSTRUMENTS: Final[list[str]] = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "BTC-PERPETUAL",
    "ETH-USDC",
    "ES.FUT",
    "NQ.FUT",
    "NG_STRADDLE",
    "CL_RISK_REVERSAL",
]

STRATEGIES: Final[list[str]] = [
    "momentum-macd-v2",
    "breakout-v1",
    "mean-reversion-v3",
    "NG_STRADDLE_TRENDING",
    "CL_RISK_REVERSAL_TRENDING",
]

SCENARIO_COUNTS: Final[dict[str, dict[str, int]]] = {
    "normal": {"ml_events": 50, "strategy_events": 30, "execution_events": 40},
    "heavy": {"ml_events": 150, "strategy_events": 80, "execution_events": 120},
    "light": {"ml_events": 15, "strategy_events": 10, "execution_events": 12},
    "big_ranges": {"ml_events": 80, "strategy_events": 50, "execution_events": 60},
    "bust": {"ml_events": 100, "strategy_events": 60, "execution_events": 80},
    "no_system_overload": {"ml_events": 30, "strategy_events": 20, "execution_events": 25},
    "missing_data": {"ml_events": 20, "strategy_events": 15, "execution_events": 18},
    "delayed_data": {"ml_events": 40, "strategy_events": 25, "execution_events": 35},
}


# ---------------------------------------------------------------------------
# Event generators (batch + live pairs for reconciliation)
# ---------------------------------------------------------------------------


def _generate_ml_events(
    count: int,
    rng: random.Random,
    date_str: str,
    scenario: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Generate paired batch/live ML inference events for Stage 1 recon.

    Returns:
        Tuple of (batch_events, live_events).
    """
    batch_events: list[dict[str, object]] = []
    live_events: list[dict[str, object]] = []
    base_time = datetime.fromisoformat(f"{date_str}T09:30:00+00:00")

    for i in range(count):
        instrument = INSTRUMENTS[i % len(INSTRUMENTS)]
        timeframe = ["1m", "5m", "15m", "1h"][i % 4]

        # Base signal values (live = ground truth)
        live_direction = 1 if rng.random() > 0.5 else -1
        live_magnitude = round(rng.random() * 0.8 + 0.1, 4)
        live_ts = base_time + timedelta(minutes=i * 5)

        # Batch values (should match closely in normal scenario)
        direction_noise = 0.0 if scenario != "bust" else 0.15
        magnitude_noise = 0.02 if scenario == "normal" else 0.1
        batch_direction = live_direction
        if rng.random() < direction_noise:
            batch_direction = -live_direction
        batch_magnitude = round(
            live_magnitude + (rng.random() - 0.5) * magnitude_noise,
            4,
        )

        event_base: dict[str, object] = {
            "event_type": "ML_INFERENCE",
            "instrument_id": instrument,
            "timeframe": timeframe,
            "timestamp": live_ts.isoformat(),
        }

        live_event: dict[str, object] = {
            **event_base,
            "signal_direction": live_direction,
            "magnitude": live_magnitude,
            "model_version": "v2.3.1",
            "latency_ms": round(50 + rng.random() * 200, 1),
        }
        live_events.append(live_event)

        batch_event: dict[str, object] = {
            **event_base,
            "signal_direction": batch_direction,
            "magnitude": batch_magnitude,
            "model_version": "v2.3.1",
            "latency_ms": round(50 + rng.random() * 300, 1),
        }
        batch_events.append(batch_event)

    # In missing_data scenario, drop some batch events
    if scenario == "missing_data":
        drop_count = max(1, len(batch_events) // 5)
        for _ in range(drop_count):
            if batch_events:
                batch_events.pop(rng.randint(0, len(batch_events) - 1))

    return batch_events, live_events


def _generate_strategy_events(
    count: int,
    rng: random.Random,
    date_str: str,
    scenario: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Generate paired batch/live strategy events for Stage 2 recon.

    Returns:
        Tuple of (batch_events, live_events).
    """
    batch_events: list[dict[str, object]] = []
    live_events: list[dict[str, object]] = []
    base_time = datetime.fromisoformat(f"{date_str}T09:30:00+00:00")

    for i in range(count):
        instrument = INSTRUMENTS[i % len(INSTRUMENTS)]
        strategy = STRATEGIES[i % len(STRATEGIES)]
        side = "BUY" if rng.random() > 0.4 else "SELL"

        # Instruction events
        live_instruction: dict[str, object] = {
            "event_type": "INSTRUCTION",
            "instrument_id": instrument,
            "strategy_id": strategy,
            "side": side,
            "quantity": round(rng.random() * 10 + 0.1, 4),
            "timestamp": (base_time + timedelta(minutes=i * 10)).isoformat(),
        }
        live_events.append(live_instruction)

        if scenario == "bust" and rng.random() < 0.2:
            # Skip some batch instructions in bust scenario
            continue

        batch_instruction: dict[str, object] = {
            **live_instruction,
            "quantity": round(
                float(str(live_instruction["quantity"])) + (rng.random() - 0.5) * 0.5,
                4,
            ),
        }
        batch_events.append(batch_instruction)

    # Position snapshots
    for instrument in INSTRUMENTS[:5]:
        live_pos = round((rng.random() - 0.5) * 20, 4)
        pos_noise = 0.1 if scenario == "normal" else 2.0
        batch_pos = round(live_pos + (rng.random() - 0.5) * pos_noise, 4)

        live_snapshot: dict[str, object] = {
            "event_type": "POSITION_SNAPSHOT",
            "instrument_id": instrument,
            "net_position": live_pos,
            "unrealized_pnl": round(live_pos * (rng.random() - 0.3) * 100, 2),
            "timestamp": f"{date_str}T16:00:00+00:00",
        }
        live_events.append(live_snapshot)

        batch_snapshot: dict[str, object] = {
            "event_type": "POSITION_SNAPSHOT",
            "instrument_id": instrument,
            "net_position": batch_pos,
            "unrealized_pnl": round(batch_pos * (rng.random() - 0.3) * 100, 2),
            "timestamp": f"{date_str}T16:00:00+00:00",
        }
        batch_events.append(batch_snapshot)

    # Risk snapshots (VaR)
    live_var = round(5000 + rng.random() * 10000, 2)
    var_noise = 0.02 if scenario == "normal" else 0.15
    batch_var = round(live_var * (1 + (rng.random() - 0.5) * var_noise), 2)

    live_events.append(
        {
            "event_type": "RISK_SNAPSHOT",
            "var_1d": live_var,
            "timestamp": f"{date_str}T16:00:00+00:00",
        }
    )
    batch_events.append(
        {
            "event_type": "RISK_SNAPSHOT",
            "var_1d": batch_var,
            "timestamp": f"{date_str}T16:00:00+00:00",
        }
    )

    return batch_events, live_events


def _generate_execution_events(
    count: int,
    rng: random.Random,
    date_str: str,
    scenario: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Generate paired batch/live execution events for Stage 3 recon.

    Returns:
        Tuple of (batch_events, live_events).
    """
    batch_events: list[dict[str, object]] = []
    live_events: list[dict[str, object]] = []
    base_time = datetime.fromisoformat(f"{date_str}T09:30:00+00:00")

    algos = ["TWAP", "VWAP", "POV", "ICEBERG", "SNIPER"]

    for i in range(count):
        instrument = INSTRUMENTS[i % len(INSTRUMENTS)]
        algo = algos[i % len(algos)]
        ts = base_time + timedelta(minutes=i * 3)

        # ORDER_SUBMITTED events
        order_id = f"ord-{uuid.uuid4().hex[:8]}"
        order_event: dict[str, object] = {
            "event_type": "ORDER_SUBMITTED",
            "order_id": order_id,
            "instrument_id": instrument,
            "algo_configured": algo,
            "algo_used": algo if rng.random() > 0.05 else algos[(i + 1) % 5],
            "timestamp": ts.isoformat(),
        }
        live_events.append(order_event)
        batch_events.append({**order_event})

        # FILL events (most orders get filled)
        if rng.random() > 0.1:
            fill_price = round(100 + rng.random() * 50000, 2)
            filled_qty = round(rng.random() * 5 + 0.01, 4)
            slippage_base = round(rng.random() * 8, 2)  # 0-8 bps
            slippage_noise = 2 if scenario == "normal" else 15
            latency_base = round(50 + rng.random() * 400, 1)

            live_fill: dict[str, object] = {
                "event_type": "FILL",
                "order_id": order_id,
                "instrument_id": instrument,
                "fill_price": fill_price,
                "filled_qty": filled_qty,
                "slippage_bps": slippage_base,
                "latency_ms": latency_base,
                "timestamp": (ts + timedelta(seconds=rng.randint(1, 30))).isoformat(),
            }
            live_events.append(live_fill)

            batch_fill: dict[str, object] = {
                "event_type": "FILL",
                "order_id": order_id,
                "instrument_id": instrument,
                "fill_price": round(fill_price * (1 + (rng.random() - 0.5) * 0.002), 2),
                "filled_qty": filled_qty,
                "slippage_bps": round(
                    slippage_base + (rng.random() - 0.5) * slippage_noise,
                    2,
                ),
                "latency_ms": round(latency_base + (rng.random() - 0.5) * 100, 1),
                "timestamp": (ts + timedelta(seconds=rng.randint(1, 30))).isoformat(),
            }
            batch_events.append(batch_fill)

    return batch_events, live_events


# ---------------------------------------------------------------------------
# Reconciliation report generation
# ---------------------------------------------------------------------------


def _build_recon_report(
    date_str: str,
    ml_batch: list[dict[str, object]],
    ml_live: list[dict[str, object]],
    strategy_batch: list[dict[str, object]],
    strategy_live: list[dict[str, object]],
    execution_batch: list[dict[str, object]],
    execution_live: list[dict[str, object]],
    rng: random.Random,
) -> dict[str, object]:
    """Build a summary reconciliation report dict."""
    run_id = str(uuid.uuid4())
    started_at = datetime.fromisoformat(f"{date_str}T02:00:00+00:00")

    stages: list[dict[str, object]] = []

    # Stage 1: ML Recon summary
    ml_matched = min(len(ml_batch), len(ml_live))
    ml_coverage = ml_matched / max(len(ml_live), 1)
    ml_direction_match = round(0.85 + rng.random() * 0.15, 4)
    ml_mae = round(rng.random() * 0.15, 4)

    ml_deviations: list[dict[str, object]] = []
    if ml_direction_match < 0.95:
        ml_deviations.append(
            {
                "metric_name": "signal_direction_match_rate",
                "stage": "ml_recon",
                "actual_value": ml_direction_match,
                "threshold": 0.95,
                "direction": "below",
                "description": (
                    f"Signal direction match rate {ml_direction_match:.1%} < 95% threshold"
                ),
            }
        )
    if ml_mae > 0.1:
        ml_deviations.append(
            {
                "metric_name": "signal_magnitude_mae",
                "stage": "ml_recon",
                "actual_value": ml_mae,
                "threshold": 0.1,
                "direction": "above",
                "description": (f"Signal magnitude MAE {ml_mae:.3f} > 0.1 threshold"),
            }
        )

    stages.append(
        {
            "stage": "ml_recon",
            "status": "failed" if ml_deviations else "passed",
            "started_at": started_at.isoformat(),
            "completed_at": (started_at + timedelta(minutes=5)).isoformat(),
            "deviations": ml_deviations,
            "metrics": {
                "signal_direction_match_rate": ml_direction_match,
                "signal_magnitude_mae": ml_mae,
                "instrument_coverage_pct": round(ml_coverage, 4),
                "batch_event_count": float(len(ml_batch)),
                "live_event_count": float(len(ml_live)),
            },
        }
    )

    # Stage 2: Strategy Recon summary
    strat_alignment = round(0.80 + rng.random() * 0.20, 4)
    pnl_delta = round(rng.random() * 0.04, 4)
    pos_delta = round(rng.random() * 2.0, 4)
    var_delta = round(rng.random() * 0.15, 4)

    strat_deviations: list[dict[str, object]] = []
    if strat_alignment < 0.85:
        strat_deviations.append(
            {
                "metric_name": "instruction_alignment_pct",
                "stage": "strategy_recon",
                "actual_value": strat_alignment,
                "threshold": 0.85,
                "direction": "below",
                "description": (f"Instruction alignment {strat_alignment:.1%} < 85%"),
            }
        )
    if pnl_delta > 0.02:
        strat_deviations.append(
            {
                "metric_name": "benchmark_pnl_delta",
                "stage": "strategy_recon",
                "actual_value": pnl_delta,
                "threshold": 0.02,
                "direction": "above",
                "description": (f"Benchmark P&L delta {pnl_delta:.1%} > 2% of notional"),
            }
        )

    stages.append(
        {
            "stage": "strategy_recon",
            "status": "failed" if strat_deviations else "passed",
            "started_at": (started_at + timedelta(minutes=5)).isoformat(),
            "completed_at": (started_at + timedelta(minutes=12)).isoformat(),
            "deviations": strat_deviations,
            "metrics": {
                "instruction_alignment_pct": strat_alignment,
                "benchmark_pnl_delta": pnl_delta,
                "position_snapshot_delta": pos_delta,
                "var_delta_pct": var_delta,
                "batch_event_count": float(len(strategy_batch)),
                "live_event_count": float(len(strategy_live)),
            },
        }
    )

    # Stage 3: Execution Recon summary
    alpha_gap = round(rng.random() * 0.02, 4)
    fill_rate_delta = round(rng.random() * 0.08, 4)
    slippage_delta = round(rng.random() * 15, 2)
    algo_accuracy = round(0.95 + rng.random() * 0.05, 4)
    p99_latency = round(200 + rng.random() * 500, 1)

    exec_deviations: list[dict[str, object]] = []
    if alpha_gap > 0.01:
        exec_deviations.append(
            {
                "metric_name": "alpha_pnl_gap",
                "stage": "execution_recon",
                "actual_value": alpha_gap,
                "threshold": 0.01,
                "direction": "above",
                "description": (f"Alpha P&L gap {alpha_gap:.1%} > 1% of notional"),
            }
        )
    if slippage_delta > 10.0:
        exec_deviations.append(
            {
                "metric_name": "slippage_delta_bps",
                "stage": "execution_recon",
                "actual_value": slippage_delta,
                "threshold": 10.0,
                "direction": "above",
                "description": (f"Slippage delta {slippage_delta:.1f}bps > 10bps"),
            }
        )

    stages.append(
        {
            "stage": "execution_recon",
            "status": "failed" if exec_deviations else "passed",
            "started_at": (started_at + timedelta(minutes=12)).isoformat(),
            "completed_at": (started_at + timedelta(minutes=18)).isoformat(),
            "deviations": exec_deviations,
            "metrics": {
                "alpha_pnl_gap": alpha_gap,
                "fill_rate_delta": fill_rate_delta,
                "slippage_delta_bps": slippage_delta,
                "algo_selection_accuracy": algo_accuracy,
                "order_latency_p99_ms": p99_latency,
                "batch_fill_count": float(len(execution_batch)),
                "live_fill_count": float(len(execution_live)),
            },
        }
    )

    all_deviations: list[dict[str, object]] = []
    for stage in stages:
        stage_devs = stage.get("deviations", [])
        if isinstance(stage_devs, list):
            all_deviations.extend(stage_devs)

    failed_stages = [str(s["stage"]) for s in stages if s.get("status") == "failed"]
    overall_status = "failed" if failed_stages else "passed"

    return {
        "date": date_str,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": (started_at + timedelta(minutes=20)).isoformat(),
        "status": overall_status,
        "stages": stages,
        "total_deviations": len(all_deviations),
        "failed_stages": failed_stages,
    }


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------


def generate_recon_data(
    scenario: str,
    seed: int,
    date_str: str,
) -> dict[str, object]:
    """Generate all reconciliation mock data.

    Returns:
        Dict with all generated data keyed by category.
    """
    rng = random.Random(seed)
    counts = SCENARIO_COUNTS.get(
        scenario,
        {"ml_events": 50, "strategy_events": 30, "execution_events": 40},
    )

    ml_batch, ml_live = _generate_ml_events(counts["ml_events"], rng, date_str, scenario)
    strat_batch, strat_live = _generate_strategy_events(
        counts["strategy_events"], rng, date_str, scenario
    )
    exec_batch, exec_live = _generate_execution_events(
        counts["execution_events"], rng, date_str, scenario
    )

    report = _build_recon_report(
        date_str,
        ml_batch,
        ml_live,
        strat_batch,
        strat_live,
        exec_batch,
        exec_live,
        rng,
    )

    log.info(
        "Generated recon data: ML=%d/%d, Strategy=%d/%d, Execution=%d/%d",
        len(ml_batch),
        len(ml_live),
        len(strat_batch),
        len(strat_live),
        len(exec_batch),
        len(exec_live),
    )

    return {
        "ml_batch_events": ml_batch,
        "ml_live_events": ml_live,
        "strategy_batch_events": strat_batch,
        "strategy_live_events": strat_live,
        "execution_batch_events": exec_batch,
        "execution_live_events": exec_live,
        "recon_report": report,
    }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def _write_ndjson(
    events: list[dict[str, object]],
    out_path: Path,
) -> None:
    """Write events as NDJSON (one JSON object per line)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(e, default=str) for e in events]
    out_path.write_text("\n".join(lines) + "\n")


def write_event_files(
    data: dict[str, object],
    output_root: Path,
    date_str: str,
) -> list[Path]:
    """Write all event files in GCS-compatible layout."""
    written: list[Path] = []

    # ML events
    ml_batch = data["ml_batch_events"]
    ml_live = data["ml_live_events"]
    if isinstance(ml_batch, list):
        p = output_root / f"t1-recon/events/{date_str}/ml-inference-service/events.ndjson"
        _write_ndjson(ml_batch, p)
        written.append(p)
        log.info("ML batch events: %d -> %s", len(ml_batch), p)

    if isinstance(ml_live, list):
        p = output_root / f"live/events/{date_str}/ml-inference-service/events.ndjson"
        _write_ndjson(ml_live, p)
        written.append(p)
        log.info("ML live events: %d -> %s", len(ml_live), p)

    # Strategy events
    strat_batch = data["strategy_batch_events"]
    strat_live = data["strategy_live_events"]
    if isinstance(strat_batch, list):
        p = output_root / f"t1-recon/events/{date_str}/strategy-service/events.ndjson"
        _write_ndjson(strat_batch, p)
        written.append(p)
        log.info("Strategy batch events: %d -> %s", len(strat_batch), p)

    if isinstance(strat_live, list):
        p = output_root / f"live/events/{date_str}/strategy-service/events.ndjson"
        _write_ndjson(strat_live, p)
        written.append(p)
        log.info("Strategy live events: %d -> %s", len(strat_live), p)

    # Execution events
    exec_batch = data["execution_batch_events"]
    exec_live = data["execution_live_events"]
    if isinstance(exec_batch, list):
        p = output_root / f"t1-recon/events/{date_str}/execution-service/events.ndjson"
        _write_ndjson(exec_batch, p)
        written.append(p)
        log.info("Execution batch events: %d -> %s", len(exec_batch), p)

    if isinstance(exec_live, list):
        p = output_root / f"live/events/{date_str}/execution-service/events.ndjson"
        _write_ndjson(exec_live, p)
        written.append(p)
        log.info("Execution live events: %d -> %s", len(exec_live), p)

    return written


def write_recon_report(
    data: dict[str, object],
    output_root: Path,
) -> Path:
    """Write the reconciliation report as JSON."""
    report = data["recon_report"]
    out_dir = output_root / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "recon_report.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))
    log.info("Recon report -> %s", out_path)
    return out_path


def write_seed_manifest(
    output_root: Path,
    written_paths: list[Path],
    scenario_name: str,
    seed: int,
    date_str: str,
    data: dict[str, object],
) -> Path:
    """Write seed manifest JSON."""
    manifest: dict[str, object] = {
        "service": SERVICE_NAME,
        "layer": LAYER,
        "scenario": scenario_name,
        "seed": seed,
        "date": date_str,
        "generated_at": datetime.now(UTC).isoformat(),
        "event_counts": {
            "ml_batch": len(data.get("ml_batch_events", [])),  # noqa: qg-empty-fallback
            "ml_live": len(data.get("ml_live_events", [])),  # noqa: qg-empty-fallback
            "strategy_batch": len(data.get("strategy_batch_events", [])),  # noqa: qg-empty-fallback
            "strategy_live": len(data.get("strategy_live_events", [])),  # noqa: qg-empty-fallback
            "execution_batch": len(data.get("execution_batch_events", [])),  # noqa: qg-empty-fallback
            "execution_live": len(data.get("execution_live_events", [])),  # noqa: qg-empty-fallback
        },
        "files": [str(p) for p in written_paths],
    }

    manifest_path = output_root / "seed_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    log.info("Manifest: %s", manifest_path)
    return manifest_path


def write_seed_complete_marker(writer: SeedWriter) -> str:
    """Write .seed-complete marker file."""
    marker_data = json.dumps(
        {
            "service": SERVICE_NAME,
            "completed_at": datetime.now(UTC).isoformat(),
            "layer": LAYER,
        }
    )
    return writer.write_text(marker_data, ".seed-complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate mock reconciliation data for local dev / CI.",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="normal",
        choices=[s.value for s in MockScenario],
        help="Named scenario (default: normal)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic output (default: 42)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="local",
        choices=["local", "dev"],
        help="Target environment (default: local)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(UTC).strftime("%Y-%m-%d"),
        help="Date string for reconciliation (default: today)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Override output directory",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for seed_mock_data."""
    args = parse_args(argv)

    log.info("=== batch-live-reconciliation-service mock data seed ===")
    log.info("Scenario: %s (seed=%d)", args.scenario, args.seed)
    log.info("Env: %s, Date: %s", args.env, args.date)

    # Determine output root
    if args.output_dir:
        output_root = Path(args.output_dir)
    else:
        script_dir = Path(__file__).resolve().parent
        repo_root = script_dir.parent
        workspace_root = repo_root.parent
        output_root = workspace_root / ".local-dev-cache" / "mock-seed" / SERVICE_NAME

    output_root.mkdir(parents=True, exist_ok=True)
    # Create seed writer (local filesystem or cloud storage)
    writer = get_seed_writer(args.env, SERVICE_NAME, args.output_dir)
    log.info("Writer: %s", type(writer).__name__)
    log.info("Output: %s", output_root)

    # Generate data
    data = generate_recon_data(
        scenario=args.scenario,
        seed=args.seed,
        date_str=args.date,
    )

    # Write output files
    written: list[Path] = []
    written.extend(write_event_files(data, output_root, args.date))
    written.append(write_recon_report(data, output_root))

    # Write manifest and marker
    write_seed_manifest(output_root, written, args.scenario, args.seed, args.date, data)
    write_seed_complete_marker(writer)

    # Print summary
    report = data["recon_report"]
    if isinstance(report, dict):
        total_devs = report.get("total_deviations", 0)
        status = report.get("status", "unknown")
        log.info("=== SEED COMPLETE ===")
        log.info("Recon status: %s", status)
        log.info("Total deviations: %s", total_devs)
        log.info("Files written: %d", len(written))
        log.info("Marker: %s/.seed-complete", output_root)

    return 0


if __name__ == "__main__":
    sys.exit(main())
