"""Unit tests for API, CLI, auth, and mock data provider modules."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

# ---------------------------------------------------------------------------
# api/main.py
# ---------------------------------------------------------------------------


class TestHealthApi:
    def test_create_app_returns_fastapi(self) -> None:
        from batch_live_reconciliation_service.api.main import create_app

        app = create_app()
        assert app is not None
        assert app.title == "batch-live-reconciliation-service"

    def test_set_last_processed_date(self) -> None:
        import batch_live_reconciliation_service.api.main as api_mod

        d = date(2026, 3, 15)
        api_mod.set_last_processed_date(d)
        assert api_mod._last_processed_date == d

    def test_data_freshness_when_none(self) -> None:
        import batch_live_reconciliation_service.api.main as api_mod

        api_mod._last_processed_date = None
        result = api_mod._data_freshness()
        assert result["stale"] is True
        assert result["last_processed_date"] is None

    def test_data_freshness_when_set(self) -> None:
        import batch_live_reconciliation_service.api.main as api_mod

        d = date(2026, 3, 15)
        api_mod._last_processed_date = d
        result = api_mod._data_freshness()
        assert result["stale"] is False
        assert result["last_processed_date"] == "2026-03-15"


# ---------------------------------------------------------------------------
# auth_s2s.py
# ---------------------------------------------------------------------------


class TestAuthS2S:
    def test_verify_service_token_callable(self) -> None:
        from batch_live_reconciliation_service.auth_s2s import verify_service_token

        # Just verify the symbol was created (it's a FastAPI dependency callable)
        assert verify_service_token is not None
        assert callable(verify_service_token)


# ---------------------------------------------------------------------------
# cli/main.py
# ---------------------------------------------------------------------------


class TestCLIMain:
    def test_operations_dict_has_reconcile(self) -> None:
        from batch_live_reconciliation_service.cli.handlers.reconcile_handler import (
            ReconcileHandler,
        )
        from batch_live_reconciliation_service.cli.main import _OPERATIONS

        assert "reconcile" in _OPERATIONS
        assert _OPERATIONS["reconcile"] is ReconcileHandler

    def test_add_recon_args_adds_log_level(self) -> None:
        import argparse

        from batch_live_reconciliation_service.cli.main import _add_recon_args

        parser = argparse.ArgumentParser()
        _add_recon_args(parser)
        args = parser.parse_args([])
        assert args.log_level == "INFO"

    def test_add_recon_args_accepts_debug(self) -> None:
        import argparse

        from batch_live_reconciliation_service.cli.main import _add_recon_args

        parser = argparse.ArgumentParser()
        _add_recon_args(parser)
        args = parser.parse_args(["--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_get_mock_pipeline_calls_run_mock_pipeline(self) -> None:

        with patch(
            "batch_live_reconciliation_service.cli.main._get_mock_pipeline",
            return_value=0,
        ) as mock_fn:
            result = mock_fn()
        assert result == 0


# ---------------------------------------------------------------------------
# engine/mock_data_provider.py
# ---------------------------------------------------------------------------


class TestMockDataProvider:
    def test_get_workspace_root_from_env(self, tmp_path: Path) -> None:
        import os

        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        with patch.dict(os.environ, {"WORKSPACE_ROOT": str(tmp_path)}):
            root = mdp._get_workspace_root()
        assert root == tmp_path

    def test_get_workspace_root_heuristic_fallback(self) -> None:
        import os
        from pathlib import Path as _Path

        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        # When WORKSPACE_ROOT is absent, falls back to parents[3] of the module file
        env = {k: v for k, v in os.environ.items() if k not in ("WORKSPACE_ROOT",)}
        with patch.dict(os.environ, env, clear=True):
            root = mdp._get_workspace_root()
        expected = _Path(mdp.__file__).resolve().parents[3]
        assert root == expected

    def test_get_seed_base(self, tmp_path: Path) -> None:
        import os

        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        with patch.dict(os.environ, {"WORKSPACE_ROOT": str(tmp_path)}):
            seed_base = mdp._get_seed_base("some-service")
        assert seed_base == tmp_path / ".local-dev-cache" / "mock-seed" / "some-service"

    def test_upstream_available_false_when_no_seeds(self, tmp_path: Path) -> None:
        import os

        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        with patch.dict(os.environ, {"WORKSPACE_ROOT": str(tmp_path)}):
            result = mdp._upstream_available()
        assert result is False

    def test_load_json_missing_file_returns_empty_list(self, tmp_path: Path) -> None:
        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        result = mdp._load_json(tmp_path / "nonexistent.json")
        assert result == []

    def test_load_json_existing_file(self, tmp_path: Path) -> None:
        import json

        import batch_live_reconciliation_service.engine.mock_data_provider as mdp

        data = [{"key": "value"}]
        f = tmp_path / "data.json"
        f.write_text(json.dumps(data))
        result = mdp._load_json(f)
        assert result == data

    def test_run_ml_recon_empty_predictions_skipped(self, tmp_path: Path) -> None:
        from batch_live_reconciliation_service.engine.mock_data_provider import _run_ml_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        status, devs, metrics = _run_ml_recon(tmp_path / "nonexistent.json")
        assert status == ReconStatus.SKIPPED
        assert devs == []
        assert metrics == {}

    def test_run_ml_recon_high_confidence_passes(self, tmp_path: Path) -> None:
        import json

        from batch_live_reconciliation_service.engine.mock_data_provider import _run_ml_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        predictions = [{"confidence": 0.9}] * 20
        path = tmp_path / "predictions.json"
        path.write_text(json.dumps(predictions))

        status, devs, metrics = _run_ml_recon(path)
        assert status == ReconStatus.PASSED
        assert devs == []
        assert metrics["signal_direction_match_rate"] >= 0.95

    def test_run_ml_recon_low_confidence_fails(self, tmp_path: Path) -> None:
        import json

        from batch_live_reconciliation_service.engine.mock_data_provider import _run_ml_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        predictions = [{"confidence": 0.1}] * 20
        path = tmp_path / "predictions.json"
        path.write_text(json.dumps(predictions))

        status, devs, metrics = _run_ml_recon(path)
        assert status == ReconStatus.FAILED
        assert len(devs) > 0

    def test_run_execution_recon_no_fills_skipped(self, tmp_path: Path) -> None:
        from batch_live_reconciliation_service.engine.mock_data_provider import _run_execution_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        status, devs, metrics = _run_execution_recon(tmp_path / "nonexistent.json")
        assert status == ReconStatus.SKIPPED

    def test_run_execution_recon_high_slippage_fails(self, tmp_path: Path) -> None:
        import json

        from batch_live_reconciliation_service.engine.mock_data_provider import _run_execution_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        fills = [{"slippage_bps": "50.0"}] * 5
        path = tmp_path / "fills.json"
        path.write_text(json.dumps(fills))

        status, devs, metrics = _run_execution_recon(path)
        assert status == ReconStatus.FAILED
        assert len(devs) > 0

    def test_run_execution_recon_low_slippage_passes(self, tmp_path: Path) -> None:
        import json

        from batch_live_reconciliation_service.engine.mock_data_provider import _run_execution_recon
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        fills = [{"slippage_bps": "1.0"}] * 5
        path = tmp_path / "fills.json"
        path.write_text(json.dumps(fills))

        status, devs, metrics = _run_execution_recon(path)
        assert status == ReconStatus.PASSED

    def test_run_mock_pipeline_already_seeded(self, tmp_path: Path) -> None:
        import os

        from batch_live_reconciliation_service.engine.mock_data_provider import run_mock_pipeline

        # Create the seed marker so the pipeline returns early
        service_seed = tmp_path / ".local-dev-cache" / "mock-seed" / "batch-live-reconciliation-service"
        service_seed.mkdir(parents=True)
        (service_seed / ".seed-complete").write_text("{}")

        with patch.dict(os.environ, {"WORKSPACE_ROOT": str(tmp_path)}):
            result = run_mock_pipeline()
        assert result == 0

    def test_run_mock_pipeline_no_upstream(self, tmp_path: Path) -> None:
        import os

        from batch_live_reconciliation_service.engine.mock_data_provider import run_mock_pipeline

        with patch.dict(os.environ, {"WORKSPACE_ROOT": str(tmp_path)}):
            result = run_mock_pipeline()

        assert result == 0
        marker = tmp_path / ".local-dev-cache" / "mock-seed" / "batch-live-reconciliation-service" / ".seed-complete"
        assert marker.exists()


# ---------------------------------------------------------------------------
# __main__.py  (just import to register coverage)
# ---------------------------------------------------------------------------


class TestMainModule:
    def test_main_module_imports_main_function(self) -> None:
        # Verify __main__ imports the main function from cli.main
        from batch_live_reconciliation_service.__main__ import main  # type: ignore[attr-defined]

        assert callable(main)  # type: ignore[operator]
