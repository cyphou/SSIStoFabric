"""
Unit tests for the public Python API (SSISMigrator facade).
Validates that the API surface correctly wraps the parser, engine,
deployer, and regression runner.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ssis_to_fabric.analyzer.models import SSISPackage
from ssis_to_fabric.api import SSISMigrator
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.migration_engine import MigrationPlan, TargetArtifact

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


# =========================================================================
# Construction
# =========================================================================


class TestSSISMigratorConstruction:
    """Tests for SSISMigrator initialisation."""

    @pytest.mark.unit
    def test_default_construction(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        assert migrator.strategy == "hybrid"
        assert migrator.output_dir == tmp_path

    @pytest.mark.unit
    def test_strategy_override(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(strategy="spark", output_dir=tmp_path)
        assert migrator.strategy == "spark"

    @pytest.mark.unit
    def test_from_config(self, default_config: MigrationConfig) -> None:
        migrator = SSISMigrator.from_config(default_config)
        assert migrator.config is default_config
        assert migrator.strategy == default_config.strategy.value

    @pytest.mark.unit
    def test_from_config_with_overrides(self, default_config: MigrationConfig) -> None:
        migrator = SSISMigrator.from_config(default_config, strategy="spark")
        assert migrator.strategy == "spark"

    @pytest.mark.unit
    def test_project_name_override(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(project_name="my-project", output_dir=tmp_path)
        assert migrator.config.project_name == "my-project"

    @pytest.mark.unit
    def test_config_property(self, default_config: MigrationConfig) -> None:
        migrator = SSISMigrator.from_config(default_config)
        assert isinstance(migrator.config, MigrationConfig)


# =========================================================================
# Analyze
# =========================================================================


class TestAnalyze:
    """Tests for SSISMigrator.analyze()."""

    @pytest.mark.unit
    def test_analyze_single_file(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")

        assert len(packages) == 1
        assert isinstance(packages[0], SSISPackage)
        assert packages[0].name == "SampleETLPackage"

    @pytest.mark.unit
    def test_analyze_directory(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR)

        assert len(packages) >= 2  # simple_etl + complex_etl at minimum

    @pytest.mark.unit
    def test_analyze_nonexistent_path(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            migrator.analyze("/nonexistent/path.dtsx")

    @pytest.mark.unit
    def test_analyze_empty_directory(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty"
        empty.mkdir()
        migrator = SSISMigrator(output_dir=tmp_path)
        with pytest.raises(ValueError, match="No SSIS packages found"):
            migrator.analyze(empty)

    @pytest.mark.unit
    def test_analyze_returns_package_metadata(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        pkg = packages[0]

        assert pkg.total_tasks > 0
        assert pkg.overall_complexity is not None


# =========================================================================
# Plan
# =========================================================================


class TestPlan:
    """Tests for SSISMigrator.plan()."""

    @pytest.mark.unit
    def test_plan_returns_migration_plan(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        assert isinstance(plan, MigrationPlan)
        assert len(plan.items) > 0
        assert "total" in plan.summary

    @pytest.mark.unit
    def test_plan_hybrid_routes_dataflows(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(strategy="hybrid", output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        # Default dataflow_type=notebook → simple data flows go to Spark
        notebook_items = [i for i in plan.items if i.target_artifact == TargetArtifact.SPARK_NOTEBOOK]
        assert len(notebook_items) > 0

    @pytest.mark.unit
    def test_plan_hybrid_routes_dataflows_gen2(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(strategy="hybrid", dataflow_type="dataflow_gen2", output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        dataflow_items = [i for i in plan.items if i.target_artifact == TargetArtifact.DATAFLOW_GEN2]
        assert len(dataflow_items) > 0

    @pytest.mark.unit
    def test_plan_spark_strategy(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(strategy="spark", output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        for item in plan.items:
            assert item.target_artifact == TargetArtifact.SPARK_NOTEBOOK

    @pytest.mark.unit
    def test_plan_to_dict(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path)
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        d = plan.to_dict()
        assert "project_name" in d
        assert "items" in d
        assert "summary" in d


# =========================================================================
# Migrate
# =========================================================================


class TestMigrate:
    """Tests for SSISMigrator.migrate()."""

    @pytest.mark.unit
    def test_migrate_produces_artifacts(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path / "output")
        result = migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx")

        assert isinstance(result, MigrationPlan)
        completed = [i for i in result.items if i.status == "completed"]
        assert len(completed) > 0

    @pytest.mark.unit
    def test_migrate_generates_pipeline(self, tmp_path: Path) -> None:
        out = tmp_path / "output"
        migrator = SSISMigrator(output_dir=out)
        migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx")

        pipelines = list((out / "pipelines").glob("*.json"))
        assert len(pipelines) >= 1

    @pytest.mark.unit
    def test_migrate_generates_report(self, tmp_path: Path) -> None:
        out = tmp_path / "output"
        migrator = SSISMigrator(output_dir=out)
        migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx")

        report_path = out / "migration_report.json"
        assert report_path.exists()

        report = json.loads(report_path.read_text())
        assert "items" in report

    @pytest.mark.unit
    def test_migrate_with_precomputed_plan(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path / "output")
        packages = migrator.analyze(FIXTURES_DIR / "simple_etl.dtsx")
        plan = migrator.plan(packages)

        # Re-migrate using the pre-computed plan
        result = migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx", plan=plan)
        assert isinstance(result, MigrationPlan)
        assert len(result.items) == len(plan.items)

    @pytest.mark.unit
    def test_migrate_complex_package(self, tmp_path: Path) -> None:
        out = tmp_path / "output"
        migrator = SSISMigrator(output_dir=out)
        result = migrator.migrate(FIXTURES_DIR / "complex_etl.dtsx")

        assert isinstance(result, MigrationPlan)
        # Complex package should produce spark notebooks
        spark_items = [i for i in result.items if i.target_artifact == TargetArtifact.SPARK_NOTEBOOK]
        assert len(spark_items) > 0


# =========================================================================
# Deploy (mocked)
# =========================================================================


class TestDeploy:
    """Tests for SSISMigrator.deploy() — FabricDeployer is mocked."""

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_deploy_calls_deployer(self, mock_deployer_cls, tmp_path: Path) -> None:
        """Patch FabricDeployer at its source module (lazy-imported by api.py)."""
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(total=3, succeeded=3, failed=0, skipped=0, results=[])
        mock_deployer_cls.return_value = mock_deployer

        migrator = SSISMigrator(output_dir=tmp_path)
        migrator.deploy(workspace_id="test-ws-id")

        # Deployer constructed with correct args
        mock_deployer_cls.assert_called_once_with(
            workspace_id="test-ws-id",
            credential=None,
            skip_existing=True,
            dry_run=False,
        )
        mock_deployer.authenticate.assert_called_once()
        mock_deployer.deploy_all.assert_called_once_with(tmp_path)

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_deploy_dry_run(self, mock_deployer_cls, tmp_path: Path) -> None:
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(results=[])
        mock_deployer_cls.return_value = mock_deployer

        migrator = SSISMigrator(output_dir=tmp_path)
        migrator.deploy(workspace_id="ws-id", dry_run=True)

        mock_deployer_cls.assert_called_once_with(
            workspace_id="ws-id",
            credential=None,
            skip_existing=True,
            dry_run=True,
        )

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_deploy_clean(self, mock_deployer_cls, tmp_path: Path) -> None:
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(results=[])
        mock_deployer_cls.return_value = mock_deployer

        migrator = SSISMigrator(output_dir=tmp_path)
        migrator.deploy(workspace_id="ws-id", clean=True)

        mock_deployer.delete_all_items.assert_called_once()

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_deploy_clean_skipped_on_dry_run(self, mock_deployer_cls, tmp_path: Path) -> None:
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(results=[])
        mock_deployer_cls.return_value = mock_deployer

        migrator = SSISMigrator(output_dir=tmp_path)
        migrator.deploy(workspace_id="ws-id", clean=True, dry_run=True)

        mock_deployer.delete_all_items.assert_not_called()

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_deploy_custom_output_dir(self, mock_deployer_cls, tmp_path: Path) -> None:
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(results=[])
        mock_deployer_cls.return_value = mock_deployer

        custom_dir = tmp_path / "custom_output"
        custom_dir.mkdir()

        migrator = SSISMigrator(output_dir=tmp_path)
        migrator.deploy(workspace_id="ws-id", output_dir=custom_dir)

        mock_deployer.deploy_all.assert_called_once_with(custom_dir)


# =========================================================================
# Validate
# =========================================================================


class TestValidate:
    """Tests for SSISMigrator.validate()."""

    @pytest.mark.unit
    def test_validate_matching_files(self, tmp_path: Path) -> None:
        baseline = tmp_path / "baseline"
        output = tmp_path / "output"
        baseline.mkdir()
        output.mkdir()

        # Create matching JSON files
        data = {"key": "value", "items": [1, 2, 3]}
        (baseline / "test.json").write_text(json.dumps(data))
        (output / "test.json").write_text(json.dumps(data))

        migrator = SSISMigrator(output_dir=output)
        results = migrator.validate(baseline)

        assert len(results) >= 1
        assert all(r["status"] == "pass" for r in results if r["file"] == "test.json")

    @pytest.mark.unit
    def test_validate_missing_file(self, tmp_path: Path) -> None:
        baseline = tmp_path / "baseline"
        output = tmp_path / "output"
        baseline.mkdir()
        output.mkdir()

        (baseline / "expected.json").write_text('{"a": 1}')
        # No corresponding file in output

        migrator = SSISMigrator(output_dir=output)
        results = migrator.validate(baseline)

        failed = [r for r in results if r["status"] == "fail"]
        assert len(failed) >= 1


# =========================================================================
# Run (convenience)
# =========================================================================


class TestRun:
    """Tests for SSISMigrator.run() convenience method."""

    @pytest.mark.unit
    def test_run_without_deploy(self, tmp_path: Path) -> None:
        migrator = SSISMigrator(output_dir=tmp_path / "output")
        result = migrator.run(FIXTURES_DIR / "simple_etl.dtsx")

        assert isinstance(result, MigrationPlan)
        completed = [i for i in result.items if i.status == "completed"]
        assert len(completed) > 0

    @pytest.mark.unit
    @patch("ssis_to_fabric.engine.fabric_deployer.FabricDeployer", autospec=False)
    def test_run_with_deploy(self, mock_deployer_cls, tmp_path: Path) -> None:
        mock_deployer = MagicMock()
        mock_deployer.deploy_all.return_value = MagicMock(results=[])
        mock_deployer_cls.return_value = mock_deployer

        migrator = SSISMigrator(output_dir=tmp_path / "output")
        result = migrator.run(
            FIXTURES_DIR / "simple_etl.dtsx",
            workspace_id="test-ws-id",
        )

        assert isinstance(result, MigrationPlan)
        mock_deployer.authenticate.assert_called_once()
        mock_deployer.deploy_all.assert_called_once()


# =========================================================================
# Import surface
# =========================================================================


class TestImportSurface:
    """Verify that the public API is importable from the package root."""

    @pytest.mark.unit
    def test_import_ssismigrator(self) -> None:
        from ssis_to_fabric import SSISMigrator

        assert SSISMigrator is not None

    @pytest.mark.unit
    def test_import_config(self) -> None:
        from ssis_to_fabric import MigrationConfig, MigrationStrategy

        assert MigrationConfig is not None
        assert MigrationStrategy is not None

    @pytest.mark.unit
    def test_import_engine_types(self) -> None:
        from ssis_to_fabric import (
            MigrationEngine,
            MigrationItem,
            MigrationPlan,
            TargetArtifact,
        )

        assert MigrationEngine is not None
        assert MigrationItem is not None
        assert MigrationPlan is not None
        assert TargetArtifact is not None

    @pytest.mark.unit
    def test_import_load_config(self) -> None:
        from ssis_to_fabric import load_config

        assert callable(load_config)
