"""
Tests for Phase 5 — Quality & Completeness features (v1.3.0).

Covers:
- Event handler → pipeline failure activity conversion
- Pre-migration assessment scoring (assess command)
- Config validation (validate-config command)
- Migrate --dry-run mode
- API facade assess() method
- Assessment report data model
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    EventHandler,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.cli import main
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.migration_engine import (
    AssessmentReport,
    MigrationEngine,
    PackageAssessment,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "sample_packages"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_package(
    name: str = "TestPkg",
    tasks: list[ControlFlowTask] | None = None,
    event_handlers: list[EventHandler] | None = None,
    connections: list[ConnectionManager] | None = None,
) -> SSISPackage:
    """Create a minimal SSISPackage for testing."""
    pkg = SSISPackage(
        name=name,
        file_path=f"/fake/{name}.dtsx",
        control_flow_tasks=tasks or [],
        event_handlers=event_handlers or [],
        connection_managers=connections or [],
    )
    pkg.compute_stats()
    return pkg


def _sql_task(name: str = "RunSQL", **kwargs) -> ControlFlowTask:  # type: ignore[no-untyped-def]
    return ControlFlowTask(
        name=name,
        task_type=TaskType.EXECUTE_SQL,
        sql_statement="SELECT 1",
        migration_complexity=MigrationComplexity.LOW,
        **kwargs,
    )


def _script_task(name: str = "MyScript") -> ControlFlowTask:
    return ControlFlowTask(
        name=name,
        task_type=TaskType.SCRIPT,
        migration_complexity=MigrationComplexity.HIGH,
    )


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(
        project_name="test-phase5",
        strategy=MigrationStrategy.HYBRID,
        output_dir=tmp_path / "output",
    )


# ===========================================================================
# 1. Event Handler → Pipeline Failure Activities
# ===========================================================================


class TestEventHandlerConversion:
    """Event handlers should be converted to pipeline failure-path activities."""

    @pytest.mark.unit
    def test_on_error_handler_creates_failed_dependency(self, config: MigrationConfig) -> None:
        """OnError handler tasks should depend on main activities with 'Failed' condition."""
        error_task = _sql_task("LogError")
        eh = EventHandler(event_type="OnError", tasks=[error_task])
        main_task = _sql_task("MainWork")
        pkg = _make_package(tasks=[main_task], event_handlers=[eh])

        gen = DataFactoryGenerator(config)
        activities = gen._event_handlers_to_activities(pkg)

        assert len(activities) == 1
        act = activities[0]
        assert "OnError" in act["name"]
        assert act["dependsOn"][0]["dependencyConditions"] == ["Failed"]
        assert act["dependsOn"][0]["activity"] == "MainWork"

    @pytest.mark.unit
    def test_on_error_handler_depends_on_all_top_tasks(self, config: MigrationConfig) -> None:
        """OnError handler should depend on ALL top-level activities."""
        eh = EventHandler(event_type="OnError", tasks=[_sql_task("AlertError")])
        pkg = _make_package(
            tasks=[_sql_task("Step1"), _sql_task("Step2"), _sql_task("Step3")],
            event_handlers=[eh],
        )

        gen = DataFactoryGenerator(config)
        activities = gen._event_handlers_to_activities(pkg)

        assert len(activities) == 1
        dep_names = {d["activity"] for d in activities[0]["dependsOn"]}
        assert dep_names == {"Step1", "Step2", "Step3"}

    @pytest.mark.unit
    def test_on_pre_execute_creates_placeholder(self, config: MigrationConfig) -> None:
        """Non-OnError event handlers should emit a placeholder Wait activity."""
        eh = EventHandler(event_type="OnPreExecute", tasks=[_sql_task("Audit")])
        pkg = _make_package(tasks=[_sql_task("Work")], event_handlers=[eh])

        gen = DataFactoryGenerator(config)
        activities = gen._event_handlers_to_activities(pkg)

        assert len(activities) == 1
        act = activities[0]
        assert act["type"] == "Wait"
        assert "OnPreExecute" in act["description"]
        assert "TODO" in act["description"]

    @pytest.mark.unit
    def test_no_event_handlers_returns_empty(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_sql_task("Work")])
        gen = DataFactoryGenerator(config)
        assert gen._event_handlers_to_activities(pkg) == []

    @pytest.mark.unit
    def test_multiple_on_error_tasks(self, config: MigrationConfig) -> None:
        """Multiple tasks inside OnError handler should produce multiple activities."""
        eh = EventHandler(
            event_type="OnError",
            tasks=[_sql_task("LogError"), _sql_task("SendAlert")],
        )
        pkg = _make_package(tasks=[_sql_task("MainWork")], event_handlers=[eh])

        gen = DataFactoryGenerator(config)
        activities = gen._event_handlers_to_activities(pkg)
        assert len(activities) == 2

    @pytest.mark.unit
    def test_event_handlers_in_pipeline_json(self, config: MigrationConfig, tmp_path: Path) -> None:
        """Event handler activities should appear in the generated pipeline JSON."""
        eh = EventHandler(event_type="OnError", tasks=[_sql_task("LogError")])
        pkg = _make_package(tasks=[_sql_task("MainWork")], event_handlers=[eh])

        gen = DataFactoryGenerator(config)
        pipeline_path = gen.generate_package_pipeline(pkg, {}, tmp_path)

        pipeline = json.loads(pipeline_path.read_text())
        activity_names = [a["name"] for a in pipeline["properties"]["activities"]]
        assert any("OnError" in n for n in activity_names)

    @pytest.mark.unit
    def test_disabled_tasks_excluded_from_deps(self, config: MigrationConfig) -> None:
        """Disabled tasks should not appear in OnError dependency list."""
        eh = EventHandler(event_type="OnError", tasks=[_sql_task("LogError")])
        disabled = _sql_task("DisabledStep", disabled=True)
        enabled = _sql_task("EnabledStep")
        pkg = _make_package(tasks=[disabled, enabled], event_handlers=[eh])

        gen = DataFactoryGenerator(config)
        activities = gen._event_handlers_to_activities(pkg)

        dep_names = {d["activity"] for d in activities[0]["dependsOn"]}
        assert "EnabledStep" in dep_names
        assert "DisabledStep" not in dep_names


# ===========================================================================
# 2. Pre-Migration Assessment Scoring
# ===========================================================================


class TestAssessmentReport:
    """Test the AssessmentReport data model."""

    @pytest.mark.unit
    def test_assessment_report_to_dict(self) -> None:
        report = AssessmentReport(
            project_name="test",
            total_packages=2,
            readiness_score=75,
        )
        d = report.to_dict()
        assert d["project_name"] == "test"
        assert d["readiness_score"] == 75
        assert "packages" in d

    @pytest.mark.unit
    def test_package_assessment_fields(self) -> None:
        pa = PackageAssessment(
            name="pkg1",
            file_path="/test/pkg1.dtsx",
            tasks=5,
            data_flows=2,
            connections=3,
            parameters=1,
            event_handlers=0,
            complexity="MEDIUM",
            estimated_hours=4.0,
            auto_migrate_pct=80.0,
            risks=["Script Task needs review"],
        )
        assert pa.name == "pkg1"
        assert pa.estimated_hours == 4.0
        assert len(pa.risks) == 1


class TestMigrationEngineAssess:
    """Test the MigrationEngine.assess() method."""

    @pytest.mark.unit
    def test_assess_simple_package(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_sql_task("Step1")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        assert report.total_packages == 1
        assert report.total_tasks == 1
        assert report.readiness_score >= 0
        assert report.readiness_score <= 100
        assert report.estimated_total_hours > 0

    @pytest.mark.unit
    def test_assess_multiple_packages(self, config: MigrationConfig) -> None:
        pkg1 = _make_package("Pkg1", tasks=[_sql_task("A")])
        pkg2 = _make_package("Pkg2", tasks=[_sql_task("B"), _sql_task("C")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg1, pkg2])

        assert report.total_packages == 2
        assert report.total_tasks == 3
        assert len(report.packages) == 2

    @pytest.mark.unit
    def test_assess_script_task_flagged_as_risk(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_script_task("CustomScript")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        risks = report.packages[0].risks
        assert any("Script Task" in r for r in risks)

    @pytest.mark.unit
    def test_assess_high_complexity_lowers_score(self, config: MigrationConfig) -> None:
        simple_pkg = _make_package("Simple", tasks=[_sql_task("A")])
        complex_pkg = _make_package("Complex", tasks=[_script_task("S1"), _script_task("S2")])

        engine = MigrationEngine(config)
        simple_report = engine.assess([simple_pkg])
        complex_report = engine.assess([complex_pkg])

        assert simple_report.readiness_score >= complex_report.readiness_score

    @pytest.mark.unit
    def test_assess_connection_inventory(self, config: MigrationConfig) -> None:
        conn = ConnectionManager(
            name="Source_OLEDB",
            connection_type=ConnectionType.OLEDB,
            server="myserver",
            database="mydb",
        )
        pkg = _make_package(tasks=[_sql_task("A")], connections=[conn])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        assert report.total_connections == 1
        assert report.connection_inventory[0]["name"] == "Source_OLEDB"
        assert report.connection_inventory[0]["server"] == "myserver"

    @pytest.mark.unit
    def test_assess_event_handler_risk(self, config: MigrationConfig) -> None:
        eh = EventHandler(event_type="OnPreExecute", tasks=[_sql_task("Audit")])
        pkg = _make_package(tasks=[_sql_task("Work")], event_handlers=[eh])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        risks = report.packages[0].risks
        assert any("OnPreExecute" in r for r in risks)

    @pytest.mark.unit
    def test_assess_on_error_not_flagged_as_risk(self, config: MigrationConfig) -> None:
        """OnError handlers are auto-migrated, so should NOT be flagged."""
        eh = EventHandler(event_type="OnError", tasks=[_sql_task("LogError")])
        pkg = _make_package(tasks=[_sql_task("Work")], event_handlers=[eh])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        risks = report.packages[0].risks
        assert not any("OnError" in r for r in risks)

    @pytest.mark.unit
    def test_assess_partial_parse_flagged(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_sql_task("A")])
        pkg.status = "partial"
        pkg.warnings.append("XML error in element X")
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        risks = report.packages[0].risks
        assert any("parse warnings" in r for r in risks)

    @pytest.mark.unit
    def test_assess_to_dict_serializable(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_sql_task("A"), _script_task("B")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        # Must be JSON-serializable
        d = report.to_dict()
        json_str = json.dumps(d)
        assert "readiness_score" in json_str

    @pytest.mark.unit
    def test_assess_auto_migrate_pct(self, config: MigrationConfig) -> None:
        # All LOW tasks = 100% auto-migratable
        pkg = _make_package(tasks=[_sql_task("A"), _sql_task("B")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])
        assert report.auto_migrate_pct == 100.0

    @pytest.mark.unit
    def test_assess_effort_estimation(self, config: MigrationConfig) -> None:
        pkg = _make_package(tasks=[_sql_task("A")])
        engine = MigrationEngine(config)
        report = engine.assess([pkg])

        assert report.estimated_total_hours > 0
        assert report.packages[0].estimated_hours > 0


# ===========================================================================
# 3. CLI: assess command
# ===========================================================================


class TestAssessCLI:
    """Tests for the 'assess' CLI command."""

    @pytest.mark.unit
    def test_assess_single_file(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["assess", str(FIXTURES_DIR / "simple_etl.dtsx")])
        assert result.exit_code == 0
        assert "Readiness Score" in result.output

    @pytest.mark.unit
    def test_assess_directory(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["assess", str(FIXTURES_DIR)])
        assert result.exit_code == 0
        assert "Package Assessment" in result.output

    @pytest.mark.unit
    def test_assess_with_strategy(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main, ["assess", str(FIXTURES_DIR / "simple_etl.dtsx"), "--strategy", "spark"]
        )
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_assess_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        out_file = tmp_path / "assessment.json"
        result = runner.invoke(
            main,
            ["assess", str(FIXTURES_DIR / "simple_etl.dtsx"), "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert "readiness_score" in data
        assert "packages" in data

    @pytest.mark.unit
    def test_assess_nonexistent_path(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["assess", "/nonexistent/path"])
        assert result.exit_code != 0

    @pytest.mark.unit
    def test_assess_complex_package(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["assess", str(FIXTURES_DIR / "complex_etl.dtsx")])
        assert result.exit_code == 0
        assert "hours" in result.output.lower()

    @pytest.mark.unit
    def test_assess_shows_connection_inventory(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["assess", str(FIXTURES_DIR / "simple_etl.dtsx")])
        assert result.exit_code == 0
        # simple_etl.dtsx should have connections
        assert "Connection Inventory" in result.output or "Readiness Score" in result.output


# ===========================================================================
# 4. CLI: validate-config command
# ===========================================================================


class TestValidateConfigCLI:
    """Tests for the 'validate-config' CLI command."""

    @pytest.mark.unit
    def test_validate_config_default(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["validate-config"])
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_validate_config_shows_strategy(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["validate-config"])
        assert result.exit_code == 0
        # Default config with no workspace_id shows warnings
        assert "Strategy" in result.output or "warning" in result.output.lower()

    @pytest.mark.unit
    def test_validate_config_custom_config(self, runner: CliRunner, tmp_path: Path) -> None:
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text("project_name: test-validate\nstrategy: spark\n")
        result = runner.invoke(main, ["-c", str(config_file), "validate-config"])
        assert result.exit_code == 0


# ===========================================================================
# 5. CLI: migrate --dry-run
# ===========================================================================


class TestMigrateDryRun:
    """Tests for the migrate --dry-run flag."""

    @pytest.mark.unit
    def test_dry_run_shows_plan(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--dry-run"],
        )
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "Migration Plan" in result.output

    @pytest.mark.unit
    def test_dry_run_does_not_write_files(self, runner: CliRunner, tmp_path: Path) -> None:
        out = tmp_path / "dryrun_output"
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--dry-run", "--output", str(out)],
        )
        assert result.exit_code == 0
        # No output files should be created
        assert not out.exists() or not list(out.rglob("*"))

    @pytest.mark.unit
    def test_dry_run_with_strategy(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "migrate",
                str(FIXTURES_DIR / "simple_etl.dtsx"),
                "--strategy", "spark",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    @pytest.mark.unit
    def test_dry_run_complex_package(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "complex_etl.dtsx"), "--dry-run"],
        )
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    @pytest.mark.unit
    def test_normal_migrate_still_works(self, runner: CliRunner, tmp_path: Path) -> None:
        """Ensure the --dry-run flag doesn't break normal migration."""
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--output", str(tmp_path / "out")],
        )
        assert result.exit_code == 0
        assert "Migration Results" in result.output
        assert (tmp_path / "out" / "pipelines").exists()


# ===========================================================================
# 6. API Facade assess() method
# ===========================================================================


class TestAPIAssess:
    """Test the SSISMigrator.assess() API method."""

    @pytest.mark.unit
    def test_assess_returns_report(self) -> None:
        from ssis_to_fabric.api import SSISMigrator

        migrator = SSISMigrator(strategy="hybrid", output_dir="output")
        report = migrator.assess(FIXTURES_DIR / "simple_etl.dtsx")
        assert report.total_packages == 1
        assert report.readiness_score >= 0

    @pytest.mark.unit
    def test_assess_directory(self) -> None:
        from ssis_to_fabric.api import SSISMigrator

        migrator = SSISMigrator(strategy="hybrid", output_dir="output")
        report = migrator.assess(FIXTURES_DIR)
        assert report.total_packages >= 2

    @pytest.mark.unit
    def test_assess_nonexistent_raises(self) -> None:
        from ssis_to_fabric.api import SSISMigrator

        migrator = SSISMigrator(strategy="hybrid", output_dir="output")
        with pytest.raises(FileNotFoundError):
            migrator.assess("/nonexistent/path")


# ===========================================================================
# 7. Imports & Exports
# ===========================================================================


class TestPhase5Exports:
    """Verify that Phase 5 types are properly exported."""

    @pytest.mark.unit
    def test_assessment_report_importable(self) -> None:
        from ssis_to_fabric import AssessmentReport

        assert AssessmentReport is not None

    @pytest.mark.unit
    def test_package_assessment_importable(self) -> None:
        from ssis_to_fabric.engine.migration_engine import PackageAssessment

        assert PackageAssessment is not None
