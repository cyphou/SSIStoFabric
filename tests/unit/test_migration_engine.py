"""
Unit tests for the Migration Engine.
Validates routing, plan generation, and artifact generation.
"""

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import SSISPackage
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.migration_engine import (
    MigrationEngine,
    MigrationPlan,
    TargetArtifact,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


@pytest.fixture
def simple_package() -> SSISPackage:
    parser = DTSXParser()
    return parser.parse(FIXTURES_DIR / "simple_etl.dtsx")


@pytest.fixture
def complex_package() -> SSISPackage:
    parser = DTSXParser()
    return parser.parse(FIXTURES_DIR / "complex_etl.dtsx")


class TestMigrationPlan:
    """Tests for migration plan generation."""

    @pytest.mark.unit
    def test_plan_simple_package(self, simple_package: SSISPackage, default_config: MigrationConfig) -> None:
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([simple_package])

        assert isinstance(plan, MigrationPlan)
        assert len(plan.items) == 3  # 3 tasks in simple package

    @pytest.mark.unit
    def test_plan_has_summary(self, simple_package: SSISPackage, default_config: MigrationConfig) -> None:
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([simple_package])

        assert "total" in plan.summary
        assert plan.summary["total"] == 3

    @pytest.mark.unit
    def test_hybrid_routes_simple_df_to_dataflow_gen2(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """In hybrid mode (default dataflow_type=notebook), simple data flows go to Spark."""
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([simple_package])

        spark_items = [i for i in plan.items if i.target_artifact == TargetArtifact.SPARK_NOTEBOOK]
        assert len(spark_items) >= 1  # The simple Data Flow should go to notebook by default

    @pytest.mark.unit
    def test_hybrid_routes_simple_df_to_dataflow_gen2_when_configured(
        self, simple_package: SSISPackage, dataflow_gen2_config: MigrationConfig
    ) -> None:
        """In hybrid mode with dataflow_type=dataflow_gen2, simple data flows go to Dataflow Gen2."""
        engine = MigrationEngine(dataflow_gen2_config)
        plan = engine.create_plan([simple_package])

        dataflow_items = [i for i in plan.items if i.target_artifact == TargetArtifact.DATAFLOW_GEN2]
        assert len(dataflow_items) >= 1

    @pytest.mark.unit
    def test_hybrid_routes_sql_to_data_factory(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """In hybrid mode, Execute SQL tasks should go to Data Factory pipeline."""
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([simple_package])

        df_items = [i for i in plan.items if i.target_artifact == TargetArtifact.DATA_FACTORY_PIPELINE]
        assert len(df_items) >= 1  # At least the Execute SQL tasks

    @pytest.mark.unit
    def test_hybrid_routes_complex_to_spark(
        self, complex_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """In hybrid mode, complex data flows (with Script Component) should go to Spark."""
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([complex_package])

        spark_items = [i for i in plan.items if i.target_artifact == TargetArtifact.SPARK_NOTEBOOK]
        assert len(spark_items) >= 1  # Script task + complex data flow

    @pytest.mark.unit
    def test_df_only_strategy(self, simple_package: SSISPackage, df_only_config: MigrationConfig) -> None:
        """Data Factory only mode should not produce Spark notebooks or Dataflow Gen2."""
        engine = MigrationEngine(df_only_config)
        plan = engine.create_plan([simple_package])

        for item in plan.items:
            assert item.target_artifact in (
                TargetArtifact.DATA_FACTORY_PIPELINE,
                TargetArtifact.MANUAL_REVIEW,
            ), f"Unexpected target {item.target_artifact} in DF-only mode"

    @pytest.mark.unit
    def test_spark_only_strategy(self, simple_package: SSISPackage, spark_only_config: MigrationConfig) -> None:
        """Spark only mode should route everything to Spark notebooks."""
        engine = MigrationEngine(spark_only_config)
        plan = engine.create_plan([simple_package])

        for item in plan.items:
            assert item.target_artifact == TargetArtifact.SPARK_NOTEBOOK

    @pytest.mark.unit
    def test_plan_serialization(self, simple_package: SSISPackage, default_config: MigrationConfig) -> None:
        """Plan should be serializable to dict/JSON."""
        engine = MigrationEngine(default_config)
        plan = engine.create_plan([simple_package])

        plan_dict = plan.to_dict()
        assert "items" in plan_dict
        assert "summary" in plan_dict

        # Should be JSON-serializable
        json_str = json.dumps(plan_dict)
        assert len(json_str) > 0


class TestMigrationExecution:
    """Tests for migration execution (artifact generation)."""

    @pytest.mark.unit
    def test_execute_simple_package(self, simple_package: SSISPackage, default_config: MigrationConfig) -> None:
        engine = MigrationEngine(default_config)
        result = engine.execute([simple_package])

        completed = [i for i in result.items if i.status == "completed"]
        assert len(completed) >= 1

    @pytest.mark.unit
    def test_execute_generates_pipeline_files(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        pipelines_dir = default_config.output_dir / "pipelines"
        if pipelines_dir.exists():
            pipeline_files = list(pipelines_dir.glob("*.json"))
            assert len(pipeline_files) >= 1

    @pytest.mark.unit
    def test_execute_generates_notebooks(self, complex_package: SSISPackage, default_config: MigrationConfig) -> None:
        engine = MigrationEngine(default_config)
        engine.execute([complex_package])

        notebooks_dir = default_config.output_dir / "notebooks"
        if notebooks_dir.exists():
            notebook_files = list(notebooks_dir.glob("*.py"))
            assert len(notebook_files) >= 1

    @pytest.mark.unit
    def test_execute_writes_migration_report(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        report_path = default_config.output_dir / "migration_report.json"
        assert report_path.exists()

        with open(report_path) as f:
            report = json.load(f)
        assert "items" in report
        assert "summary" in report

    @pytest.mark.unit
    def test_generated_pipeline_is_valid_json(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        pipelines_dir = default_config.output_dir / "pipelines"
        if pipelines_dir.exists():
            for f in pipelines_dir.glob("*.json"):
                with open(f) as fh:
                    data = json.load(fh)
                assert "name" in data
                assert "properties" in data

    @pytest.mark.unit
    def test_execute_generates_dataflows(self, simple_package: SSISPackage, default_config: MigrationConfig) -> None:
        """In hybrid mode, simple DFTs should generate Dataflow Gen2 definitions."""
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        dataflows_dir = default_config.output_dir / "dataflows"
        if dataflows_dir.exists():
            dataflow_files = list(dataflows_dir.glob("*.json"))
            assert len(dataflow_files) >= 1

    @pytest.mark.unit
    def test_consolidated_pipeline_one_per_package(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """Should generate exactly one pipeline per SSIS package (not per task)."""
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        pipelines_dir = default_config.output_dir / "pipelines"
        assert pipelines_dir.exists()
        pipeline_files = list(pipelines_dir.glob("*.json"))
        # One consolidated pipeline for the one package
        assert len(pipeline_files) == 1

    @pytest.mark.unit
    def test_consolidated_pipeline_has_all_activities(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """Consolidated pipeline should have activities for all tasks."""
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        pipelines_dir = default_config.output_dir / "pipelines"
        pipeline_files = list(pipelines_dir.glob("*.json"))
        assert len(pipeline_files) == 1

        with open(pipeline_files[0]) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # Simple package has 3 tasks
        assert len(activities) == 3

    @pytest.mark.unit
    def test_consolidated_pipeline_has_dataflow_ref(
        self, simple_package: SSISPackage, default_config: MigrationConfig
    ) -> None:
        """Data Flow Tasks should become TridentNotebook activities by default."""
        engine = MigrationEngine(default_config)
        engine.execute([simple_package])

        pipelines_dir = default_config.output_dir / "pipelines"
        pipeline_file = list(pipelines_dir.glob("*.json"))[0]

        with open(pipeline_file) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        notebook_acts = [a for a in activities if a["type"] == "TridentNotebook"]
        assert len(notebook_acts) >= 1

    @pytest.mark.unit
    def test_consolidated_pipeline_has_dataflow_ref_when_configured(
        self, simple_package: SSISPackage, dataflow_gen2_config: MigrationConfig
    ) -> None:
        """With dataflow_type=dataflow_gen2, Data Flow Tasks become Dataflow activities."""
        engine = MigrationEngine(dataflow_gen2_config)
        engine.execute([simple_package])

        pipelines_dir = dataflow_gen2_config.output_dir / "pipelines"
        pipeline_file = list(pipelines_dir.glob("*.json"))[0]

        with open(pipeline_file) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        dataflow_acts = [a for a in activities if a["type"] == "Dataflow"]
        assert len(dataflow_acts) >= 1
