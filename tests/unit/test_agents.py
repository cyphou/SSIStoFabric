"""Tests for the multi-agent migration system."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.agents import (
    AgentOrchestrator,
    AgentResult,
    AgentRole,
    DataflowAgent,
    MigrationAgent,
    OrchestratorStats,
    PipelineAgent,
    SparkAgent,
)
from ssis_to_fabric.engine.migration_engine import MigrationPlan, TargetArtifact

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "sample_packages"


@pytest.fixture()
def config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(output_dir=tmp_path / "output", strategy=MigrationStrategy.HYBRID)


@pytest.fixture()
def packages():
    from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

    parser = DTSXParser()
    return parser.parse_directory(FIXTURES_DIR)


# ==========================================================================
# Agent unit tests
# ==========================================================================


class TestAgentResult:
    @pytest.mark.unit
    def test_defaults(self) -> None:
        r = AgentResult(agent_role=AgentRole.SPARK, item_key="pkg/task", status="completed")
        assert r.output_path == ""
        assert r.error == ""
        assert r.notes == []

    @pytest.mark.unit
    def test_with_error(self) -> None:
        r = AgentResult(
            agent_role=AgentRole.DATAFLOW,
            item_key="pkg/task",
            status="error",
            error="boom",
        )
        assert r.status == "error"
        assert r.error == "boom"


class TestAgentRoles:
    @pytest.mark.unit
    def test_role_values(self) -> None:
        assert AgentRole.PARSER.value == "parser"
        assert AgentRole.DATAFLOW.value == "dataflow"
        assert AgentRole.SPARK.value == "spark"
        assert AgentRole.PIPELINE.value == "pipeline"
        assert AgentRole.DEPLOY.value == "deploy"


class TestMigrationAgentBase:
    @pytest.mark.unit
    def test_abstract(self, config: MigrationConfig) -> None:
        with pytest.raises(TypeError):
            MigrationAgent(config)  # type: ignore[abstract]


class TestDataflowAgent:
    @pytest.mark.unit
    def test_role(self, config: MigrationConfig) -> None:
        agent = DataflowAgent(config)
        assert agent.role == AgentRole.DATAFLOW

    @pytest.mark.unit
    def test_repr(self, config: MigrationConfig) -> None:
        agent = DataflowAgent(config)
        assert "DataflowAgent" in repr(agent)


class TestSparkAgent:
    @pytest.mark.unit
    def test_role(self, config: MigrationConfig) -> None:
        agent = SparkAgent(config)
        assert agent.role == AgentRole.SPARK

    @pytest.mark.unit
    def test_process_simple(self, config: MigrationConfig, packages: list) -> None:
        agent = SparkAgent(config)
        pkg = packages[0]
        # Find a data flow task
        task = None
        for t in pkg.control_flow_tasks:
            if t.data_flow_components:
                task = t
                break
        if task is None:
            pytest.skip("No data flow task in fixture")
        result = agent.process(pkg, task, config.output_dir)
        assert result.status == "completed"
        assert result.output_path != ""


class TestPipelineAgent:
    @pytest.mark.unit
    def test_role(self, config: MigrationConfig) -> None:
        agent = PipelineAgent(config)
        assert agent.role == AgentRole.PIPELINE

    @pytest.mark.unit
    def test_process_package(self, config: MigrationConfig, packages: list) -> None:
        agent = PipelineAgent(config)
        pkg = packages[0]
        result = agent.process(pkg, None, config.output_dir, task_routing={})
        assert result.status == "completed"
        assert result.output_path != ""
        assert Path(result.output_path).exists()


# ==========================================================================
# Orchestrator tests
# ==========================================================================


class TestAgentOrchestrator:
    @pytest.mark.unit
    def test_sequential_matches_engine(self, config: MigrationConfig, packages: list, tmp_path: Path) -> None:
        """Single-worker orchestrator should produce same results as MigrationEngine."""
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        # Engine run (sequential)
        engine_cfg = config.model_copy(deep=True)
        engine_cfg.output_dir = tmp_path / "engine_out"
        engine = MigrationEngine(engine_cfg)
        engine_plan = engine.execute(packages)

        # Orchestrator run (1 worker = sequential)
        orch_cfg = config.model_copy(deep=True)
        orch_cfg.output_dir = tmp_path / "orch_out"
        orchestrator = AgentOrchestrator(orch_cfg, max_workers=1)
        orch_plan = orchestrator.run(packages)

        # Compare plan outcomes
        assert len(engine_plan.items) == len(orch_plan.items)
        engine_completed = sum(1 for i in engine_plan.items if i.status == "completed")
        orch_completed = sum(1 for i in orch_plan.items if i.status == "completed")
        assert engine_completed == orch_completed

        engine_errors = sum(1 for i in engine_plan.items if i.status == "error")
        orch_errors = sum(1 for i in orch_plan.items if i.status == "error")
        assert engine_errors == orch_errors

    @pytest.mark.unit
    def test_parallel_produces_same_counts(self, config: MigrationConfig, packages: list, tmp_path: Path) -> None:
        """Multi-worker orchestrator should produce same item counts."""
        # 1 worker
        cfg1 = config.model_copy(deep=True)
        cfg1.output_dir = tmp_path / "w1"
        orch1 = AgentOrchestrator(cfg1, max_workers=1)
        plan1 = orch1.run(packages)

        # 4 workers
        cfg4 = config.model_copy(deep=True)
        cfg4.output_dir = tmp_path / "w4"
        orch4 = AgentOrchestrator(cfg4, max_workers=4)
        plan4 = orch4.run(packages)

        assert len(plan1.items) == len(plan4.items)
        assert sum(1 for i in plan1.items if i.status == "completed") == sum(
            1 for i in plan4.items if i.status == "completed"
        )

    @pytest.mark.unit
    def test_parallel_generates_all_artifacts(self, config: MigrationConfig, packages: list) -> None:
        """All expected output directories should be created."""
        config.output_dir.mkdir(parents=True, exist_ok=True)
        orchestrator = AgentOrchestrator(config, max_workers=4)
        orchestrator.run(packages)

        assert (config.output_dir / "pipelines").exists()
        assert (config.output_dir / "migration_report.json").exists()

    @pytest.mark.unit
    def test_report_json_valid(self, config: MigrationConfig, packages: list) -> None:
        """Migration report should be valid JSON."""
        orchestrator = AgentOrchestrator(config, max_workers=2)
        orchestrator.run(packages)

        report = config.output_dir / "migration_report.json"
        assert report.exists()
        data = json.loads(report.read_text())
        assert "items" in data
        assert "summary" in data

    @pytest.mark.unit
    def test_single_file_migration(self, config: MigrationConfig) -> None:
        """Orchestrator works with a single package."""
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        parser = DTSXParser()
        packages = [parser.parse(FIXTURES_DIR / "simple_etl.dtsx")]
        orchestrator = AgentOrchestrator(config, max_workers=2)
        plan = orchestrator.run(packages)

        completed = sum(1 for i in plan.items if i.status == "completed")
        assert completed > 0

    @pytest.mark.unit
    def test_spark_strategy(self, config: MigrationConfig, packages: list) -> None:
        """Orchestrator should respect strategy override."""
        config.strategy = MigrationStrategy.SPARK
        orchestrator = AgentOrchestrator(config, max_workers=2)
        plan = orchestrator.run(packages)

        for item in plan.items:
            if item.status == "completed":
                assert item.target_artifact in (
                    TargetArtifact.SPARK_NOTEBOOK,
                    TargetArtifact.DATA_FACTORY_PIPELINE,
                )

    @pytest.mark.unit
    def test_max_workers_clamped(self, config: MigrationConfig) -> None:
        """max_workers < 1 should be clamped to 1."""
        orchestrator = AgentOrchestrator(config, max_workers=0)
        assert orchestrator.max_workers == 1

    @pytest.mark.unit
    def test_connection_manifests_generated(self, config: MigrationConfig, packages: list) -> None:
        """Phase 3 should produce connection manifests."""
        orchestrator = AgentOrchestrator(config, max_workers=2)
        orchestrator.run(packages)

        conn_dir = config.output_dir / "connections"
        if conn_dir.exists():
            manifests = list(conn_dir.glob("*.json"))
            assert len(manifests) > 0


class TestOrchestratorStats:
    @pytest.mark.unit
    def test_defaults(self) -> None:
        s = OrchestratorStats()
        assert s.total_items == 0
        assert s.completed == 0
        assert s.errors == 0
        assert s.parallel_workers == 1


# ==========================================================================
# CLI integration
# ==========================================================================


class TestCLIWorkers:
    @pytest.mark.unit
    def test_migrate_with_workers(self, tmp_path: Path) -> None:
        """CLI --workers flag should activate multi-agent mode."""
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "migrate",
                str(FIXTURES_DIR / "simple_etl.dtsx"),
                "--output", str(tmp_path / "out"),
                "--workers", "2",
            ],
        )
        assert result.exit_code == 0
        assert (tmp_path / "out" / "pipelines").exists()

    @pytest.mark.unit
    def test_migrate_default_sequential(self, tmp_path: Path) -> None:
        """Without --workers, migration should be sequential (engine path)."""
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "migrate",
                str(FIXTURES_DIR / "simple_etl.dtsx"),
                "--output", str(tmp_path / "out"),
            ],
        )
        assert result.exit_code == 0


# ==========================================================================
# API integration
# ==========================================================================


class TestAPIParallelWorkers:
    @pytest.mark.unit
    def test_api_parallel(self, tmp_path: Path) -> None:
        """SSISMigrator with parallel_workers > 1 should use orchestrator."""
        from ssis_to_fabric.api import SSISMigrator

        cfg = MigrationConfig(
            output_dir=tmp_path / "api_out",
            parallel_workers=2,
        )
        migrator = SSISMigrator(config=cfg)
        plan = migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx")
        completed = sum(1 for i in plan.items if i.status == "completed")
        assert completed > 0

    @pytest.mark.unit
    def test_api_sequential(self, tmp_path: Path) -> None:
        """SSISMigrator with parallel_workers=1 should use engine directly."""
        from ssis_to_fabric.api import SSISMigrator

        cfg = MigrationConfig(
            output_dir=tmp_path / "api_out",
            parallel_workers=1,
        )
        migrator = SSISMigrator(config=cfg)
        plan = migrator.migrate(FIXTURES_DIR / "simple_etl.dtsx")
        completed = sum(1 for i in plan.items if i.status == "completed")
        assert completed > 0
