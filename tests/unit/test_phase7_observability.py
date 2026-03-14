"""Phase 7 — Observability & Diagnostics tests.

Tests for:
- Correlation ID binding / retrieval / clearing
- Audit log writing
- Progress callbacks in MigrationEngine.execute()
- Progress callbacks in AgentOrchestrator.run()
- Per-item timing fields on MigrationItem / MigrationPlan
- HTML report timing header and SVG status chart
- to_dict() includes new timing fields
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.migration_engine import (
    MigrationItem,
    MigrationPlan,
    TargetArtifact,
)

# ===================================================================
# Fixtures
# ===================================================================


def _make_package(name: str = "TestPkg") -> SSISPackage:
    """Create a minimal SSIS package for testing."""
    return SSISPackage(
        name=name,
        file_path="",
        connection_managers=[
            ConnectionManager(
                name="src_conn",
                connection_type=ConnectionType.OLEDB,
                connection_string="Server=.;Database=db;",
            ),
        ],
        variables=[],
        parameters=[],
        control_flow_tasks=[
            ControlFlowTask(
                id="1",
                ref_id="1",
                name="Load_Data",
                task_type=TaskType.DATA_FLOW,
                description="",
                migration_complexity=MigrationComplexity.LOW,
                data_flow_components=[
                    DataFlowComponent(
                        name="src",
                        component_type=DataFlowComponentType.OLE_DB_SOURCE,
                        migration_complexity=MigrationComplexity.LOW,
                    ),
                    DataFlowComponent(
                        name="dest",
                        component_type=DataFlowComponentType.OLE_DB_DESTINATION,
                        migration_complexity=MigrationComplexity.LOW,
                    ),
                ],
            ),
            ControlFlowTask(
                id="2",
                ref_id="2",
                name="Execute_SQL",
                task_type=TaskType.EXECUTE_SQL,
                description="",
                migration_complexity=MigrationComplexity.LOW,
            ),
        ],
        precedence_constraints=[],
        event_handlers=[],
    )


# ===================================================================
# Correlation ID tests
# ===================================================================


class TestCorrelationID:
    def test_bind_generates_id(self):
        from ssis_to_fabric.logging_config import bind_correlation_id, clear_correlation_id, get_correlation_id

        cid = bind_correlation_id()
        assert cid.startswith("ssis2fabric-")
        assert len(cid) > 12
        assert get_correlation_id() == cid
        clear_correlation_id()

    def test_bind_custom_id(self):
        from ssis_to_fabric.logging_config import bind_correlation_id, clear_correlation_id, get_correlation_id

        cid = bind_correlation_id("my-custom-id")
        assert cid == "my-custom-id"
        assert get_correlation_id() == "my-custom-id"
        clear_correlation_id()

    def test_clear_resets(self):
        from ssis_to_fabric.logging_config import bind_correlation_id, clear_correlation_id, get_correlation_id

        bind_correlation_id("test-id")
        clear_correlation_id()
        assert get_correlation_id() == ""

    def test_default_empty(self):
        from ssis_to_fabric.logging_config import clear_correlation_id, get_correlation_id

        clear_correlation_id()
        assert get_correlation_id() == ""


# ===================================================================
# Audit log tests
# ===================================================================


class TestAuditLog:
    def test_write_audit_entry(self):
        from ssis_to_fabric.logging_config import (
            bind_correlation_id,
            clear_correlation_id,
            configure_audit_log,
            write_audit_entry,
        )

        with tempfile.TemporaryDirectory() as tmp:
            audit_path = Path(tmp) / "audit.jsonl"
            configure_audit_log(audit_path)
            bind_correlation_id("test-cid")

            write_audit_entry("migration_started", detail={"packages": 3})
            write_audit_entry("migration_completed", detail={"completed": 3})

            lines = audit_path.read_text().strip().split("\n")
            assert len(lines) == 2

            entry1 = json.loads(lines[0])
            assert entry1["action"] == "migration_started"
            assert entry1["correlation_id"] == "test-cid"
            assert entry1["detail"]["packages"] == 3
            assert "timestamp" in entry1

            entry2 = json.loads(lines[1])
            assert entry2["action"] == "migration_completed"

            # Cleanup
            configure_audit_log(None)
            clear_correlation_id()

    def test_no_audit_when_unconfigured(self):
        from ssis_to_fabric.logging_config import configure_audit_log, write_audit_entry

        configure_audit_log(None)
        # Should not raise
        write_audit_entry("test_action")

    def test_audit_creates_parent_dirs(self):
        from ssis_to_fabric.logging_config import configure_audit_log, write_audit_entry

        with tempfile.TemporaryDirectory() as tmp:
            audit_path = Path(tmp) / "sub" / "dir" / "audit.jsonl"
            configure_audit_log(audit_path)
            write_audit_entry("test")
            assert audit_path.exists()
            configure_audit_log(None)


# ===================================================================
# MigrationItem & MigrationPlan timing fields
# ===================================================================


class TestTimingFields:
    def test_migration_item_has_timing(self):
        item = MigrationItem(
            source_package="pkg",
            source_task="task",
            task_type="DATA_FLOW",
            target_artifact=TargetArtifact.SPARK_NOTEBOOK,
            complexity=MigrationComplexity.LOW,
        )
        assert item.started_at == ""
        assert item.completed_at == ""
        assert item.elapsed_ms == 0.0

    def test_migration_plan_has_timing(self):
        plan = MigrationPlan(project_name="test")
        assert plan.completed_at == ""
        assert plan.total_elapsed_ms == 0.0
        assert plan.correlation_id == ""

    def test_to_dict_includes_timing(self):
        item = MigrationItem(
            source_package="pkg",
            source_task="task",
            task_type="DATA_FLOW",
            target_artifact=TargetArtifact.SPARK_NOTEBOOK,
            complexity=MigrationComplexity.LOW,
            started_at="2026-01-01T00:00:00Z",
            completed_at="2026-01-01T00:00:01Z",
            elapsed_ms=1000.0,
        )
        plan = MigrationPlan(
            project_name="test",
            completed_at="2026-01-01T00:01:00Z",
            total_elapsed_ms=60000.0,
            correlation_id="test-cid",
        )
        plan.items.append(item)

        d = plan.to_dict()
        assert d["completed_at"] == "2026-01-01T00:01:00Z"
        assert d["total_elapsed_ms"] == 60000.0
        assert d["correlation_id"] == "test-cid"
        assert d["items"][0]["started_at"] == "2026-01-01T00:00:00Z"
        assert d["items"][0]["elapsed_ms"] == 1000.0


# ===================================================================
# Progress callback tests for MigrationEngine.execute()
# ===================================================================


class TestEngineProgressCallbacks:
    def test_execute_fires_callbacks(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.migration_engine import MigrationEngine

            engine = MigrationEngine(config)
            pkg = _make_package()

            events = []

            def on_progress(event: str, data: dict) -> None:
                events.append((event, data))

            engine.execute([pkg], progress_callback=on_progress)

            event_types = [e[0] for e in events]
            assert "item_completed" in event_types
            assert "phase_completed" in event_types
            assert "migration_completed" in event_types

    def test_execute_records_timing(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.migration_engine import MigrationEngine

            engine = MigrationEngine(config)
            pkg = _make_package()

            result = engine.execute([pkg])

            assert result.total_elapsed_ms > 0
            assert result.completed_at != ""
            assert result.correlation_id != ""

            # At least some items should have timing
            completed = [i for i in result.items if i.status == "completed"]
            assert len(completed) >= 1

    def test_execute_without_callback(self):
        """execute() without progress_callback should still work."""
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.migration_engine import MigrationEngine

            engine = MigrationEngine(config)
            pkg = _make_package()

            result = engine.execute([pkg])
            assert result.total_elapsed_ms > 0

    def test_execute_writes_timing_in_report(self):
        """migration_report.json should include timing fields."""
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.migration_engine import MigrationEngine

            engine = MigrationEngine(config)
            pkg = _make_package()
            engine.execute([pkg])

            report_path = Path(tmp) / "migration_report.json"
            assert report_path.exists()

            with open(report_path) as f:
                report = json.load(f)

            assert "completed_at" in report
            assert "total_elapsed_ms" in report
            assert "correlation_id" in report
            assert report["total_elapsed_ms"] > 0


# ===================================================================
# Progress callback tests for AgentOrchestrator.run()
# ===================================================================


class TestOrchestratorProgressCallbacks:
    def test_orchestrator_fires_callbacks(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        config.parallel_workers = 2
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.agents import AgentOrchestrator

            orchestrator = AgentOrchestrator(config, max_workers=2)
            pkg = _make_package()

            events = []

            def on_progress(event: str, data: dict) -> None:
                events.append((event, data))

            orchestrator.run([pkg], progress_callback=on_progress)

            event_types = [e[0] for e in events]
            assert "item_completed" in event_types
            assert "phase_completed" in event_types
            assert "migration_completed" in event_types

    def test_orchestrator_records_timing(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        config.parallel_workers = 2
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.agents import AgentOrchestrator

            orchestrator = AgentOrchestrator(config, max_workers=2)
            pkg = _make_package()

            result = orchestrator.run([pkg])

            assert result.total_elapsed_ms > 0
            assert result.completed_at != ""
            assert result.correlation_id != ""

    def test_orchestrator_stats_timing(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        config.parallel_workers = 2
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)
            from ssis_to_fabric.engine.agents import AgentOrchestrator

            orchestrator = AgentOrchestrator(config, max_workers=2)
            pkg = _make_package()

            result = orchestrator.run([pkg])

            stats = orchestrator._compute_stats(result)
            assert stats.completed >= 1


# ===================================================================
# API facade progress_callback threading
# ===================================================================


class TestAPIProgressCallback:
    def test_migrate_with_callback(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)

            from ssis_to_fabric.api import SSISMigrator

            migrator = SSISMigrator(config=config)

            events = []

            def on_progress(event: str, data: dict) -> None:
                events.append(event)

            # We need a real package to analyze
            # Use the mock approach — patch analyze to return our fixture
            with patch.object(migrator, "analyze", return_value=[_make_package()]):
                result = migrator.migrate(
                    "fake/path",
                    progress_callback=on_progress,
                )

            assert "migration_completed" in events
            assert result.total_elapsed_ms > 0

    def test_migrate_without_callback(self):
        config = MigrationConfig()
        config.strategy = MigrationStrategy.HYBRID
        with tempfile.TemporaryDirectory() as tmp:
            config.output_dir = Path(tmp)

            from ssis_to_fabric.api import SSISMigrator

            migrator = SSISMigrator(config=config)
            with patch.object(migrator, "analyze", return_value=[_make_package()]):
                result = migrator.migrate("fake/path")

            assert result.total_elapsed_ms > 0


# ===================================================================
# HTML Report with timing and SVG chart
# ===================================================================


class TestReportWithTiming:
    def _make_report_json(self, tmp_dir: Path) -> Path:
        """Create a migration_report.json with timing data."""
        report = {
            "project_name": "TestProject",
            "created_at": "2026-03-14T10:00:00Z",
            "completed_at": "2026-03-14T10:00:05Z",
            "total_elapsed_ms": 5000.0,
            "correlation_id": "ssis2fabric-abc123",
            "strategy": "hybrid",
            "items": [
                {
                    "source_package": "Pkg1",
                    "source_task": "Task1",
                    "task_type": "DATA_FLOW",
                    "target_artifact": "spark_notebook",
                    "complexity": "LOW",
                    "output_path": "notebooks/task1.py",
                    "status": "completed",
                    "notes": [],
                    "started_at": "2026-03-14T10:00:01Z",
                    "completed_at": "2026-03-14T10:00:02Z",
                    "elapsed_ms": 1200.5,
                },
                {
                    "source_package": "Pkg1",
                    "source_task": "Task2",
                    "task_type": "EXECUTE_SQL",
                    "target_artifact": "data_factory_pipeline",
                    "complexity": "LOW",
                    "output_path": "pipelines/Pkg1.json",
                    "status": "completed",
                    "notes": [],
                    "started_at": "2026-03-14T10:00:02Z",
                    "completed_at": "2026-03-14T10:00:03Z",
                    "elapsed_ms": 800.0,
                },
                {
                    "source_package": "Pkg2",
                    "source_task": "Task3",
                    "task_type": "DATA_FLOW",
                    "target_artifact": "spark_notebook",
                    "complexity": "HIGH",
                    "output_path": "",
                    "status": "error",
                    "notes": ["Generation error: something failed"],
                    "started_at": "2026-03-14T10:00:03Z",
                    "completed_at": "2026-03-14T10:00:04Z",
                    "elapsed_ms": 500.0,
                },
            ],
            "summary": {
                "total": 3,
                "spark_notebook": 2,
                "data_factory_pipeline": 1,
                "complexity_breakdown": {"LOW": 2, "HIGH": 1},
            },
            "errors": [
                {
                    "source": "Pkg2/Task3",
                    "severity": "error",
                    "message": "Generation error: something failed",
                    "suggested_fix": "Check logs",
                },
            ],
        }
        path = tmp_dir / "migration_report.json"
        path.write_text(json.dumps(report, indent=2))
        return path

    def test_html_includes_timing_header(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        with tempfile.TemporaryDirectory() as tmp:
            self._make_report_json(Path(tmp))
            gen = ReportGenerator()
            html_path = gen.generate(tmp)
            html = html_path.read_text()

            assert "5.0s" in html  # total elapsed
            assert "ssis2fabric-abc123" in html  # correlation ID

    def test_html_includes_per_item_timing(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        with tempfile.TemporaryDirectory() as tmp:
            self._make_report_json(Path(tmp))
            gen = ReportGenerator()
            html_path = gen.generate(tmp)
            html = html_path.read_text()

            assert "1201ms" in html or "1200ms" in html  # item elapsed formatting
            assert "800ms" in html

    def test_html_includes_svg_chart(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        with tempfile.TemporaryDirectory() as tmp:
            self._make_report_json(Path(tmp))
            gen = ReportGenerator()
            html_path = gen.generate(tmp)
            html = html_path.read_text()

            assert "<svg" in html
            assert "Status Distribution" in html
            assert "#16a34a" in html  # green for completed
            assert "#dc2626" in html  # red for errors

    def test_html_no_chart_when_no_items(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        with tempfile.TemporaryDirectory() as tmp:
            report = {
                "project_name": "Empty",
                "strategy": "hybrid",
                "items": [],
                "summary": {"total": 0},
                "errors": [],
            }
            (Path(tmp) / "migration_report.json").write_text(json.dumps(report))
            gen = ReportGenerator()
            html_path = gen.generate(tmp)
            html = html_path.read_text()

            # No SVG chart for empty report
            assert "<svg" not in html

    def test_html_time_column_shows_dash_when_zero(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        with tempfile.TemporaryDirectory() as tmp:
            report = {
                "project_name": "Test",
                "strategy": "hybrid",
                "items": [
                    {
                        "source_package": "P",
                        "source_task": "T",
                        "task_type": "DATA_FLOW",
                        "target_artifact": "spark_notebook",
                        "complexity": "LOW",
                        "status": "completed",
                        "notes": [],
                        "elapsed_ms": 0,
                    },
                ],
                "summary": {"total": 1, "complexity_breakdown": {"LOW": 1}},
                "errors": [],
            }
            (Path(tmp) / "migration_report.json").write_text(json.dumps(report))
            gen = ReportGenerator()
            html_path = gen.generate(tmp)
            html = html_path.read_text()

            # The time column should show "-" when elapsed_ms is 0
            assert "<td>-</td>" in html


# ===================================================================
# SVG chart rendering edge cases
# ===================================================================


class TestStatusChart:
    def test_chart_all_completed(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        svg = gen._render_status_chart(10, 0, 0, 10)
        assert "Completed: 10" in svg
        assert "100%" in svg

    def test_chart_mixed(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        svg = gen._render_status_chart(5, 3, 2, 10)
        assert "Completed: 5" in svg
        assert "Errors: 3" in svg
        assert "Manual: 2" in svg

    def test_chart_empty(self):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        svg = gen._render_status_chart(0, 0, 0, 0)
        assert svg == ""

    def test_chart_with_other(self):
        """When total > completed + errors + manual, 'Other' slice appears."""
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        svg = gen._render_status_chart(5, 1, 1, 10)
        assert "Other: 3" in svg


# ===================================================================
# OrchestratorStats new fields
# ===================================================================


class TestOrchestratorStatsFields:
    def test_stats_has_timing_fields(self):
        from ssis_to_fabric.engine.agents import OrchestratorStats

        stats = OrchestratorStats()
        assert stats.total_elapsed_ms == 0.0
        assert stats.phase1_elapsed_ms == 0.0
        assert stats.phase2_elapsed_ms == 0.0

    def test_stats_fields_populated(self):
        from ssis_to_fabric.engine.agents import OrchestratorStats

        stats = OrchestratorStats(
            total_items=10,
            completed=8,
            errors=1,
            manual=1,
            parallel_workers=4,
            total_elapsed_ms=5000.0,
            phase1_elapsed_ms=3000.0,
            phase2_elapsed_ms=2000.0,
        )
        assert stats.total_elapsed_ms == 5000.0
        assert stats.phase1_elapsed_ms == 3000.0
