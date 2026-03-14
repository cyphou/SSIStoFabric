"""Tests for Phase 24 — Performance Optimization."""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.performance import (
    MigrationProfiler,
    OptimizationType,
    analyze_notebook_for_optimizations,
    analyze_pipeline_parallelism,
    recommend_capacity,
    write_benchmark_report,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestProfiler:
    def test_start_stop(self):
        p = MigrationProfiler()
        p.start("parse", "pkg1")
        m = p.stop(artifacts=2, lines=100)
        assert m.operation == "parse"
        assert m.duration_seconds >= 0
        assert m.artifacts_generated == 2

    def test_multiple_operations(self):
        p = MigrationProfiler()
        p.start("op1")
        p.stop()
        p.start("op2")
        p.stop()
        assert len(p.metrics) == 2

    def test_summary(self):
        p = MigrationProfiler()
        p.start("op")
        p.stop(artifacts=1, lines=50)
        s = p.summary()
        assert s["total_operations"] == 1
        assert s["total_artifacts"] == 1


class TestSparkOptimizer:
    def test_broadcast_hint(self):
        code = "df = source.join(lookup, on='key')"
        opts = analyze_notebook_for_optimizations(code)
        assert any(o.optimization_type == OptimizationType.BROADCAST_JOIN for o in opts)

    def test_cache_suggestion(self):
        code = "df.count()\ndf.show()"
        opts = analyze_notebook_for_optimizations(code)
        assert any(o.optimization_type == OptimizationType.CACHE for o in opts)

    def test_coalesce_suggestion(self):
        code = "df.write.format('delta').save(path)"
        opts = analyze_notebook_for_optimizations(code)
        assert any(o.optimization_type == OptimizationType.COALESCE for o in opts)

    def test_repartition_suggestion(self):
        code = "df.groupBy('key').agg(sum('val'))"
        opts = analyze_notebook_for_optimizations(code)
        assert any(o.optimization_type == OptimizationType.REPARTITION for o in opts)

    def test_no_issues(self):
        code = "x = 1 + 2"
        opts = analyze_notebook_for_optimizations(code)
        assert len(opts) == 0


class TestParallelismAdvisor:
    def test_sequential_pipeline(self):
        pipeline = {
            "name": "p1",
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": [{"activity": "A"}]},
                ],
            },
        }
        advice = analyze_pipeline_parallelism(pipeline)
        assert advice.max_parallelism == 1

    def test_parallel_pipeline(self):
        pipeline = {
            "name": "p1",
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": []},
                    {"name": "C", "dependsOn": [{"activity": "A"}, {"activity": "B"}]},
                ],
            },
        }
        advice = analyze_pipeline_parallelism(pipeline)
        assert advice.max_parallelism >= 2


class TestCapacity:
    def test_small_workload(self):
        cap = recommend_capacity(2, 1)
        assert cap.recommended_sku in ("F2", "F4")
        assert cap.monthly_cost_estimate > 0

    def test_large_workload(self):
        cap = recommend_capacity(100, 50, daily_executions=10)
        assert cap.estimated_cu_per_hour > 10

    def test_workload_type(self):
        cap = recommend_capacity(5, 0)
        assert cap.workload_type == "pipelines"
        cap2 = recommend_capacity(0, 5)
        assert cap2.workload_type == "notebooks"


class TestBenchmarkReport:
    def test_write_report(self, tmp_path: Path):
        p = MigrationProfiler()
        p.start("op")
        p.stop()
        cap = recommend_capacity(1, 1)
        path = write_benchmark_report(p, [], cap, tmp_path / "report.json")
        assert path.exists()


class TestCLI:
    def test_benchmark_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["benchmark", "--help"])
        assert result.exit_code == 0
