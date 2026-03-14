"""Tests for Phase 26 — Intelligent Migration."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.intelligence import (
    KNOWLEDGE_BASE,
    GeneratedTest,
    PackagePattern,
    PatternMatch,
    StrategyRecommendation,
    classify_package,
    generate_validation_tests,
    natural_language_query,
    recommend_strategy,
    search_knowledge_base,
    write_intelligence_report,
    write_test_file,
)

if TYPE_CHECKING:
    from pathlib import Path


def _make_pkg(name="test", tasks=None, data_flows=None, connections=None, variables=None):
    return SimpleNamespace(
        name=name,
        control_flow_tasks=tasks or [],
        data_flow_components=data_flows or [],
        connection_managers=connections or [],
        variables=variables or [],
        total_tasks=len(tasks or []),
        total_data_flows=len(data_flows or []),
    )


def _task(name="t", task_type="ExecuteSQLTask"):
    return SimpleNamespace(name=name, task_type=task_type)


def _var(name="v"):
    return SimpleNamespace(name=name)


class TestPatternClassification:
    def test_simple_copy(self):
        pkg = _make_pkg(tasks=[_task("Copy Data")])
        m = classify_package(pkg)
        assert isinstance(m, PatternMatch)
        assert m.confidence > 0

    def test_orchestrator(self):
        pkg = _make_pkg(tasks=[_task("Run Child", "ExecutePackageTask"), _task("Run Child 2", "ExecutePackageTask")])
        m = classify_package(pkg)
        assert m.pattern == PackagePattern.ORCHESTRATOR

    def test_cdc_detection(self):
        pkg = _make_pkg(tasks=[_task("CDC_Load_Customers", "CDCSource")])
        m = classify_package(pkg)
        assert m.pattern == PackagePattern.CDC_STREAMING

    def test_scd_detection(self):
        pkg = _make_pkg(tasks=[_task("Load_Dimension_SCD", "DataFlowTask"), _task("SCD_Update")])
        m = classify_package(pkg)
        assert m.pattern == PackagePattern.SCD_DIMENSION

    def test_incremental_load(self):
        pkg = _make_pkg(tasks=[_task("Load")], variables=[_var("watermark_date"), _var("lastrun")])
        m = classify_package(pkg)
        assert m.pattern == PackagePattern.INCREMENTAL_LOAD

    def test_data_warehouse(self):
        tasks = [_task(f"task_{i}") for i in range(10)]
        flows = [SimpleNamespace(name=f"df_{i}") for i in range(4)]
        pkg = _make_pkg(tasks=tasks, data_flows=flows)
        m = classify_package(pkg)
        assert m.pattern == PackagePattern.DATA_WAREHOUSE

    def test_strategy_recommendation(self):
        pkg = _make_pkg(tasks=[_task("Copy", "ExecutePackageTask")])
        m = classify_package(pkg)
        assert m.suggested_strategy in ("data_factory", "hybrid", "spark")


class TestNLQuery:
    def test_table_reference(self):
        pkg = _make_pkg("OrdersETL", tasks=[_task("Load_DimCustomer")])
        result = natural_language_query("show packages that write to DimCustomer", [pkg])
        assert result.match_count >= 1

    def test_task_count(self):
        pkg1 = _make_pkg("small", tasks=[_task("t1")])
        pkg2 = _make_pkg("big", tasks=[_task(f"t{i}") for i in range(10)])
        result = natural_language_query("packages with more than 5 tasks", [pkg1, pkg2])
        assert result.match_count == 1

    def test_keyword_search(self):
        pkg = _make_pkg("CDC_Incremental")
        result = natural_language_query("find cdc packages", [pkg])
        assert result.match_count >= 1

    def test_empty_results(self):
        result = natural_language_query("find xyz", [_make_pkg("other")])
        assert result.match_count == 0


class TestAutoTestGeneration:
    def test_generate_from_tasks(self):
        pkg = _make_pkg(tasks=[_task("Load_Customers"), _task("Insert_Orders")])
        tests = generate_validation_tests(pkg)
        assert len(tests) >= 2  # at least table_exists and not_empty for each

    def test_write_test_file(self, tmp_path: Path):
        tests = [GeneratedTest(
            test_name="test_customers_exists", test_type="table_exists",
            target_table="Customers", code="assert True",
        )]
        path = write_test_file(tests, tmp_path / "tests.py")
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "def test_customers_exists" in content


class TestStrategyRecommendation:
    def test_recommend(self):
        pkg = _make_pkg(tasks=[_task("Load")], data_flows=[SimpleNamespace(name="df")])
        rec = recommend_strategy(pkg)
        assert isinstance(rec, StrategyRecommendation)
        assert rec.recommended_strategy in ("data_factory", "hybrid", "spark")
        assert rec.estimated_effort_hours > 0

    def test_reasoning(self):
        pkg = _make_pkg(tasks=[_task("Run", "ExecutePackageTask")])
        rec = recommend_strategy(pkg)
        assert len(rec.reasoning) > 0


class TestKnowledgeBase:
    def test_entries_exist(self):
        assert len(KNOWLEDGE_BASE) >= 8

    def test_search(self):
        results = search_knowledge_base("lookup")
        assert len(results) >= 1
        assert any("Lookup" in r.pattern for r in results)

    def test_search_no_results(self):
        results = search_knowledge_base("xyznonexistent")
        assert len(results) == 0


class TestIntelligenceReport:
    def test_write_report(self, tmp_path: Path):
        c = PatternMatch(package_name="p", pattern=PackagePattern.SIMPLE_COPY, confidence=0.9)
        r = StrategyRecommendation(package_name="p", recommended_strategy="data_factory", confidence=0.9)
        path = write_intelligence_report([c], [r], tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["total_packages"] == 1


class TestCLI:
    def test_smart_analyze_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["smart-analyze", "--help"])
        assert result.exit_code == 0
