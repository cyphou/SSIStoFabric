"""Tests for Phase 38 — Legacy Modernization Patterns."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.engine.migration_engine import PackageAssessment
from ssis_to_fabric.engine.modernization import (
    DebtScore,
    LayerClassification,
    MedallionBlueprint,
    MedallionLayer,
    MigrationWave,
    ModernizationEngine,
    ModernizationPattern,
    ModernizationReport,
    PatternScore,
    classify_component,
    generate_medallion_blueprint,
    plan_migration_waves,
    recommend_pattern,
    score_technical_debt,
)


# =====================================================================
# Helpers
# =====================================================================


def _make_package(
    name: str = "TestPkg",
    tasks: list[ControlFlowTask] | None = None,
    connections: list[ConnectionManager] | None = None,
) -> SSISPackage:
    pkg = SSISPackage(
        name=name,
        control_flow_tasks=tasks or [],
        connection_managers=connections or [],
    )
    pkg.compute_stats()
    return pkg


def _make_task(name: str, task_type: TaskType = TaskType.EXECUTE_SQL, components: list[DataFlowComponent] | None = None) -> ControlFlowTask:
    return ControlFlowTask(
        name=name,
        task_type=task_type,
        data_flow_components=components or [],
    )


def _make_component(name: str, comp_type: DataFlowComponentType) -> DataFlowComponent:
    return DataFlowComponent(name=name, component_type=comp_type)


def _make_assessment(
    name: str = "TestPkg",
    tasks: int = 5,
    data_flows: int = 2,
    complexity: str = "MEDIUM",
) -> PackageAssessment:
    return PackageAssessment(
        name=name,
        tasks=tasks,
        data_flows=data_flows,
        complexity=complexity,
    )


# =====================================================================
# Layer Classification
# =====================================================================


class TestClassifyComponent:

    def test_bronze_by_name(self):
        cls = classify_component("staging_customers", "Execute SQL Task")
        assert cls.layer == MedallionLayer.BRONZE

    def test_bronze_by_type(self):
        cls = classify_component("SomeSource", "OLE DB Source")
        assert cls.layer == MedallionLayer.BRONZE

    def test_silver_by_name(self):
        cls = classify_component("clean_addresses", "Script Component")
        assert cls.layer == MedallionLayer.SILVER

    def test_silver_by_type(self):
        cls = classify_component("DataTransform", "Derived Column")
        assert cls.layer == MedallionLayer.SILVER

    def test_gold_by_name(self):
        cls = classify_component("fact_sales", "OLE DB Destination")
        assert cls.layer == MedallionLayer.GOLD

    def test_gold_by_type(self):
        cls = classify_component("SomeDest", "OLE DB Destination")
        assert cls.layer == MedallionLayer.GOLD

    def test_unclassified(self):
        cls = classify_component("mystery", "UnknownType")
        assert cls.layer == MedallionLayer.UNCLASSIFIED

    def test_name_priority_over_type(self):
        # Name says "staging" → BRONZE, even though type is a destination
        cls = classify_component("staging_load", "OLE DB Destination")
        assert cls.layer == MedallionLayer.BRONZE

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("raw_events", MedallionLayer.BRONZE),
            ("ingest_orders", MedallionLayer.BRONZE),
            ("extract_users", MedallionLayer.BRONZE),
            ("src_products", MedallionLayer.BRONZE),
            ("stg_inventory", MedallionLayer.BRONZE),
            ("transform_data", MedallionLayer.SILVER),
            ("validate_records", MedallionLayer.SILVER),
            ("enrich_customers", MedallionLayer.SILVER),
            ("lookup_codes", MedallionLayer.SILVER),
            ("dim_customer", MedallionLayer.GOLD),
            ("report_sales", MedallionLayer.GOLD),
            ("agg_monthly", MedallionLayer.GOLD),
            ("summary_kpis", MedallionLayer.GOLD),
        ],
    )
    def test_name_patterns(self, name, expected):
        cls = classify_component(name, "Execute SQL Task")
        assert cls.layer == expected

    def test_classification_to_dict(self):
        cls = classify_component("staging_data", "Source")
        d = cls.to_dict()
        assert d["layer"] == "bronze"
        assert "component_name" in d


# =====================================================================
# Medallion Blueprint
# =====================================================================


class TestMedallionBlueprint:

    def test_empty_packages(self):
        bp = generate_medallion_blueprint([])
        assert bp.total == 0

    def test_single_package(self):
        tasks = [
            _make_task("staging_load", TaskType.DATA_FLOW, components=[
                _make_component("OLE DB Source 1", DataFlowComponentType.OLE_DB_SOURCE),
                _make_component("clean_transform", DataFlowComponentType.DERIVED_COLUMN),
                _make_component("fact_sales_dest", DataFlowComponentType.OLE_DB_DESTINATION),
            ]),
        ]
        pkg = _make_package("ETL", tasks=tasks)
        bp = generate_medallion_blueprint([pkg])
        assert len(bp.bronze) >= 1  # staging_load + OLE DB Source
        assert len(bp.silver) >= 1  # clean_transform + Derived Column
        assert len(bp.gold) >= 1    # fact_sales_dest + OLE DB Destination

    def test_blueprint_to_dict(self):
        bp = MedallionBlueprint()
        bp.bronze.append(LayerClassification("a", "Source", MedallionLayer.BRONZE))
        d = bp.to_dict()
        assert d["summary"]["bronze"] == 1
        assert len(d["bronze"]) == 1

    def test_total_property(self):
        bp = MedallionBlueprint()
        bp.bronze.append(LayerClassification("a", "t", MedallionLayer.BRONZE))
        bp.silver.append(LayerClassification("b", "t", MedallionLayer.SILVER))
        bp.gold.append(LayerClassification("c", "t", MedallionLayer.GOLD))
        bp.unclassified.append(LayerClassification("d", "t", MedallionLayer.UNCLASSIFIED))
        assert bp.total == 4


# =====================================================================
# Technical Debt Scoring
# =====================================================================


class TestTechnicalDebtScoring:

    def test_low_complexity(self):
        a = _make_assessment(complexity="LOW")
        ds = score_technical_debt(a)
        assert ds.total <= 10
        assert ds.risk_level == "LOW"

    def test_high_complexity(self):
        a = _make_assessment(complexity="HIGH")
        ds = score_technical_debt(a)
        assert ds.complexity_score == 25.0  # HIGH (5.0) * 5

    def test_manual_complexity(self):
        a = _make_assessment(complexity="MANUAL")
        ds = score_technical_debt(a)
        assert ds.complexity_score == 40.0  # MANUAL (8.0) * 5 = 40, capped at 40

    def test_todo_count_contribution(self):
        a = _make_assessment(complexity="LOW")
        ds = score_technical_debt(a, todo_count=10)
        assert ds.todo_score == 25.0  # 10 * 2.5 = 25, capped at 25

    def test_manual_review_contribution(self):
        a = _make_assessment(tasks=10, data_flows=0, complexity="LOW")
        ds = score_technical_debt(a, manual_review_count=5)
        assert ds.manual_review_score == 20.0  # (5/10) * 100 = 50, capped at 20

    def test_unsupported_contribution(self):
        a = _make_assessment(complexity="LOW")
        ds = score_technical_debt(a, unsupported_count=3)
        assert ds.unsupported_score == 15.0  # 3 * 5 = 15, capped at 15

    def test_risk_level_high(self):
        a = _make_assessment(complexity="MANUAL")
        ds = score_technical_debt(a, todo_count=10, manual_review_count=5, unsupported_count=3)
        assert ds.risk_level == "HIGH"
        assert ds.total >= 60

    def test_risk_level_medium(self):
        a = _make_assessment(complexity="MEDIUM")
        ds = score_technical_debt(a, todo_count=6)
        assert ds.risk_level == "MEDIUM"

    def test_debt_score_to_dict(self):
        a = _make_assessment(complexity="LOW")
        ds = score_technical_debt(a)
        d = ds.to_dict()
        assert "total" in d
        assert "risk_level" in d
        assert d["package_name"] == "TestPkg"


# =====================================================================
# Pattern Recommendation
# =====================================================================


class TestPatternRecommendation:

    def test_default_is_medallion(self):
        pkg = _make_package("Simple")
        scores = recommend_pattern([pkg])
        assert scores[0].pattern == ModernizationPattern.MEDALLION

    def test_star_schema_boosted(self):
        tasks = [
            _make_task("dim_customer"),
            _make_task("fact_sales"),
            _make_task("dim_product"),
            _make_task("dim_date"),
            _make_task("fact_orders"),
        ]
        pkg = _make_package("DW", tasks=tasks)
        scores = recommend_pattern([pkg])
        star = next(s for s in scores if s.pattern == ModernizationPattern.STAR_SCHEMA)
        assert star.score >= 60

    def test_hub_and_spoke_with_many_connections(self):
        conns = [ConnectionManager(name=f"Conn{i}", connection_type="OLEDB") for i in range(10)]
        pkg = _make_package("Multi", connections=conns)
        scores = recommend_pattern([pkg])
        hub = next(s for s in scores if s.pattern == ModernizationPattern.HUB_AND_SPOKE)
        assert hub.score >= 40

    def test_event_driven_with_cdc(self):
        tasks = [
            _make_task("cdc_capture"),
            _make_task("stream_events"),
            _make_task("realtime_load"),
        ]
        pkg = _make_package("CDC", tasks=tasks)
        scores = recommend_pattern([pkg])
        event = next(s for s in scores if s.pattern == ModernizationPattern.EVENT_DRIVEN)
        assert event.score >= 60

    def test_data_mesh_with_many_packages(self):
        packages = [
            _make_package("sales_etl"),
            _make_package("finance_etl"),
            _make_package("hr_import"),
            _make_package("marketing_load"),
            _make_package("supply_chain_sync"),
        ]
        scores = recommend_pattern(packages)
        mesh = next(s for s in scores if s.pattern == ModernizationPattern.DATA_MESH)
        assert mesh.score > 10

    def test_scores_sorted_descending(self):
        pkg = _make_package("Test")
        scores = recommend_pattern([pkg])
        for i in range(len(scores) - 1):
            assert scores[i].score >= scores[i + 1].score

    def test_pattern_score_to_dict(self):
        ps = PatternScore(pattern=ModernizationPattern.MEDALLION, score=75.0, rationale=["Reason"])
        d = ps.to_dict()
        assert d["pattern"] == "medallion"
        assert d["score"] == 75.0

    def test_blueprint_boosts_medallion(self):
        tasks = [
            _make_task("staging_load", TaskType.DATA_FLOW, components=[
                _make_component("OLE DB Source 1", DataFlowComponentType.OLE_DB_SOURCE),
                _make_component("fact_sales_dest", DataFlowComponentType.OLE_DB_DESTINATION),
            ]),
        ]
        pkg = _make_package("ETL", tasks=tasks)
        bp = generate_medallion_blueprint([pkg])
        scores = recommend_pattern([pkg], blueprint=bp)
        medallion = scores[0]
        assert medallion.pattern == ModernizationPattern.MEDALLION
        assert medallion.score > 60  # Should be boosted beyond base 60


# =====================================================================
# Wave Planner
# =====================================================================


class TestWavePlanner:

    def test_empty_input(self):
        assert plan_migration_waves([]) == []

    def test_single_package(self):
        ds = DebtScore(package_name="A", total=10.0)
        waves = plan_migration_waves([ds])
        assert len(waves) == 1
        assert len(waves[0].packages) == 1

    def test_wave_size_limit(self):
        scores = [DebtScore(package_name=f"P{i}", total=5.0) for i in range(12)]
        waves = plan_migration_waves(scores, max_wave_size=5)
        assert len(waves) == 3
        assert len(waves[0].packages) == 5
        assert len(waves[1].packages) == 5
        assert len(waves[2].packages) == 2

    def test_debt_limit(self):
        scores = [
            DebtScore(package_name="Low1", total=10.0),
            DebtScore(package_name="Low2", total=10.0),
            DebtScore(package_name="High1", total=80.0),
        ]
        waves = plan_migration_waves(scores, max_wave_size=10, max_wave_debt=30.0)
        # Low packages (10+10=20) fit in wave 1; High (80) goes to wave 2
        assert len(waves) >= 2

    def test_ordered_by_debt(self):
        scores = [
            DebtScore(package_name="High", total=80.0),
            DebtScore(package_name="Low", total=5.0),
            DebtScore(package_name="Med", total=30.0),
        ]
        waves = plan_migration_waves(scores, max_wave_size=1)
        assert waves[0].packages == ["Low"]
        assert waves[1].packages == ["Med"]
        assert waves[2].packages == ["High"]

    def test_wave_descriptions(self):
        scores = [
            DebtScore(package_name="A", total=5.0),
            DebtScore(package_name="B", total=70.0),
        ]
        waves = plan_migration_waves(scores, max_wave_size=1)
        assert "quick win" in waves[0].description.lower()

    def test_wave_to_dict(self):
        w = MigrationWave(wave_number=1, packages=["A", "B"], total_debt=15.0, avg_debt=7.5)
        d = w.to_dict()
        assert d["wave_number"] == 1
        assert len(d["packages"]) == 2


# =====================================================================
# Modernization Engine
# =====================================================================


class TestModernizationEngine:

    def test_basic_analysis(self):
        pkg = _make_package("Simple", tasks=[_make_task("Load Data")])
        engine = ModernizationEngine()
        report = engine.analyze([pkg])
        assert isinstance(report, ModernizationReport)
        assert report.total_packages == 1
        assert report.recommended_pattern in ModernizationPattern

    def test_forced_pattern(self):
        pkg = _make_package("Simple")
        engine = ModernizationEngine(pattern=ModernizationPattern.DATA_MESH)
        report = engine.analyze([pkg])
        assert report.recommended_pattern == ModernizationPattern.DATA_MESH

    def test_with_assessments(self):
        pkg = _make_package("Etl", tasks=[_make_task("Load")])
        assessment = _make_assessment("Etl", tasks=5, complexity="MEDIUM")
        engine = ModernizationEngine()
        report = engine.analyze([pkg], assessments=[assessment])
        assert len(report.debt_scores) == 1
        assert report.debt_scores[0].package_name == "Etl"

    def test_without_assessments_synthesises(self):
        pkg = _make_package("Auto", tasks=[_make_task("T1"), _make_task("T2")])
        engine = ModernizationEngine()
        report = engine.analyze([pkg])
        assert len(report.debt_scores) == 1

    def test_wave_planning_runs(self):
        packages = [
            _make_package(f"Pkg{i}", tasks=[_make_task(f"T{i}")])
            for i in range(8)
        ]
        engine = ModernizationEngine(max_wave_size=3)
        report = engine.analyze(packages)
        assert len(report.waves) >= 2

    def test_report_to_dict(self):
        pkg = _make_package("Test")
        engine = ModernizationEngine()
        report = engine.analyze([pkg])
        d = report.to_dict()
        assert "recommended_pattern" in d
        assert "blueprint" in d
        assert "debt_scores" in d
        assert "waves" in d

    def test_write_report_json(self, tmp_path):
        pkg = _make_package("Test")
        engine = ModernizationEngine()
        report = engine.analyze([pkg])
        path = engine.write_report(report, tmp_path)
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["recommended_pattern"] == report.recommended_pattern.value

    def test_avg_debt_computed(self):
        packages = [
            _make_package("Low", tasks=[_make_task("T1")]),
            _make_package("High", tasks=[_make_task("T1"), _make_task("T2"), _make_task("T3")]),
        ]
        assessments = [
            _make_assessment("Low", complexity="LOW"),
            _make_assessment("High", complexity="HIGH"),
        ]
        engine = ModernizationEngine()
        report = engine.analyze(packages, assessments=assessments)
        assert report.avg_debt > 0


# =====================================================================
# Enums
# =====================================================================


class TestEnums:

    def test_medallion_layer_values(self):
        assert MedallionLayer.BRONZE.value == "bronze"
        assert MedallionLayer.SILVER.value == "silver"
        assert MedallionLayer.GOLD.value == "gold"

    def test_modernization_pattern_values(self):
        assert ModernizationPattern.MEDALLION.value == "medallion"
        assert ModernizationPattern.STAR_SCHEMA.value == "star_schema"
        assert ModernizationPattern.HUB_AND_SPOKE.value == "hub_and_spoke"
        assert ModernizationPattern.EVENT_DRIVEN.value == "event_driven"
        assert ModernizationPattern.DATA_MESH.value == "data_mesh"


# =====================================================================
# Edge Cases
# =====================================================================


class TestEdgeCases:

    def test_package_with_no_tasks(self):
        pkg = _make_package("Empty")
        engine = ModernizationEngine()
        report = engine.analyze([pkg])
        assert report.total_packages == 1

    def test_debt_score_caps(self):
        # All components at max
        a = _make_assessment(tasks=1, data_flows=0, complexity="MANUAL")
        ds = score_technical_debt(a, todo_count=100, manual_review_count=100, unsupported_count=100)
        assert ds.complexity_score == 40.0
        assert ds.todo_score == 25.0
        assert ds.manual_review_score == 20.0
        assert ds.unsupported_score == 15.0
        assert ds.total == 100.0

    def test_pattern_with_zero_tasks(self):
        pkg = _make_package("Zero")
        scores = recommend_pattern([pkg])
        assert all(isinstance(s, PatternScore) for s in scores)

    def test_wave_with_single_high_debt(self):
        ds = DebtScore(package_name="Monster", total=500.0)
        waves = plan_migration_waves([ds], max_wave_debt=100.0)
        assert len(waves) == 1
        assert waves[0].packages == ["Monster"]
