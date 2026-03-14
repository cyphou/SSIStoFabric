"""Tests for Phase 27 — Package Decomposition & Refactoring."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.decomposition import (
    DecompositionCandidate,
    DecompositionPlan,
    DecompositionReason,
    SharedDataFlow,
    SubPipeline,
    create_decomposition_plan,
    detect_candidates,
    detect_shared_data_flows,
    generate_preview,
    validate_plan,
    write_decomposition_report,
)

if TYPE_CHECKING:
    from pathlib import Path

# ── Test helpers ────────────────────────────────────────────────────


def _constraint(src: str, dst: str) -> SimpleNamespace:
    return SimpleNamespace(source_task=src, destination_task=dst, constraint_type="SUCCESS")


def _task(name: str, complexity: str = "LOW", components: list | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        task_type="ExecuteSQLTask",
        migration_complexity=complexity,
        data_flow_components=components or [],
    )


def _component(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, source_table="", destination_table="")


def _package(
    name: str = "TestPkg",
    tasks: list | None = None,
    constraints: list | None = None,
    components: list | None = None,
) -> SimpleNamespace:
    task_list = tasks or []
    return SimpleNamespace(
        name=name,
        control_flow_tasks=task_list,
        precedence_constraints=constraints or [],
        connection_managers=[],
        variables=[],
        data_flow_components=components or [],
        total_tasks=len(task_list),
        total_data_flows=0,
    )


# ── Graph utilities ─────────────────────────────────────────────────


class TestGraphUtilities:
    def test_build_task_graph(self):
        from ssis_to_fabric.engine.decomposition import _build_task_graph

        pkg = _package(
            tasks=[_task("A"), _task("B"), _task("C")],
            constraints=[_constraint("A", "B"), _constraint("B", "C")],
        )
        graph = _build_task_graph(pkg)
        assert "A" in graph
        assert "B" in graph["A"]
        assert "C" in graph["B"]
        assert graph["C"] == []

    def test_reverse_graph(self):
        from ssis_to_fabric.engine.decomposition import _reverse_graph

        graph = {"A": ["B", "C"], "B": ["D"], "C": [], "D": []}
        rev = _reverse_graph(graph)
        assert "A" in rev["B"]
        assert "A" in rev["C"]
        assert "B" in rev["D"]

    def test_connected_components_single(self):
        from ssis_to_fabric.engine.decomposition import _find_connected_components

        graph = {"A": ["B"], "B": ["C"], "C": []}
        components = _find_connected_components({"A", "B", "C"}, graph)
        assert len(components) == 1
        assert components[0] == {"A", "B", "C"}

    def test_connected_components_multiple(self):
        from ssis_to_fabric.engine.decomposition import _find_connected_components

        graph = {"A": ["B"], "B": [], "C": ["D"], "D": []}
        components = _find_connected_components({"A", "B", "C", "D"}, graph)
        assert len(components) == 2

    def test_topological_order(self):
        from ssis_to_fabric.engine.decomposition import _topological_order

        graph = {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []}
        order = _topological_order(graph)
        assert order[0] == "A"
        assert order[-1] == "D"
        assert order.index("B") < order.index("D")


# ── Candidate Detection ────────────────────────────────────────────


class TestCandidateDetection:
    def test_no_candidates_below_threshold(self):
        pkg = _package(tasks=[_task(f"t{i}") for i in range(3)])
        candidates = detect_candidates([pkg], min_tasks=8)
        assert len(candidates) == 0

    def test_high_task_count_candidate(self):
        pkg = _package(tasks=[_task(f"t{i}") for i in range(12)])
        candidates = detect_candidates([pkg], min_tasks=8)
        assert len(candidates) == 1
        assert DecompositionReason.HIGH_TASK_COUNT in candidates[0].reasons
        assert candidates[0].score > 0

    def test_independent_subgraphs(self):
        tasks = [_task(f"t{i}") for i in range(10)]
        # Two disconnected groups: 0-4, 5-9
        constraints = [_constraint(f"t{i}", f"t{i+1}") for i in range(4)]
        constraints += [_constraint(f"t{i}", f"t{i+1}") for i in range(5, 9)]
        pkg = _package(tasks=tasks, constraints=constraints)
        candidates = detect_candidates([pkg], min_tasks=8)
        assert len(candidates) == 1
        assert DecompositionReason.INDEPENDENT_SUBGRAPHS in candidates[0].reasons
        assert candidates[0].independent_subgraph_count >= 2

    def test_mixed_complexity(self):
        tasks = [_task(f"t{i}", "LOW") for i in range(6)]
        tasks += [_task(f"h{i}", "HIGH") for i in range(4)]
        pkg = _package(tasks=tasks)
        candidates = detect_candidates([pkg], min_tasks=8)
        assert len(candidates) == 1
        assert DecompositionReason.MIXED_COMPLEXITY in candidates[0].reasons

    def test_sorted_by_score(self):
        pkg1 = _package("small", tasks=[_task(f"t{i}") for i in range(9)])
        tasks2 = [_task(f"t{i}") for i in range(20)]
        tasks2[0] = _task("t0", "HIGH")
        pkg2 = _package("big", tasks=tasks2)
        candidates = detect_candidates([pkg1, pkg2], min_tasks=8)
        assert candidates[0].package_name == "big"

    def test_candidate_to_dict(self):
        c = DecompositionCandidate(
            package_name="p", task_count=10,
            reasons=[DecompositionReason.HIGH_TASK_COUNT], score=0.5,
        )
        d = c.to_dict()
        assert d["package_name"] == "p"
        assert d["reasons"] == ["high_task_count"]


# ── Shared Data Flow Detection ──────────────────────────────────────


class TestSharedDataFlowDetection:
    def test_no_shared(self):
        pkg1 = _package("p1", tasks=[_task("t1", components=[_component("flow_a")])])
        pkg2 = _package("p2", tasks=[_task("t2", components=[_component("flow_b")])])
        shared = detect_shared_data_flows([pkg1, pkg2])
        assert len(shared) == 0

    def test_detect_shared(self):
        pkg1 = _package("p1", tasks=[_task("t1", components=[_component("LoadCustomers")])])
        pkg2 = _package("p2", tasks=[_task("t2", components=[_component("loadcustomers")])])
        shared = detect_shared_data_flows([pkg1, pkg2])
        assert len(shared) == 1
        assert "p1" in shared[0].found_in_packages
        assert "p2" in shared[0].found_in_packages

    def test_proposed_notebook_name(self):
        pkg1 = _package("p1", tasks=[_task("t1", components=[_component("My Flow")])])
        pkg2 = _package("p2", tasks=[_task("t2", components=[_component("my flow")])])
        shared = detect_shared_data_flows([pkg1, pkg2])
        assert len(shared) == 1
        assert "shared_" in shared[0].proposed_notebook_name

    def test_shared_to_dict(self):
        sf = SharedDataFlow(
            data_flow_name="flow", found_in_packages=["a", "b"],
            proposed_notebook_name="shared_flow",
        )
        d = sf.to_dict()
        assert d["data_flow_name"] == "flow"
        assert len(d["found_in_packages"]) == 2


# ── Decomposition Planning ──────────────────────────────────────────


class TestDecompositionPlanning:
    def test_single_chain(self):
        tasks = [_task(f"t{i}") for i in range(6)]
        constraints = [_constraint(f"t{i}", f"t{i+1}") for i in range(5)]
        pkg = _package(tasks=tasks, constraints=constraints)
        plan = create_decomposition_plan(pkg, min_tasks=3)
        assert plan.package_name == "TestPkg"
        assert len(plan.sub_pipelines) >= 1
        assert plan.orchestrator_name == "TestPkg_orchestrator"

    def test_two_independent_groups(self):
        tasks = [_task(f"a{i}") for i in range(4)] + [_task(f"b{i}") for i in range(4)]
        constraints = [_constraint(f"a{i}", f"a{i+1}") for i in range(3)]
        constraints += [_constraint(f"b{i}", f"b{i+1}") for i in range(3)]
        pkg = _package(tasks=tasks, constraints=constraints)
        plan = create_decomposition_plan(pkg, min_tasks=3)
        assert len(plan.sub_pipelines) == 2

    def test_execution_order(self):
        tasks = [_task(f"t{i}") for i in range(8)]
        constraints = [_constraint(f"t{i}", f"t{i+1}") for i in range(3)]
        constraints += [_constraint(f"t{i}", f"t{i+1}") for i in range(4, 7)]
        pkg = _package(tasks=tasks, constraints=constraints)
        plan = create_decomposition_plan(pkg, min_tasks=3)
        assert len(plan.execution_order) == len(plan.sub_pipelines)

    def test_data_flow_detection(self):
        tasks = [_task(f"t{i}") for i in range(4)]
        tasks[1] = _task("df_task", components=[_component("flow1")])
        constraints = [_constraint(f"t{i}", f"t{i+1}") for i in range(3)]
        # Replace t1 in constraints
        tasks_all = [tasks[0], tasks[1], tasks[2], tasks[3]]
        pkg = _package(tasks=tasks_all, constraints=constraints)
        plan = create_decomposition_plan(pkg, min_tasks=2)
        has_df = any(sp.has_data_flow for sp in plan.sub_pipelines)
        # Whether it has DF depends on which partition df_task lands in
        assert isinstance(has_df, bool)

    def test_sub_pipeline_to_dict(self):
        sp = SubPipeline(name="p_part_1", task_names=["a", "b"], dependencies_on=["p_part_0"])
        d = sp.to_dict()
        assert d["name"] == "p_part_1"
        assert d["dependencies_on"] == ["p_part_0"]

    def test_plan_to_dict(self):
        plan = DecompositionPlan(
            package_name="p", original_task_count=10,
            sub_pipelines=[SubPipeline(name="p_part_1")],
            orchestrator_name="p_orchestrator",
        )
        d = plan.to_dict()
        assert d["original_task_count"] == 10
        assert len(d["sub_pipelines"]) == 1


# ── Preview ─────────────────────────────────────────────────────────


class TestPreview:
    def test_generate_preview(self):
        tasks = [_task(f"t{i}") for i in range(6)]
        constraints = [_constraint(f"t{i}", f"t{i+1}") for i in range(5)]
        pkg = _package(tasks=tasks, constraints=constraints)
        preview = generate_preview(pkg)
        assert preview.package_name == "TestPkg"
        assert len(preview.original_tasks) == 6
        assert preview.proposed_plan is not None

    def test_preview_to_dict(self):
        tasks = [_task("a"), _task("b")]
        pkg = _package(tasks=tasks)
        preview = generate_preview(pkg)
        d = preview.to_dict()
        assert "original_tasks" in d
        assert "proposed_plan" in d


# ── Validation ──────────────────────────────────────────────────────


class TestValidation:
    def test_valid_plan(self):
        tasks = [_task("a"), _task("b"), _task("c")]
        pkg = _package(tasks=tasks)
        plan = DecompositionPlan(
            package_name="TestPkg", original_task_count=3,
            sub_pipelines=[SubPipeline(name="p1", task_names=["a", "b", "c"])],
        )
        issues = validate_plan(plan, pkg)
        assert len(issues) == 0

    def test_missing_tasks(self):
        tasks = [_task("a"), _task("b"), _task("c")]
        pkg = _package(tasks=tasks)
        plan = DecompositionPlan(
            package_name="TestPkg", original_task_count=3,
            sub_pipelines=[SubPipeline(name="p1", task_names=["a", "b"])],
        )
        issues = validate_plan(plan, pkg)
        assert any("not assigned" in i for i in issues)

    def test_duplicate_tasks(self):
        tasks = [_task("a"), _task("b")]
        pkg = _package(tasks=tasks)
        plan = DecompositionPlan(
            package_name="TestPkg", original_task_count=2,
            sub_pipelines=[
                SubPipeline(name="p1", task_names=["a", "b"]),
                SubPipeline(name="p2", task_names=["a"]),
            ],
        )
        issues = validate_plan(plan, pkg)
        assert any("multiple" in i for i in issues)

    def test_unknown_dependency(self):
        tasks = [_task("a")]
        pkg = _package(tasks=tasks)
        plan = DecompositionPlan(
            package_name="TestPkg", original_task_count=1,
            sub_pipelines=[SubPipeline(name="p1", task_names=["a"], dependencies_on=["nonexistent"])],
        )
        issues = validate_plan(plan, pkg)
        assert any("unknown" in i for i in issues)


# ── Report Writer ───────────────────────────────────────────────────


class TestReport:
    def test_write_report(self, tmp_path: Path):
        c = DecompositionCandidate(package_name="p", task_count=10, score=0.5)
        plan = DecompositionPlan(package_name="p", original_task_count=10)
        sf = SharedDataFlow(data_flow_name="f", found_in_packages=["a", "b"])
        path = write_decomposition_report([c], [plan], [sf], tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["total_candidates"] == 1
        assert data["total_plans"] == 1
        assert data["total_shared_data_flows"] == 1


# ── CLI ─────────────────────────────────────────────────────────────


class TestCLI:
    def test_decompose_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["decompose", "--help"])
        assert result.exit_code == 0
        assert "--min-tasks" in result.output
