"""
Package Decomposition & Refactoring
=====================================
Phase 27: Automatic detection and splitting of monolithic SSIS packages
into smaller, focused Fabric pipelines with dependency preservation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Graph Utilities
# =====================================================================


def _build_task_graph(package: Any) -> dict[str, list[str]]:
    """Build an adjacency-list task graph from precedence constraints."""
    tasks = getattr(package, "control_flow_tasks", [])
    constraints = getattr(package, "precedence_constraints", [])
    graph: dict[str, list[str]] = {}
    for t in tasks:
        name = getattr(t, "name", str(t))
        graph.setdefault(name, [])
    for c in constraints:
        src = getattr(c, "source_task", "")
        dst = getattr(c, "destination_task", "")
        if src and dst:
            graph.setdefault(src, [])
            graph.setdefault(dst, [])
            graph[src].append(dst)
    return graph


def _reverse_graph(graph: dict[str, list[str]]) -> dict[str, list[str]]:
    """Build the reverse adjacency list."""
    rev: dict[str, list[str]] = {n: [] for n in graph}
    for node, children in graph.items():
        for child in children:
            rev.setdefault(child, [])
            rev[child].append(node)
    return rev


def _find_connected_components(
    nodes: set[str], graph: dict[str, list[str]],
) -> list[set[str]]:
    """Find connected components (treating edges as undirected)."""
    visited: set[str] = set()
    components: list[set[str]] = []
    rev = _reverse_graph(graph)
    for node in nodes:
        if node in visited:
            continue
        component: set[str] = set()
        stack = [node]
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.add(current)
            for neighbor in graph.get(current, []):
                if neighbor in nodes and neighbor not in visited:
                    stack.append(neighbor)
            for neighbor in rev.get(current, []):
                if neighbor in nodes and neighbor not in visited:
                    stack.append(neighbor)
        if component:
            components.append(component)
    return components


def _topological_order(graph: dict[str, list[str]]) -> list[str]:
    """Return nodes in topological order (Kahn's algorithm)."""
    in_degree: dict[str, int] = {n: 0 for n in graph}
    for children in graph.values():
        for c in children:
            in_degree[c] = in_degree.get(c, 0) + 1
    queue = [n for n, d in in_degree.items() if d == 0]
    order: list[str] = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for child in graph.get(node, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
    return order


# =====================================================================
# Decomposition Models
# =====================================================================


class DecompositionReason(Enum):
    """Why a package is a decomposition candidate."""

    HIGH_TASK_COUNT = "high_task_count"
    INDEPENDENT_SUBGRAPHS = "independent_subgraphs"
    MIXED_COMPLEXITY = "mixed_complexity"
    SHARED_DATA_FLOWS = "shared_data_flows"
    LONG_CRITICAL_PATH = "long_critical_path"


@dataclass
class DecompositionCandidate:
    """A package identified as a decomposition candidate."""

    package_name: str
    task_count: int
    reasons: list[DecompositionReason] = field(default_factory=list)
    independent_subgraph_count: int = 0
    score: float = 0.0  # 0-1 indicating decomposition urgency

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "task_count": self.task_count,
            "reasons": [r.value for r in self.reasons],
            "independent_subgraph_count": self.independent_subgraph_count,
            "score": round(self.score, 2),
        }


@dataclass
class SubPipeline:
    """A proposed sub-pipeline extracted from a monolithic package."""

    name: str
    task_names: list[str] = field(default_factory=list)
    dependencies_on: list[str] = field(default_factory=list)
    has_data_flow: bool = False
    estimated_complexity: str = "LOW"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "task_names": self.task_names,
            "dependencies_on": self.dependencies_on,
            "has_data_flow": self.has_data_flow,
            "estimated_complexity": self.estimated_complexity,
        }


@dataclass
class DecompositionPlan:
    """Complete decomposition plan for a package."""

    package_name: str
    original_task_count: int
    sub_pipelines: list[SubPipeline] = field(default_factory=list)
    orchestrator_name: str = ""
    execution_order: list[str] = field(default_factory=list)
    shared_notebooks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "original_task_count": self.original_task_count,
            "sub_pipelines": [sp.to_dict() for sp in self.sub_pipelines],
            "orchestrator_name": self.orchestrator_name,
            "execution_order": self.execution_order,
            "shared_notebooks": self.shared_notebooks,
        }


@dataclass
class SharedDataFlow:
    """A data flow that appears in multiple packages (DRY candidate)."""

    data_flow_name: str
    source_table: str = ""
    destination_table: str = ""
    found_in_packages: list[str] = field(default_factory=list)
    proposed_notebook_name: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_flow_name": self.data_flow_name,
            "source_table": self.source_table,
            "destination_table": self.destination_table,
            "found_in_packages": self.found_in_packages,
            "proposed_notebook_name": self.proposed_notebook_name,
        }


@dataclass
class DecompositionPreview:
    """Side-by-side preview of original vs proposed split."""

    package_name: str
    original_tasks: list[str] = field(default_factory=list)
    proposed_plan: DecompositionPlan | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "original_tasks": self.original_tasks,
            "proposed_plan": self.proposed_plan.to_dict() if self.proposed_plan else None,
        }


# =====================================================================
# Candidate Detection
# =====================================================================


def detect_candidates(
    packages: list[Any],
    min_tasks: int = 8,
) -> list[DecompositionCandidate]:
    """Detect packages that are candidates for decomposition."""
    candidates: list[DecompositionCandidate] = []
    for pkg in packages:
        tasks = getattr(pkg, "control_flow_tasks", [])
        task_count = len(tasks)
        if task_count < min_tasks:
            continue

        reasons: list[DecompositionReason] = []
        score = 0.0

        # Check high task count
        if task_count >= min_tasks:
            reasons.append(DecompositionReason.HIGH_TASK_COUNT)
            score += min(task_count / 20.0, 0.4)

        # Check for independent subgraphs
        graph = _build_task_graph(pkg)
        components = _find_connected_components(set(graph.keys()), graph)
        num_subgraphs = len(components)
        if num_subgraphs > 1:
            reasons.append(DecompositionReason.INDEPENDENT_SUBGRAPHS)
            score += min(num_subgraphs * 0.15, 0.3)

        # Check for mixed complexity (both simple and complex tasks)
        complexities = set()
        for t in tasks:
            c = getattr(t, "migration_complexity", "LOW")
            complexities.add(str(c).upper() if c else "LOW")
        if len(complexities) > 1 and "HIGH" in complexities:
            reasons.append(DecompositionReason.MIXED_COMPLEXITY)
            score += 0.15

        # Check for long critical path
        order = _topological_order(graph)
        if len(order) > 10:
            reasons.append(DecompositionReason.LONG_CRITICAL_PATH)
            score += 0.15

        if reasons:
            candidates.append(DecompositionCandidate(
                package_name=getattr(pkg, "name", "unknown"),
                task_count=task_count,
                reasons=reasons,
                independent_subgraph_count=num_subgraphs,
                score=min(score, 1.0),
            ))

    return sorted(candidates, key=lambda c: c.score, reverse=True)


# =====================================================================
# Shared Data Flow Detection
# =====================================================================


def detect_shared_data_flows(packages: list[Any]) -> list[SharedDataFlow]:
    """Find data flows that appear in multiple packages (DRY candidates)."""
    flow_map: dict[str, list[str]] = {}
    flow_info: dict[str, dict[str, str]] = {}

    for pkg in packages:
        pkg_name = getattr(pkg, "name", "unknown")
        tasks = getattr(pkg, "control_flow_tasks", [])
        for task in tasks:
            components = getattr(task, "data_flow_components", [])
            for comp in components:
                comp_name = getattr(comp, "name", "")
                if not comp_name:
                    continue
                # Normalize name for matching
                normalized = comp_name.strip().lower()
                flow_map.setdefault(normalized, [])
                if pkg_name not in flow_map[normalized]:
                    flow_map[normalized].append(pkg_name)
                # Capture source/dest info
                if normalized not in flow_info:
                    source = getattr(comp, "source_table", "") or ""
                    dest = getattr(comp, "destination_table", "") or ""
                    flow_info[normalized] = {"source": source, "dest": dest, "name": comp_name}

    shared: list[SharedDataFlow] = []
    for normalized, pkg_list in flow_map.items():
        if len(pkg_list) > 1:
            info = flow_info.get(normalized, {})
            notebook_name = f"shared_{normalized.replace(' ', '_')}"
            shared.append(SharedDataFlow(
                data_flow_name=info.get("name", normalized),
                source_table=info.get("source", ""),
                destination_table=info.get("dest", ""),
                found_in_packages=pkg_list,
                proposed_notebook_name=notebook_name,
            ))

    return shared


# =====================================================================
# Decomposition Planning
# =====================================================================


def _assign_complexity(tasks: list[Any], task_names: set[str]) -> str:
    """Determine complexity of a sub-pipeline based on its tasks."""
    task_map = {getattr(t, "name", ""): t for t in tasks}
    has_high = False
    has_dataflow = False
    for name in task_names:
        t = task_map.get(name)
        if not t:
            continue
        c = str(getattr(t, "migration_complexity", "LOW")).upper()
        if c == "HIGH":
            has_high = True
        if getattr(t, "data_flow_components", []):
            has_dataflow = True
    if has_high:
        return "HIGH"
    if has_dataflow:
        return "MEDIUM"
    return "LOW"


def create_decomposition_plan(
    package: Any,
    min_tasks: int = 3,
) -> DecompositionPlan:
    """Create a decomposition plan for a monolithic package."""
    pkg_name = getattr(package, "name", "unknown")
    tasks = getattr(package, "control_flow_tasks", [])
    graph = _build_task_graph(package)

    # Find connected components
    components = _find_connected_components(set(graph.keys()), graph)

    # If only one component, try to split by phases (groups between fan-in/fan-out)
    if len(components) <= 1 and len(graph) >= min_tasks:
        components = _split_by_phases(graph, min_tasks)

    sub_pipelines: list[SubPipeline] = []
    shared_notebooks: list[str] = []
    sub_graph: dict[str, list[str]] = {}

    for i, component in enumerate(components):
        if len(component) < 1:
            continue
        sub_name = f"{pkg_name}_part_{i + 1}"
        task_names = sorted(component)

        # Check for data flows
        has_df = False
        for t in tasks:
            if getattr(t, "name", "") in component and getattr(t, "data_flow_components", []):
                has_df = True
                break

        # Find dependencies on other sub-pipelines
        deps: list[str] = []
        rev = _reverse_graph(graph)
        for task_name in component:
            for upstream in rev.get(task_name, []):
                if upstream not in component:
                    # Find which sub-pipeline owns the upstream task
                    for j, other in enumerate(components):
                        if upstream in other and j != i:
                            dep_name = f"{pkg_name}_part_{j + 1}"
                            if dep_name not in deps:
                                deps.append(dep_name)

        complexity = _assign_complexity(tasks, component)

        sp = SubPipeline(
            name=sub_name,
            task_names=task_names,
            dependencies_on=deps,
            has_data_flow=has_df,
            estimated_complexity=complexity,
        )
        sub_pipelines.append(sp)
        sub_graph[sub_name] = deps

    # Build execution order for the orchestrator
    orch_graph: dict[str, list[str]] = {sp.name: [] for sp in sub_pipelines}
    for sp in sub_pipelines:
        for dep in sp.dependencies_on:
            orch_graph.setdefault(dep, [])
            orch_graph[dep].append(sp.name)

    execution_order = _topological_order(orch_graph)

    # Detect shared data flows within the package
    for t in tasks:
        comps = getattr(t, "data_flow_components", [])
        if len(comps) > 2:
            shared_notebooks.append(f"{pkg_name}_{getattr(t, 'name', 'df')}_shared")

    orchestrator_name = f"{pkg_name}_orchestrator"

    return DecompositionPlan(
        package_name=pkg_name,
        original_task_count=len(tasks),
        sub_pipelines=sub_pipelines,
        orchestrator_name=orchestrator_name,
        execution_order=execution_order,
        shared_notebooks=shared_notebooks,
    )


def _split_by_phases(
    graph: dict[str, list[str]], min_group_size: int = 3,
) -> list[set[str]]:
    """Split a single connected component into phases based on topology.

    Groups consecutive nodes (by topological order) into chunks,
    breaking at fan-in/fan-out points.
    """
    order = _topological_order(graph)
    if len(order) <= min_group_size:
        return [set(order)]

    rev = _reverse_graph(graph)
    phases: list[set[str]] = []
    current_phase: set[str] = set()

    for node in order:
        current_phase.add(node)
        # Break at fan-in (multiple predecessors) or fan-out (multiple successors)
        out_degree = len(graph.get(node, []))
        in_degree = len(rev.get(node, []))
        if (out_degree > 1 or in_degree > 1) and len(current_phase) >= min_group_size:
            phases.append(current_phase)
            current_phase = set()

    if current_phase:
        # Merge small trailing phases with the last one
        if phases and len(current_phase) < min_group_size:
            phases[-1].update(current_phase)
        else:
            phases.append(current_phase)

    return phases if len(phases) > 1 else [set(order)]


# =====================================================================
# Preview Generation
# =====================================================================


def generate_preview(package: Any) -> DecompositionPreview:
    """Generate a side-by-side decomposition preview."""
    tasks = getattr(package, "control_flow_tasks", [])
    original_tasks = [getattr(t, "name", str(t)) for t in tasks]
    plan = create_decomposition_plan(package)

    return DecompositionPreview(
        package_name=getattr(package, "name", "unknown"),
        original_tasks=original_tasks,
        proposed_plan=plan,
    )


# =====================================================================
# Validation
# =====================================================================


def validate_plan(plan: DecompositionPlan, package: Any) -> list[str]:
    """Validate that a decomposition plan preserves all tasks and dependencies."""
    issues: list[str] = []
    tasks = getattr(package, "control_flow_tasks", [])
    task_names = {getattr(t, "name", "") for t in tasks}

    # Check all tasks are assigned
    assigned: set[str] = set()
    for sp in plan.sub_pipelines:
        for tn in sp.task_names:
            if tn in assigned:
                issues.append(f"Task '{tn}' assigned to multiple sub-pipelines")
            assigned.add(tn)

    missing = task_names - assigned
    if missing:
        issues.append(f"Tasks not assigned: {', '.join(sorted(missing))}")

    extra = assigned - task_names
    if extra:
        issues.append(f"Unknown tasks in plan: {', '.join(sorted(extra))}")

    # Check dependency references are valid
    sp_names = {sp.name for sp in plan.sub_pipelines}
    for sp in plan.sub_pipelines:
        for dep in sp.dependencies_on:
            if dep not in sp_names:
                issues.append(f"Sub-pipeline '{sp.name}' depends on unknown '{dep}'")

    # Check for circular dependencies in sub-pipeline graph
    orch_graph: dict[str, list[str]] = {sp.name: [] for sp in plan.sub_pipelines}
    for sp in plan.sub_pipelines:
        for dep in sp.dependencies_on:
            orch_graph.setdefault(dep, [])
            orch_graph[dep].append(sp.name)

    order = _topological_order(orch_graph)
    if len(order) < len(sp_names):
        issues.append("Circular dependency detected in sub-pipeline graph")

    return issues


# =====================================================================
# Report Writer
# =====================================================================


def write_decomposition_report(
    candidates: list[DecompositionCandidate],
    plans: list[DecompositionPlan],
    shared_flows: list[SharedDataFlow],
    output_path: Path,
) -> Path:
    """Write a JSON decomposition report."""
    report = {
        "total_candidates": len(candidates),
        "total_plans": len(plans),
        "total_shared_data_flows": len(shared_flows),
        "candidates": [c.to_dict() for c in candidates],
        "plans": [p.to_dict() for p in plans],
        "shared_data_flows": [sf.to_dict() for sf in shared_flows],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("decomposition_report_written", path=str(output_path))
    return output_path
