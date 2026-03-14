"""
Migration Engine - Orchestrator
================================
Coordinates the end-to-end migration of SSIS packages to Fabric.
Decides which generator to use for each component,
produces a migration plan, and executes the conversion.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    DataFlowComponentType,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator
from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator
from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# Directory and filename for persisted migration state
_STATE_DIR = ".ssis2fabric"
_STATE_FILE = "state.json"


class TargetArtifact(str, Enum):
    """Type of Fabric artifact to generate."""

    DATA_FACTORY_PIPELINE = "data_factory_pipeline"
    DATAFLOW_GEN2 = "dataflow_gen2"
    SPARK_NOTEBOOK = "spark_notebook"
    MANUAL_REVIEW = "manual_review"


@dataclass
class MigrationError:
    """Structured error/warning entry for the migration report."""

    source: str  # file path or task name
    severity: str  # "error", "warning", or "info"
    message: str
    suggested_fix: str = ""


@dataclass
class MigrationItem:
    """A single unit of migration work."""

    source_package: str
    source_task: str
    task_type: str
    target_artifact: TargetArtifact
    complexity: MigrationComplexity
    output_path: str = ""
    status: str = "pending"
    notes: list[str] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""
    elapsed_ms: float = 0.0


# Type alias for progress callbacks used by the engine and agents.
ProgressCallback = Any  # Callable[[str, dict[str, Any]], None] | None


@dataclass
class MigrationPlan:
    """Complete migration plan for one or more SSIS packages."""

    project_name: str
    created_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    strategy: str = ""
    items: list[MigrationItem] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    errors: list[MigrationError] = field(default_factory=list)
    completed_at: str = ""
    total_elapsed_ms: float = 0.0
    correlation_id: str = ""

    def add_error(self, source: str, severity: str, message: str, suggested_fix: str = "") -> None:
        """Append a structured error/warning to the plan."""
        self.errors.append(
            MigrationError(source=source, severity=severity, message=message, suggested_fix=suggested_fix)
        )

    def to_dict(self) -> dict[str, Any]:
        items_list = []
        for item in self.items:
            items_list.append(
                {
                    "source_package": item.source_package,
                    "source_task": item.source_task,
                    "task_type": item.task_type,
                    "target_artifact": item.target_artifact.value,
                    "complexity": item.complexity.value,
                    "output_path": item.output_path,
                    "status": item.status,
                    "notes": item.notes,
                    "started_at": item.started_at,
                    "completed_at": item.completed_at,
                    "elapsed_ms": item.elapsed_ms,
                }
            )
        errors_list = [
            {
                "source": e.source,
                "severity": e.severity,
                "message": e.message,
                "suggested_fix": e.suggested_fix,
            }
            for e in self.errors
        ]
        return {
            "project_name": self.project_name,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "total_elapsed_ms": self.total_elapsed_ms,
            "correlation_id": self.correlation_id,
            "strategy": self.strategy,
            "items": items_list,
            "summary": self.summary,
            "errors": errors_list,
        }


# ---------------------------------------------------------------------------
# Pre-migration assessment
# ---------------------------------------------------------------------------

# Effort weights per complexity level (person-hours per item)
_EFFORT_WEIGHTS: dict[MigrationComplexity, float] = {
    MigrationComplexity.LOW: 0.5,
    MigrationComplexity.MEDIUM: 2.0,
    MigrationComplexity.HIGH: 8.0,
    MigrationComplexity.MANUAL: 16.0,
}


@dataclass
class PackageAssessment:
    """Assessment result for a single package."""

    name: str
    file_path: str
    tasks: int
    data_flows: int
    connections: int
    parameters: int
    event_handlers: int
    complexity: str
    estimated_hours: float
    auto_migrate_pct: float
    risks: list[str] = field(default_factory=list)
    unsupported_components: list[str] = field(default_factory=list)


@dataclass
class AssessmentReport:
    """Pre-migration assessment across all packages."""

    project_name: str
    total_packages: int = 0
    total_tasks: int = 0
    total_data_flows: int = 0
    total_connections: int = 0
    estimated_total_hours: float = 0.0
    auto_migrate_pct: float = 0.0
    readiness_score: int = 0  # 0–100
    packages: list[PackageAssessment] = field(default_factory=list)
    connection_inventory: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_name": self.project_name,
            "total_packages": self.total_packages,
            "total_tasks": self.total_tasks,
            "total_data_flows": self.total_data_flows,
            "total_connections": self.total_connections,
            "estimated_total_hours": round(self.estimated_total_hours, 1),
            "auto_migrate_pct": round(self.auto_migrate_pct, 1),
            "readiness_score": self.readiness_score,
            "packages": [
                {
                    "name": p.name,
                    "file_path": p.file_path,
                    "tasks": p.tasks,
                    "data_flows": p.data_flows,
                    "connections": p.connections,
                    "parameters": p.parameters,
                    "event_handlers": p.event_handlers,
                    "complexity": p.complexity,
                    "estimated_hours": round(p.estimated_hours, 1),
                    "auto_migrate_pct": round(p.auto_migrate_pct, 1),
                    "risks": p.risks,
                    "unsupported_components": p.unsupported_components,
                }
                for p in self.packages
            ],
            "connection_inventory": self.connection_inventory,
        }


# ---------------------------------------------------------------------------
# Incremental migration helpers
# ---------------------------------------------------------------------------


def _file_sha256(path: Path) -> str:
    """Compute the SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_migration_state(state_dir: Path) -> dict[str, Any]:
    """Load the persisted migration state from ``.ssis2fabric/state.json``."""
    state_file = state_dir / _STATE_FILE
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_migration_state(state_dir: Path, state: dict[str, Any]) -> None:
    """Persist migration state to ``.ssis2fabric/state.json``."""
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / _STATE_FILE
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")


class MigrationEngine:
    """
    Core engine that orchestrates SSIS-to-Fabric migration.

    Workflow:
    1. Analyze SSIS packages (via DTSXParser)
    2. Generate migration plan (route to DF or Spark)
    3. Execute generators to produce Fabric artifacts
    4. Write outputs and migration report
    """

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config
        self.df_generator = DataFactoryGenerator(config)
        self.dataflow_generator = DataflowGen2Generator(config)
        self.spark_generator = SparkNotebookGenerator(config)
        self.plan: MigrationPlan | None = None

        from ssis_to_fabric.engine.plugin_registry import get_hook_manager
        self._hooks = get_hook_manager()

    def create_plan(self, packages: list[SSISPackage]) -> MigrationPlan:
        """
        Create a migration plan from analyzed SSIS packages.
        Determines the target artifact type for each task.
        """
        self._hooks.fire("pre_plan", {
            "packages": [p.name for p in packages],
        })

        plan = MigrationPlan(
            project_name=self.config.project_name,
            strategy=self.config.strategy.value,
        )

        for package in packages:
            self._plan_package(package, plan)
            # Propagate parser warnings as structured errors in the report
            for warning in package.warnings:
                plan.add_error(
                    source=package.file_path or package.name,
                    severity="warning",
                    message=warning,
                    suggested_fix="Review the .dtsx file for XML or structural issues.",
                )

        # Compute summary
        plan.summary = self._compute_summary(plan)
        self.plan = plan

        self._hooks.fire("post_plan", {
            "plan": plan,
            "total_items": len(plan.items),
        })

        logger.info(
            "migration_plan_created",
            total_items=len(plan.items),
            df_items=plan.summary.get("data_factory_pipeline", 0),
            spark_items=plan.summary.get("spark_notebook", 0),
            manual_items=plan.summary.get("manual_review", 0),
        )
        return plan

    def execute(
        self,
        packages: list[SSISPackage],
        plan: MigrationPlan | None = None,
        *,
        incremental: bool = False,
        state_dir: Path | None = None,
        progress_callback: ProgressCallback = None,
    ) -> MigrationPlan:
        """
        Execute the migration: generate all Fabric artifacts.

        Two-pass approach:
          Phase 1: Generate standalone artifacts (Dataflow Gen2, Spark notebooks)
                   for individual tasks that require them.
          Phase 2: Generate one consolidated Data Factory pipeline per SSIS package
                   with all tasks as inline activities (Script, Dataflow,
                   TridentNotebook, InvokePipeline, etc.).

        Args:
            packages: Parsed SSIS packages
            plan: Optional pre-computed plan. If None, creates one.
            incremental: When ``True``, compare SHA-256 hashes of ``.dtsx``
                files against persisted state and skip unchanged packages.
            state_dir: Directory for ``state.json``; defaults to
                ``.ssis2fabric/`` in the current working directory.
            progress_callback: Optional callable ``(event: str, data: dict) -> None``
                invoked on ``item_started``, ``item_completed``, ``phase_completed``.
        Returns:
            Updated migration plan with execution status.
        """
        from ssis_to_fabric.logging_config import bind_correlation_id, write_audit_entry

        execution_start = time.monotonic()

        if plan is None:
            plan = self.create_plan(packages)

        # Bind correlation ID for structured log context
        cid = bind_correlation_id()
        plan.correlation_id = cid

        write_audit_entry("migration_started", detail={
            "project": plan.project_name,
            "strategy": plan.strategy,
            "packages": len(packages),
            "items": len(plan.items),
        })

        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # ------------------------------------------------------------------
        # Incremental change detection
        # ------------------------------------------------------------------
        _state_dir = state_dir or Path(_STATE_DIR)
        prev_state = _load_migration_state(_state_dir) if incremental else {}
        current_hashes: dict[str, str] = {}

        changed_packages: set[str] = set()
        for pkg in packages:
            if pkg.file_path:
                try:
                    sha = _file_sha256(Path(pkg.file_path))
                except OSError:
                    sha = ""
                current_hashes[pkg.name] = sha
                if not incremental or prev_state.get(pkg.name, {}).get("hash") != sha:
                    changed_packages.add(pkg.name)
            else:
                changed_packages.add(pkg.name)

        if incremental:
            skipped = [p.name for p in packages if p.name not in changed_packages]
            if skipped:
                logger.info("incremental_skip", skipped=skipped)
                for item in plan.items:
                    if item.source_package not in changed_packages:
                        item.status = "skipped"
                        item.notes.append("Skipped (incremental — no changes detected)")

            # Task-level checkpoint: skip tasks that completed in a prior run
            # (for packages that changed but have partially completed tasks from error recovery)
            for item in plan.items:
                if item.status == "skipped":
                    continue
                pkg_state = prev_state.get(item.source_package, {})
                prior_tasks = pkg_state.get("completed_tasks", [])
                prior_hash = pkg_state.get("hash", "")
                cur_hash = current_hashes.get(item.source_package, "")
                if prior_hash == cur_hash and item.source_task in prior_tasks:
                    item.status = "skipped"
                    item.notes.append("Skipped (task completed in prior run)")

        # Clean previous output to prevent stale artifacts from prior runs
        import shutil

        if incremental:
            # In incremental mode, only remove artifacts for changed packages
            from ssis_to_fabric.engine.utils import sanitize_name as _sanitize

            for subdir in ("pipelines", "notebooks", "dataflows"):
                target = output_dir / subdir
                if not target.exists():
                    continue
                for f in target.iterdir():
                    if any(f.stem.startswith(_sanitize(cp)) for cp in changed_packages):
                        if f.is_file():
                            f.unlink()
                        elif f.is_dir():
                            shutil.rmtree(f)
        else:
            for subdir in ("pipelines", "notebooks", "dataflows", "connections"):
                target = output_dir / subdir
                if target.exists():
                    shutil.rmtree(target)

        # Build a lookup of packages by name
        pkg_map = {pkg.name: pkg for pkg in packages}

        # Fire pre_generate hook
        self._hooks.fire("pre_generate", {
            "packages": [p.name for p in packages],
            "plan": plan,
            "output_dir": str(output_dir),
        })

        # --- Phase 1: Generate standalone artifacts (dataflows, notebooks) ---
        _cb = progress_callback
        phase1_start = time.monotonic()
        for item in plan.items:
            if item.status == "skipped":
                continue
            try:
                package = pkg_map.get(item.source_package)
                if package is None:
                    item.status = "error"
                    item.notes.append(f"Package not found: {item.source_package}")
                    continue

                task = self._find_task(package.control_flow_tasks, item.source_task)

                item.started_at = datetime.now(tz=timezone.utc).isoformat()
                item_start = time.monotonic()
                if _cb:
                    _cb("item_started", {"package": item.source_package, "task": item.source_task})

                if item.target_artifact == TargetArtifact.DATAFLOW_GEN2:
                    output_path = self.dataflow_generator.generate(package, task, output_dir)
                    item.output_path = str(output_path)
                    item.status = "completed"

                elif item.target_artifact == TargetArtifact.SPARK_NOTEBOOK:
                    output_path = self.spark_generator.generate(package, task, output_dir)
                    item.output_path = str(output_path)
                    item.status = "completed"

                elif item.target_artifact == TargetArtifact.MANUAL_REVIEW:
                    item.status = "manual_review_required"
                    item.notes.append("This task requires manual migration review.")

                elif item.target_artifact == TargetArtifact.DATA_FACTORY_PIPELINE:
                    # Inline activities — generated as part of the package pipeline in Phase 2
                    item.status = "pending_pipeline"

                item.elapsed_ms = (time.monotonic() - item_start) * 1000
                item.completed_at = datetime.now(tz=timezone.utc).isoformat()
                if _cb:
                    _cb("item_completed", {
                        "package": item.source_package,
                        "task": item.source_task,
                        "status": item.status,
                        "elapsed_ms": item.elapsed_ms,
                    })

            except Exception as e:
                item.status = "error"
                item.elapsed_ms = (time.monotonic() - item_start) * 1000 if 'item_start' in dir() else 0
                item.completed_at = datetime.now(tz=timezone.utc).isoformat()
                item.notes.append(f"Generation error: {str(e)}")
                logger.error("generation_failed", task=item.source_task, error=str(e))
                plan.add_error(
                    source=f"{item.source_package}/{item.source_task}",
                    severity="error",
                    message=f"Generation error: {e}",
                    suggested_fix="Check the SSIS component structure and generator logs.",
                )

        phase1_ms = (time.monotonic() - phase1_start) * 1000
        if _cb:
            _cb("phase_completed", {"phase": "phase1_standalone", "elapsed_ms": phase1_ms})

        # --- Phase 2: Generate consolidated pipelines (one per package) ---
        phase2_start = time.monotonic()
        for package in packages:
            try:
                # Build task routing from plan items
                task_routing: dict[str, TargetArtifact] = {}
                for item in plan.items:
                    if item.source_package == package.name:
                        task_routing[item.source_task] = item.target_artifact

                p2_start = time.monotonic()
                pipeline_path = self.df_generator.generate_package_pipeline(package, task_routing, output_dir)
                p2_elapsed = (time.monotonic() - p2_start) * 1000

                # Mark inline items as completed with pipeline path
                for item in plan.items:
                    if item.source_package == package.name and item.status == "pending_pipeline":
                        item.output_path = str(pipeline_path)
                        item.status = "completed"
                        item.elapsed_ms = p2_elapsed
                        item.completed_at = datetime.now(tz=timezone.utc).isoformat()

                if _cb:
                    _cb("item_completed", {
                        "package": package.name,
                        "task": "pipeline",
                        "status": "completed",
                        "elapsed_ms": p2_elapsed,
                    })

            except Exception as e:
                for item in plan.items:
                    if item.source_package == package.name and item.status == "pending_pipeline":
                        item.status = "error"
                        item.notes.append(f"Pipeline generation error: {str(e)}")
                logger.error("package_pipeline_failed", package=package.name, error=str(e))
                plan.add_error(
                    source=package.file_path or package.name,
                    severity="error",
                    message=f"Pipeline generation error: {e}",
                    suggested_fix="Review the package structure and Data Factory generator logs.",
                )

        phase2_ms = (time.monotonic() - phase2_start) * 1000
        if _cb:
            _cb("phase_completed", {"phase": "phase2_pipelines", "elapsed_ms": phase2_ms})

        # Fire post_generate hook
        self._hooks.fire("post_generate", {
            "plan": plan,
            "output_dir": str(output_dir),
            "phase1_ms": phase1_ms,
            "phase2_ms": phase2_ms,
        })

        # --- Phase 3: Generate connection manifests & logging config ---
        self._generate_connection_manifests(packages, output_dir)
        self._generate_logging_config(packages, output_dir)

        # Persist updated state for incremental runs (package-level + task-level checkpoints)
        if incremental or current_hashes:
            new_state = dict(prev_state)
            for pkg in packages:
                if pkg.name in current_hashes:
                    # Task-level checkpoint: record which tasks completed
                    completed_tasks = [
                        item.source_task
                        for item in plan.items
                        if item.source_package == pkg.name and item.status == "completed"
                    ]
                    new_state[pkg.name] = {
                        "hash": current_hashes[pkg.name],
                        "migrated_at": datetime.now(tz=timezone.utc).isoformat(),
                        "file_path": pkg.file_path,
                        "completed_tasks": completed_tasks,
                    }
            _save_migration_state(_state_dir, new_state)

        # Write migration report
        plan.completed_at = datetime.now(tz=timezone.utc).isoformat()
        plan.total_elapsed_ms = (time.monotonic() - execution_start) * 1000
        self._write_report(plan, output_dir, packages)

        completed_count = sum(1 for i in plan.items if i.status == "completed")
        error_count = sum(1 for i in plan.items if i.status == "error")
        manual_count = sum(1 for i in plan.items if i.status == "manual_review_required")
        logger.info(
            "migration_completed",
            completed=completed_count,
            errors=error_count,
            manual=manual_count,
            elapsed_ms=plan.total_elapsed_ms,
        )
        write_audit_entry("migration_completed", detail={
            "completed": completed_count,
            "errors": error_count,
            "manual": manual_count,
            "elapsed_ms": round(plan.total_elapsed_ms, 1),
        })
        if _cb:
            _cb("migration_completed", {
                "elapsed_ms": plan.total_elapsed_ms,
                "completed": completed_count,
                "errors": error_count,
            })
        return plan

    # =========================================================================
    # Planning Logic
    # =========================================================================

    def _plan_package(self, package: SSISPackage, plan: MigrationPlan) -> None:
        """Add migration items for all tasks in a package."""
        for task in package.control_flow_tasks:
            self._plan_task(package.name, task, plan)

    def _plan_task(self, package_name: str, task: ControlFlowTask, plan: MigrationPlan) -> None:
        """Determine target artifact for a single task and add to plan."""
        target = self._route_task(task)

        plan.items.append(
            MigrationItem(
                source_package=package_name,
                source_task=task.name,
                task_type=task.task_type.value,
                target_artifact=target,
                complexity=task.migration_complexity,
            )
        )

        # Recursively plan child tasks
        for child in task.child_tasks:
            self._plan_task(package_name, child, plan)

    def _route_task(self, task: ControlFlowTask) -> TargetArtifact:
        """Decide which Fabric artifact to generate for a task."""
        strategy = self.config.strategy

        # Tasks that always require manual review
        if task.migration_complexity == MigrationComplexity.MANUAL:
            return TargetArtifact.MANUAL_REVIEW

        # Pure Data Factory strategy — no Dataflow Gen2 or Spark
        if strategy == MigrationStrategy.DATA_FACTORY:
            if task.migration_complexity == MigrationComplexity.HIGH:
                return TargetArtifact.MANUAL_REVIEW
            return TargetArtifact.DATA_FACTORY_PIPELINE

        # Pure Spark strategy
        if strategy == MigrationStrategy.SPARK:
            return TargetArtifact.SPARK_NOTEBOOK

        # Hybrid strategy (default):
        #   - Data flows → Notebook (default) or Dataflow Gen2
        #   - Complex/script data flows → always Spark notebook
        #   - Execute SQL → inline pipeline activity
        #   - Containers → flattened in pipeline
        if task.task_type == TaskType.DATA_FLOW:
            has_complex = any(
                c.migration_complexity in (MigrationComplexity.HIGH, MigrationComplexity.MANUAL)
                for c in task.data_flow_components
            )
            has_script = any(
                c.component_type == DataFlowComponentType.SCRIPT_COMPONENT for c in task.data_flow_components
            )
            if has_complex or has_script:
                return TargetArtifact.SPARK_NOTEBOOK

            # Respect dataflow_type configuration
            from ssis_to_fabric.config import DataflowType

            if self.config.dataflow_type == DataflowType.DATAFLOW_GEN2:
                return TargetArtifact.DATAFLOW_GEN2
            return TargetArtifact.SPARK_NOTEBOOK

        if task.task_type == TaskType.EXECUTE_SQL:
            return TargetArtifact.DATA_FACTORY_PIPELINE

        if task.task_type == TaskType.SCRIPT:
            return TargetArtifact.SPARK_NOTEBOOK

        if task.task_type == TaskType.EXECUTE_PROCESS:
            return TargetArtifact.DATA_FACTORY_PIPELINE

        if task.task_type == TaskType.SEND_MAIL:
            return TargetArtifact.DATA_FACTORY_PIPELINE

        if task.task_type in (TaskType.SEQUENCE_CONTAINER, TaskType.FOR_LOOP, TaskType.FOREACH_LOOP):
            return TargetArtifact.DATA_FACTORY_PIPELINE

        if task.migration_complexity in (MigrationComplexity.HIGH,):
            return TargetArtifact.SPARK_NOTEBOOK

        return TargetArtifact.DATA_FACTORY_PIPELINE

    # =========================================================================
    # Helpers
    # =========================================================================

    def _find_task(self, tasks: list[ControlFlowTask], task_name: str) -> ControlFlowTask | None:
        """Find a task by name in a potentially nested task list."""
        for task in tasks:
            if task.name == task_name:
                return task
            found = self._find_task(task.child_tasks, task_name)
            if found:
                return found
        return None

    def _compute_summary(self, plan: MigrationPlan) -> dict[str, Any]:
        """Compute summary statistics for the plan."""
        summary: dict[str, int] = {}
        for item in plan.items:
            key = item.target_artifact.value
            summary[key] = summary.get(key, 0) + 1

        complexity_counts: dict[str, int] = {}
        for item in plan.items:
            key = item.complexity.value
            complexity_counts[key] = complexity_counts.get(key, 0) + 1

        return {
            **summary,
            "total": len(plan.items),
            "complexity_breakdown": complexity_counts,
        }

    def _write_report(self, plan: MigrationPlan, output_dir: Path, packages: list[SSISPackage] | None = None) -> None:
        """Write migration report to output directory."""
        report_data = plan.to_dict()

        # Generate column lineage and embed Sankey data in report
        if packages:
            from ssis_to_fabric.engine.column_lineage import ColumnLineageGraph

            col_graph = ColumnLineageGraph()
            col_graph.build(packages)
            if col_graph.edges:
                col_graph.write_json(output_dir)
                report_data["column_lineage"] = {
                    "sankey": col_graph.to_sankey_data(),
                    "summary": col_graph.to_dict()["summary"],
                }

        report_path = output_dir / "migration_report.json"
        with open(report_path, "w") as f:
            json.dump(report_data, f, indent=2)
        logger.info("report_written", path=str(report_path))

    # =========================================================================
    # Pre-Migration Assessment
    # =========================================================================

    def assess(self, packages: list[SSISPackage]) -> AssessmentReport:
        """Perform a pre-migration readiness assessment.

        Produces per-package effort estimates, risk flags, and an overall
        readiness score (0–100) without generating any artifacts.
        """
        report = AssessmentReport(project_name=self.config.project_name)
        report.total_packages = len(packages)

        all_connections: dict[str, dict[str, str]] = {}
        total_auto = 0
        total_items = 0

        # Create plan once for all packages (O(N) instead of O(N^2))
        full_plan = self.create_plan(packages)

        # Index plan items by package name
        plan_items_by_pkg: dict[str, list[MigrationItem]] = {}
        for item in full_plan.items:
            plan_items_by_pkg.setdefault(item.source_package, []).append(item)

        for pkg in packages:
            pkg_items = plan_items_by_pkg.get(pkg.name, [])
            pkg_assessment = self._assess_package(pkg, pkg_items)
            report.packages.append(pkg_assessment)
            report.total_tasks += pkg_assessment.tasks
            report.total_data_flows += pkg_assessment.data_flows
            report.estimated_total_hours += pkg_assessment.estimated_hours

            # Track automatable items
            auto = sum(
                1 for item in pkg_items
                if item.target_artifact != TargetArtifact.MANUAL_REVIEW
            )
            total_auto += auto
            total_items += len(pkg_items)

            # Collect connections
            for cm in pkg.connection_managers:
                if cm.name not in all_connections:
                    all_connections[cm.name] = {
                        "name": cm.name,
                        "type": cm.connection_type.value,
                        "server": cm.server,
                        "database": cm.database,
                    }

        report.total_connections = len(all_connections)
        report.connection_inventory = list(all_connections.values())
        report.auto_migrate_pct = (total_auto / max(total_items, 1)) * 100

        # Readiness score: 0–100 based on auto-migratable %, warnings, and complexity
        risk_penalty = min(
            30,
            sum(len(p.risks) for p in report.packages) * 5,
        )
        report.readiness_score = max(0, min(100, int(report.auto_migrate_pct - risk_penalty)))

        logger.info(
            "assessment_completed",
            packages=report.total_packages,
            readiness=report.readiness_score,
            estimated_hours=round(report.estimated_total_hours, 1),
        )
        return report

    def _assess_package(self, package: SSISPackage, plan_items: list[MigrationItem]) -> PackageAssessment:
        """Assess a single SSIS package."""
        risks: list[str] = []
        unsupported: list[str] = []

        # Gather all complexities and component types
        def _walk_tasks(tasks: list[ControlFlowTask]) -> None:
            for task in tasks:
                if task.task_type == TaskType.SCRIPT:
                    risks.append(f"Script Task '{task.name}' requires C# transpilation review")
                for comp in task.data_flow_components:
                    if comp.migration_complexity == MigrationComplexity.MANUAL:
                        unsupported.append(f"{comp.component_type.value} in '{task.name}'")
                    if comp.component_type == DataFlowComponentType.SCRIPT_COMPONENT:
                        risks.append(f"Script Component in '{task.name}' needs manual review")
                _walk_tasks(task.child_tasks)

        _walk_tasks(package.control_flow_tasks)

        if package.event_handlers:
            for eh in package.event_handlers:
                if eh.event_type != "OnError":
                    risks.append(f"Event handler '{eh.event_type}' requires manual review")

        if package.status == "partial":
            risks.append("Package had parse warnings — review migration output")

        # Estimate effort
        effort = 0.0
        for task in package.control_flow_tasks:
            effort += self._estimate_task_effort(task)

        # Auto-migrate percentage from pre-computed plan items
        auto = sum(1 for i in plan_items if i.target_artifact != TargetArtifact.MANUAL_REVIEW)
        auto_pct = (auto / max(len(plan_items), 1)) * 100

        return PackageAssessment(
            name=package.name,
            file_path=package.file_path,
            tasks=package.total_tasks,
            data_flows=package.total_data_flows,
            connections=len(package.connection_managers),
            parameters=len(package.parameters) + len(package.project_parameters),
            event_handlers=len(package.event_handlers),
            complexity=package.overall_complexity.value,
            estimated_hours=effort,
            auto_migrate_pct=auto_pct,
            risks=risks,
            unsupported_components=unsupported,
        )

    def _estimate_task_effort(self, task: ControlFlowTask) -> float:
        """Estimate effort in person-hours for a single task tree."""
        effort = _EFFORT_WEIGHTS.get(task.migration_complexity, 1.0)
        for comp in task.data_flow_components:
            effort += _EFFORT_WEIGHTS.get(comp.migration_complexity, 0.5) * 0.5
        for child in task.child_tasks:
            effort += self._estimate_task_effort(child)
        return effort

    # =========================================================================
    # Connection Manifest Generation
    # =========================================================================

    # SQL-type SSIS connection types that map to Fabric Warehouse connections
    _SQL_CONNECTION_TYPES = {"OLEDB", "ADO.NET", "ODBC", "ORACLE"}

    def _generate_connection_manifests(self, packages: list[SSISPackage], output_dir: Path) -> None:
        """Generate one JSON file per unique SSIS connection manager.

        The ``connections/`` folder enables the deployer to auto-discover or
        auto-create Fabric connections at deploy time, eliminating the need
        for a manual ``--connection-id`` flag.

        Each file contains the SSIS connection metadata and a suggested
        ``fabric_target_type`` (``Warehouse`` for SQL connections,
        ``Lakehouse`` for file-based ones).
        """
        seen: dict[str, dict[str, Any]] = {}
        for pkg in packages:
            for cm in pkg.connection_managers:
                if cm.name in seen:
                    continue
                ct = cm.connection_type.value
                if ct in self._SQL_CONNECTION_TYPES:
                    fabric_target = "Warehouse"
                elif ct in ("FLAT_FILE", "EXCEL", "FILE"):
                    fabric_target = "Lakehouse"
                else:
                    fabric_target = "Other"
                seen[cm.name] = {
                    "ssis_name": cm.name,
                    "ssis_id": cm.id,
                    "ssis_type": ct,
                    "server": cm.server,
                    "database": cm.database,
                    "provider": cm.provider,
                    "fabric_target_type": fabric_target,
                }

        if not seen:
            return

        conn_dir = output_dir / "connections"
        conn_dir.mkdir(parents=True, exist_ok=True)
        for name, manifest in seen.items():
            safe_name = name.replace(" ", "_").replace("/", "_")
            path = conn_dir / f"{safe_name}.json"
            path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        logger.info("connection_manifests_generated", count=len(seen), path=str(conn_dir))

    def _generate_logging_config(self, packages: list[SSISPackage], output_dir: Path) -> None:
        """Generate a logging configuration manifest from SSIS log providers.

        Collects all ``LogProvider`` objects across packages and writes a
        single ``logging_config.json`` summarising each provider with its
        type and connection reference.
        """
        providers: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        for pkg in packages:
            for lp in pkg.logging_providers:
                if lp.name in seen_names:
                    continue
                seen_names.add(lp.name)
                providers.append({
                    "name": lp.name,
                    "provider_type": lp.provider_type,
                    "connection_manager_ref": lp.connection_manager_ref,
                    "description": lp.description,
                    "source_package": pkg.name,
                })

        if not providers:
            return

        path = output_dir / "logging_config.json"
        path.write_text(json.dumps({"logging_providers": providers}, indent=2), encoding="utf-8")
        logger.info("logging_config_generated", count=len(providers), path=str(path))
