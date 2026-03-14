"""
Migration Engine - Orchestrator
================================
Coordinates the end-to-end migration of SSIS packages to Fabric.
Decides which generator to use for each component,
produces a migration plan, and executes the conversion.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional

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

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)

# Type alias for progress callbacks (used by agents and CLI)
ProgressCallback = Optional[Callable[[str, dict[str, Any]], None]]



class TargetArtifact(str, Enum):
    """Type of Fabric artifact to generate."""

    DATA_FACTORY_PIPELINE = "data_factory_pipeline"
    DATAFLOW_GEN2 = "dataflow_gen2"
    SPARK_NOTEBOOK = "spark_notebook"
    MANUAL_REVIEW = "manual_review"


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


@dataclass
class PackageAssessment:
    """Pre-migration assessment for a single package."""

    name: str
    file_path: str = ""
    tasks: int = 0
    data_flows: int = 0
    connections: int = 0
    parameters: int = 0
    event_handlers: int = 0
    complexity: str = "LOW"
    estimated_hours: float = 0.0
    auto_migrate_pct: float = 100.0
    risks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "file_path": self.file_path,
            "tasks": self.tasks,
            "data_flows": self.data_flows,
            "connections": self.connections,
            "parameters": self.parameters,
            "event_handlers": self.event_handlers,
            "complexity": self.complexity,
            "estimated_hours": self.estimated_hours,
            "auto_migrate_pct": self.auto_migrate_pct,
            "risks": self.risks,
        }


@dataclass
class AssessmentReport:
    """Pre-migration readiness assessment report."""

    project_name: str = ""
    total_packages: int = 0
    total_tasks: int = 0
    total_data_flows: int = 0
    total_connections: int = 0
    readiness_score: int = 0
    estimated_total_hours: float = 0.0
    auto_migrate_pct: float = 100.0
    packages: list[PackageAssessment] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    unsupported_components: list[str] = field(default_factory=list)
    connection_inventory: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "project_name": self.project_name,
            "total_packages": self.total_packages,
            "total_tasks": self.total_tasks,
            "total_data_flows": self.total_data_flows,
            "total_connections": self.total_connections,
            "readiness_score": self.readiness_score,
            "estimated_total_hours": self.estimated_total_hours,
            "auto_migrate_pct": self.auto_migrate_pct,
            "packages": [p.to_dict() for p in self.packages],
            "risks": self.risks,
            "unsupported_components": self.unsupported_components,
            "connection_inventory": self.connection_inventory,
        }


@dataclass
class MigrationPlan:
    """Complete migration plan for one or more SSIS packages."""

    project_name: str
    created_at: str = field(default_factory=lambda: datetime.now(tz=__import__("datetime").timezone.utc).isoformat())
    strategy: str = ""
    items: list[MigrationItem] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    completed_at: str = ""
    total_elapsed_ms: float = 0.0
    correlation_id: str = ""
    errors: list[Any] = field(default_factory=list)

    def add_error(self, *, source: str, severity: str = "error", message: str = "", suggested_fix: str = "") -> None:
        """Append a structured error to the plan."""
        self.errors.append(MigrationError(source=source, severity=severity, message=message, suggested_fix=suggested_fix))

    def to_dict(self) -> dict:
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
        return {
            "project_name": self.project_name,
            "created_at": self.created_at,
            "strategy": self.strategy,
            "items": items_list,
            "summary": self.summary,
            "completed_at": self.completed_at,
            "total_elapsed_ms": self.total_elapsed_ms,
            "correlation_id": self.correlation_id,
            "errors": [e.to_dict() for e in self.errors],
        }


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

    def create_plan(self, packages: list[SSISPackage]) -> MigrationPlan:
        """
        Create a migration plan from analyzed SSIS packages.
        Determines the target artifact type for each task.
        """
        # Fire pre_plan hook
        try:
            from ssis_to_fabric.engine.plugin_registry import get_hook_manager
            hm = get_hook_manager()
            hm.fire("pre_plan", {"packages": [p.name for p in packages]})
        except Exception:
            pass

        plan = MigrationPlan(
            project_name=self.config.project_name,
            strategy=self.config.strategy.value,
        )

        for package in packages:
            self._plan_package(package, plan)

        # Compute summary
        plan.summary = self._compute_summary(plan)
        self.plan = plan

        logger.info(
            "migration_plan_created",
            total_items=len(plan.items),
            df_items=plan.summary.get("data_factory_pipeline", 0),
            spark_items=plan.summary.get("spark_notebook", 0),
            manual_items=plan.summary.get("manual_review", 0),
        )

        # Fire post_plan hook
        try:
            from ssis_to_fabric.engine.plugin_registry import get_hook_manager
            hm = get_hook_manager()
            hm.fire("post_plan", {"plan": plan})
        except Exception:
            pass

        return plan

    def assess(self, packages: list[SSISPackage]) -> AssessmentReport:
        """Pre-migration readiness assessment with effort estimates.

        Args:
            packages: Parsed SSIS packages.
        Returns:
            AssessmentReport with readiness score and per-package estimates.
        """
        plan = self.create_plan(packages)
        _EFFORT = {"LOW": 0.5, "MEDIUM": 2.0, "HIGH": 8.0, "MANUAL": 16.0}

        pkg_assessments: list[PackageAssessment] = []
        all_risks: list[str] = []

        for pkg in packages:
            tasks = pkg.control_flow_tasks
            n_tasks = pkg.total_tasks
            n_dfs = pkg.total_data_flows
            n_conns = len(pkg.connection_managers)
            n_params = len(pkg.parameters) + len(pkg.project_parameters)
            n_events = len(pkg.event_handlers)
            complexity = pkg.overall_complexity.value

            pkg_items = [i for i in plan.items if i.source_package == pkg.name]
            hours = sum(_EFFORT.get(i.complexity.value, 2.0) for i in pkg_items)
            auto = sum(1 for i in pkg_items if i.complexity.value in ("LOW", "MEDIUM"))
            auto_pct = (auto / len(pkg_items) * 100) if pkg_items else 100.0

            risks: list[str] = []
            for t in tasks:
                if t.task_type == TaskType.SCRIPT:
                    risks.append(f"Script Task '{t.name}' needs manual review")
                for c in t.data_flow_components:
                    if c.component_type == DataFlowComponentType.SCRIPT_COMPONENT:
                        risks.append(f"Script Component in '{t.name}'")
            if n_events > 0:
                for eh in pkg.event_handlers:
                    if eh.event_type != "OnError":
                        risks.append(f"Event handler '{eh.event_type}' requires review")
            # Flag partially parsed packages
            if pkg.status == "partial":
                risks.append(f"Package '{pkg.name}' has parse warnings ({len(pkg.warnings)} warnings)")

            all_risks.extend(risks)
            pkg_assessments.append(PackageAssessment(
                name=pkg.name, file_path=pkg.file_path, tasks=n_tasks,
                data_flows=n_dfs, connections=n_conns, parameters=n_params,
                event_handlers=n_events, complexity=complexity,
                estimated_hours=hours, auto_migrate_pct=round(auto_pct, 1),
                risks=risks,
            ))

        total_hours = sum(p.estimated_hours for p in pkg_assessments)
        total_tasks = sum(p.tasks for p in pkg_assessments)
        total_dfs = sum(p.data_flows for p in pkg_assessments)
        total_conns = len({c.name for pkg in packages for c in pkg.connection_managers})

        avg_auto = (sum(p.auto_migrate_pct for p in pkg_assessments) / len(pkg_assessments)
                    if pkg_assessments else 100.0)
        readiness = max(0, min(100, int(avg_auto - len(all_risks) * 2)))

        # Build connection inventory
        conn_inventory: list[dict] = []
        seen_conns: set[str] = set()
        for pkg in packages:
            for cm in pkg.connection_managers:
                if cm.name not in seen_conns:
                    seen_conns.add(cm.name)
                    conn_inventory.append({
                        "name": cm.name,
                        "type": cm.connection_type.value,
                        "server": cm.server,
                        "database": cm.database,
                    })

        return AssessmentReport(
            project_name=self.config.project_name,
            total_packages=len(packages),
            total_tasks=total_tasks,
            total_data_flows=total_dfs,
            total_connections=total_conns,
            readiness_score=readiness,
            estimated_total_hours=round(total_hours, 1),
            auto_migrate_pct=round(avg_auto, 1),
            packages=pkg_assessments,
            risks=all_risks,
            connection_inventory=conn_inventory,
        )

    def execute(
        self,
        packages: list[SSISPackage],
        plan: MigrationPlan | None = None,
        *,
        progress_callback: ProgressCallback = None,
        incremental: bool = False,
        state_dir: Any | None = None,
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
            progress_callback: Optional callback for progress events.
            incremental: Only regenerate changed packages.
            state_dir: Directory for incremental state persistence.
        Returns:
            Updated migration plan with execution status.
        """
        import time
        import uuid

        _start = time.monotonic()

        if plan is None:
            plan = self.create_plan(packages)

        # Assign correlation id for traceability
        plan.correlation_id = plan.correlation_id or f"ssis2fabric-{uuid.uuid4().hex[:12]}"

        # Fire pre_generate hook
        try:
            from ssis_to_fabric.engine.plugin_registry import get_hook_manager
            hm = get_hook_manager()
            hm.fire("pre_generate", {"packages": [p.name for p in packages], "plan": plan})
        except Exception:
            pass

        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Clean previous output to prevent stale artifacts from prior runs
        import shutil

        if not incremental:
            for subdir in ("pipelines", "notebooks", "dataflows", "connections"):
                target = output_dir / subdir
                if target.exists():
                    shutil.rmtree(target)

        # Build a lookup of packages by name
        pkg_map = {pkg.name: pkg for pkg in packages}

        # --- Phase 1: Generate standalone artifacts (dataflows, notebooks) ---
        for item in plan.items:
            item_start = time.monotonic()
            item.started_at = datetime.now(tz=__import__("datetime").timezone.utc).isoformat()
            try:
                package = pkg_map.get(item.source_package)
                if package is None:
                    item.status = "error"
                    item.notes.append(f"Package not found: {item.source_package}")
                    continue

                task = self._find_task(package.control_flow_tasks, item.source_task)

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

            except Exception as e:
                item.status = "error"
                item.notes.append(f"Generation error: {str(e)}")
                logger.error("generation_failed", task=item.source_task, error=str(e))
            finally:
                item.completed_at = datetime.now(tz=__import__("datetime").timezone.utc).isoformat()
                item.elapsed_ms = (time.monotonic() - item_start) * 1000
                if progress_callback:
                    progress_callback("item_completed", {"item": item.source_task, "status": item.status})

        if progress_callback:
            progress_callback("phase_completed", {"phase": "standalone_artifacts"})

        # --- Phase 2: Generate consolidated pipelines (one per package) ---
        for package in packages:
            try:
                # Build task routing from plan items
                task_routing: dict[str, TargetArtifact] = {}
                for item in plan.items:
                    if item.source_package == package.name:
                        task_routing[item.source_task] = item.target_artifact

                pipeline_path = self.df_generator.generate_package_pipeline(package, task_routing, output_dir)

                # Mark inline items as completed with pipeline path
                for item in plan.items:
                    if item.source_package == package.name and item.status == "pending_pipeline":
                        item.output_path = str(pipeline_path)
                        item.status = "completed"

            except Exception as e:
                for item in plan.items:
                    if item.source_package == package.name and item.status == "pending_pipeline":
                        item.status = "error"
                        item.notes.append(f"Pipeline generation error: {str(e)}")
                logger.error("package_pipeline_failed", package=package.name, error=str(e))

        # --- Phase 3: Generate connection manifests ---
        self._generate_connection_manifests(packages, output_dir)

        # --- Phase 4: Generate logging config ---
        self._generate_logging_config(packages, output_dir)

        # --- Incremental state persistence ---
        if incremental and state_dir is not None:
            from pathlib import Path as _P
            _state: dict = {}
            for pkg in packages:
                _state[pkg.name] = {
                    "hash": _file_sha256(_P(pkg.file_path)) if pkg.file_path and _P(pkg.file_path).exists() else "",
                    "migrated_at": datetime.now(tz=__import__("datetime").timezone.utc).isoformat(),
                    "file_path": pkg.file_path,
                    "completed_tasks": [i.source_task for i in plan.items if i.source_package == pkg.name and i.status == "completed"],
                }
            _save_migration_state(state_dir, _state)

        # Finalize timing
        plan.completed_at = datetime.now(tz=__import__("datetime").timezone.utc).isoformat()
        plan.total_elapsed_ms = (time.monotonic() - _start) * 1000

        # Write migration report
        self._write_report(plan, output_dir)

        # Fire post_generate hook
        try:
            from ssis_to_fabric.engine.plugin_registry import get_hook_manager
            hm = get_hook_manager()
            hm.fire("post_generate", {"plan": plan})
        except Exception:
            pass

        if progress_callback:
            progress_callback("migration_completed", {
                "completed": sum(1 for i in plan.items if i.status == "completed"),
                "errors": sum(1 for i in plan.items if i.status == "error"),
            })

        logger.info(
            "migration_completed",
            completed=sum(1 for i in plan.items if i.status == "completed"),
            errors=sum(1 for i in plan.items if i.status == "error"),
            manual=sum(1 for i in plan.items if i.status == "manual_review_required"),
        )
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

    def _compute_summary(self, plan: MigrationPlan) -> dict:
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

    def _write_report(self, plan: MigrationPlan, output_dir: Path) -> None:
        """Write migration report to output directory."""
        report_path = output_dir / "migration_report.json"
        with open(report_path, "w") as f:
            json.dump(plan.to_dict(), f, indent=2)
        logger.info("report_written", path=str(report_path))

    def _generate_logging_config(self, packages: list[SSISPackage], output_dir: Path) -> None:
        """Write logging_config.json collecting all log providers from packages."""
        seen: dict[str, dict] = {}
        for pkg in packages:
            for lp in pkg.logging_providers:
                if lp.name not in seen:
                    seen[lp.name] = {
                        "name": lp.name,
                        "provider_type": lp.provider_type,
                        "connection_manager_ref": lp.connection_manager_ref,
                    }
        if not seen:
            return
        config_path = output_dir / "logging_config.json"
        config_path.write_text(
            json.dumps({"logging_providers": list(seen.values())}, indent=2),
            encoding="utf-8",
        )

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
        seen: dict[str, dict] = {}
        overrides = self.config.connection_mappings.fabric_target_type_overrides
        for pkg in packages:
            for cm in pkg.connection_managers:
                if cm.name in seen:
                    continue
                ct = cm.connection_type.value

                # Check for an explicit override first
                if cm.name in overrides:
                    fabric_target = overrides[cm.name]
                elif ct in self._SQL_CONNECTION_TYPES:
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


# =========================================================================
# Structured Error Reporting
# =========================================================================


@dataclass
class MigrationError:
    """Structured error/warning reported during migration."""

    source: str
    severity: str = "error"  # error, warning, info
    message: str = ""
    suggested_fix: str = ""

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "severity": self.severity,
            "message": self.message,
            "suggested_fix": self.suggested_fix,
        }


# =========================================================================
# Incremental / Delta Migration Helpers
# =========================================================================

_STATE_DIR = ".ssis2fabric"
_STATE_FILE = "state.json"


def _file_sha256(path: Path) -> str:
    """Return SHA-256 hex digest of a file."""
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_migration_state(base_dir: Path) -> dict:
    """Load persisted migration state from ``state.json`` inside *base_dir*."""
    state_path = base_dir / _STATE_FILE
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {}


def _save_migration_state(base_dir: Path, state: dict) -> None:
    """Persist migration state to ``state.json`` inside *base_dir*."""
    base_dir.mkdir(parents=True, exist_ok=True)
    state_path = base_dir / _STATE_FILE
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
