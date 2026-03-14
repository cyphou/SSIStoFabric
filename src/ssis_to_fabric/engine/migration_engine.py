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
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

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


@dataclass
class MigrationPlan:
    """Complete migration plan for one or more SSIS packages."""

    project_name: str
    created_at: str = field(default_factory=lambda: datetime.now(tz=__import__("datetime").timezone.utc).isoformat())
    strategy: str = ""
    items: list[MigrationItem] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    errors: list[MigrationError] = field(default_factory=list)

    def add_error(self, source: str, severity: str, message: str, suggested_fix: str = "") -> None:
        """Append a structured error/warning to the plan."""
        self.errors.append(
            MigrationError(source=source, severity=severity, message=message, suggested_fix=suggested_fix)
        )

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
            "strategy": self.strategy,
            "items": items_list,
            "summary": self.summary,
            "errors": errors_list,
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


def _load_migration_state(state_dir: Path) -> dict:
    """Load the persisted migration state from ``.ssis2fabric/state.json``."""
    state_file = state_dir / _STATE_FILE
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_migration_state(state_dir: Path, state: dict) -> None:
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

    def create_plan(self, packages: list[SSISPackage]) -> MigrationPlan:
        """
        Create a migration plan from analyzed SSIS packages.
        Determines the target artifact type for each task.
        """
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
        Returns:
            Updated migration plan with execution status.
        """
        if plan is None:
            plan = self.create_plan(packages)

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

        # Clean previous output to prevent stale artifacts from prior runs
        import shutil

        for subdir in ("pipelines", "notebooks", "dataflows", "connections"):
            target = output_dir / subdir
            if target.exists():
                shutil.rmtree(target)

        # Build a lookup of packages by name
        pkg_map = {pkg.name: pkg for pkg in packages}

        # --- Phase 1: Generate standalone artifacts (dataflows, notebooks) ---
        for item in plan.items:
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
                plan.add_error(
                    source=f"{item.source_package}/{item.source_task}",
                    severity="error",
                    message=f"Generation error: {e}",
                    suggested_fix="Check the SSIS component structure and generator logs.",
                )

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
                plan.add_error(
                    source=package.file_path or package.name,
                    severity="error",
                    message=f"Pipeline generation error: {e}",
                    suggested_fix="Review the package structure and Data Factory generator logs.",
                )

        # --- Phase 3: Generate connection manifests ---
        self._generate_connection_manifests(packages, output_dir)

        # Persist updated state for incremental runs
        if incremental or current_hashes:
            new_state = dict(prev_state)
            for pkg in packages:
                if pkg.name in current_hashes:
                    new_state[pkg.name] = {
                        "hash": current_hashes[pkg.name],
                        "migrated_at": datetime.now(tz=__import__("datetime").timezone.utc).isoformat(),
                        "file_path": pkg.file_path,
                    }
            _save_migration_state(_state_dir, new_state)

        # Write migration report
        self._write_report(plan, output_dir)

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
