"""
Multi-Agent Migration System
==============================
Specialized agents that execute migration work in parallel.

Architecture
------------
- Each ``MigrationAgent`` handles one type of artifact generation.
- The ``AgentOrchestrator`` dispatches plan items to agents using a
  thread pool, preserving phase ordering (standalone artifacts first,
  then consolidated pipelines).

Usage::

    from ssis_to_fabric.engine.agents import AgentOrchestrator

    orchestrator = AgentOrchestrator(config, max_workers=4)
    plan = orchestrator.run(packages)
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.engine.migration_engine import (
    MigrationEngine,
    MigrationItem,
    MigrationPlan,
    TargetArtifact,
)

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import ControlFlowTask, SSISPackage
    from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Agent role enumeration
# ---------------------------------------------------------------------------


class AgentRole(str, Enum):
    """Identifies the specialisation of a migration agent."""

    PARSER = "parser"
    DATAFLOW = "dataflow"
    SPARK = "spark"
    PIPELINE = "pipeline"
    DEPLOY = "deploy"


# ---------------------------------------------------------------------------
# Agent result container
# ---------------------------------------------------------------------------


@dataclass
class AgentResult:
    """Outcome produced by an agent for a single work item."""

    agent_role: AgentRole
    item_key: str  # "{package}/{task}" or package name
    status: str  # "completed", "error", "manual_review_required"
    output_path: str = ""
    error: str = ""
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Abstract base agent
# ---------------------------------------------------------------------------


class MigrationAgent(ABC):
    """Base class for all specialised migration agents.

    Subclasses implement ``process`` which handles a single work unit
    (one plan item or one package).  Agents are designed to be
    **stateless** and **thread-safe** — they receive all context through
    method arguments.
    """

    role: AgentRole

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config

    @abstractmethod
    def process(
        self,
        package: SSISPackage,
        task: ControlFlowTask | None,
        output_dir: Path,
    ) -> AgentResult:
        """Execute the agent's work for a single item.

        Parameters
        ----------
        package:
            The parsed SSIS package.
        task:
            The specific control-flow task (``None`` for package-level work
            like pipeline consolidation).
        output_dir:
            Root output directory for generated artifacts.
        """
        ...

    def __repr__(self) -> str:
        return f"<{type(self).__name__} role={self.role.value}>"


# ---------------------------------------------------------------------------
# Concrete agents
# ---------------------------------------------------------------------------


class DataflowAgent(MigrationAgent):
    """Generates Dataflow Gen2 (Power Query / M code) artifacts."""

    role = AgentRole.DATAFLOW

    def __init__(self, config: MigrationConfig) -> None:
        super().__init__(config)
        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        self._generator = DataflowGen2Generator(config)

    def process(
        self,
        package: SSISPackage,
        task: ControlFlowTask | None,
        output_dir: Path,
    ) -> AgentResult:
        key = f"{package.name}/{task.name if task else '*'}"
        try:
            output_path = self._generator.generate(package, task, output_dir)
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="completed",
                output_path=str(output_path),
            )
        except Exception as e:
            logger.error("dataflow_agent_error", key=key, error=str(e))
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="error",
                error=str(e),
            )


class SparkAgent(MigrationAgent):
    """Generates PySpark notebook artifacts."""

    role = AgentRole.SPARK

    def __init__(self, config: MigrationConfig) -> None:
        super().__init__(config)
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        self._generator = SparkNotebookGenerator(config)

    def process(
        self,
        package: SSISPackage,
        task: ControlFlowTask | None,
        output_dir: Path,
    ) -> AgentResult:
        key = f"{package.name}/{task.name if task else '*'}"
        try:
            output_path = self._generator.generate(package, task, output_dir)
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="completed",
                output_path=str(output_path),
            )
        except Exception as e:
            logger.error("spark_agent_error", key=key, error=str(e))
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="error",
                error=str(e),
            )


class PipelineAgent(MigrationAgent):
    """Generates consolidated Data Factory pipeline per package."""

    role = AgentRole.PIPELINE

    def __init__(self, config: MigrationConfig) -> None:
        super().__init__(config)
        from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator

        self._generator = DataFactoryGenerator(config)

    def process(
        self,
        package: SSISPackage,
        task: ControlFlowTask | None,
        output_dir: Path,
        task_routing: dict[str, TargetArtifact] | None = None,
    ) -> AgentResult:
        key = f"{package.name}/*"
        try:
            routing = task_routing or {}
            pipeline_path = self._generator.generate_package_pipeline(
                package, routing, output_dir,
            )
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="completed",
                output_path=str(pipeline_path),
            )
        except Exception as e:
            logger.error("pipeline_agent_error", key=key, error=str(e))
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="error",
                error=str(e),
            )


class DeployAgent(MigrationAgent):
    """Deploys a single artifact to a Fabric workspace.

    Wraps ``FabricDeployer.deploy_pipeline / deploy_notebook /
    deploy_dataflow`` so individual items can be dispatched in parallel
    by the orchestrator.

    Unlike the generation agents, the DeployAgent is *not* used by the
    ``AgentOrchestrator.run()`` method (which only handles generation).
    Instead it is used by ``deploy_parallel()`` or can be called
    directly for single-item deployment jobs.
    """

    role = AgentRole.DEPLOY

    def __init__(self, config: MigrationConfig) -> None:
        super().__init__(config)
        self._deployer: Any | None = None

    def set_deployer(self, deployer: Any) -> None:
        """Inject a configured ``FabricDeployer`` instance."""
        self._deployer = deployer

    def process(
        self,
        package: SSISPackage,
        task: ControlFlowTask | None,
        output_dir: Path,
    ) -> AgentResult:
        """Not used — call ``deploy_item`` instead."""
        return AgentResult(
            agent_role=self.role,
            item_key="*",
            status="error",
            error="Use deploy_item() for deployment work",
        )

    def deploy_item(
        self,
        artifact_path: Path,
        artifact_type: str,
    ) -> AgentResult:
        """Deploy a single artifact file to Fabric.

        Parameters
        ----------
        artifact_path:
            Path to the artifact file (``.json`` or ``.py``).
        artifact_type:
            One of ``"DataPipeline"``, ``"Dataflow"``, ``"Notebook"``.
        """
        if self._deployer is None:
            return AgentResult(
                agent_role=self.role,
                item_key=artifact_path.stem,
                status="error",
                error="No deployer configured — call set_deployer() first",
            )

        key = artifact_path.stem
        try:
            deploy_fn = {
                "DataPipeline": self._deployer.deploy_pipeline,
                "Dataflow": self._deployer.deploy_dataflow,
                "Notebook": self._deployer.deploy_notebook,
            }.get(artifact_type)

            if deploy_fn is None:
                return AgentResult(
                    agent_role=self.role,
                    item_key=key,
                    status="error",
                    error=f"Unknown artifact type: {artifact_type}",
                )

            result = deploy_fn(artifact_path)
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="completed" if result.status == "success" else result.status,
                output_path=result.item_id or "",
                error=result.error or "",
            )
        except Exception as e:
            logger.error("deploy_agent_error", key=key, error=str(e))
            return AgentResult(
                agent_role=self.role,
                item_key=key,
                status="error",
                error=str(e),
            )


# ---------------------------------------------------------------------------
# Agent Orchestrator
# ---------------------------------------------------------------------------

# Mapping from TargetArtifact → AgentRole used for Phase-1 dispatch
_ARTIFACT_TO_ROLE: dict[TargetArtifact, AgentRole] = {
    TargetArtifact.DATAFLOW_GEN2: AgentRole.DATAFLOW,
    TargetArtifact.SPARK_NOTEBOOK: AgentRole.SPARK,
}


@dataclass
class OrchestratorStats:
    """Aggregate statistics from an orchestrator run."""

    total_items: int = 0
    completed: int = 0
    errors: int = 0
    manual: int = 0
    skipped: int = 0
    parallel_workers: int = 1


class AgentOrchestrator:
    """Dispatches migration work to specialised agents in parallel.

    The orchestrator respects the existing phase ordering:

    1. **Phase 1** — Standalone artifacts (dataflows, notebooks) are
       generated in parallel via ``DataflowAgent`` and ``SparkAgent``.
    2. **Phase 2** — Consolidated pipelines are generated in parallel per
       package via ``PipelineAgent``.
    3. **Phase 3** — Connection manifests (delegated to the engine).

    Items whose ``target_artifact`` is ``DATA_FACTORY_PIPELINE`` (inline
    activities) or ``MANUAL_REVIEW`` are handled synchronously since they
    have no heavyweight generation step.

    Parameters
    ----------
    config:
        The migration configuration.
    max_workers:
        Upper bound on concurrent threads (default 4).
    """

    def __init__(
        self,
        config: MigrationConfig,
        max_workers: int = 4,
    ) -> None:
        self.config = config
        self.max_workers = max(1, max_workers)

        # Create per-role agents
        self._agents: dict[AgentRole, MigrationAgent] = {
            AgentRole.DATAFLOW: DataflowAgent(config),
            AgentRole.SPARK: SparkAgent(config),
            AgentRole.PIPELINE: PipelineAgent(config),
        }

        # Underlying engine for plan creation, connection manifests, and
        # report writing — never generates artifacts itself.
        self._engine = MigrationEngine(config)

        # Lock protecting plan item mutations from multiple worker threads
        self._plan_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        packages: list[SSISPackage],
        plan: MigrationPlan | None = None,
        *,
        incremental: bool = False,
        state_dir: Path | None = None,
    ) -> MigrationPlan:
        """Execute the full multi-agent migration pipeline.

        Drop-in replacement for ``MigrationEngine.execute`` — same
        signature, same return type, but with parallel artifact generation.
        """
        if plan is None:
            plan = self._engine.create_plan(packages)

        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Delegate incremental hashing / state to the engine's helpers
        from ssis_to_fabric.engine.migration_engine import (
            _STATE_DIR,
            _file_sha256,
            _load_migration_state,
            _save_migration_state,
        )

        _sd = state_dir or Path(_STATE_DIR)
        prev_state = _load_migration_state(_sd) if incremental else {}
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
            for item in plan.items:
                if item.source_package not in changed_packages:
                    item.status = "skipped"
                    item.notes.append("Skipped (incremental — no changes detected)")

        # Clean previous output
        import shutil

        if incremental:
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

        pkg_map = {pkg.name: pkg for pkg in packages}

        # ============================
        # Phase 1 — parallel artifacts
        # ============================
        self._phase1_parallel(plan, pkg_map, output_dir)

        # ============================
        # Phase 2 — parallel pipelines
        # ============================
        self._phase2_parallel(plan, packages, output_dir)

        # ============================
        # Phase 3 — connection manifests
        # ============================
        self._engine._generate_connection_manifests(packages, output_dir)

        # Persist state
        if incremental or current_hashes:
            from datetime import datetime, timezone

            new_state = dict(prev_state)
            for pkg in packages:
                if pkg.name in current_hashes:
                    new_state[pkg.name] = {
                        "hash": current_hashes[pkg.name],
                        "migrated_at": datetime.now(tz=timezone.utc).isoformat(),
                        "file_path": pkg.file_path,
                    }
            _save_migration_state(_sd, new_state)

        # Write report
        self._engine._write_report(plan, output_dir)

        stats = self._compute_stats(plan)
        logger.info(
            "multi_agent_migration_completed",
            workers=self.max_workers,
            completed=stats.completed,
            errors=stats.errors,
            manual=stats.manual,
        )
        return plan

    # ------------------------------------------------------------------
    # Internal phases
    # ------------------------------------------------------------------

    def _phase1_parallel(
        self,
        plan: MigrationPlan,
        pkg_map: dict[str, SSISPackage],
        output_dir: Path,
    ) -> None:
        """Phase 1: Generate standalone artifacts in parallel."""

        # Partition items by type
        parallel_items: list[MigrationItem] = []
        for item in plan.items:
            if item.status in ("skipped",):
                continue
            if item.target_artifact in _ARTIFACT_TO_ROLE:
                parallel_items.append(item)
            elif item.target_artifact == TargetArtifact.MANUAL_REVIEW:
                item.status = "manual_review_required"
                item.notes.append("This task requires manual migration review.")
            elif item.target_artifact == TargetArtifact.DATA_FACTORY_PIPELINE:
                item.status = "pending_pipeline"

        if not parallel_items:
            return

        effective_workers = min(self.max_workers, len(parallel_items))
        logger.info(
            "phase1_dispatch",
            items=len(parallel_items),
            workers=effective_workers,
        )

        with ThreadPoolExecutor(
            max_workers=effective_workers,
            thread_name_prefix="agent",
        ) as pool:
            future_to_item: dict[Future[AgentResult], MigrationItem] = {}

            for item in parallel_items:
                package = pkg_map.get(item.source_package)
                if package is None:
                    item.status = "error"
                    item.notes.append(f"Package not found: {item.source_package}")
                    continue

                task = self._engine._find_task(
                    package.control_flow_tasks, item.source_task,
                )
                role = _ARTIFACT_TO_ROLE[item.target_artifact]
                agent = self._agents[role]

                future = pool.submit(agent.process, package, task, output_dir)
                future_to_item[future] = item

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                result = future.result()
                with self._plan_lock:
                    item.status = result.status
                    item.output_path = result.output_path
                    if result.error:
                        item.notes.append(f"Generation error: {result.error}")
                        plan.add_error(
                            source=f"{item.source_package}/{item.source_task}",
                            severity="error",
                            message=f"Generation error: {result.error}",
                            suggested_fix="Check the SSIS component structure and generator logs.",
                        )

    def _phase2_parallel(
        self,
        plan: MigrationPlan,
        packages: list[SSISPackage],
        output_dir: Path,
    ) -> None:
        """Phase 2: Generate consolidated pipelines per package in parallel."""

        # Build per-package task routing from the plan
        routings: dict[str, dict[str, TargetArtifact]] = {}
        for item in plan.items:
            routings.setdefault(item.source_package, {})[
                item.source_task
            ] = item.target_artifact

        effective_workers = min(self.max_workers, len(packages))
        pipeline_agent = self._agents[AgentRole.PIPELINE]
        assert isinstance(pipeline_agent, PipelineAgent)

        logger.info(
            "phase2_dispatch",
            packages=len(packages),
            workers=effective_workers,
        )

        with ThreadPoolExecutor(
            max_workers=effective_workers,
            thread_name_prefix="pipeline",
        ) as pool:
            future_to_pkg: dict[Future[AgentResult], SSISPackage] = {}

            for package in packages:
                routing = routings.get(package.name, {})
                future = pool.submit(
                    pipeline_agent.process, package, None, output_dir,
                    task_routing=routing,
                )
                future_to_pkg[future] = package

            for future in as_completed(future_to_pkg):
                package = future_to_pkg[future]
                result = future.result()
                with self._plan_lock:
                    if result.status == "completed":
                        for item in plan.items:
                            if (
                                item.source_package == package.name
                                and item.status == "pending_pipeline"
                            ):
                                item.output_path = result.output_path
                                item.status = "completed"
                    else:
                        for item in plan.items:
                            if (
                                item.source_package == package.name
                                and item.status == "pending_pipeline"
                            ):
                                item.status = "error"
                                item.notes.append(
                                    f"Pipeline generation error: {result.error}",
                                )
                        plan.add_error(
                            source=package.file_path or package.name,
                            severity="error",
                            message=f"Pipeline generation error: {result.error}",
                            suggested_fix="Review the package structure and Data Factory generator logs.",
                        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_stats(plan: MigrationPlan) -> OrchestratorStats:
        stats = OrchestratorStats()
        for item in plan.items:
            stats.total_items += 1
            if item.status == "completed":
                stats.completed += 1
            elif item.status == "error":
                stats.errors += 1
            elif item.status == "manual_review_required":
                stats.manual += 1
            elif item.status == "skipped":
                stats.skipped += 1
        return stats
