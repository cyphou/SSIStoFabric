"""
Public Python API
==================
High-level facade for programmatic usage of the SSIS-to-Fabric migration tool.

Usage::

    from ssis_to_fabric import SSISMigrator

    migrator = SSISMigrator(strategy="hybrid", output_dir="./output")
    packages = migrator.analyze("path/to/ssis_project/")
    plan     = migrator.plan(packages)
    result   = migrator.migrate("path/to/ssis_project/")
    report   = migrator.deploy(workspace_id="<guid>")
    results  = migrator.validate("tests/regression/baselines/", "./output")
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import (
    DataflowType,
    MigrationConfig,
    MigrationStrategy,
    load_config,
)
from ssis_to_fabric.engine.migration_engine import MigrationEngine, MigrationPlan
from ssis_to_fabric.logging_config import setup_logging

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import SSISPackage
    from ssis_to_fabric.engine.fabric_deployer import DeploymentReport


class SSISMigrator:
    """
    One-stop facade for SSIS-to-Fabric migration.

    Wraps the parser, migration engine, deployer, and regression runner
    behind a clean, importable API so callers never need to touch the CLI.

    Parameters
    ----------
    strategy : str, optional
        Migration strategy: ``"hybrid"`` (default), ``"data_factory"``, or
        ``"spark"``.
    output_dir : str or Path, optional
        Directory for generated Fabric artifacts (default ``"output"``).
    config_path : str or Path or None, optional
        Path to a ``migration_config.yaml`` file.  When *None* the tool
        loads defaults (with environment-variable overrides).
    config : MigrationConfig or None, optional
        Pre-built config object.  Takes precedence over *config_path*.
    log_level : str, optional
        One of ``"DEBUG"``, ``"INFO"`` (default), ``"WARNING"``, ``"ERROR"``.
    project_name : str or None, optional
        Override the project name in the config.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        *,
        strategy: str = "hybrid",
        dataflow_type: str = "notebook",
        output_dir: str | Path = "output",
        config_path: str | Path | None = None,
        config: MigrationConfig | None = None,
        log_level: str = "INFO",
        project_name: str | None = None,
    ) -> None:
        # Build or accept configuration
        if config is not None:
            self._config = config
        elif config_path is not None:
            self._config = load_config(Path(config_path))
        else:
            self._config = MigrationConfig()

        # Apply explicit overrides
        self._config.strategy = MigrationStrategy(strategy)
        self._config.dataflow_type = DataflowType(dataflow_type)
        self._config.output_dir = Path(output_dir)
        if project_name:
            self._config.project_name = project_name

        # Logging
        setup_logging(level=log_level, log_format="console")

        # Internal components (lazy-init where expensive)
        self._parser = DTSXParser()
        self._engine = MigrationEngine(self._config)

    @classmethod
    def from_config(cls, config: MigrationConfig, **overrides) -> SSISMigrator:
        """
        Create a migrator from an existing :class:`MigrationConfig`.

        Any keyword argument accepted by :meth:`__init__` can be passed as an
        override (e.g. ``strategy``, ``output_dir``).
        """
        defaults = {
            "config": config,
            "strategy": overrides.pop("strategy", config.strategy.value),
            "output_dir": overrides.pop("output_dir", config.output_dir),
            "log_level": overrides.pop("log_level", config.log_level.value),
        }
        defaults.update(overrides)
        return cls(**defaults)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def config(self) -> MigrationConfig:
        """Return the active migration configuration."""
        return self._config

    @property
    def strategy(self) -> str:
        """Return the current migration strategy name."""
        return self._config.strategy.value

    @property
    def output_dir(self) -> Path:
        """Return the current output directory."""
        return self._config.output_dir

    # ------------------------------------------------------------------
    # 1. Analyze
    # ------------------------------------------------------------------

    def analyze(self, path: str | Path) -> list[SSISPackage]:
        """
        Parse SSIS packages from a file or directory.

        Parameters
        ----------
        path : str or Path
            A single ``.dtsx`` file or a directory containing SSIS packages
            (including ``Project.params`` if present).

        Returns
        -------
        list[SSISPackage]
            Parsed package models with control flow tasks, data flow
            components, parameters, variables, and migration-complexity
            annotations.

        Raises
        ------
        FileNotFoundError
            If *path* does not exist.
        ValueError
            If no SSIS packages are found at the given location.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Path does not exist: {p}")

        packages = [self._parser.parse(p)] if p.is_file() else self._parser.parse_directory(p)

        if not packages:
            raise ValueError(f"No SSIS packages found at: {p}")

        return packages

    # ------------------------------------------------------------------
    # 2. Plan
    # ------------------------------------------------------------------

    def plan(self, packages: list[SSISPackage]) -> MigrationPlan:
        """
        Generate a migration plan without executing generation.

        Parameters
        ----------
        packages : list[SSISPackage]
            Packages returned by :meth:`analyze`.

        Returns
        -------
        MigrationPlan
            Plan with items, routing decisions, and summary statistics.
        """
        return self._engine.create_plan(packages)

    # ------------------------------------------------------------------
    # 3. Migrate
    # ------------------------------------------------------------------

    def migrate(
        self,
        path: str | Path,
        *,
        plan: MigrationPlan | None = None,
    ) -> MigrationPlan:
        """
        End-to-end migration: parse → plan → generate Fabric artifacts.

        Parameters
        ----------
        path : str or Path
            SSIS package file or project directory.
        plan : MigrationPlan, optional
            Pre-computed plan.  If *None* a new plan is created automatically.

        Returns
        -------
        MigrationPlan
            Completed plan with ``status`` set on every item and
            ``output_path`` pointing to generated files.
        """
        packages = self.analyze(path)
        return self._engine.execute(packages, plan=plan)

    # ------------------------------------------------------------------
    # 4. Deploy
    # ------------------------------------------------------------------

    def deploy(
        self,
        workspace_id: str,
        *,
        output_dir: str | Path | None = None,
        clean: bool = False,
        dry_run: bool = False,
        skip_existing: bool = True,
        credential: object | None = None,
    ) -> DeploymentReport:
        """
        Deploy generated artifacts to a Microsoft Fabric workspace.

        Parameters
        ----------
        workspace_id : str
            Target Fabric workspace GUID.
        output_dir : str or Path, optional
            Directory containing generated artifacts.  Defaults to
            ``self.output_dir``.
        clean : bool
            If *True*, delete **all** existing items in the workspace
            before deploying (pipelines, dataflows, notebooks).
        dry_run : bool
            If *True*, simulate deployment without creating items.
        skip_existing : bool
            If *True* (default), skip items that already exist.
        credential : object, optional
            An ``azure.identity`` credential to use for authentication.
            When *None* the deployer uses its default credential chain.

        Returns
        -------
        DeploymentReport
            Report with per-item results and summary counts.
        """
        from ssis_to_fabric.engine.fabric_deployer import (
            FabricDeployer,
        )

        deploy_dir = Path(output_dir) if output_dir else self._config.output_dir

        deployer = FabricDeployer(
            workspace_id=workspace_id,
            credential=credential,
            skip_existing=skip_existing,
            dry_run=dry_run,
        )
        deployer.authenticate()

        if clean and not dry_run:
            deployer.delete_all_items()

        return deployer.deploy_all(deploy_dir)

    # ------------------------------------------------------------------
    # 5. Validate
    # ------------------------------------------------------------------

    def validate(
        self,
        baseline_dir: str | Path,
        output_dir: str | Path | None = None,
    ) -> list[dict]:
        """
        Run non-regression validation against approved baselines.

        Parameters
        ----------
        baseline_dir : str or Path
            Directory containing the approved baseline artifacts.
        output_dir : str or Path, optional
            Directory containing generated artifacts to validate.
            Defaults to ``self.output_dir``.

        Returns
        -------
        list[dict]
            Per-file results with ``file``, ``status``, and ``details`` keys.
        """
        from ssis_to_fabric.testing.regression_runner import RegressionRunner

        out = Path(output_dir) if output_dir else self._config.output_dir
        runner = RegressionRunner(self._config)
        return runner.run_file_comparison(Path(baseline_dir), out)

    # ------------------------------------------------------------------
    # Convenience: full pipeline
    # ------------------------------------------------------------------

    def run(
        self,
        path: str | Path,
        *,
        workspace_id: str | None = None,
        clean: bool = False,
        dry_run: bool = False,
        credential: object | None = None,
    ) -> MigrationPlan:
        """
        Convenience: migrate and optionally deploy in one call.

        Parameters
        ----------
        path : str or Path
            SSIS package file or project directory.
        workspace_id : str, optional
            If provided, deploy artifacts after migration.
        clean : bool
            Delete existing items before deploying.
        dry_run : bool
            Simulate deployment.
        credential : object, optional
            Azure credential for deployment.

        Returns
        -------
        MigrationPlan
            Completed migration plan.
        """
        result = self.migrate(path)

        if workspace_id:
            self.deploy(
                workspace_id=workspace_id,
                clean=clean,
                dry_run=dry_run,
                credential=credential,
            )

        return result
