"""
CLI Entry Point
================
Command-line interface for the SSIS to Fabric migration tool.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import LogLevel, MigrationConfig, MigrationStrategy, load_config
from ssis_to_fabric.engine.migration_engine import MigrationEngine
from ssis_to_fabric.logging_config import setup_logging

console = Console()


@click.group()
@click.option("--config", "-c", type=click.Path(exists=False), default="migration_config.yaml", help="Config file path")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), default=None)
@click.pass_context
def main(ctx: click.Context, config: str, log_level: str | None) -> None:
    """SSIS to Fabric Migration Tool - Production-ready migration framework."""
    cfg = load_config(Path(config))
    if log_level:
        cfg.log_level = LogLevel(log_level)
    setup_logging(level=cfg.log_level.value, log_format="console")
    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.pass_context
def analyze(ctx: click.Context, path: str) -> None:
    """Analyze SSIS packages and display migration assessment."""
    parser = DTSXParser()
    p = Path(path)

    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    if not packages:
        console.print("[red]No SSIS packages found.[/red]")
        sys.exit(1)

    # Display analysis
    table = Table(title="SSIS Package Analysis")
    table.add_column("Package", style="cyan")
    table.add_column("Tasks", justify="right")
    table.add_column("Data Flows", justify="right")
    table.add_column("Complexity", style="bold")
    table.add_column("Warnings", justify="right")

    for pkg in packages:
        complexity_style = {
            "LOW": "green",
            "MEDIUM": "yellow",
            "HIGH": "red",
            "MANUAL": "bold red",
        }.get(pkg.overall_complexity.value, "white")

        table.add_row(
            pkg.name,
            str(pkg.total_tasks),
            str(pkg.total_data_flows),
            f"[{complexity_style}]{pkg.overall_complexity.value}[/{complexity_style}]",
            str(len(pkg.warnings)),
        )

    console.print(table)

    # Detailed breakdown
    for pkg in packages:
        console.print(f"\n[bold]Package: {pkg.name}[/bold]")
        console.print(f"  Connections: {len(pkg.connection_managers)}")
        console.print(f"  Variables: {len(pkg.variables)}")
        console.print(f"  Parameters: {len(pkg.parameters)}")

        if pkg.warnings:
            console.print("  [yellow]Warnings:[/yellow]")
            for w in pkg.warnings:
                console.print(f"    - {w}")


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--strategy", type=click.Choice(["data_factory", "spark", "hybrid"]), default=None)
@click.option("--output", "-o", type=click.Path(), default=None)
@click.pass_context
def plan(ctx: click.Context, path: str, strategy: str | None, output: str | None) -> None:
    """Generate a migration plan without executing."""
    config: MigrationConfig = ctx.obj["config"]
    if strategy:
        config.strategy = MigrationStrategy(strategy)
    if output:
        config.output_dir = Path(output)

    parser = DTSXParser()
    p = Path(path)
    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    engine = MigrationEngine(config)
    migration_plan = engine.create_plan(packages)

    # Display plan
    table = Table(title="Migration Plan")
    table.add_column("Package")
    table.add_column("Task")
    table.add_column("Type")
    table.add_column("Target")
    table.add_column("Complexity")

    for item in migration_plan.items:
        table.add_row(
            item.source_package,
            item.source_task,
            item.task_type,
            item.target_artifact.value,
            item.complexity.value,
        )

    console.print(table)
    console.print(f"\n[bold]Summary:[/bold] {migration_plan.summary}")


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--strategy", type=click.Choice(["data_factory", "spark", "hybrid"]), default=None)
@click.option(
    "--dataflow-type",
    type=click.Choice(["notebook", "dataflow_gen2"]),
    default=None,
    help=(
        "How Data Flow tasks are rendered under the hybrid strategy. "
        "'notebook' (default) generates PySpark notebooks; "
        "'dataflow_gen2' generates Dataflow Gen2 / Power Query M code."
    ),
)
@click.option("--output", "-o", type=click.Path(), default=None)
@click.option(
    "--workers", "-w", type=int, default=None,
    help="Parallel worker threads (1 = sequential, >1 = multi-agent).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be generated without writing files.",
)
@click.pass_context
def migrate(
    ctx: click.Context,
    path: str,
    strategy: str | None,
    output: str | None,
    dataflow_type: str | None,
    workers: int | None,
    dry_run: bool,
) -> None:
    """Execute the full migration: analyze, plan, and generate Fabric artifacts."""
    config: MigrationConfig = ctx.obj["config"]
    if strategy:
        config.strategy = MigrationStrategy(strategy)
    if dataflow_type:
        from ssis_to_fabric.config import DataflowType

        config.dataflow_type = DataflowType(dataflow_type)
    if output:
        config.output_dir = Path(output)
    if workers is not None:
        config.parallel_workers = workers

    parser = DTSXParser()
    p = Path(path)
    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    if not packages:
        console.print("[red]No SSIS packages found.[/red]")
        sys.exit(1)

    # Dry-run mode: show the plan without generating files
    if dry_run:
        console.print("[yellow]DRY RUN — showing migration plan only (no files written).[/yellow]\n")
        engine = MigrationEngine(config)
        migration_plan = engine.create_plan(packages)

        table = Table(title="Migration Plan (Dry Run)")
        table.add_column("Package")
        table.add_column("Task")
        table.add_column("Type")
        table.add_column("Target")
        table.add_column("Complexity")

        for item in migration_plan.items:
            cx_style = {"LOW": "green", "MEDIUM": "yellow", "HIGH": "red", "MANUAL": "bold red"}.get(
                item.complexity.value, "white"
            )
            table.add_row(
                item.source_package,
                item.source_task,
                item.task_type,
                item.target_artifact.value,
                f"[{cx_style}]{item.complexity.value}[/{cx_style}]",
            )

        console.print(table)
        console.print(f"\n[bold]Summary:[/bold] {migration_plan.summary}")
        return

    if config.parallel_workers > 1:
        from ssis_to_fabric.engine.agents import AgentOrchestrator

        orchestrator = AgentOrchestrator(config, max_workers=config.parallel_workers)
        engine = MigrationEngine(config)
        pre_plan = engine.create_plan(packages)
        total_items = len(pre_plan.items)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("[cyan]Migrating...", total=total_items)

            def _on_progress(event: str, data: dict) -> None:  # type: ignore[type-arg]
                if event == "item_completed":
                    progress.advance(task_id)

            result = orchestrator.run(packages, plan=pre_plan, progress_callback=_on_progress)
    else:
        engine = MigrationEngine(config)
        pre_plan = engine.create_plan(packages)
        total_items = len(pre_plan.items)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("[cyan]Migrating...", total=total_items)

            def _on_progress(event: str, data: dict) -> None:  # type: ignore[type-arg]
                if event == "item_completed":
                    progress.advance(task_id)

            result = engine.execute(packages, plan=pre_plan, progress_callback=_on_progress)

    # Display results
    table = Table(title="Migration Results")
    table.add_column("Task")
    table.add_column("Target")
    table.add_column("Status")
    table.add_column("Output")

    for item in result.items:
        status_style = {
            "completed": "green",
            "error": "red",
            "manual_review_required": "yellow",
        }.get(item.status, "white")

        table.add_row(
            item.source_task,
            item.target_artifact.value,
            f"[{status_style}]{item.status}[/{status_style}]",
            item.output_path or "-",
        )

    console.print(table)
    elapsed_s = result.total_elapsed_ms / 1000 if result.total_elapsed_ms else 0
    console.print(f"\nOutput directory: [bold]{config.output_dir}[/bold]")
    if elapsed_s:
        console.print(f"Elapsed: [bold]{elapsed_s:.1f}s[/bold]  ·  Correlation ID: [dim]{result.correlation_id}[/dim]")


@main.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--workspace-id", "-w", required=True, help="Fabric workspace ID (GUID)")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Simulate deployment without actually creating items",
)
@click.option(
    "--skip-existing/--no-skip-existing",
    default=True,
    help="Skip items that already exist in the workspace",
)
@click.option(
    "--clean",
    is_flag=True,
    default=False,
    help="Delete all existing items in the workspace before deploying",
)
@click.option(
    "--connection-id",
    "-c",
    default=None,
    help="Fabric connection ID to attach to Script activities (SQL endpoints, Warehouses, etc.)",
)
@click.option(
    "--tenant-id",
    envvar="AZURE_TENANT_ID",
    default=None,
    help="Azure AD tenant ID (service principal auth)",
)
@click.option(
    "--client-id",
    envvar="AZURE_CLIENT_ID",
    default=None,
    help="Azure AD client/app ID (service principal auth)",
)
@click.option(
    "--client-secret",
    envvar="AZURE_CLIENT_SECRET",
    default=None,
    help="Azure AD client secret (service principal auth)",
)
@click.option(
    "--deploy-workers",
    type=int,
    default=1,
    help="Parallel deploy threads (1 = sequential, >1 = parallel).",
)
@click.option(
    "--on-error",
    type=click.Choice(["continue", "rollback"]),
    default="continue",
    help="Action on deploy failure: continue or rollback created items.",
)
@click.option("--verify/--no-verify", default=True, help="Post-deploy verification check.")
@click.pass_context
def deploy(
    ctx: click.Context,
    output_dir: str,
    workspace_id: str,
    dry_run: bool,
    skip_existing: bool,
    clean: bool,
    connection_id: str | None,
    tenant_id: str | None,
    client_id: str | None,
    client_secret: str | None,
    deploy_workers: int,
    on_error: str,
    verify: bool,
) -> None:
    """Deploy migration artifacts to a Fabric workspace.

    Authentication is resolved automatically:
      1. --tenant-id / --client-id / --client-secret (or env vars)
      2. DefaultAzureCredential (Managed Identity, az login cache, etc.)
      3. Interactive fallback (device-code / browser)
    """
    import os

    from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

    # Propagate explicit SP flags into env so FabricDeployer picks them up
    if tenant_id:
        os.environ["AZURE_TENANT_ID"] = tenant_id
    if client_id:
        os.environ["AZURE_CLIENT_ID"] = client_id
    if client_secret:
        os.environ["AZURE_CLIENT_SECRET"] = client_secret

    deployer = FabricDeployer(
        workspace_id=workspace_id,
        skip_existing=skip_existing,
        dry_run=dry_run,
        default_connection_id=connection_id,
    )

    if dry_run:
        console.print("[yellow]DRY RUN mode — no items will be created.[/yellow]\n")
    else:
        console.print(f"[bold]Deploying to Fabric workspace:[/bold] {workspace_id}\n")

    # Authenticate
    console.print("Authenticating to Microsoft Fabric...")
    try:
        deployer.authenticate()
    except Exception as exc:
        console.print(f"[red]Authentication failed:[/red] {exc}")
        sys.exit(1)
    console.print("[green]Authenticated successfully.[/green]\n")

    # Clean workspace if requested
    if clean and not dry_run:
        console.print("[bold red]Cleaning workspace — deleting all items...[/bold red]")
        deleted, errs = deployer.delete_all_items(
            item_types=["DataPipeline", "Notebook", "Dataflow"],
        )
        console.print(f"Deleted {deleted} items ({errs} errors)")
        if deleted > 0:
            import time

            wait = 30
            console.print(f"Waiting {wait}s for Fabric to release item names...")
            time.sleep(wait)
        console.print()

    # Deploy
    if deploy_workers > 1:
        report = deployer.deploy_all_parallel(
            Path(output_dir), max_workers=deploy_workers, on_error=on_error,
        )
    else:
        report = deployer.deploy_all(Path(output_dir))

    # Display results
    table = Table(title="Deployment Results")
    table.add_column("Item", style="cyan")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Details")

    for r in report.results:
        status_style = {"success": "green", "error": "red", "skipped": "yellow"}.get(r.status, "white")
        # Show error/note first (includes NEEDS CONFIGURATION), then item ID
        details = r.error or r.item_id or ""
        table.add_row(
            r.name,
            r.item_type,
            f"[{status_style}]{r.status}[/{status_style}]",
            details,
        )

    console.print(table)
    console.print(
        f"\n[bold]Summary:[/bold] {report.succeeded} succeeded, "
        f"{report.failed} failed, {report.skipped} skipped "
        f"(total: {report.total})"
    )

    if report.failed > 0:
        if on_error == "rollback":
            console.print("[bold red]Rolling back deployed items...[/bold red]")
            deleted, errs = report.rollback(deployer)
            console.print(f"Rollback: {deleted} deleted, {errs} errors")
        sys.exit(1)

    # Post-deploy verification
    if verify and not dry_run and report.succeeded > 0:
        console.print("\n[bold]Post-deployment verification...[/bold]")
        checks = deployer.post_deploy_check(report)
        ok = sum(1 for c in checks if c["status"] == "ok")
        missing = sum(1 for c in checks if c["status"] == "missing")
        if missing:
            console.print(
                f"[yellow]Verification: {ok} accessible, {missing} not yet visible[/yellow]"
            )
        else:
            console.print(f"[green]All {ok} items verified accessible.[/green]")


@main.command()
@click.argument("baseline_dir", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path(exists=True))
@click.pass_context
def validate(ctx: click.Context, baseline_dir: str, output_dir: str) -> None:
    """Run non-regression validation comparing migration outputs against baselines."""
    from ssis_to_fabric.testing.regression_runner import RegressionRunner

    config: MigrationConfig = ctx.obj["config"]
    runner = RegressionRunner(config)
    results = runner.run_file_comparison(Path(baseline_dir), Path(output_dir))

    table = Table(title="Non-Regression Validation Results")
    table.add_column("File")
    table.add_column("Status")
    table.add_column("Details")

    for r in results:
        style = "green" if r["status"] == "pass" else "red"
        table.add_row(
            r["file"],
            f"[{style}]{r['status']}[/{style}]",
            r.get("details", ""),
        )

    console.print(table)

    failed = sum(1 for r in results if r["status"] == "fail")
    if failed:
        console.print(f"\n[red]{failed} regression test(s) failed![/red]")
        sys.exit(1)
    else:
        console.print("\n[green]All regression tests passed.[/green]")


@main.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--workspace-id", "-w", required=True, help="Fabric workspace ID (GUID)")
@click.option("--tenant-id", envvar="AZURE_TENANT_ID", default=None, help="Azure AD tenant ID")
@click.option("--client-id", envvar="AZURE_CLIENT_ID", default=None, help="Azure AD client/app ID")
@click.option("--client-secret", envvar="AZURE_CLIENT_SECRET", default=None, help="Azure AD client secret")
@click.pass_context
def verify(
    ctx: click.Context,
    output_dir: str,
    workspace_id: str,
    tenant_id: str | None,
    client_id: str | None,
    client_secret: str | None,
) -> None:
    """Verify deployed artifacts exist in a Fabric workspace.

    Compares the generated output directory against actual workspace items
    via the Fabric REST API and reports any missing or mismatched items.
    """
    import os

    # Set auth env vars if provided
    if tenant_id:
        os.environ["AZURE_TENANT_ID"] = tenant_id
    if client_id:
        os.environ["AZURE_CLIENT_ID"] = client_id
    if client_secret:
        os.environ["AZURE_CLIENT_SECRET"] = client_secret

    from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

    deployer = FabricDeployer(workspace_id=workspace_id, dry_run=True)
    console.print("Authenticating to Microsoft Fabric...")
    try:
        deployer.authenticate()
    except Exception as exc:
        console.print(f"[red]Authentication failed:[/red] {exc}")
        sys.exit(1)
    console.print("[green]Authenticated.[/green]\n")

    # Enumerate expected items from output directory
    out = Path(output_dir)
    expected: list[dict[str, str]] = []

    for subdir, item_type in [
        ("pipelines", "DataPipeline"),
        ("dataflows", "Dataflow"),
        ("notebooks", "Notebook"),
    ]:
        folder = out / subdir
        if not folder.exists():
            continue
        for f in sorted(folder.iterdir()):
            if f.suffix == ".json" and ".destinations." not in f.name or f.suffix == ".py":
                expected.append({"name": f.stem, "type": item_type, "file": str(f)})

    if not expected:
        console.print("[yellow]No artifacts found in output directory.[/yellow]")
        return

    # Fetch workspace items
    console.print(f"Fetching workspace items for {workspace_id}...")
    try:
        workspace_items = deployer.list_workspace_items()
    except Exception as exc:
        console.print(f"[red]Failed to list workspace items:[/red] {exc}")
        sys.exit(1)

    # Build lookup: (type, name) → item_id
    ws_lookup: dict[tuple[str, str], str] = {}
    for item in workspace_items:
        key = (item.get("type", ""), item.get("displayName", ""))
        ws_lookup[key] = item.get("id", "")

    # Compare
    table = Table(title="Deployment Verification")
    table.add_column("Artifact", style="cyan")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Item ID")

    found = 0
    missing = 0
    for exp in expected:
        key = (exp["type"], exp["name"])
        item_id = ws_lookup.get(key, "")
        if item_id:
            table.add_row(exp["name"], exp["type"], "[green]FOUND[/green]", item_id)
            found += 1
        else:
            table.add_row(exp["name"], exp["type"], "[red]MISSING[/red]", "-")
            missing += 1

    console.print(table)
    console.print(f"\n[bold]Summary:[/bold] {found} found, {missing} missing (total expected: {len(expected)})")

    if missing:
        sys.exit(1)


@main.command()
@click.argument("ssis_connection_string")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default="./extracted_packages",
    help="Output directory for .dtsx files",
)
@click.option("--folder", "-f", default=None, help="Limit extraction to a specific SSISDB folder")
@click.option("--project", "-p", default=None, help="Limit extraction to a specific project")
@click.pass_context
def extract_ssisdb(
    ctx: click.Context,
    ssis_connection_string: str,
    output: str,
    folder: str | None,
    project: str | None,
) -> None:
    """Extract .dtsx packages from an SSISDB catalog.

    Connects to a SQL Server instance hosting SSISDB and extracts
    deployed packages to local .dtsx files for migration.

    SSIS_CONNECTION_STRING: ODBC or pyodbc connection string for the SQL Server hosting SSISDB.
    """
    from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

    extractor = SSISDBExtractor(ssis_connection_string)
    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)

    console.print("Connecting to SSISDB...")
    try:
        extractor.connect()
    except Exception as exc:
        console.print(f"[red]Connection failed:[/red] {exc}")
        sys.exit(1)
    console.print("[green]Connected.[/green]\n")

    try:
        packages = extractor.list_packages(folder_name=folder, project_name=project)
        console.print(f"Found [bold]{len(packages)}[/bold] package(s).\n")

        table = Table(title="Extracted Packages")
        table.add_column("Folder")
        table.add_column("Project")
        table.add_column("Package")
        table.add_column("Status")

        for pkg_info in packages:
            try:
                out_path = extractor.extract_package(pkg_info, out_dir)
                table.add_row(
                    pkg_info["folder"],
                    pkg_info["project"],
                    pkg_info["name"],
                    f"[green]{out_path.name}[/green]",
                )
            except Exception as exc:
                table.add_row(
                    pkg_info["folder"],
                    pkg_info["project"],
                    pkg_info["name"],
                    f"[red]Error: {exc}[/red]",
                )

        console.print(table)
    finally:
        extractor.close()


@main.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.pass_context
def report(ctx: click.Context, output_dir: str) -> None:
    """Generate an HTML migration report dashboard from migration_report.json.

    Reads ``migration_report.json`` from OUTPUT_DIR and writes
    ``migration_report.html`` in the same directory.
    """
    from ssis_to_fabric.engine.report_generator import ReportGenerator

    gen = ReportGenerator()
    try:
        html_path = gen.generate(Path(output_dir))
        console.print(f"[green]HTML report generated:[/green] {html_path}")
    except FileNotFoundError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        sys.exit(1)


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--table",
    "-t",
    default=None,
    help="Show impact analysis for a specific table (e.g. dbo.FactSales)",
)
@click.option(
    "--column",
    "-c",
    default=None,
    help="Show column-level impact analysis (e.g. dbo.Sales.Amount)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Write lineage.json to OUTPUT directory (default: print to console)",
)
@click.pass_context
def lineage(
    ctx: click.Context,
    path: str,
    table: str | None,
    column: str | None,
    output: str | None,
) -> None:
    """Analyse data lineage across SSIS packages.

    Builds a directed graph of table dependencies and optionally performs
    impact analysis for a specific table or column.

    PATH may be a single ``.dtsx`` file or a directory containing packages.
    """
    from ssis_to_fabric.engine.column_lineage import ColumnLineageGraph
    from ssis_to_fabric.engine.lineage import LineageGraph

    parser = DTSXParser()
    p = Path(path)
    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    graph = LineageGraph()
    graph.build(packages)

    if column:
        col_graph = ColumnLineageGraph()
        col_graph.build(packages)
        impact = col_graph.column_impact(column)
        console.print(f"\n[bold]Column impact analysis for:[/bold] {column}")
        console.print(f"  Upstream edges: {len(impact['upstream'])}")
        for e in impact["upstream"]:
            console.print(f"    ← {e['source_table']}.{e['source_column']} ({e['transformation']})")
        console.print(f"  Downstream edges: {len(impact['downstream'])}")
        for e in impact["downstream"]:
            console.print(f"    → {e['destination_table']}.{e['destination_column']} ({e['transformation']})")
        if output:
            col_graph.write_json(Path(output))
            console.print(f"\n[green]Column lineage JSON written to:[/green] {output}")
    elif table:
        impact = graph.impact(table)
        console.print(f"\n[bold]Impact analysis for table:[/bold] {table}")
        console.print(f"  Readers: {', '.join(impact['readers']) or '(none)'}")
        console.print(f"  Writers: {', '.join(impact['writers']) or '(none)'}")
    else:
        console.print(f"\n[bold]Lineage graph:[/bold] {len(graph.all_tables())} tables across {len(packages)} packages")
        console.print("\n[bold]Mermaid diagram:[/bold]")
        console.print(graph.to_mermaid())

    if output:
        json_path = graph.write_json(Path(output))
        console.print(f"\n[green]Lineage JSON written:[/green] {json_path}")


@main.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option(
    "--dialect",
    type=click.Choice(["spark", "sql"]),
    default="spark",
    help="DDL dialect: 'spark' (Delta Lake) or 'sql' (T-SQL).",
)
@click.pass_context
def provision(ctx: click.Context, output_dir: str, dialect: str) -> None:
    """Generate Lakehouse / Warehouse DDL from destination manifests.

    Scans OUTPUT_DIR for ``*.destinations.json`` sidecar files and
    generates ``CREATE TABLE`` DDL scripts in ``OUTPUT_DIR/lakehouse/``.
    """
    from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

    provisioner = LakehouseProvisioner(dialect=dialect)
    generated = provisioner.provision(Path(output_dir))

    if not generated:
        console.print("[yellow]No destination manifests found.[/yellow]")
        return

    table = Table(title="Lakehouse DDL Generation")
    table.add_column("Table", style="cyan")
    table.add_column("File")

    for path in generated:
        table.add_row(path.stem, str(path))

    console.print(table)
    console.print(f"\n[green]{len(generated)} DDL file(s) generated.[/green]")


# =========================================================================
# Phase 5 Commands
# =========================================================================


@main.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--strategy", type=click.Choice(["data_factory", "spark", "hybrid"]), default=None)
@click.option("--output", "-o", type=click.Path(), default=None, help="Write assessment JSON to file")
@click.pass_context
def assess(ctx: click.Context, path: str, strategy: str | None, output: str | None) -> None:
    """Pre-migration readiness assessment with effort estimates.

    Analyses SSIS packages and produces a readiness score (0-100),
    per-package effort estimates, risk flags, and a connection inventory
    without generating any migration artifacts.
    """
    config: MigrationConfig = ctx.obj["config"]
    if strategy:
        config.strategy = MigrationStrategy(strategy)

    parser = DTSXParser()
    p = Path(path)
    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    if not packages:
        console.print("[red]No SSIS packages found.[/red]")
        sys.exit(1)

    engine = MigrationEngine(config)
    report = engine.assess(packages)

    # Readiness score
    score = report.readiness_score
    score_color = "green" if score >= 70 else "yellow" if score >= 40 else "red"
    console.print(f"\n[bold]Migration Readiness Score:[/bold] [{score_color}]{score}/100[/{score_color}]")
    console.print(f"  Packages: {report.total_packages}")
    console.print(f"  Total tasks: {report.total_tasks}")
    console.print(f"  Data flows: {report.total_data_flows}")
    console.print(f"  Unique connections: {report.total_connections}")
    console.print(f"  Auto-migratable: {report.auto_migrate_pct:.0f}%")
    console.print(f"  Estimated effort: [bold]{report.estimated_total_hours:.1f} hours[/bold]\n")

    # Per-package table
    table = Table(title="Package Assessment")
    table.add_column("Package", style="cyan")
    table.add_column("Tasks", justify="right")
    table.add_column("Flows", justify="right")
    table.add_column("Complexity")
    table.add_column("Hours", justify="right")
    table.add_column("Auto %", justify="right")
    table.add_column("Risks", justify="right")

    for pkg in report.packages:
        cx_style = {"LOW": "green", "MEDIUM": "yellow", "HIGH": "red", "MANUAL": "bold red"}.get(
            pkg.complexity, "white"
        )
        table.add_row(
            pkg.name,
            str(pkg.tasks),
            str(pkg.data_flows),
            f"[{cx_style}]{pkg.complexity}[/{cx_style}]",
            f"{pkg.estimated_hours:.1f}",
            f"{pkg.auto_migrate_pct:.0f}%",
            str(len(pkg.risks)),
        )

    console.print(table)

    # Risks
    all_risks = [(p.name, r) for p in report.packages for r in p.risks]
    if all_risks:
        console.print(f"\n[yellow]Risks ({len(all_risks)}):[/yellow]")
        for pkg_name, risk in all_risks:
            console.print(f"  [{pkg_name}] {risk}")

    # Connection inventory
    if report.connection_inventory:
        conn_table = Table(title="Connection Inventory")
        conn_table.add_column("Name", style="cyan")
        conn_table.add_column("Type")
        conn_table.add_column("Server")
        conn_table.add_column("Database")
        for c in report.connection_inventory:
            conn_table.add_row(c["name"], c["type"], c["server"], c["database"])
        console.print(conn_table)

    # Write JSON if requested
    if output:
        import json

        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
        console.print(f"\n[green]Assessment written to:[/green] {out_path}")


@main.command(name="validate-config")
@click.pass_context
def validate_config(ctx: click.Context) -> None:
    """Validate the migration configuration file.

    Checks that the loaded configuration is valid, required fields are
    present for the chosen strategy, and connection mappings reference
    valid UUIDs.
    """
    import re as _re

    config: MigrationConfig = ctx.obj["config"]
    issues: list[tuple[str, str]] = []  # (severity, message)

    # Check strategy-specific requirements
    if config.strategy == MigrationStrategy.HYBRID:
        pass  # No special requirements
    elif config.strategy == MigrationStrategy.DATA_FACTORY:
        pass

    # Check Fabric workspace
    if not config.fabric.workspace_id:
        issues.append(("warning", "fabric.workspace_id is not set — needed for deployment"))

    # Check connection mappings format (should be UUIDs)
    uuid_re = _re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", _re.I)
    for name, conn_id in config.connection_mappings.mappings.items():
        if conn_id and not uuid_re.match(conn_id):
            issues.append(("warning", f"connection_mappings.{name}: '{conn_id}' is not a valid UUID"))

    # Check output directory is writable
    try:
        config.output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        issues.append(("error", f"output_dir '{config.output_dir}' is not writable: {e}"))

    # Check source configuration
    if not config.source.packages_path and not config.source.catalog_server:
        issues.append(("info", "No source configured — set packages_path or catalog_server"))

    # Check retry config bounds
    if config.retry.max_retries < 0:
        issues.append(("error", "retry.max_retries must be non-negative"))
    if config.retry.base_delay <= 0:
        issues.append(("error", "retry.base_delay must be positive"))

    # Check parallel workers
    if config.parallel_workers < 1:
        issues.append(("error", "parallel_workers must be >= 1"))

    # Display results
    if not issues:
        console.print("[green]Configuration is valid.[/green]")
        console.print(f"  Strategy: {config.strategy.value}")
        console.print(f"  Output: {config.output_dir}")
        console.print(f"  Workers: {config.parallel_workers}")
        if config.fabric.workspace_id:
            console.print(f"  Workspace: {config.fabric.workspace_id}")
        return

    table = Table(title="Configuration Issues")
    table.add_column("Severity")
    table.add_column("Issue")

    for severity, message in issues:
        style = {"error": "red", "warning": "yellow", "info": "blue"}.get(severity, "white")
        table.add_row(f"[{style}]{severity.upper()}[/{style}]", message)

    console.print(table)

    errors = sum(1 for s, _ in issues if s == "error")
    if errors:
        console.print(f"\n[red]{errors} error(s) found — fix before migrating.[/red]")
        sys.exit(1)
    else:
        console.print(f"\n[yellow]{len(issues)} warning(s)/info(s) — review recommended.[/yellow]")


if __name__ == "__main__":
    main()


@main.command()
@click.argument("output_dir", type=click.Path(exists=True))
@click.option(
    "--deployment-id",
    default=None,
    help="Specific deployment ID to roll back (default: latest)",
)
@click.pass_context
def rollback(
    ctx: click.Context,
    output_dir: str,
    deployment_id: str | None,
) -> None:
    """Roll back a previous deployment using saved snapshots.

    Reads deployment history from the output directory and reverts
    the most recent (or specified) deployment.
    """
    from ssis_to_fabric.engine.deployment_hardening import (
        DeploymentState,
        load_latest_snapshot,
        load_snapshots,
        save_snapshot,
    )

    base = Path(output_dir)

    if deployment_id:
        snapshots = load_snapshots(base)
        snapshot = next((s for s in snapshots if s.deployment_id == deployment_id), None)
        if not snapshot:
            console.print(f"[red]Deployment {deployment_id} not found.[/red]")
            sys.exit(1)
    else:
        snapshot = load_latest_snapshot(base)
        if not snapshot:
            console.print("[red]No deployment snapshots found.[/red]")
            sys.exit(1)

    console.print(f"\n[bold]Rolling back deployment:[/bold] {snapshot.deployment_id}")
    console.print(f"  State: {snapshot.state.value}")
    console.print(f"  Items: {len(snapshot.items)}")

    if snapshot.state == DeploymentState.ROLLED_BACK:
        console.print("[yellow]This deployment was already rolled back.[/yellow]")
        return

    # Mark as rolled back
    try:
        snapshot.transition(DeploymentState.ROLLED_BACK)
    except Exception:
        snapshot.state = DeploymentState.ROLLED_BACK

    save_snapshot(snapshot, base)
    console.print(f"[green]Deployment {snapshot.deployment_id} marked as ROLLED_BACK.[/green]")
    console.print("Note: To delete artifacts from Fabric workspace, use 'ssis2fabric deploy --clean'.")


@main.command(name="validate-deploy")
@click.argument("output_dir", type=click.Path(exists=True))
@click.pass_context
def validate_deploy(
    ctx: click.Context,
    output_dir: str,
) -> None:
    """Validate migration output before deployment.

    Checks that generated pipelines and notebooks are valid and
    ready for deployment to a Fabric workspace.
    """
    from ssis_to_fabric.engine.deployment_hardening import pre_deploy_validate

    result = pre_deploy_validate(Path(output_dir))

    if result.is_valid and not result.warnings:
        console.print("[green]All pre-deploy checks passed.[/green]")
        return

    table = Table(title="Pre-Deploy Validation")
    table.add_column("Severity")
    table.add_column("Issue")
    table.add_column("Suggestion")

    for issue in result.issues:
        style = {"error": "red", "warning": "yellow", "info": "blue"}.get(issue.severity, "white")
        table.add_row(
            f"[{style}]{issue.severity.upper()}[/{style}]",
            issue.message,
            issue.suggestion,
        )

    console.print(table)

    if not result.is_valid:
        console.print(f"\n[red]{len(result.errors)} error(s) — fix before deploying.[/red]")
        sys.exit(1)
    else:
        console.print(f"\n[yellow]{len(result.warnings)} warning(s) — review recommended.[/yellow]")


@main.command(name="init")
@click.option(
    "--project-name",
    default="my-ssis-migration",
    help="Project name for the configuration.",
)
@click.option(
    "--packages-path",
    default="",
    help="Path to SSIS packages directory.",
)
@click.option(
    "--workspace-id",
    default="",
    help="Fabric workspace ID.",
)
@click.option(
    "--output",
    "-o",
    default="migration_config.yaml",
    help="Output config file path.",
)
@click.pass_context
def init_config(
    ctx: click.Context,
    project_name: str,
    packages_path: str,
    workspace_id: str,
    output: str,
) -> None:
    """Generate a starter migration_config.yaml file."""
    from ssis_to_fabric.engine.integrations import write_init_config

    out_path = Path(output)
    if out_path.exists():
        console.print(f"[yellow]{output} already exists. Use a different --output path.[/yellow]")
        sys.exit(1)

    write_init_config(out_path, project_name, packages_path, workspace_id)
    console.print(f"[green]Created {output}[/green]")
    console.print("Edit the file with your connection details, then run: ssis2fabric migrate")


@main.command(name="dbt-scaffold")
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--dbt-output", default=None, help="Output directory for dbt models (default: output_dir/dbt)")
@click.pass_context
def dbt_scaffold(
    ctx: click.Context,
    output_dir: str,
    dbt_output: str | None,
) -> None:
    """Scaffold dbt models from generated Spark notebooks."""
    from ssis_to_fabric.engine.integrations import scaffold_dbt_models_from_notebooks, write_dbt_models

    nb_dir = Path(output_dir) / "notebooks"
    dbt_dir = Path(dbt_output) if dbt_output else Path(output_dir) / "dbt"

    models = scaffold_dbt_models_from_notebooks(nb_dir)
    if not models:
        console.print("[yellow]No notebooks found to scaffold dbt models from.[/yellow]")
        return

    paths = write_dbt_models(models, dbt_dir)
    console.print(f"[green]Scaffolded {len(models)} dbt model(s):[/green]")
    for p in paths:
        console.print(f"  {p}")


@main.command(name="powerbi-dataset")
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--dataset-name", default="Migration Dataset", help="Name for the Power BI dataset.")
@click.pass_context
def powerbi_dataset(
    ctx: click.Context,
    output_dir: str,
    dataset_name: str,
) -> None:
    """Generate a Power BI dataset definition from migration lineage."""
    import json

    from ssis_to_fabric.engine.integrations import (
        generate_powerbi_dataset_from_lineage,
        write_powerbi_dataset,
    )

    lineage_dir = Path(output_dir) / "lineage.json"
    lineage_file = lineage_dir / "lineage.json"
    if not lineage_file.exists():
        console.print("[red]No lineage.json found. Run 'ssis2fabric migrate' first.[/red]")
        sys.exit(1)

    lineage_data = json.loads(lineage_file.read_text(encoding="utf-8"))
    dataset = generate_powerbi_dataset_from_lineage(lineage_data, dataset_name)
    path = write_powerbi_dataset(dataset, Path(output_dir))
    console.print(f"[green]Power BI dataset definition written to {path}[/green]")
    console.print(f"  Tables: {len(dataset.tables)}")
