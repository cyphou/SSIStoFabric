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
from rich.table import Table

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy, load_config
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
        cfg.log_level = log_level  # type: ignore
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
@click.pass_context
def migrate(
    ctx: click.Context,
    path: str,
    strategy: str | None,
    output: str | None,
    dataflow_type: str | None,
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

    parser = DTSXParser()
    p = Path(path)
    packages = [parser.parse(p)] if p.is_file() else parser.parse_directory(p)

    if not packages:
        console.print("[red]No SSIS packages found.[/red]")
        sys.exit(1)

    engine = MigrationEngine(config)
    result = engine.execute(packages)

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
    console.print(f"\nOutput directory: [bold]{config.output_dir}[/bold]")


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
        sys.exit(1)


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


if __name__ == "__main__":
    main()
