"""
Web Dashboard
==============
Phase 19: Lightweight web UI for migration management with REST API,
real-time progress via SSE, and package browsing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# API Route Definitions
# =====================================================================


@dataclass
class APIRoute:
    """Definition of a REST API endpoint."""

    method: str  # GET, POST, PUT, DELETE
    path: str
    handler: str  # function name
    description: str = ""
    auth_required: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "path": self.path,
            "handler": self.handler,
            "description": self.description,
            "auth_required": self.auth_required,
        }


# All API routes mirroring CLI commands
API_ROUTES: list[APIRoute] = [
    APIRoute("GET", "/api/v1/health", "health_check", "Health check endpoint", auth_required=False),
    APIRoute("POST", "/api/v1/analyze", "analyze_packages", "Analyze SSIS packages"),
    APIRoute("POST", "/api/v1/plan", "create_plan", "Generate migration plan"),
    APIRoute("POST", "/api/v1/migrate", "run_migration", "Execute migration"),
    APIRoute("POST", "/api/v1/deploy", "deploy_artifacts", "Deploy to Fabric workspace"),
    APIRoute("GET", "/api/v1/packages", "list_packages", "List analyzed packages"),
    APIRoute("GET", "/api/v1/packages/{id}", "get_package", "Get package details"),
    APIRoute("GET", "/api/v1/status", "migration_status", "Get migration status"),
    APIRoute("GET", "/api/v1/lineage", "get_lineage", "Get data lineage graph"),
    APIRoute("GET", "/api/v1/report", "get_report", "Get migration report"),
    APIRoute("POST", "/api/v1/validate", "validate_migration", "Run validation checks"),
    APIRoute("POST", "/api/v1/rollback", "rollback_deployment", "Rollback deployment"),
]


def get_api_spec() -> dict[str, Any]:
    """Generate OpenAPI-like specification for the REST API."""
    paths: dict[str, Any] = {}
    for route in API_ROUTES:
        if route.path not in paths:
            paths[route.path] = {}
        paths[route.path][route.method.lower()] = {
            "summary": route.description,
            "handler": route.handler,
            "security": [{"bearerAuth": []}] if route.auth_required else [],
        }
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "SSIS to Fabric Migration API",
            "version": "2.7.0",
            "description": "REST API for SSIS to Microsoft Fabric migration management",
        },
        "paths": paths,
    }


# =====================================================================
# SSE Progress Streaming
# =====================================================================


@dataclass
class SSEEvent:
    """A Server-Sent Event for real-time progress."""

    event: str  # progress, status, error, complete
    data: dict[str, Any] = field(default_factory=dict)
    id: str = ""  # noqa: A003

    def format(self) -> str:
        """Format as SSE wire protocol."""
        lines = []
        if self.id:
            lines.append(f"id: {self.id}")
        lines.append(f"event: {self.event}")
        lines.append(f"data: {json.dumps(self.data)}")
        lines.append("")
        return "\n".join(lines) + "\n"


class ProgressStream:
    """Manages SSE progress events for a migration run."""

    def __init__(self) -> None:
        self.events: list[SSEEvent] = []
        self._counter = 0

    def emit(self, event_type: str, **data: Any) -> SSEEvent:
        self._counter += 1
        evt = SSEEvent(
            event=event_type,
            data={"timestamp": datetime.now(tz=timezone.utc).isoformat(), **data},
            id=str(self._counter),
        )
        self.events.append(evt)
        return evt

    def progress(self, package: str, step: str, percent: float) -> SSEEvent:
        return self.emit("progress", package=package, step=step, percent=round(percent, 1))

    def status(self, message: str) -> SSEEvent:
        return self.emit("status", message=message)

    def error(self, message: str, package: str = "") -> SSEEvent:
        return self.emit("error", message=message, package=package)

    def complete(self, summary: dict[str, Any] | None = None) -> SSEEvent:
        return self.emit("complete", summary=summary or {})

    def get_stream(self) -> str:
        """Return all events as SSE stream text."""
        return "".join(e.format() for e in self.events)


# =====================================================================
# Package Browser
# =====================================================================


@dataclass
class PackageView:
    """Browser-friendly view of an SSIS package."""

    name: str
    complexity: str
    task_count: int = 0
    dataflow_count: int = 0
    connection_count: int = 0
    warnings: list[str] = field(default_factory=list)
    tasks: list[dict[str, str]] = field(default_factory=list)
    connections: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "complexity": self.complexity,
            "task_count": self.task_count,
            "dataflow_count": self.dataflow_count,
            "connection_count": self.connection_count,
            "warnings": self.warnings,
            "tasks": self.tasks,
            "connections": self.connections,
        }


def package_to_view(package: Any) -> PackageView:
    """Convert an SSISPackage model to a browser-friendly view."""
    tasks = []
    for t in getattr(package, "control_flow_tasks", []):
        tasks.append({
            "name": getattr(t, "name", str(t)),
            "type": getattr(t, "task_type", "unknown"),
        })
    connections = []
    for c in getattr(package, "connection_managers", []):
        connections.append({
            "name": getattr(c, "name", str(c)),
            "type": getattr(c, "connection_type", "unknown"),
        })
    return PackageView(
        name=getattr(package, "name", "unknown"),
        complexity=getattr(package, "overall_complexity", type("", (), {"value": "MEDIUM"})()).value
        if hasattr(package, "overall_complexity") else "MEDIUM",
        task_count=getattr(package, "total_tasks", len(tasks)),
        dataflow_count=getattr(package, "total_data_flows", 0),
        connection_count=len(connections),
        warnings=[str(w) for w in getattr(package, "warnings", [])],
        tasks=tasks,
        connections=connections,
    )


# =====================================================================
# Dashboard HTML Generator
# =====================================================================


def generate_dashboard_html(packages: list[PackageView] | None = None) -> str:
    """Generate self-contained dashboard HTML."""
    pkg_json = json.dumps([p.to_dict() for p in (packages or [])])
    routes_json = json.dumps([r.to_dict() for r in API_ROUTES])

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>SSIS to Fabric — Migration Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f8; color: #333; }}
  header {{ background: #0078d4; color: #fff; padding: 16px 24px; }}
  header h1 {{ font-size: 20px; }}
  .container {{ max-width: 1200px; margin: 20px auto; padding: 0 20px; }}
  .card {{ background: #fff; border-radius: 8px; padding: 20px; margin: 16px 0;
           box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
  .card h2 {{ color: #0078d4; margin-bottom: 12px; font-size: 16px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ text-align: left; padding: 8px 12px; border-bottom: 1px solid #eee; }}
  th {{ background: #f8f9fa; font-weight: 600; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 12px; font-weight: 600; }}
  .badge-low {{ background: #d4edda; color: #155724; }}
  .badge-medium {{ background: #fff3cd; color: #856404; }}
  .badge-high {{ background: #f8d7da; color: #721c24; }}
  #progress {{ height: 20px; background: #e9ecef; border-radius: 4px; overflow: hidden; }}
  #progress-bar {{ height: 100%; background: #0078d4; transition: width 0.3s; width: 0%; }}
  .api-endpoint {{ font-family: monospace; background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
</style>
</head>
<body>
<header><h1>SSIS to Fabric — Migration Dashboard</h1></header>
<div class="container">
  <div class="card">
    <h2>Migration Progress</h2>
    <div id="progress"><div id="progress-bar"></div></div>
    <p id="status" style="margin-top:8px;color:#666;">Ready</p>
  </div>
  <div class="card">
    <h2>Packages</h2>
    <table><thead><tr><th>Name</th><th>Tasks</th><th>Data Flows</th><th>Complexity</th><th>Warnings</th></tr></thead>
    <tbody id="pkg-table"></tbody></table>
  </div>
  <div class="card">
    <h2>API Endpoints</h2>
    <table><thead><tr><th>Method</th><th>Path</th><th>Description</th></tr></thead>
    <tbody id="api-table"></tbody></table>
  </div>
</div>
<script>
const packages = {pkg_json};
const routes = {routes_json};

const pkgTbody = document.getElementById('pkg-table');
packages.forEach(p => {{
  const cls = p.complexity === 'LOW' ? 'badge-low' : p.complexity === 'HIGH' ? 'badge-high' : 'badge-medium';
  pkgTbody.innerHTML += '<tr><td>' + p.name + '</td><td>' + p.task_count +
    '</td><td>' + p.dataflow_count + '</td><td><span class="badge ' + cls + '">' +
    p.complexity + '</span></td><td>' + p.warnings.length + '</td></tr>';
}});

const apiTbody = document.getElementById('api-table');
routes.forEach(r => {{
  apiTbody.innerHTML += '<tr><td><strong>' + r.method + '</strong></td>' +
    '<td class="api-endpoint">' + r.path + '</td><td>' + r.description + '</td></tr>';
}});
</script>
</body>
</html>"""


def write_dashboard(output_dir: Path, packages: list[PackageView] | None = None) -> Path:
    """Write dashboard HTML to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "dashboard.html"
    path.write_text(generate_dashboard_html(packages), encoding="utf-8")
    logger.info("dashboard_written", path=str(path))
    return path
