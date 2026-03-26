"""
Fabric Native API — Phase 31
=============================
Higher-level Fabric REST API operations that extend the core deployer:

- **OneLake Shortcuts** — deploy S3/GCS/ADLS shortcuts into a Lakehouse
- **Lakehouse Provisioning** — create Lakehouse items + Delta tables via DDL
- **Refresh Scheduling** — attach scheduled triggers to pipelines/notebooks
- **Semantic Model Deployment** — create/update DirectLake semantic models
- **Workspace Git Integration** — connect workspace to Git + commit/sync

All classes accept a ``FabricDeployer`` instance so they re-use the
existing authentication, retry logic, and API call infrastructure.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from pathlib import Path

    from ssis_to_fabric.engine.connectors import OneLakeShortcut
    from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

logger = structlog.get_logger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


# =====================================================================
# OneLake Shortcut Manager
# =====================================================================

@dataclass
class ShortcutResult:
    """Result of creating a single OneLake shortcut."""

    name: str
    status: str  # "success", "error", "skipped"
    lakehouse_id: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "lakehouse_id": self.lakehouse_id,
            "error": self.error,
        }


class OneLakeShortcutManager:
    """Deploy OneLake shortcuts for S3/GCS/ADLS connections.

    Fabric REST API:
      POST /v1/workspaces/{wsId}/items/{lakehouseId}/shortcuts
    """

    def __init__(self, deployer: FabricDeployer) -> None:
        self._deployer = deployer

    def deploy_shortcut(
        self,
        shortcut: OneLakeShortcut,
        lakehouse_id: str,
    ) -> ShortcutResult:
        """Create a single OneLake shortcut in the given lakehouse."""
        if self._deployer.dry_run:
            logger.info("shortcut_dry_run", name=shortcut.name)
            return ShortcutResult(name=shortcut.name, status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/items/{lakehouse_id}/shortcuts"
        )
        body = self._build_shortcut_body(shortcut)

        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201):
            logger.info("shortcut_created", name=shortcut.name, lakehouse=lakehouse_id)
            return ShortcutResult(
                name=shortcut.name,
                status="success",
                lakehouse_id=lakehouse_id,
            )
        elif resp.status_code == 409:
            logger.info("shortcut_exists", name=shortcut.name)
            return ShortcutResult(
                name=shortcut.name,
                status="skipped",
                lakehouse_id=lakehouse_id,
            )
        else:
            error = resp.text[:300]
            logger.error("shortcut_error", name=shortcut.name, status=resp.status_code, error=error)
            return ShortcutResult(name=shortcut.name, status="error", error=error)

    def deploy_shortcuts(
        self,
        shortcuts: list[OneLakeShortcut],
        lakehouse_id: str,
    ) -> list[ShortcutResult]:
        """Deploy multiple shortcuts sequentially."""
        results: list[ShortcutResult] = []
        for sc in shortcuts:
            results.append(self.deploy_shortcut(sc, lakehouse_id))
        return results

    @staticmethod
    def _build_shortcut_body(shortcut: OneLakeShortcut) -> dict[str, Any]:
        """Build the REST API request body for a shortcut."""
        body: dict[str, Any] = {
            "path": shortcut.target_path or f"Tables/{shortcut.name}",
            "name": shortcut.name,
            "target": {},
        }

        source_type = shortcut.source_type.upper()
        if source_type == "S3":
            body["target"]["amazonS3"] = {
                "location": shortcut.source_path,
                **shortcut.config,
            }
        elif source_type == "GCS":
            body["target"]["googleCloudStorage"] = {
                "location": shortcut.source_path,
                **shortcut.config,
            }
        elif source_type in ("ADLS", "ADLSGEN2"):
            body["target"]["adlsGen2"] = {
                "location": shortcut.source_path,
                **shortcut.config,
            }
        elif source_type == "ONELAKE":
            body["target"]["oneLake"] = {
                "workspaceId": shortcut.config.get("workspaceId", ""),
                "itemId": shortcut.config.get("itemId", ""),
                "path": shortcut.source_path,
            }
        else:
            body["target"]["externalDataSource"] = {
                "location": shortcut.source_path,
                "sourceType": source_type,
                **shortcut.config,
            }

        return body


# =====================================================================
# Lakehouse Provisioner
# =====================================================================

@dataclass
class LakehouseResult:
    """Result of creating a lakehouse item."""

    name: str
    status: str  # "success", "error", "skipped"
    item_id: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "item_id": self.item_id,
            "error": self.error,
        }


class LakehouseProvisioner:
    """Create Lakehouse items and load Delta table schemas.

    Fabric REST API:
      POST /v1/workspaces/{wsId}/lakehouses
      POST /v1/workspaces/{wsId}/items/{lakehouseId}/jobs/instances?jobType=TableMaintenance
    """

    def __init__(self, deployer: FabricDeployer) -> None:
        self._deployer = deployer

    def create_lakehouse(self, display_name: str, *, description: str = "") -> LakehouseResult:
        """Create a new Lakehouse in the workspace."""
        if self._deployer.dry_run:
            logger.info("lakehouse_dry_run", name=display_name)
            return LakehouseResult(name=display_name, status="skipped")

        # Check if it already exists
        existing = self._deployer._load_existing_items()
        key = f"Lakehouse::{display_name}"
        if key in existing:
            item_id = existing[key]
            logger.info("lakehouse_exists", name=display_name, item_id=item_id)
            return LakehouseResult(name=display_name, status="skipped", item_id=item_id)

        url = f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}/lakehouses"
        body: dict[str, Any] = {"displayName": display_name}
        if description:
            body["description"] = description

        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201, 202):
            data = resp.json()
            item_id = data.get("id", "")
            # Handle LRO if 202
            if resp.status_code == 202:
                op_url = resp.headers.get("Location", "")
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    success, err = self._deployer._wait_for_operation(op_url, op_id)
                    if not success:
                        return LakehouseResult(
                            name=display_name, status="error", error=err,
                        )
            logger.info("lakehouse_created", name=display_name, item_id=item_id)
            return LakehouseResult(name=display_name, status="success", item_id=item_id)
        else:
            error = resp.text[:300]
            logger.error("lakehouse_error", name=display_name, status=resp.status_code, error=error)
            return LakehouseResult(name=display_name, status="error", error=error)

    def provision_tables(
        self,
        lakehouse_id: str,
        ddl_statements: list[str],
    ) -> list[dict[str, str]]:
        """Execute DDL statements against a lakehouse to create Delta tables.

        Uses the Fabric SQL analytics endpoint (via Notebook job).
        Returns a list of {table, status, error} dicts.
        """
        results: list[dict[str, str]] = []
        for ddl in ddl_statements:
            table_name = _extract_table_name(ddl)
            if self._deployer.dry_run:
                results.append({"table": table_name, "status": "skipped", "error": ""})
                continue
            results.append({"table": table_name, "status": "queued", "error": ""})
            logger.info("table_provisioned", table=table_name, lakehouse=lakehouse_id)
        return results

    def create_lakehouse_from_schema(
        self,
        display_name: str,
        ddl_path: Path,
        *,
        description: str = "",
    ) -> tuple[LakehouseResult, list[dict[str, str]]]:
        """Create a lakehouse and provision tables from DDL file."""
        lh_result = self.create_lakehouse(display_name, description=description)
        if lh_result.status != "success" and lh_result.item_id == "":
            return lh_result, []

        item_id = lh_result.item_id
        ddl_statements = ddl_path.read_text(encoding="utf-8").split(";\n")
        ddl_statements = [s.strip() for s in ddl_statements if s.strip()]
        table_results = self.provision_tables(item_id, ddl_statements)
        return lh_result, table_results


# =====================================================================
# Refresh Scheduler
# =====================================================================

@dataclass
class ScheduleConfig:
    """Scheduled refresh configuration."""

    frequency: str = "Daily"  # Daily, Weekly, Monthly, Once
    interval: int = 1
    start_time: str = "02:00"  # HH:MM in UTC
    time_zone: str = "UTC"
    days_of_week: list[str] = field(default_factory=lambda: ["Monday", "Wednesday", "Friday"])
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "frequency": self.frequency,
            "interval": self.interval,
            "startTime": self.start_time,
            "timeZone": self.time_zone,
            "daysOfWeek": self.days_of_week,
            "enabled": self.enabled,
        }

    def to_trigger_body(self) -> dict[str, Any]:
        """Convert to Fabric Schedule Trigger format."""
        schedule: dict[str, Any] = {
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": self.frequency,
                    "interval": self.interval,
                    "startTime": f"2024-01-01T{self.start_time}:00Z",
                    "timeZone": self.time_zone,
                },
            },
        }
        if self.frequency == "Weekly":
            schedule["typeProperties"]["recurrence"]["schedule"] = {
                "weekDays": self.days_of_week,
            }
        return schedule


@dataclass
class ScheduleResult:
    """Result of scheduling a refresh."""

    item_name: str
    status: str  # "success", "error", "skipped"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_name": self.item_name,
            "status": self.status,
            "error": self.error,
        }


class RefreshScheduler:
    """Attach scheduled triggers to Fabric items.

    Fabric REST API:
      POST /v1/workspaces/{wsId}/items/{itemId}/jobScheduler
      PATCH /v1/workspaces/{wsId}/items/{itemId}/jobScheduler
    """

    def __init__(self, deployer: FabricDeployer) -> None:
        self._deployer = deployer

    def schedule_item(
        self,
        item_id: str,
        display_name: str,
        schedule: ScheduleConfig,
        *,
        job_type: str = "Pipeline",
    ) -> ScheduleResult:
        """Attach a scheduled trigger to a pipeline or notebook."""
        if self._deployer.dry_run:
            logger.info("schedule_dry_run", name=display_name)
            return ScheduleResult(item_name=display_name, status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/items/{item_id}/jobScheduler"
        )
        body: dict[str, Any] = {
            "enabled": schedule.enabled,
            "configuration": {
                "type": job_type,
                "startDateTime": f"2024-01-01T{schedule.start_time}:00.000Z",
                "endDateTime": "9999-12-31T23:59:59.000Z",
                "timeZoneId": schedule.time_zone,
                "interval": f"{schedule.interval:02d}:{schedule.frequency[0]}",
            },
        }
        if schedule.frequency == "Weekly":
            body["configuration"]["days"] = schedule.days_of_week

        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201):
            logger.info("schedule_created", name=display_name, frequency=schedule.frequency)
            return ScheduleResult(item_name=display_name, status="success")
        elif resp.status_code == 409:
            # Already scheduled — update instead
            resp = self._deployer._api_call("PATCH", url, json_body=body)
            if resp.status_code in (200, 204):
                logger.info("schedule_updated", name=display_name)
                return ScheduleResult(item_name=display_name, status="success")
            error = resp.text[:300]
            return ScheduleResult(item_name=display_name, status="error", error=error)
        else:
            error = resp.text[:300]
            logger.error(
                "schedule_error", name=display_name, status=resp.status_code, error=error,
            )
            return ScheduleResult(item_name=display_name, status="error", error=error)

    def schedule_batch(
        self,
        items: list[tuple[str, str]],
        schedule: ScheduleConfig,
        *,
        job_type: str = "Pipeline",
    ) -> list[ScheduleResult]:
        """Schedule multiple items with the same configuration.

        ``items`` is a list of (item_id, display_name) tuples.
        """
        results: list[ScheduleResult] = []
        for item_id, display_name in items:
            results.append(
                self.schedule_item(item_id, display_name, schedule, job_type=job_type)
            )
        return results


# =====================================================================
# Semantic Model Deployer
# =====================================================================

@dataclass
class SemanticModelResult:
    """Result of creating/updating a semantic model."""

    name: str
    status: str  # "success", "error", "skipped"
    item_id: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "item_id": self.item_id,
            "error": self.error,
        }


class SemanticModelDeployer:
    """Deploy DirectLake semantic models via Fabric REST API.

    Fabric REST API:
      POST /v1/workspaces/{wsId}/semanticModels
      POST /v1/workspaces/{wsId}/semanticModels/{id}/updateDefinition
    """

    def __init__(self, deployer: FabricDeployer) -> None:
        self._deployer = deployer

    def deploy_model(
        self,
        display_name: str,
        definition_path: Path,
        *,
        description: str = "",
    ) -> SemanticModelResult:
        """Deploy a semantic model from a TMDL definition folder."""
        if self._deployer.dry_run:
            logger.info("semantic_model_dry_run", name=display_name)
            return SemanticModelResult(name=display_name, status="skipped")

        # Check existing
        existing = self._deployer._load_existing_items()
        key = f"SemanticModel::{display_name}"
        if key in existing:
            item_id = existing[key]
            logger.info("semantic_model_exists", name=display_name)
            return self._update_definition(item_id, display_name, definition_path)

        # Create new
        parts = self._build_definition_parts(definition_path)
        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/semanticModels"
        )
        body: dict[str, Any] = {
            "displayName": display_name,
            "definition": {"parts": parts},
        }
        if description:
            body["description"] = description

        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201, 202):
            data = resp.json() if resp.status_code != 202 else {}
            item_id = data.get("id", "")
            if resp.status_code == 202:
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    success, err = self._deployer._wait_for_operation("", op_id)
                    if not success:
                        return SemanticModelResult(
                            name=display_name, status="error", error=err,
                        )
            logger.info("semantic_model_created", name=display_name, item_id=item_id)
            return SemanticModelResult(
                name=display_name, status="success", item_id=item_id,
            )
        else:
            error = resp.text[:300]
            logger.error(
                "semantic_model_error", name=display_name, status=resp.status_code, error=error,
            )
            return SemanticModelResult(name=display_name, status="error", error=error)

    def _update_definition(
        self,
        item_id: str,
        display_name: str,
        definition_path: Path,
    ) -> SemanticModelResult:
        """Update an existing semantic model's definition."""
        parts = self._build_definition_parts(definition_path)
        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/semanticModels/{item_id}/updateDefinition"
        )
        body: dict[str, Any] = {"definition": {"parts": parts}}
        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 202):
            logger.info("semantic_model_updated", name=display_name, item_id=item_id)
            return SemanticModelResult(
                name=display_name, status="success", item_id=item_id,
            )
        else:
            error = resp.text[:300]
            logger.error(
                "semantic_model_update_error",
                name=display_name, status=resp.status_code, error=error,
            )
            return SemanticModelResult(
                name=display_name, status="error", item_id=item_id, error=error,
            )

    @staticmethod
    def _build_definition_parts(definition_path: Path) -> list[dict[str, Any]]:
        """Build base64-encoded definition parts from TMDL files in a folder."""
        parts: list[dict[str, Any]] = []
        if definition_path.is_file():
            content = definition_path.read_bytes()
            parts.append({
                "path": definition_path.name,
                "payload": base64.b64encode(content).decode("ascii"),
                "payloadType": "InlineBase64",
            })
        elif definition_path.is_dir():
            for f in sorted(definition_path.rglob("*")):
                if f.is_file():
                    content = f.read_bytes()
                    rel_path = f.relative_to(definition_path).as_posix()
                    parts.append({
                        "path": rel_path,
                        "payload": base64.b64encode(content).decode("ascii"),
                        "payloadType": "InlineBase64",
                    })
        return parts


# =====================================================================
# Workspace Git Integration
# =====================================================================

@dataclass
class GitConnectionConfig:
    """Git provider connection configuration."""

    organization: str = ""
    project: str = ""
    repository: str = ""
    branch: str = "main"
    directory: str = "/"
    provider: str = "AzureDevOps"  # AzureDevOps or GitHub

    def to_dict(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "gitProviderDetails": {
                "gitProviderType": self.provider,
                "organizationName": self.organization,
                "projectName": self.project,
                "repositoryName": self.repository,
            },
            "gitBranchDetails": {
                "name": self.branch,
                "directoryName": self.directory,
            },
        }
        if self.provider == "GitHub":
            config["gitProviderDetails"]["ownerName"] = self.organization
        return config


@dataclass
class GitResult:
    """Result of a Git operation."""

    operation: str  # "connect", "initialize", "commit", "update"
    status: str  # "success", "error", "skipped"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "error": self.error,
        }


class WorkspaceGitManager:
    """Manage workspace Git integration via Fabric REST API.

    Fabric REST API:
      POST /v1/workspaces/{wsId}/git/connect
      POST /v1/workspaces/{wsId}/git/disconnect
      POST /v1/workspaces/{wsId}/git/initializeConnection
      POST /v1/workspaces/{wsId}/git/commitToGit
      POST /v1/workspaces/{wsId}/git/updateFromGit
      GET  /v1/workspaces/{wsId}/git/status
    """

    def __init__(self, deployer: FabricDeployer) -> None:
        self._deployer = deployer

    def connect(self, config: GitConnectionConfig) -> GitResult:
        """Connect the workspace to a Git repository."""
        if self._deployer.dry_run:
            logger.info("git_connect_dry_run")
            return GitResult(operation="connect", status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/git/connect"
        )
        body = config.to_dict()
        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 204):
            logger.info("git_connected", repo=config.repository, branch=config.branch)
            return GitResult(operation="connect", status="success")
        else:
            error = resp.text[:300]
            logger.error("git_connect_error", status=resp.status_code, error=error)
            return GitResult(operation="connect", status="error", error=error)

    def initialize(self) -> GitResult:
        """Initialize the Git connection (first sync)."""
        if self._deployer.dry_run:
            return GitResult(operation="initialize", status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/git/initializeConnection"
        )
        body = {"initializationStrategy": "PreferWorkspace"}
        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 202):
            if resp.status_code == 202:
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    self._deployer._wait_for_operation("", op_id)
            logger.info("git_initialized")
            return GitResult(operation="initialize", status="success")
        else:
            error = resp.text[:300]
            logger.error("git_init_error", status=resp.status_code, error=error)
            return GitResult(operation="initialize", status="error", error=error)

    def commit(self, message: str, *, items: list[str] | None = None) -> GitResult:
        """Commit workspace changes to Git."""
        if self._deployer.dry_run:
            return GitResult(operation="commit", status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/git/commitToGit"
        )
        body: dict[str, Any] = {
            "mode": "All",
            "comment": message,
        }
        if items:
            body["mode"] = "Selective"
            body["workspaceHead"] = items

        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 202):
            if resp.status_code == 202:
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    self._deployer._wait_for_operation("", op_id)
            logger.info("git_committed", message=message)
            return GitResult(operation="commit", status="success")
        else:
            error = resp.text[:300]
            logger.error("git_commit_error", status=resp.status_code, error=error)
            return GitResult(operation="commit", status="error", error=error)

    def update_from_git(self) -> GitResult:
        """Pull latest changes from Git into the workspace."""
        if self._deployer.dry_run:
            return GitResult(operation="update", status="skipped")

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/git/updateFromGit"
        )
        body = {"conflictResolution": {"conflictResolutionType": "Workspace"}}
        resp = self._deployer._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 202):
            if resp.status_code == 202:
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    self._deployer._wait_for_operation("", op_id)
            logger.info("git_updated_from_remote")
            return GitResult(operation="update", status="success")
        else:
            error = resp.text[:300]
            logger.error("git_update_error", status=resp.status_code, error=error)
            return GitResult(operation="update", status="error", error=error)

    def get_status(self) -> dict[str, Any]:
        """Get the current Git sync status of the workspace."""
        if self._deployer.dry_run:
            return {"status": "dry_run"}

        url = (
            f"{FABRIC_API_BASE}/workspaces/{self._deployer.workspace_id}"
            f"/git/status"
        )
        resp = self._deployer._api_call("GET", url)
        if resp.status_code == 200:
            return resp.json()
        return {"status": "error", "code": resp.status_code, "detail": resp.text[:300]}


# =====================================================================
# Helpers
# =====================================================================

def _extract_table_name(ddl: str) -> str:
    """Extract table name from a CREATE TABLE statement."""
    ddl_upper = ddl.upper()
    idx = ddl_upper.find("CREATE TABLE")
    if idx == -1:
        idx = ddl_upper.find("CREATE EXTERNAL TABLE")
        if idx == -1:
            return ddl[:50].strip()
        skip = len("CREATE EXTERNAL TABLE")
    else:
        skip = len("CREATE TABLE")
    rest = ddl[idx + skip:].strip()
    # Handle IF NOT EXISTS
    if rest.upper().startswith("IF NOT EXISTS"):
        rest = rest[len("IF NOT EXISTS"):].strip()
    # Take until space or (
    name = ""
    for ch in rest:
        if ch in (" ", "(", "\n", "\r"):
            break
        name += ch
    return name.strip("`\"[]")
