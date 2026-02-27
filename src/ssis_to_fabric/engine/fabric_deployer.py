"""
Fabric Deployer
================
Deploys generated migration artifacts (Data Factory pipelines, Dataflow Gen2
items, and Spark notebooks) to a Microsoft Fabric workspace via REST APIs.

API References:
  - Create Data Pipeline: POST /v1/workspaces/{workspaceId}/dataPipelines
  - Create Dataflow: POST /v1/workspaces/{workspaceId}/dataflows
  - Create Notebook: POST /v1/workspaces/{workspaceId}/notebooks
  - Long Running Operations: GET /v1/operations/{operationId}
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import requests
import structlog

if TYPE_CHECKING:
    from pathlib import Path

logger = structlog.get_logger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Rate-limit defaults
DEFAULT_RETRY_AFTER = 30
MAX_RETRIES = 5
POLL_INTERVAL = 5


@dataclass
class DeploymentResult:
    """Result of deploying a single item."""

    name: str
    item_type: str  # "DataPipeline", "Dataflow", or "Notebook"
    status: str  # "success", "error", "skipped"
    item_id: str | None = None
    error: str | None = None


@dataclass
class DeploymentReport:
    """Aggregated deployment report."""

    workspace_id: str
    results: list[DeploymentResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.status == "success")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "error")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "skipped")


class FabricDeployer:
    """
    Deploys Data Factory pipelines and Spark notebooks to a Fabric workspace.

    Uses azure.identity for authentication (InteractiveBrowserCredential for
    user login, or DefaultAzureCredential for automated scenarios).
    """

    def __init__(
        self,
        workspace_id: str,
        *,
        credential: object | None = None,
        skip_existing: bool = True,
        dry_run: bool = False,
        default_connection_id: str | None = None,
    ) -> None:
        self.workspace_id = workspace_id
        self.skip_existing = skip_existing
        self.dry_run = dry_run
        self.default_connection_id = default_connection_id
        self._token: str | None = None
        self._credential = credential
        self._existing_items: dict[str, dict] | None = None
        # Populated by Phase 0 — maps SSIS conn name → Fabric connection GUID
        self._connection_map: dict[str, str] = {}
        # Populated by Phase 0.5 — maps pipeline_name → folder_id
        self._folder_map: dict[str, str] = {}
        # Sorted longest-first for prefix matching
        self._folder_names_sorted: list[str] = []

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def authenticate(self) -> None:
        """Acquire an access token for the Fabric API.

        Credential resolution order:
        1. Externally supplied *credential* (passed to constructor).
        2. Service-principal env vars (``AZURE_TENANT_ID``,
           ``AZURE_CLIENT_ID``, ``AZURE_CLIENT_SECRET``).
        3. ``DefaultAzureCredential`` — covers Managed Identity,
           ``az login`` cache, VS Code token cache, etc.
        4. Interactive fallback: SharedTokenCache → DeviceCode →
           InteractiveBrowser.
        """
        import os

        if self._credential is None:
            tenant = os.environ.get("AZURE_TENANT_ID", "")
            client_id = os.environ.get("AZURE_CLIENT_ID", "")
            client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")

            if tenant and client_id and client_secret:
                # Service-principal authentication (CI/CD)
                from azure.identity import ClientSecretCredential

                self._credential = ClientSecretCredential(
                    tenant_id=tenant,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                logger.info("using_service_principal", client_id=client_id)
            else:
                # Interactive / local-dev authentication
                # Try DefaultAzureCredential first; if it cannot obtain a token
                # fall back to interactive browser / device-code auth.
                from azure.identity import DefaultAzureCredential

                try:
                    cred = DefaultAzureCredential()
                    token = cred.get_token(FABRIC_SCOPE)
                    self._credential = cred
                    self._token = token.token
                    logger.info("fabric_auth_success", scope=FABRIC_SCOPE)
                    return
                except Exception:
                    logger.info("default_credential_failed_falling_back_interactive")

                from azure.identity import InteractiveBrowserCredential

                try:
                    self._credential = InteractiveBrowserCredential()
                except Exception:
                    from azure.identity import DeviceCodeCredential

                    self._credential = DeviceCodeCredential()

        token = self._credential.get_token(FABRIC_SCOPE)  # type: ignore[union-attr]
        self._token = token.token
        logger.info("fabric_auth_success", scope=FABRIC_SCOPE)

    def _headers(self) -> dict[str, str]:
        if not self._token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_guid(value: str) -> bool:
        """Return True if *value* looks like a UUID/GUID (8-4-4-4-12 hex)."""
        import re as _re

        return bool(_re.fullmatch(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value.strip()))

    # ------------------------------------------------------------------
    # Phase 0 — Connection resolution
    # ------------------------------------------------------------------

    def _resolve_connections(self, output_dir: Path) -> None:
        """Build ``_connection_map`` from connection manifests + auto-discovery.

        Resolution priority for each SSIS connection:
          1. ``default_connection_id`` (CLI ``-c`` override → all connections)
          2. Existing Fabric connection matched by display name
          3. Newly created SQL-type ShareableCloud connection using the
             server/database from the SSIS connection manifest
          4. Nothing (user will configure manually in Fabric UI)

        Auto-created connections use `WorkspaceIdentity` credentials (no
        secrets required) and use the server/database defined in the SSIS
        project-level connection manager.
        """
        conn_dir = output_dir / "connections"
        if not conn_dir.exists():
            logger.info("no_connections_dir", path=str(conn_dir))
            if self.default_connection_id:
                logger.info(
                    "using_default_connection_for_all",
                    connection_id=self.default_connection_id,
                )
            return

        manifests = sorted(conn_dir.glob("*.json"))
        if not manifests:
            return

        # Read all connection manifests
        connections: list[dict] = []
        for mf in manifests:
            try:
                data = json.loads(mf.read_text(encoding="utf-8"))
                connections.append(data)
            except Exception as e:
                logger.warning("connection_manifest_parse_error", file=mf.name, error=str(e))

        if not connections:
            return

        logger.info("phase0_resolving_connections", count=len(connections))

        # If user provided a single --connection-id, map ALL connections to it
        if self.default_connection_id:
            for conn in connections:
                ssis_name = conn["ssis_name"]
                self._connection_map[ssis_name] = self.default_connection_id
                logger.info(
                    "connection_mapped_via_override",
                    ssis_name=ssis_name,
                    connection_id=self.default_connection_id,
                )
            return

        # Auto-discover existing Fabric connections (name → id)
        existing = self._discover_connections()

        for conn in connections:
            ssis_name = conn["ssis_name"]
            server = conn.get("server", "")
            database = conn.get("database", "")

            # 1. Match by display name (exact)
            if ssis_name in existing:
                self._connection_map[ssis_name] = existing[ssis_name]
                logger.info(
                    "connection_matched_by_name",
                    ssis_name=ssis_name,
                    connection_id=existing[ssis_name],
                )
                continue

            # 2. Auto-create SQL connection from SSIS manifest data
            if server and database:
                new_id = self._create_sql_connection(ssis_name, server, database)
                if new_id:
                    self._connection_map[ssis_name] = new_id
                    # Also register so next manifest doesn't re-create
                    existing[ssis_name] = new_id
                    continue

            logger.warning(
                "connection_unresolved",
                ssis_name=ssis_name,
                hint="No server/database in manifest. Configure manually in Fabric UI or pass --connection-id",
            )

    def _discover_connections(self) -> dict[str, str]:
        """Find existing Fabric connections and return a name → id mapping.

        Returns a dict keyed by ``displayName`` for SQL and Warehouse type
        connections.  Prefers ``ShareableCloud`` connections over
        ``PersonalCloud`` when duplicates exist.
        """
        if self.dry_run:
            return {}

        try:
            resp = self._api_call("GET", f"{FABRIC_API_BASE}/connections")
            if resp.status_code != 200:
                logger.warning("connections_api_error", status=resp.status_code)
                return {}

            all_conns = resp.json().get("value", [])
            result: dict[str, str] = {}
            for c in all_conns:
                ctype = c.get("connectionDetails", {}).get("type", "")
                if ctype not in ("SQL", "Warehouse", "FabricSQL"):
                    continue
                name = c.get("displayName", "")
                if not name:
                    continue
                # Prefer ShareableCloud over PersonalCloud
                existing_conn_type = None
                if name in result:
                    # Already have one — check if new is better
                    existing_conn_type = next(
                        (x.get("connectivityType") for x in all_conns if x["id"] == result[name]),
                        None,
                    )
                if name not in result or (
                    existing_conn_type == "PersonalCloud"
                    and c.get("connectivityType") == "ShareableCloud"
                ):
                    result[name] = c["id"]

            logger.info(
                "connections_discovered",
                count=len(result),
                names=list(result.keys()),
            )
            return result

        except Exception as e:
            logger.warning("connection_discovery_failed", error=str(e))
            return {}

    def _create_sql_connection(
        self, display_name: str, server: str, database: str
    ) -> str | None:
        """Create a SQL-type ShareableCloud connection with WorkspaceIdentity.

        This creates a Fabric connection that uses WorkspaceIdentity
        credentials (no secrets needed) matching the SSIS project-level
        connection manager.  The connection is compatible with Script
        activities.

        Returns the new connection GUID or ``None`` on failure.
        """
        if self.dry_run:
            return None

        body = {
            "connectivityType": "ShareableCloud",
            "displayName": display_name,
            "connectionDetails": {
                "type": "SQL",
                "creationMethod": "Sql",
                "parameters": [
                    {"dataType": "Text", "name": "server", "value": server},
                    {"dataType": "Text", "name": "database", "value": database},
                ],
            },
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "credentials": {"credentialType": "WorkspaceIdentity"},
            },
            "privacyLevel": "Organizational",
        }

        try:
            resp = self._api_call("POST", f"{FABRIC_API_BASE}/connections", json_body=body)
            if resp.status_code in (200, 201):
                new_id = resp.json().get("id", "")
                logger.info(
                    "sql_connection_created",
                    connection_id=new_id,
                    display_name=display_name,
                    server=server,
                    database=database,
                )
                return new_id

            logger.warning(
                "sql_connection_create_failed",
                status=resp.status_code,
                detail=resp.text[:300],
                display_name=display_name,
            )
            return None

        except Exception as e:
            logger.warning("connection_creation_failed", error=str(e))
            return None

    def _resolve_connection_for_activity(self, ssis_name_hint: str) -> str | None:
        """Resolve a Fabric connection ID for a single pipeline activity.

        Looks up the SSIS connection name (extracted from the TODO placeholder)
        in ``_connection_map``.  Falls back to ``default_connection_id``.
        """
        if ssis_name_hint and ssis_name_hint in self._connection_map:
            return self._connection_map[ssis_name_hint]
        # Fallback: try without exact match — any connection in the map
        if self._connection_map:
            return next(iter(self._connection_map.values()))
        return self.default_connection_id

    def _api_call(
        self,
        method: str,
        url: str,
        *,
        json_body: dict | None = None,
        retries: int = MAX_RETRIES,
    ) -> requests.Response:
        """Make an API call with retry-after handling for 429s."""
        for attempt in range(1, retries + 1):
            resp = requests.request(method, url, headers=self._headers(), json=json_body, timeout=120)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", DEFAULT_RETRY_AFTER))
                logger.warning(
                    "rate_limited",
                    retry_after=retry_after,
                    attempt=attempt,
                    url=url,
                )
                time.sleep(retry_after)
                continue
            return resp
        # Return last response even if still 429
        return resp  # type: ignore[possibly-undefined]

    def _wait_for_operation(self, operation_url: str, operation_id: str) -> tuple[bool, str]:
        """Poll a long-running operation until it completes.

        Returns (success, error_message).
        """
        url = f"{FABRIC_API_BASE}/operations/{operation_id}"
        for _ in range(60):  # max ~5 min
            resp = self._api_call("GET", url)
            if resp.status_code != 200:
                msg = f"Poll error HTTP {resp.status_code}: {resp.text[:300]}"
                logger.error("lro_poll_error", status=resp.status_code, body=resp.text[:300])
                return False, msg
            data = resp.json()
            status = data.get("status", "Unknown")
            if status in ("Succeeded", "Completed"):
                return True, ""
            if status in ("Failed", "Cancelled"):
                error_info = json.dumps(data.get("error", data), indent=2)[:500]
                logger.error("lro_failed", status=status, error=error_info)
                return False, f"LRO {status}: {error_info}"
            time.sleep(POLL_INTERVAL)
        logger.error("lro_timeout", operation_id=operation_id)
        return False, f"LRO timeout after 5 min: {operation_id}"

    # ------------------------------------------------------------------
    # Existing items check
    # ------------------------------------------------------------------

    def _load_existing_items(self) -> dict[str, dict]:
        """Fetch all items in the workspace to detect duplicates."""
        items: dict[str, dict] = {}
        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/items"
        while url:
            resp = self._api_call("GET", url)
            if resp.status_code != 200:
                logger.warning("list_items_error", status=resp.status_code, body=resp.text)
                break
            data = resp.json()
            for item in data.get("value", []):
                key = f"{item['type']}::{item['displayName']}"
                items[key] = item
            url = data.get("continuationUri")
        logger.info("existing_items_loaded", count=len(items))
        return items

    def _item_exists(self, item_type: str, display_name: str) -> bool:
        if self._existing_items is None:
            self._existing_items = self._load_existing_items()
        return f"{item_type}::{display_name}" in self._existing_items

    def list_workspace_items(self) -> list[dict]:
        """Public API: return all workspace items as a list of dicts.

        Each dict has at least: id, type, displayName.
        """
        items_map = self._load_existing_items()
        return list(items_map.values())

    # ------------------------------------------------------------------
    # Delete helpers
    # ------------------------------------------------------------------

    def delete_item(self, item_id: str, item_type: str, display_name: str) -> bool:
        """Delete a single item from the workspace by its ID.

        Returns True if deleted (or already gone), False on error.
        """
        if self.dry_run:
            logger.info("dry_run_delete", name=display_name, item_type=item_type)
            return True

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/items/{item_id}"
        resp = self._api_call("DELETE", url)
        if resp.status_code in (200, 204):
            logger.info("item_deleted", name=display_name, item_type=item_type)
            return True
        if resp.status_code == 404:
            logger.info("item_already_gone", name=display_name, item_type=item_type)
            return True
        logger.error(
            "delete_failed",
            name=display_name,
            status=resp.status_code,
            body=resp.text[:300],
        )
        return False

    def delete_all_items(
        self,
        *,
        item_types: list[str] | None = None,
    ) -> tuple[int, int]:
        """Delete all items in the workspace (or only those of given types).

        Args:
            item_types: If provided, only delete items whose ``type`` is in this
                list (e.g. ``["DataPipeline", "Notebook", "Dataflow"]``).

        Returns:
            (deleted_count, error_count)
        """
        items = self._load_existing_items()
        deleted = 0
        errors = 0

        for _key, item in sorted(items.items()):
            if item_types and item["type"] not in item_types:
                continue
            ok = self.delete_item(item["id"], item["type"], item["displayName"])
            if ok:
                deleted += 1
            else:
                errors += 1

        # Invalidate cache so next deploy sees a clean workspace
        self._existing_items = None
        logger.info("workspace_cleaned", deleted=deleted, errors=errors)
        return deleted, errors

    # ------------------------------------------------------------------
    # Workspace folders
    # ------------------------------------------------------------------

    def _list_folders(self) -> dict[str, str]:
        """Return ``{display_name: folder_id}`` for all folders in the workspace."""
        folders: dict[str, str] = {}
        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/folders"
        while url:
            resp = self._api_call("GET", url)
            if resp.status_code != 200:
                logger.warning("list_folders_error", status=resp.status_code, body=resp.text[:300])
                break
            data = resp.json()
            for f in data.get("value", []):
                folders[f["displayName"]] = f["id"]
            url = data.get("continuationUri")
        return folders

    def _ensure_folder(self, folder_name: str, existing: dict[str, str]) -> str | None:
        """Return the folder ID for *folder_name*, creating it if necessary.

        *existing* is the ``{name: id}`` map from ``_list_folders()``.
        Returns ``None`` only on dry-run or API failure.
        """
        if folder_name in existing:
            return existing[folder_name]

        if self.dry_run:
            logger.info("folder_dry_run", name=folder_name)
            return None

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/folders"
        body = {"displayName": folder_name}
        resp = self._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201):
            folder_id = resp.json().get("id", "")
            existing[folder_name] = folder_id
            logger.info("folder_created", name=folder_name, folder_id=folder_id)
            return folder_id

        logger.warning(
            "folder_create_failed",
            name=folder_name,
            status=resp.status_code,
            detail=resp.text[:300],
        )
        return None

    def _precreate_folders(self, output_dir: Path) -> None:
        """Pre-create workspace folders (one per SSIS package / pipeline).

        Populates ``_folder_map`` and ``_folder_names_sorted`` for use by
        ``_move_result_to_folder`` and ``_move_remaining_to_folders``.
        """
        pipelines_dir = output_dir / "pipelines"
        if not pipelines_dir.exists():
            return

        pipeline_names = sorted(pf.stem for pf in pipelines_dir.glob("*.json"))
        if not pipeline_names:
            return

        if self.dry_run:
            logger.info("phase05_folders_dry_run", count=len(pipeline_names))
            return

        logger.info("phase05_precreating_folders", count=len(pipeline_names))
        existing = self._list_folders()

        for name in pipeline_names:
            folder_id = self._ensure_folder(name, existing)
            if folder_id:
                self._folder_map[name] = folder_id

        self._folder_names_sorted = sorted(
            self._folder_map.keys(), key=len, reverse=True
        )
        logger.info("phase05_folders_ready", count=len(self._folder_map))

    def _folder_for_item(self, display_name: str) -> str | None:
        """Return the folder ID for an item, matching by pipeline-name prefix.

        Uses longest-prefix-first ordering to avoid collisions like
        ``StgProduct`` matching before ``StgProductCategory``.
        """
        for pkg_name in self._folder_names_sorted:
            if display_name == pkg_name or display_name.startswith(f"{pkg_name}_"):
                return self._folder_map[pkg_name]
        return None

    def _move_result_to_folder(self, result: DeploymentResult) -> None:
        """Move a just-deployed item to its folder (if item_id is available)."""
        if self.dry_run or result.status != "success" or not result.item_id:
            return
        folder_id = self._folder_for_item(result.name)
        if folder_id:
            self._move_item_to_folder(result.item_id, folder_id, result.name)

    def _move_remaining_to_folders(self) -> None:
        """Move any workspace items not yet in their correct folder.

        This is a catch-all for items created via async (202) responses
        where ``item_id`` was not available at creation time.
        """
        if not self._folder_map or self.dry_run:
            return

        self._existing_items = None
        items = self._load_existing_items()
        moved = 0

        for _key, item in items.items():
            if item.get("folderId"):
                continue
            folder_id = self._folder_for_item(item["displayName"])
            if folder_id:
                self._move_item_to_folder(item["id"], folder_id, item["displayName"])
                moved += 1

        if moved:
            logger.info("remaining_items_moved", count=moved)

    def _move_item_to_folder(self, item_id: str, folder_id: str, display_name: str) -> bool:
        """Move a workspace item into a folder.  Returns True on success."""
        if self.dry_run:
            logger.info("move_dry_run", name=display_name, folder_id=folder_id)
            return True

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/items/{item_id}/move"
        body = {"targetFolderId": folder_id}
        resp = self._api_call("POST", url, json_body=body)
        if resp.status_code in (200, 201):
            logger.info("item_moved", name=display_name, folder_id=folder_id)
            return True
        logger.warning(
            "item_move_failed",
            name=display_name,
            status=resp.status_code,
            detail=resp.text[:300],
        )
        return False

    def _organize_into_folders(self, output_dir: Path) -> None:
        """Organize deployed items into workspace folders (one per source package).

        Convenience wrapper that pre-creates folders (if not already done by
        deploy_all) and moves any items not yet in their correct folder.
        """
        if not self._folder_map:
            self._precreate_folders(output_dir)
        self._move_remaining_to_folders()

    # ------------------------------------------------------------------
    # Format helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _py_to_ipynb(py_content: str) -> dict:
        """Convert a Python script into a Jupyter ipynb notebook JSON structure.

        Splits on `# --- <section> ---` markers into separate code cells.
        """
        # Split content into logical cells based on section markers
        lines = py_content.split("\n")
        cells: list[dict] = []
        current_cell: list[str] = []

        for line in lines:
            # Split on section markers like "# --- Imports ---"
            if line.startswith("# ---") and line.rstrip().endswith("---") and current_cell:
                # Flush current cell
                cells.append(
                    {
                        "cell_type": "code",
                        "source": [line + "\n" for line in current_cell],
                        "execution_count": None,
                        "outputs": [],
                        "metadata": {},
                    }
                )
                current_cell = [line]
            else:
                current_cell.append(line)

        # Flush remaining lines
        if current_cell:
            cells.append(
                {
                    "cell_type": "code",
                    "source": [line + "\n" for line in current_cell],
                    "execution_count": None,
                    "outputs": [],
                    "metadata": {},
                }
            )

        # If no cells were produced, create one cell with everything
        if not cells:
            cells = [
                {
                    "cell_type": "code",
                    "source": [py_content],
                    "execution_count": None,
                    "outputs": [],
                    "metadata": {},
                }
            ]

        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "cells": cells,
            "metadata": {
                "language_info": {"name": "python"},
                "kernel_info": {"name": "synapse_pyspark"},
            },
        }

    # ------------------------------------------------------------------
    # Deploy pipelines
    # ------------------------------------------------------------------

    def _create_empty_pipeline(self, display_name: str) -> DeploymentResult:
        """Create an empty pipeline shell (no definition).

        If the pipeline already exists, returns success (reuse existing shell).
        """
        log = logger.bind(pipeline=display_name)

        if self._item_exists("DataPipeline", display_name):
            log.info("pipeline_shell_already_exists", pipeline=display_name)
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success")

        body = {
            "displayName": display_name,
            "description": f"Migrated from SSIS - {display_name}",
        }

        if self.dry_run:
            log.info("pipeline_shell_dry_run")
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success")

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/dataPipelines"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code == 201:
            item_id = resp.json().get("id")
            log.info("pipeline_shell_created", item_id=item_id)
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success", item_id=item_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            op_url = resp.headers.get("Location", "")
            ok, err_msg = self._wait_for_operation(op_url, op_id)
            if ok:
                return DeploymentResult(name=display_name, item_type="DataPipeline", status="success")
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="error", error=err_msg)
        else:
            return DeploymentResult(
                name=display_name,
                item_type="DataPipeline",
                status="error",
                error=f"HTTP {resp.status_code}: {resp.text[:500]}",
            )

    def _update_pipeline_definition(self, display_name: str, json_path: Path) -> DeploymentResult:
        """Update an existing pipeline's definition.

        Resolves TridentNotebook and Dataflow references by looking up
        item names in the workspace items.
        """
        log = logger.bind(pipeline=display_name)

        # Find the item ID
        if self._existing_items is None:
            self._existing_items = self._load_existing_items()
        item_key = f"DataPipeline::{display_name}"
        item = self._existing_items.get(item_key)
        if not item:
            return DeploymentResult(
                name=display_name,
                item_type="DataPipeline",
                status="error",
                error="Pipeline not found in workspace for definition update",
            )

        pipeline_id = item["id"]
        raw_content = json_path.read_text(encoding="utf-8")
        pipeline_json = json.loads(raw_content)
        properties = pipeline_json.get("properties", pipeline_json)

        # Resolve Trident references to actual item IDs
        for act in properties.get("activities", []):
            act_name = act.get("name", "")
            act_type = act.get("type", "")

            if act_type == "TridentNotebook":
                # Look up notebook by matching the sanitized package_task name.
                # Try exact match first, then search by suffix since notebooks
                # are named {package}_{task} while the pipeline activity uses
                # only the task name.
                notebook_key = f"Notebook::{act_name}"
                nb_item = self._existing_items.get(notebook_key)
                if not nb_item:
                    for key, val in self._existing_items.items():
                        if key.startswith("Notebook::") and key.endswith(act_name):
                            nb_item = val
                            break
                if nb_item:
                    act["typeProperties"]["notebookId"] = nb_item["id"]
                    act["typeProperties"]["workspaceId"] = self.workspace_id
                    log.info("resolved_notebook_id", activity=act_name, notebook_id=nb_item["id"])
                else:
                    log.warning("notebook_not_found", activity=act_name, searched_key=notebook_key)

            elif act_type == "Dataflow":
                # Look up dataflow — name convention: {package}_{task}
                # Try exact match first, then search by activity name suffix
                dataflow_key = f"Dataflow::{act_name}"
                df_item = self._existing_items.get(dataflow_key)
                if not df_item:
                    # Search for a dataflow that ends with the activity name
                    for key, val in self._existing_items.items():
                        if key.startswith("Dataflow::") and key.endswith(act_name):
                            df_item = val
                            break
                if df_item:
                    act["typeProperties"]["dataflowId"] = df_item["id"]
                    act["typeProperties"]["workspaceId"] = self.workspace_id
                    log.info("resolved_dataflow_id", activity=act_name, dataflow_id=df_item["id"])
                else:
                    log.warning("dataflow_not_found", activity=act_name)

            elif act_type in ("ExecutePipeline", "InvokePipeline"):
                # Fabric Data Factory uses the ExecutePipeline activity type.
                # Convert any legacy InvokePipeline to ExecutePipeline.
                if act_type == "InvokePipeline":
                    act["type"] = "ExecutePipeline"

                # Resolve pipeline reference to workspace item ID
                tp = act.get("typeProperties", {})
                pipe_ref = tp.get("pipeline", {})
                ref_name = pipe_ref.get("referenceName", "")
                if ref_name:
                    pipe_key = f"DataPipeline::{ref_name}"
                    pipe_item = self._existing_items.get(pipe_key)
                    if pipe_item:
                        pipe_ref["referenceName"] = pipe_item["id"]
                        log.info(
                            "resolved_pipeline_ref",
                            activity=act_name,
                            ref=ref_name,
                            pipeline_id=pipe_item["id"],
                        )
                    else:
                        log.warning(
                            "pipeline_ref_not_found",
                            activity=act_name,
                            ref=ref_name,
                            searched_key=pipe_key,
                        )
                # Remove fields not expected by the Fabric API
                tp.pop("pipelineId", None)
                tp.pop("workspaceId", None)
                act.pop("externalReferences", None)

        # Remove properties-level externalReferences — not needed for
        # ExecutePipeline activities (only InvokePipeline required them).
        properties.pop("externalReferences", None)

        # Resolve SQL-activity connection references.
        # Handles both Script and SqlServerStoredProcedure activity types.
        # SqlServerStoredProcedure is ADF-only; Fabric uses Script for
        # everything, so we convert SP activities to Script on the fly.
        #
        # Fabric connection format:  The Script activity expects
        # ``externalReferences.connection`` at the activity level (sibling
        # of ``typeProperties``).  The legacy ``typeProperties.connection``
        # (ADF-style) is also migrated to ``externalReferences`` here.
        #
        # Connection resolution uses ``_resolve_connection_for_activity``
        # which checks the ``_connection_map`` (Phase 0) first, then falls
        # back to ``default_connection_id`` (CLI ``-c``).
        for act in properties.get("activities", []):
            act_type = act.get("type", "")

            # Convert ADF-only SqlServerStoredProcedure → Fabric Script
            if act_type == "SqlServerStoredProcedure":
                sp_name = act.get("typeProperties", {}).get("storedProcedureName", "")
                act["type"] = "Script"
                act["typeProperties"] = {
                    "scripts": [{"type": "NonQuery", "text": f"EXEC {sp_name}"}],
                    "scriptBlockExecutionTimeout": "02:00:00",
                }
                act_type = "Script"
                log.info(
                    "converted_sp_to_script",
                    activity=act.get("name"),
                    stored_procedure=sp_name,
                )

            if act_type == "Script":
                tp = act.get("typeProperties", {})
                ext_refs = act.get("externalReferences", {})
                ext_conn = ext_refs.get("connection", "")

                # Migrate legacy typeProperties.connection to externalReferences
                legacy_conn = tp.pop("connection", None)
                if not ext_conn and legacy_conn:
                    ref = legacy_conn.get("referenceName", "") if isinstance(legacy_conn, dict) else str(legacy_conn)
                    ext_conn = ref

                # Extract SSIS connection name from TODO placeholder
                # Format: "cmgr_DW  -- TODO: replace with Fabric connection id"
                ssis_name_hint = ""
                if ext_conn and "TODO" in ext_conn:
                    ssis_name_hint = ext_conn.split("--")[0].strip()
                elif ext_conn and not self._looks_like_guid(ext_conn):
                    ssis_name_hint = ext_conn.strip()

                if ext_conn:
                    is_todo = "TODO" in ext_conn
                    is_ssis_name = ext_conn and not is_todo and not self._looks_like_guid(ext_conn)
                    if is_todo or is_ssis_name:
                        resolved = self._resolve_connection_for_activity(ssis_name_hint)
                        if resolved:
                            act["externalReferences"] = {"connection": resolved}
                            log.info(
                                "resolved_script_connection",
                                activity=act.get("name"),
                                connection_id=resolved,
                                ssis_name=ssis_name_hint or None,
                            )
                        elif is_todo:
                            act.pop("externalReferences", None)
                    else:
                        # Already a valid GUID — keep it
                        act["externalReferences"] = {"connection": ext_conn}
                else:
                    # No connection at all — inject from map or default
                    resolved = self._resolve_connection_for_activity("")
                    if resolved:
                        act["externalReferences"] = {"connection": resolved}
                        log.info(
                            "injected_script_connection",
                            activity=act.get("name"),
                        connection_id=self.default_connection_id,
                    )

        # Auto-add missing pipeline parameters.  ExecutePipeline activities
        # may reference parameters (e.g. @pipeline().parameters.LoadExecutionId)
        # that the generator didn't add to the parent pipeline's parameter list
        # (happens when SSIS namespace-qualified variable names get stripped).
        import re as _re

        param_refs: set[str] = set()
        _param_pattern = _re.compile(r"@pipeline\(\)\.parameters\.(\w+)")
        for act in properties.get("activities", []):
            tp = act.get("typeProperties", {})
            for _key, val in (tp.get("parameters") or {}).items():
                if isinstance(val, dict) and val.get("type") == "Expression":
                    for m in _param_pattern.finditer(val.get("value", "")):
                        param_refs.add(m.group(1))
        existing_params = properties.setdefault("parameters", {})
        for pname in param_refs:
            if pname not in existing_params:
                existing_params[pname] = {"type": "String", "defaultValue": ""}
                log.info("auto_added_missing_parameter", parameter=pname)

        pipeline_content = {"properties": properties}
        pipeline_payload = json.dumps(pipeline_content, indent=2)
        payload_b64 = base64.b64encode(pipeline_payload.encode("utf-8")).decode("ascii")

        body = {
            "definition": {
                "parts": [
                    {
                        "path": "pipeline-content.json",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        if self.dry_run:
            log.info("pipeline_update_dry_run", pipeline_id=pipeline_id)
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success", item_id=pipeline_id)

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/dataPipelines/{pipeline_id}/updateDefinition"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code in (200, 201):
            log.info("pipeline_definition_updated", pipeline_id=pipeline_id)
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success", item_id=pipeline_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            ok, err_msg = self._wait_for_operation("", op_id)
            if ok:
                log.info("pipeline_definition_updated_async", pipeline_id=pipeline_id)
                return DeploymentResult(
                    name=display_name, item_type="DataPipeline", status="success", item_id=pipeline_id
                )
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="error", error=err_msg)
        else:
            error_msg = resp.text[:500]
            log.warning("pipeline_definition_update_failed", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="DataPipeline",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    def deploy_pipeline(self, json_path: Path) -> DeploymentResult:
        """Deploy a single Data Factory pipeline from a JSON file.

        If the pipeline already exists and skip_existing is False, updates
        the definition in-place.
        """
        display_name = json_path.stem  # filename without .json
        log = logger.bind(pipeline=display_name)

        already_exists = self._item_exists("DataPipeline", display_name)

        if self.skip_existing and already_exists and not self.default_connection_id:
            log.info("pipeline_skipped_exists")
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="skipped")

        # If it already exists (or skip_existing but we have a connection to
        # inject), update the definition in-place.
        if already_exists:
            log.info("pipeline_exists_updating", pipeline=display_name)
            return self._update_pipeline_definition(display_name, json_path)

        # Read the pipeline JSON content
        raw_content = json_path.read_text(encoding="utf-8")

        # The Fabric API expects pipeline-content.json with the full
        # {"properties": {"activities": [...]}} structure.
        pipeline_json = json.loads(raw_content)
        # Build the payload: keep "properties" wrapper, strip "name" if present
        pipeline_content = {"properties": pipeline_json.get("properties", pipeline_json)}
        pipeline_payload = json.dumps(pipeline_content, indent=2)
        payload_b64 = base64.b64encode(pipeline_payload.encode("utf-8")).decode("ascii")

        body = {
            "displayName": display_name,
            "description": f"Migrated from SSIS - {display_name}",
            "definition": {
                "parts": [
                    {
                        "path": "pipeline-content.json",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            },
        }

        if self.dry_run:
            log.info("pipeline_dry_run", body_keys=list(body.keys()))
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success")

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/dataPipelines"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code == 201:
            item_id = resp.json().get("id")
            log.info("pipeline_created", item_id=item_id)
            return DeploymentResult(name=display_name, item_type="DataPipeline", status="success", item_id=item_id)
        elif resp.status_code == 202:
            # Long-running operation
            op_id = resp.headers.get("x-ms-operation-id", "")
            op_url = resp.headers.get("Location", "")
            log.info("pipeline_provisioning", operation_id=op_id)
            ok, err_msg = self._wait_for_operation(op_url, op_id)
            if ok:
                log.info("pipeline_created_async", operation_id=op_id)
                return DeploymentResult(name=display_name, item_type="DataPipeline", status="success")
            else:
                return DeploymentResult(
                    name=display_name,
                    item_type="DataPipeline",
                    status="error",
                    error=err_msg,
                )
        else:
            error_msg = resp.text[:500]
            log.error("pipeline_create_error", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="DataPipeline",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    # ------------------------------------------------------------------
    # Deploy notebooks
    # ------------------------------------------------------------------

    def _update_notebook_definition(self, display_name: str, py_path: Path) -> DeploymentResult:
        """Update an existing notebook's definition in-place."""
        log = logger.bind(notebook=display_name)

        if self._existing_items is None:
            self._existing_items = self._load_existing_items()
        item = self._existing_items.get(f"Notebook::{display_name}")
        if not item:
            return DeploymentResult(
                name=display_name,
                item_type="Notebook",
                status="error",
                error="Notebook not found in workspace for definition update",
            )

        notebook_id = item["id"]
        py_content = py_path.read_text(encoding="utf-8")
        notebook_payload = self._py_to_ipynb(py_content)
        payload_b64 = base64.b64encode(json.dumps(notebook_payload, indent=2).encode("utf-8")).decode("ascii")

        body = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            }
        }

        if self.dry_run:
            log.info("notebook_update_dry_run", notebook_id=notebook_id)
            return DeploymentResult(name=display_name, item_type="Notebook", status="success", item_id=notebook_id)

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code in (200, 201):
            log.info("notebook_definition_updated", notebook_id=notebook_id)
            return DeploymentResult(name=display_name, item_type="Notebook", status="success", item_id=notebook_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            ok, err_msg = self._wait_for_operation("", op_id)
            if ok:
                log.info("notebook_definition_updated_async", notebook_id=notebook_id)
                return DeploymentResult(name=display_name, item_type="Notebook", status="success", item_id=notebook_id)
            return DeploymentResult(name=display_name, item_type="Notebook", status="error", error=err_msg)
        else:
            error_msg = resp.text[:500]
            log.warning("notebook_definition_update_failed", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="Notebook",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    def _update_dataflow_definition(self, display_name: str, json_path: Path) -> DeploymentResult:
        """Update an existing dataflow's definition in-place."""
        log = logger.bind(dataflow=display_name)

        if self._existing_items is None:
            self._existing_items = self._load_existing_items()
        item = self._existing_items.get(f"Dataflow::{display_name}")
        if not item:
            return DeploymentResult(
                name=display_name,
                item_type="Dataflow",
                status="error",
                error="Dataflow not found in workspace for definition update",
            )

        dataflow_id = item["id"]
        raw = json.loads(json_path.read_text(encoding="utf-8"))

        query_metadata = json.dumps(raw["queryMetadata"], indent=2)
        mashup = raw["mashup"]

        parts = [
            {
                "path": "queryMetadata.json",
                "payload": base64.b64encode(query_metadata.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
            {
                "path": "mashup.pq",
                "payload": base64.b64encode(mashup.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
        ]

        body = {"definition": {"parts": parts}}

        if self.dry_run:
            log.info("dataflow_update_dry_run", dataflow_id=dataflow_id)
            return DeploymentResult(name=display_name, item_type="Dataflow", status="success", item_id=dataflow_id)

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/dataflows/{dataflow_id}/updateDefinition"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code in (200, 201):
            log.info("dataflow_definition_updated", dataflow_id=dataflow_id)
            return DeploymentResult(name=display_name, item_type="Dataflow", status="success", item_id=dataflow_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            ok, err_msg = self._wait_for_operation("", op_id)
            if ok:
                log.info("dataflow_definition_updated_async", dataflow_id=dataflow_id)
                return DeploymentResult(name=display_name, item_type="Dataflow", status="success", item_id=dataflow_id)
            return DeploymentResult(name=display_name, item_type="Dataflow", status="error", error=err_msg)
        else:
            error_msg = resp.text[:500]
            log.warning("dataflow_definition_update_failed", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="Dataflow",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    def deploy_notebook(self, py_path: Path) -> DeploymentResult:
        """Deploy a single Spark notebook from a .py file.

        If the notebook already exists and skip_existing is False, updates it.
        """
        display_name = py_path.stem  # filename without .py
        log = logger.bind(notebook=display_name)

        already_exists = self._item_exists("Notebook", display_name)

        if self.skip_existing and already_exists:
            log.info("notebook_skipped_exists")
            return DeploymentResult(name=display_name, item_type="Notebook", status="skipped")

        # If it already exists and we aren't skipping, update in-place
        if already_exists:
            log.info("notebook_exists_updating", notebook=display_name)
            return self._update_notebook_definition(display_name, py_path)

        # Read the .py content and convert to ipynb notebook JSON
        py_content = py_path.read_text(encoding="utf-8")
        notebook_payload = self._py_to_ipynb(py_content)
        payload_b64 = base64.b64encode(json.dumps(notebook_payload, indent=2).encode("utf-8")).decode("ascii")

        body = {
            "displayName": display_name,
            "description": f"Migrated from SSIS - {display_name}",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        if self.dry_run:
            log.info("notebook_dry_run", body_keys=list(body.keys()))
            return DeploymentResult(name=display_name, item_type="Notebook", status="success")

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/notebooks"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code == 201:
            item_id = resp.json().get("id")
            log.info("notebook_created", item_id=item_id)
            return DeploymentResult(name=display_name, item_type="Notebook", status="success", item_id=item_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            op_url = resp.headers.get("Location", "")
            log.info("notebook_provisioning", operation_id=op_id)
            ok, err_msg = self._wait_for_operation(op_url, op_id)
            if ok:
                log.info("notebook_created_async", operation_id=op_id)
                return DeploymentResult(name=display_name, item_type="Notebook", status="success")
            else:
                return DeploymentResult(
                    name=display_name,
                    item_type="Notebook",
                    status="error",
                    error=err_msg,
                )
        else:
            error_msg = resp.text[:500]
            log.error("notebook_create_error", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="Notebook",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    # ------------------------------------------------------------------
    # Deploy dataflows
    # ------------------------------------------------------------------

    def deploy_dataflow(self, json_path: Path) -> DeploymentResult:
        """Deploy a single Dataflow Gen2 from a definition JSON file.

        The JSON file contains:
          - name: display name
          - queryMetadata: the queryMetadata.json content
          - mashup: the Power Query M code (mashup.pq content)

        If the dataflow already exists and skip_existing is False, updates it.
        """
        raw = json.loads(json_path.read_text(encoding="utf-8"))
        display_name = raw.get("name", json_path.stem)
        log = logger.bind(dataflow=display_name)

        already_exists = self._item_exists("Dataflow", display_name)

        if self.skip_existing and already_exists:
            log.info("dataflow_skipped_exists")
            return DeploymentResult(name=display_name, item_type="Dataflow", status="skipped")

        # If it already exists and we aren't skipping, update in-place
        if already_exists:
            log.info("dataflow_exists_updating", dataflow=display_name)
            return self._update_dataflow_definition(display_name, json_path)

        # Build definition parts
        query_metadata = json.dumps(raw["queryMetadata"], indent=2)
        mashup = raw["mashup"]
        platform = json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {"type": "Dataflow", "displayName": display_name},
                "config": {"version": "2.0", "logicalId": str(__import__("uuid").uuid4())},
            },
            indent=2,
        )

        parts = [
            {
                "path": "queryMetadata.json",
                "payload": base64.b64encode(query_metadata.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
            {
                "path": "mashup.pq",
                "payload": base64.b64encode(mashup.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
            {
                "path": ".platform",
                "payload": base64.b64encode(platform.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
        ]

        body = {
            "displayName": display_name,
            "description": raw.get("description", f"Migrated from SSIS - {display_name}"),
            "definition": {"parts": parts},
        }

        if self.dry_run:
            log.info("dataflow_dry_run", body_keys=list(body.keys()))
            return DeploymentResult(name=display_name, item_type="Dataflow", status="success")

        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/dataflows"
        resp = self._api_call("POST", url, json_body=body)

        if resp.status_code == 201:
            item_id = resp.json().get("id")
            log.info("dataflow_created", item_id=item_id)
            return DeploymentResult(name=display_name, item_type="Dataflow", status="success", item_id=item_id)
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            op_url = resp.headers.get("Location", "")
            log.info("dataflow_provisioning", operation_id=op_id)
            ok, err_msg = self._wait_for_operation(op_url, op_id)
            if ok:
                log.info("dataflow_created_async", operation_id=op_id)
                return DeploymentResult(name=display_name, item_type="Dataflow", status="success")
            return DeploymentResult(name=display_name, item_type="Dataflow", status="error", error=err_msg)
        else:
            error_msg = resp.text[:500]
            log.error("dataflow_create_error", status=resp.status_code, error=error_msg)
            return DeploymentResult(
                name=display_name,
                item_type="Dataflow",
                status="error",
                error=f"HTTP {resp.status_code}: {error_msg}",
            )

    # ------------------------------------------------------------------
    # Deploy all
    # ------------------------------------------------------------------

    @staticmethod
    def _has_copy_activity(pipeline_json: dict) -> bool:
        """Check if a pipeline contains Copy activities (need manual connection config)."""
        return any(act.get("type") == "Copy" for act in pipeline_json.get("properties", {}).get("activities", []))

    @staticmethod
    def _has_script_activity(pipeline_json: dict) -> bool:
        """Check if a pipeline contains Script or SqlServerStoredProcedure activities."""
        sql_types = {"Script", "SqlServerStoredProcedure"}
        return any(
            act.get("type") in sql_types
            for act in pipeline_json.get("properties", {}).get("activities", [])
        )

    @staticmethod
    def _has_cross_references(pipeline_json: dict) -> bool:
        """Check if pipeline references other pipelines, notebooks, or dataflows."""
        for act in pipeline_json.get("properties", {}).get("activities", []):
            atype = act.get("type", "")
            if atype in ("ExecutePipeline", "InvokePipeline", "TridentNotebook", "Dataflow"):
                return True
        return False

    def deploy_all(self, output_dir: Path) -> DeploymentReport:
        """
        Deploy all pipelines, dataflows, and notebooks from a migration output.

        Multi-phase deployment strategy:
          Phase 0:  Resolve connections (auto-discover/create) → build map
          Phase 1a: Deploy Dataflow Gen2 items
          Phase 1b: Deploy Spark notebooks
          Phase 1c: Deploy leaf pipelines (no cross-references, no Copy)
          Phase 1d: Deploy Copy-activity pipelines as empty shells
          Phase 2:  Create empty shells for referencing pipelines
          Phase 3:  Update definitions for referencing pipelines
                    (resolves Dataflow, TridentNotebook, ExecutePipeline refs)

        Expects:
          output_dir/connections/*.json  (optional — connection manifests)
          output_dir/dataflows/*.json
          output_dir/pipelines/*.json
          output_dir/notebooks/*.py
        """
        report = DeploymentReport(workspace_id=self.workspace_id)

        # --- Phase 0: Resolve connections ---
        self._resolve_connections(output_dir)

        # --- Phase 0.5: Pre-create workspace folders ---
        self._precreate_folders(output_dir)

        # --- Phase 1a: Dataflows first ---
        dataflows_dir = output_dir / "dataflows"
        if dataflows_dir.exists():
            dataflow_files = sorted(
                f for f in dataflows_dir.glob("*.json") if not f.name.endswith(".destinations.json")
            )
            logger.info("phase1a_deploying_dataflows", count=len(dataflow_files))
            for i, df in enumerate(dataflow_files, 1):
                logger.info("deploying_dataflow", index=i, total=len(dataflow_files), file=df.name)
                result = self.deploy_dataflow(df)
                report.results.append(result)
                self._move_result_to_folder(result)
                if not self.dry_run:
                    time.sleep(1)
        else:
            logger.info("no_dataflows_dir", path=str(dataflows_dir))

        # --- Phase 1b: Notebooks ---
        notebooks_dir = output_dir / "notebooks"
        if notebooks_dir.exists():
            notebook_files = sorted(notebooks_dir.glob("*.py"))
            logger.info("phase1b_deploying_notebooks", count=len(notebook_files))
            for i, nf in enumerate(notebook_files, 1):
                logger.info("deploying_notebook", index=i, total=len(notebook_files), file=nf.name)
                result = self.deploy_notebook(nf)
                report.results.append(result)
                self._move_result_to_folder(result)
                if not self.dry_run:
                    time.sleep(1)
        else:
            logger.warning("no_notebooks_dir", path=str(notebooks_dir))

        # --- Classify pipelines ---
        pipelines_dir = output_dir / "pipelines"
        if not pipelines_dir.exists():
            logger.warning("no_pipelines_dir", path=str(pipelines_dir))
            return report

        pipeline_files = sorted(pipelines_dir.glob("*.json"))
        leaf_pipelines: list[Path] = []
        copy_pipelines: list[Path] = []
        ref_pipelines: list[Path] = []

        for pf in pipeline_files:
            data = json.loads(pf.read_text(encoding="utf-8"))
            if self._has_cross_references(data):
                ref_pipelines.append(pf)
            elif self._has_copy_activity(data):
                copy_pipelines.append(pf)
            else:
                leaf_pipelines.append(pf)

        logger.info(
            "pipeline_classification",
            leaf=len(leaf_pipelines),
            copy=len(copy_pipelines),
            referencing=len(ref_pipelines),
        )

        # --- Phase 1c: Deploy leaf pipelines (with full definition) ---
        logger.info("phase1c_deploying_leaf_pipelines", count=len(leaf_pipelines))
        for i, pf in enumerate(leaf_pipelines, 1):
            logger.info("deploying_pipeline", index=i, total=len(leaf_pipelines), file=pf.name, tier="leaf")
            result = self.deploy_pipeline(pf)
            # Flag Script-activity pipelines that need Fabric connection setup
            # (skip warning when default_connection_id was used — connections injected)
            data = json.loads(pf.read_text(encoding="utf-8"))
            if (
                result.status == "success"
                and self._has_script_activity(data)
                and not self.default_connection_id
                and not self._connection_map
            ):
                result.error = (
                    "NEEDS CONNECTION: This pipeline contains Script activities that "
                    "require a Fabric Warehouse or SQL analytics endpoint connection. "
                    "Open the pipeline in Fabric and configure the connection for each "
                    "Script activity."
                )
            report.results.append(result)
            self._move_result_to_folder(result)
            if not self.dry_run:
                time.sleep(1)

        # --- Phase 1d: Deploy Copy-activity pipelines as empty shells ---
        logger.info("phase1d_deploying_copy_pipelines_as_shells", count=len(copy_pipelines))
        for i, pf in enumerate(copy_pipelines, 1):
            name = pf.stem
            logger.info(
                "deploying_copy_pipeline_shell",
                index=i,
                total=len(copy_pipelines),
                name=name,
            )
            result = self._create_empty_pipeline(name)
            if result.status == "success":
                result.error = (
                    "NEEDS CONFIGURATION: This pipeline contains Copy activities that "
                    "require Fabric connection setup via the UI. Open the pipeline in "
                    "Fabric and configure source/destination connections."
                )
            report.results.append(result)
            self._move_result_to_folder(result)
            if not self.dry_run:
                time.sleep(1)

        # --- Phase 2: Create empty shells for all referencing pipelines ---
        logger.info("phase2_creating_pipeline_shells", count=len(ref_pipelines))
        shell_results: dict[str, DeploymentResult] = {}
        for i, pf in enumerate(ref_pipelines, 1):
            name = pf.stem
            logger.info("creating_pipeline_shell", index=i, total=len(ref_pipelines), name=name)
            result = self._create_empty_pipeline(name)
            shell_results[name] = result
            self._move_result_to_folder(result)
            if not self.dry_run:
                time.sleep(1)

        # Refresh the existing items cache after creating all shells
        self._existing_items = None
        if not self.dry_run:
            self._existing_items = self._load_existing_items()

        # --- Phase 3: Update definitions for referencing pipelines ---
        logger.info("phase3_updating_pipeline_definitions", count=len(ref_pipelines))
        for i, pf in enumerate(ref_pipelines, 1):
            name = pf.stem
            shell_res = shell_results.get(name)
            if shell_res and shell_res.status == "error":
                # Shell creation failed, skip definition update
                report.results.append(shell_res)
                continue

            logger.info("updating_pipeline_definition", index=i, total=len(ref_pipelines), name=name)
            result = self._update_pipeline_definition(name, pf)
            report.results.append(result)
            if not self.dry_run:
                time.sleep(1)

        # --- Phase 4: Move any remaining items to folders ---
        # Items created via async (202) responses may not have been moved
        # yet; this catch-all ensures everything is properly organized.
        if not self.dry_run:
            try:
                self._move_remaining_to_folders()
            except Exception as e:
                logger.warning("folder_organization_failed", error=str(e))

        logger.info(
            "deployment_complete",
            total=report.total,
            succeeded=report.succeeded,
            failed=report.failed,
            skipped=report.skipped,
        )
        return report
