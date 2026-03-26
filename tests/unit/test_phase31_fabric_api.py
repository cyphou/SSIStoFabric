"""
Phase 31 Tests — Fabric Native API Integration (v5.1.0)
=======================================================
Tests for OneLake shortcuts, Lakehouse provisioning, Refresh scheduling,
Semantic model deployment, and Workspace Git integration.
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# =====================================================================
# Mock deployer for all tests (no real HTTP calls)
# =====================================================================


class MockResponse:
    """Fake requests.Response."""

    def __init__(self, status_code: int = 200, body: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}
        self.text = json.dumps(self._body)

    def json(self) -> dict:
        return self._body


class MockDeployer:
    """Minimal FabricDeployer stub for unit tests."""

    def __init__(self, *, dry_run: bool = False, existing_items: dict | None = None):
        self.workspace_id = "ws-test-1234"
        self.dry_run = dry_run
        self._existing = existing_items or {}
        self._api_calls: list[tuple[str, str, dict | None]] = []
        self._next_responses: list[MockResponse] = []

    def queue_response(self, resp: MockResponse) -> None:
        self._next_responses.append(resp)

    def _api_call(self, method: str, url: str, *, json_body: dict | None = None, retries: int = 5) -> MockResponse:
        self._api_calls.append((method, url, json_body))
        if self._next_responses:
            return self._next_responses.pop(0)
        return MockResponse(200, {"id": "item-new-id"})

    def _load_existing_items(self) -> dict[str, str]:
        return dict(self._existing)

    def _wait_for_operation(self, operation_url: str, operation_id: str) -> tuple[bool, str]:
        return True, ""


# =====================================================================
# OneLake Shortcut Manager
# =====================================================================


class TestOneLakeShortcutManager:
    """Test shortcut creation for various cloud sources."""

    def _make_shortcut(self, source_type: str = "S3", name: str = "test_shortcut"):
        from ssis_to_fabric.engine.connectors import OneLakeShortcut
        return OneLakeShortcut(
            name=name,
            source_type=source_type,
            source_path="s3://my-bucket/data/" if source_type == "S3" else "gs://my-bucket/data/",
            target_lakehouse="lakehouse1",
            target_path=f"Tables/{name}",
        )

    @pytest.mark.unit
    def test_deploy_shortcut_success(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        deployer = MockDeployer()
        mgr = OneLakeShortcutManager(deployer)

        sc = self._make_shortcut("S3")
        result = mgr.deploy_shortcut(sc, "lh-123")

        assert result.status == "success"
        assert result.lakehouse_id == "lh-123"
        assert len(deployer._api_calls) == 1
        method, url, body = deployer._api_calls[0]
        assert method == "POST"
        assert "/shortcuts" in url
        assert body["name"] == "test_shortcut"

    @pytest.mark.unit
    def test_deploy_shortcut_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        deployer = MockDeployer(dry_run=True)
        mgr = OneLakeShortcutManager(deployer)

        sc = self._make_shortcut()
        result = mgr.deploy_shortcut(sc, "lh-123")
        assert result.status == "skipped"
        assert len(deployer._api_calls) == 0

    @pytest.mark.unit
    def test_deploy_shortcut_conflict_skipped(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(409, {"error": "conflict"}))
        mgr = OneLakeShortcutManager(deployer)

        sc = self._make_shortcut()
        result = mgr.deploy_shortcut(sc, "lh-123")
        assert result.status == "skipped"

    @pytest.mark.unit
    def test_deploy_shortcut_error(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(500, {"error": "internal"}))
        mgr = OneLakeShortcutManager(deployer)

        sc = self._make_shortcut()
        result = mgr.deploy_shortcut(sc, "lh-123")
        assert result.status == "error"
        assert result.error is not None

    @pytest.mark.unit
    def test_deploy_multiple_shortcuts(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        deployer = MockDeployer()
        mgr = OneLakeShortcutManager(deployer)

        shortcuts = [self._make_shortcut("S3", "sc1"), self._make_shortcut("GCS", "sc2")]
        results = mgr.deploy_shortcuts(shortcuts, "lh-123")
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

    @pytest.mark.unit
    def test_build_s3_shortcut_body(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        sc = self._make_shortcut("S3")
        body = OneLakeShortcutManager._build_shortcut_body(sc)
        assert "amazonS3" in body["target"]
        assert body["target"]["amazonS3"]["location"] == "s3://my-bucket/data/"

    @pytest.mark.unit
    def test_build_gcs_shortcut_body(self):
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        sc = self._make_shortcut("GCS")
        body = OneLakeShortcutManager._build_shortcut_body(sc)
        assert "googleCloudStorage" in body["target"]

    @pytest.mark.unit
    def test_build_adls_shortcut_body(self):
        from ssis_to_fabric.engine.connectors import OneLakeShortcut
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        sc = OneLakeShortcut(
            name="adls_sc",
            source_type="ADLS",
            source_path="https://storage.dfs.core.windows.net/container/path",
        )
        body = OneLakeShortcutManager._build_shortcut_body(sc)
        assert "adlsGen2" in body["target"]

    @pytest.mark.unit
    def test_build_onelake_shortcut_body(self):
        from ssis_to_fabric.engine.connectors import OneLakeShortcut
        from ssis_to_fabric.engine.fabric_api import OneLakeShortcutManager
        sc = OneLakeShortcut(
            name="onelake_sc",
            source_type="ONELAKE",
            source_path="Tables/shared_table",
            config={"workspaceId": "ws-other", "itemId": "lh-other"},
        )
        body = OneLakeShortcutManager._build_shortcut_body(sc)
        assert "oneLake" in body["target"]
        assert body["target"]["oneLake"]["workspaceId"] == "ws-other"


# =====================================================================
# Lakehouse Provisioner
# =====================================================================


class TestLakehouseProvisioner:
    """Test lakehouse creation and table provisioning."""

    @pytest.mark.unit
    def test_create_lakehouse_success(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(201, {"id": "lh-new-id"}))
        lp = LakehouseProvisioner(deployer)

        result = lp.create_lakehouse("my_lakehouse", description="Test LH")
        assert result.status == "success"
        assert result.item_id == "lh-new-id"
        # _load_existing_items does NOT go through _api_call, so [0] is POST
        _, _, body = deployer._api_calls[0]
        assert body["displayName"] == "my_lakehouse"

    @pytest.mark.unit
    def test_create_lakehouse_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer(dry_run=True)
        lp = LakehouseProvisioner(deployer)

        result = lp.create_lakehouse("my_lakehouse")
        assert result.status == "skipped"
        assert len(deployer._api_calls) == 0

    @pytest.mark.unit
    def test_create_lakehouse_already_exists(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer(existing_items={"Lakehouse::my_lakehouse": "lh-existing"})
        lp = LakehouseProvisioner(deployer)

        result = lp.create_lakehouse("my_lakehouse")
        assert result.status == "skipped"
        assert result.item_id == "lh-existing"

    @pytest.mark.unit
    def test_create_lakehouse_error(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer()
        # Only 1 response needed: the POST (existing items don't use _api_call)
        deployer.queue_response(MockResponse(400, {"error": "bad request"}))
        lp = LakehouseProvisioner(deployer)

        result = lp.create_lakehouse("bad_name!")
        assert result.status == "error"

    @pytest.mark.unit
    def test_provision_tables(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer()
        lp = LakehouseProvisioner(deployer)

        ddls = [
            "CREATE TABLE dim_customer (id INT, name STRING)",
            "CREATE TABLE fact_sales (id INT, amount DECIMAL)",
        ]
        results = lp.provision_tables("lh-123", ddls)
        assert len(results) == 2
        assert results[0]["table"] == "dim_customer"
        assert results[1]["table"] == "fact_sales"

    @pytest.mark.unit
    def test_provision_tables_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer(dry_run=True)
        lp = LakehouseProvisioner(deployer)

        results = lp.provision_tables("lh-123", ["CREATE TABLE t1 (x INT)"])
        assert results[0]["status"] == "skipped"

    @pytest.mark.unit
    def test_create_from_schema(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import LakehouseProvisioner
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(201, {"id": "lh-schema"}))
        lp = LakehouseProvisioner(deployer)

        ddl_file = tmp_path / "tables.sql"
        ddl_file.write_text("CREATE TABLE t1 (a INT);\nCREATE TABLE t2 (b STRING)")

        lh_result, table_results = lp.create_lakehouse_from_schema("schema_lh", ddl_file)
        assert lh_result.status == "success"
        assert len(table_results) == 2


# =====================================================================
# Refresh Scheduler
# =====================================================================


class TestRefreshScheduler:
    """Test scheduled trigger creation."""

    @pytest.mark.unit
    def test_schedule_success(self):
        from ssis_to_fabric.engine.fabric_api import RefreshScheduler, ScheduleConfig
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(201))
        scheduler = RefreshScheduler(deployer)

        schedule = ScheduleConfig(frequency="Daily", start_time="03:00")
        result = scheduler.schedule_item("item-1", "MyPipeline", schedule)
        assert result.status == "success"
        _, url, _ = deployer._api_calls[0]
        assert "/jobScheduler" in url

    @pytest.mark.unit
    def test_schedule_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import RefreshScheduler, ScheduleConfig
        deployer = MockDeployer(dry_run=True)
        scheduler = RefreshScheduler(deployer)

        result = scheduler.schedule_item("item-1", "MyPipeline", ScheduleConfig())
        assert result.status == "skipped"

    @pytest.mark.unit
    def test_schedule_conflict_updates(self):
        from ssis_to_fabric.engine.fabric_api import RefreshScheduler, ScheduleConfig
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(409))  # conflict
        deployer.queue_response(MockResponse(200))  # PATCH succeeds
        scheduler = RefreshScheduler(deployer)

        result = scheduler.schedule_item("item-1", "MyPipeline", ScheduleConfig())
        assert result.status == "success"
        assert len(deployer._api_calls) == 2
        assert deployer._api_calls[1][0] == "PATCH"

    @pytest.mark.unit
    def test_schedule_error(self):
        from ssis_to_fabric.engine.fabric_api import RefreshScheduler, ScheduleConfig
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(500, {"error": "server"}))
        scheduler = RefreshScheduler(deployer)

        result = scheduler.schedule_item("item-1", "MyPipeline", ScheduleConfig())
        assert result.status == "error"

    @pytest.mark.unit
    def test_schedule_batch(self):
        from ssis_to_fabric.engine.fabric_api import RefreshScheduler, ScheduleConfig
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(201))
        deployer.queue_response(MockResponse(201))
        scheduler = RefreshScheduler(deployer)

        items = [("item-1", "Pipeline1"), ("item-2", "Pipeline2")]
        results = scheduler.schedule_batch(items, ScheduleConfig())
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

    @pytest.mark.unit
    def test_schedule_config_to_trigger(self):
        from ssis_to_fabric.engine.fabric_api import ScheduleConfig
        cfg = ScheduleConfig(frequency="Weekly", start_time="05:30", days_of_week=["Monday", "Friday"])
        trigger = cfg.to_trigger_body()
        assert trigger["type"] == "ScheduleTrigger"
        assert trigger["typeProperties"]["recurrence"]["frequency"] == "Weekly"
        assert "weekDays" in trigger["typeProperties"]["recurrence"]["schedule"]

    @pytest.mark.unit
    def test_schedule_config_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import ScheduleConfig
        cfg = ScheduleConfig(frequency="Daily", interval=2, start_time="08:00")
        d = cfg.to_dict()
        assert d["frequency"] == "Daily"
        assert d["interval"] == 2
        assert d["startTime"] == "08:00"


# =====================================================================
# Semantic Model Deployer
# =====================================================================


class TestSemanticModelDeployer:
    """Test semantic model creation and update."""

    @pytest.mark.unit
    def test_deploy_new_model(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        deployer = MockDeployer()
        # Only 1 response: the POST (existing items don't use _api_call)
        deployer.queue_response(MockResponse(201, {"id": "sm-new-id"}))
        smd = SemanticModelDeployer(deployer)

        defn = tmp_path / "model.tmdl"
        defn.write_text("table Customer { column Name : string }")

        result = smd.deploy_model("MyModel", defn, description="Test")
        assert result.status == "success"
        assert result.item_id == "sm-new-id"

    @pytest.mark.unit
    def test_deploy_model_dry_run(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        deployer = MockDeployer(dry_run=True)
        smd = SemanticModelDeployer(deployer)

        defn = tmp_path / "model.tmdl"
        defn.write_text("content")

        result = smd.deploy_model("MyModel", defn)
        assert result.status == "skipped"

    @pytest.mark.unit
    def test_deploy_model_update_existing(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        deployer = MockDeployer(existing_items={"SemanticModel::MyModel": "sm-existing"})
        deployer.queue_response(MockResponse(200))  # update succeeds
        smd = SemanticModelDeployer(deployer)

        defn = tmp_path / "model.tmdl"
        defn.write_text("content")

        result = smd.deploy_model("MyModel", defn)
        assert result.status == "success"
        assert result.item_id == "sm-existing"

    @pytest.mark.unit
    def test_deploy_model_from_folder(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(201, {"id": "sm-folder"}))
        smd = SemanticModelDeployer(deployer)

        model_dir = tmp_path / "tmdl"
        model_dir.mkdir()
        (model_dir / "model.tmdl").write_text("model")
        (model_dir / "Customer.tmdl").write_text("table Customer")

        result = smd.deploy_model("FolderModel", model_dir)
        assert result.status == "success"
        # Verify parts were built from folder — [0] is POST (no _api_call for existing items)
        _, _, body = deployer._api_calls[0]
        assert len(body["definition"]["parts"]) == 2

    @pytest.mark.unit
    def test_build_definition_parts_file(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        f = tmp_path / "test.tmdl"
        f.write_text("hello")
        parts = SemanticModelDeployer._build_definition_parts(f)
        assert len(parts) == 1
        assert parts[0]["path"] == "test.tmdl"
        assert parts[0]["payloadType"] == "InlineBase64"

    @pytest.mark.unit
    def test_build_definition_parts_directory(self, tmp_path):
        from ssis_to_fabric.engine.fabric_api import SemanticModelDeployer
        d = tmp_path / "model"
        d.mkdir()
        (d / "a.tmdl").write_text("a")
        (d / "b.tmdl").write_text("b")
        parts = SemanticModelDeployer._build_definition_parts(d)
        assert len(parts) == 2


# =====================================================================
# Workspace Git Manager
# =====================================================================


class TestWorkspaceGitManager:
    """Test Git connect/initialize/commit/update."""

    @pytest.mark.unit
    def test_connect_success(self):
        from ssis_to_fabric.engine.fabric_api import GitConnectionConfig, WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200))
        git = WorkspaceGitManager(deployer)

        config = GitConnectionConfig(
            organization="my-org", project="my-project",
            repository="my-repo", branch="main",
        )
        result = git.connect(config)
        assert result.status == "success"
        _, url, body = deployer._api_calls[0]
        assert "/git/connect" in url
        assert body["gitProviderDetails"]["repositoryName"] == "my-repo"

    @pytest.mark.unit
    def test_connect_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import GitConnectionConfig, WorkspaceGitManager
        deployer = MockDeployer(dry_run=True)
        git = WorkspaceGitManager(deployer)

        result = git.connect(GitConnectionConfig())
        assert result.status == "skipped"

    @pytest.mark.unit
    def test_connect_error(self):
        from ssis_to_fabric.engine.fabric_api import GitConnectionConfig, WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(400, {"error": "bad"}))
        git = WorkspaceGitManager(deployer)

        result = git.connect(GitConnectionConfig())
        assert result.status == "error"

    @pytest.mark.unit
    def test_initialize_success(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200))
        git = WorkspaceGitManager(deployer)

        result = git.initialize()
        assert result.status == "success"
        _, url, body = deployer._api_calls[0]
        assert "/git/initializeConnection" in url

    @pytest.mark.unit
    def test_initialize_lro(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(202, {}, {"x-ms-operation-id": "op-42"}))
        git = WorkspaceGitManager(deployer)

        result = git.initialize()
        assert result.status == "success"

    @pytest.mark.unit
    def test_commit_success(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200))
        git = WorkspaceGitManager(deployer)

        result = git.commit("feat: initial migration")
        assert result.status == "success"
        _, url, body = deployer._api_calls[0]
        assert "/git/commitToGit" in url
        assert body["comment"] == "feat: initial migration"

    @pytest.mark.unit
    def test_commit_selective(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200))
        git = WorkspaceGitManager(deployer)

        result = git.commit("fix: update pipeline", items=["item-1", "item-2"])
        assert result.status == "success"
        _, _, body = deployer._api_calls[0]
        assert body["mode"] == "Selective"

    @pytest.mark.unit
    def test_commit_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer(dry_run=True)
        git = WorkspaceGitManager(deployer)

        result = git.commit("test")
        assert result.status == "skipped"

    @pytest.mark.unit
    def test_update_from_git(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200))
        git = WorkspaceGitManager(deployer)

        result = git.update_from_git()
        assert result.status == "success"
        _, url, _ = deployer._api_calls[0]
        assert "/git/updateFromGit" in url

    @pytest.mark.unit
    def test_get_status(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer()
        deployer.queue_response(MockResponse(200, {"remoteCommitHash": "abc123", "localChanges": []}))
        git = WorkspaceGitManager(deployer)

        status = git.get_status()
        assert status["remoteCommitHash"] == "abc123"

    @pytest.mark.unit
    def test_get_status_dry_run(self):
        from ssis_to_fabric.engine.fabric_api import WorkspaceGitManager
        deployer = MockDeployer(dry_run=True)
        git = WorkspaceGitManager(deployer)

        status = git.get_status()
        assert status["status"] == "dry_run"


# =====================================================================
# GitConnectionConfig tests
# =====================================================================


class TestGitConnectionConfig:
    """Test config serialization."""

    @pytest.mark.unit
    def test_azure_devops_config(self):
        from ssis_to_fabric.engine.fabric_api import GitConnectionConfig
        cfg = GitConnectionConfig(
            organization="contoso", project="ETL", repository="ssis-migration",
            branch="develop", provider="AzureDevOps",
        )
        d = cfg.to_dict()
        assert d["gitProviderDetails"]["gitProviderType"] == "AzureDevOps"
        assert d["gitProviderDetails"]["organizationName"] == "contoso"
        assert d["gitBranchDetails"]["name"] == "develop"

    @pytest.mark.unit
    def test_github_config(self):
        from ssis_to_fabric.engine.fabric_api import GitConnectionConfig
        cfg = GitConnectionConfig(
            organization="my-org", repository="my-repo", provider="GitHub",
        )
        d = cfg.to_dict()
        assert d["gitProviderDetails"]["gitProviderType"] == "GitHub"
        assert d["gitProviderDetails"]["ownerName"] == "my-org"


# =====================================================================
# Result dataclass tests
# =====================================================================


class TestResultDataclasses:
    """Test to_dict on all result dataclasses."""

    @pytest.mark.unit
    def test_shortcut_result_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import ShortcutResult
        r = ShortcutResult(name="sc1", status="success", lakehouse_id="lh-1")
        d = r.to_dict()
        assert d["name"] == "sc1"
        assert d["status"] == "success"

    @pytest.mark.unit
    def test_lakehouse_result_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import LakehouseResult
        r = LakehouseResult(name="lh1", status="error", error="bad")
        d = r.to_dict()
        assert d["error"] == "bad"

    @pytest.mark.unit
    def test_schedule_result_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import ScheduleResult
        r = ScheduleResult(item_name="pipe1", status="success")
        d = r.to_dict()
        assert d["item_name"] == "pipe1"

    @pytest.mark.unit
    def test_semantic_model_result_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import SemanticModelResult
        r = SemanticModelResult(name="sm1", status="success", item_id="id-1")
        d = r.to_dict()
        assert d["item_id"] == "id-1"

    @pytest.mark.unit
    def test_git_result_to_dict(self):
        from ssis_to_fabric.engine.fabric_api import GitResult
        r = GitResult(operation="commit", status="error", error="timeout")
        d = r.to_dict()
        assert d["operation"] == "commit"
        assert d["error"] == "timeout"


# =====================================================================
# Helper function tests
# =====================================================================


class TestHelpers:
    """Test utility functions."""

    @pytest.mark.unit
    def test_extract_table_name_simple(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        assert _extract_table_name("CREATE TABLE dim_customer (id INT)") == "dim_customer"

    @pytest.mark.unit
    def test_extract_table_name_if_not_exists(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        assert _extract_table_name("CREATE TABLE IF NOT EXISTS fact_sales (x INT)") == "fact_sales"

    @pytest.mark.unit
    def test_extract_table_name_external(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        assert _extract_table_name("CREATE EXTERNAL TABLE ext_data (y STRING)") == "ext_data"

    @pytest.mark.unit
    def test_extract_table_name_quoted(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        assert _extract_table_name('CREATE TABLE "my_table" (z INT)') == "my_table"

    @pytest.mark.unit
    def test_extract_table_name_bracketed(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        assert _extract_table_name("CREATE TABLE [dbo].[sales] (id INT)") == "dbo].[sales"

    @pytest.mark.unit
    def test_extract_table_name_no_create(self):
        from ssis_to_fabric.engine.fabric_api import _extract_table_name
        result = _extract_table_name("ALTER TABLE foo ADD x INT")
        assert "ALTER TABLE" in result  # fallback to first 50 chars


# =====================================================================
# Deployer integration (shortcut in deploy_all)
# =====================================================================


class TestDeployerShortcutIntegration:
    """Test that _deploy_shortcuts is callable from FabricDeployer."""

    @pytest.mark.unit
    def test_deploy_shortcuts_attribute_exists(self):
        """FabricDeployer has the _deploy_shortcuts method."""
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer
        assert hasattr(FabricDeployer, "_deploy_shortcuts")

    @pytest.mark.unit
    def test_deploy_shortcuts_no_connections_dir(self, tmp_path):
        """No crash when connections/ dir doesn't exist."""
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        with patch.object(FabricDeployer, "authenticate"):
            deployer = FabricDeployer("ws-1", dry_run=True)
            deployer._deploy_shortcuts(tmp_path)  # Should not raise


# =====================================================================
# CLI command existence
# =====================================================================


class TestCLICommands:
    """Verify Phase 31 CLI commands are registered."""

    @pytest.mark.unit
    def test_create_lakehouse_registered(self):
        from ssis_to_fabric.cli import main
        names = [cmd for cmd in main.commands]
        assert "create-lakehouse" in names

    @pytest.mark.unit
    def test_schedule_refresh_registered(self):
        from ssis_to_fabric.cli import main
        names = [cmd for cmd in main.commands]
        assert "schedule-refresh" in names

    @pytest.mark.unit
    def test_deploy_semantic_model_registered(self):
        from ssis_to_fabric.cli import main
        names = [cmd for cmd in main.commands]
        assert "deploy-semantic-model" in names

    @pytest.mark.unit
    def test_git_connect_registered(self):
        from ssis_to_fabric.cli import main
        names = [cmd for cmd in main.commands]
        assert "git-connect" in names

    @pytest.mark.unit
    def test_git_commit_registered(self):
        from ssis_to_fabric.cli import main
        names = [cmd for cmd in main.commands]
        assert "git-commit" in names


# =====================================================================
# API facade methods
# =====================================================================


class TestAPIFacade:
    """Verify SSISMigrator has Phase 31 API methods."""

    @pytest.mark.unit
    def test_create_lakehouse_method_exists(self):
        from ssis_to_fabric.api import SSISMigrator
        assert hasattr(SSISMigrator, "create_lakehouse")
        assert callable(getattr(SSISMigrator, "create_lakehouse"))

    @pytest.mark.unit
    def test_schedule_refresh_method_exists(self):
        from ssis_to_fabric.api import SSISMigrator
        assert hasattr(SSISMigrator, "schedule_refresh")

    @pytest.mark.unit
    def test_deploy_semantic_model_method_exists(self):
        from ssis_to_fabric.api import SSISMigrator
        assert hasattr(SSISMigrator, "deploy_semantic_model")

    @pytest.mark.unit
    def test_git_connect_method_exists(self):
        from ssis_to_fabric.api import SSISMigrator
        assert hasattr(SSISMigrator, "git_connect")
