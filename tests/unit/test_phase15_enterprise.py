"""Phase 15 – Enterprise Scale tests.

Tests cover:
- RBAC roles, permissions, policies, authorization
- Multi-tenant configuration and deployment planning
- Queue-based orchestration (submit, claim, complete, fail, retry, cancel)
- Cost estimation and reporting
- Compliance tracking and audit trail
- CLI commands (cost-estimate, compliance-report)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ssis_to_fabric.engine.enterprise import (
    AuditEntry,
    ComplianceTracker,
    CostEstimate,
    JobStatus,
    MigrationJob,
    MigrationQueue,
    MigrationRole,
    MultiTenantConfig,
    RBACConfig,
    RBACPolicy,
    TenantDeploymentResult,
    TenantTarget,
    estimate_migration_cost,
    generate_worker_id,
    plan_multi_tenant_deployment,
    write_cost_report,
)

if TYPE_CHECKING:
    from pathlib import Path


# ── RBAC ───────────────────────────────────────────────────────────


class TestRBAC:
    def test_admin_has_all_permissions(self):
        policy = RBACPolicy(principal="admin-group", role=MigrationRole.ADMIN)
        assert policy.has_permission("analyze")
        assert policy.has_permission("deploy")
        assert policy.has_permission("manage_users")
        assert policy.has_permission("delete")

    def test_viewer_limited_permissions(self):
        policy = RBACPolicy(principal="viewer-group", role=MigrationRole.VIEWER)
        assert policy.has_permission("analyze")
        assert policy.has_permission("view_reports")
        assert not policy.has_permission("deploy")
        assert not policy.has_permission("migrate")

    def test_operator_permissions(self):
        policy = RBACPolicy(principal="ops", role=MigrationRole.OPERATOR)
        assert policy.has_permission("migrate")
        assert policy.has_permission("deploy")
        assert not policy.has_permission("manage_users")

    def test_deployer_permissions(self):
        policy = RBACPolicy(principal="deploy", role=MigrationRole.DEPLOYER)
        assert policy.has_permission("deploy")
        assert policy.has_permission("rollback")
        assert not policy.has_permission("migrate")

    def test_rbac_disabled_allows_all(self):
        config = RBACConfig(enabled=False)
        assert config.authorize("anyone", "delete")

    def test_rbac_authorize_matching_policy(self):
        config = RBACConfig(
            enabled=True,
            policies=[RBACPolicy(principal="user1", role=MigrationRole.ADMIN)],
        )
        assert config.authorize("user1", "deploy")
        assert config.authorize("user1", "manage_users")

    def test_rbac_authorize_fallback_to_default(self):
        config = RBACConfig(
            enabled=True,
            default_role=MigrationRole.VIEWER,
        )
        assert config.authorize("unknown", "analyze")
        assert not config.authorize("unknown", "deploy")

    def test_rbac_scope_filtering(self):
        config = RBACConfig(
            enabled=True,
            policies=[
                RBACPolicy(principal="user1", role=MigrationRole.OPERATOR, scope="PackageA"),
            ],
            default_role=MigrationRole.VIEWER,
        )
        assert config.authorize("user1", "deploy", package="PackageA")
        # Wrong package — falls to default
        assert not config.authorize("user1", "deploy", package="PackageB")

    def test_rbac_wildcard_scope(self):
        config = RBACConfig(
            enabled=True,
            policies=[
                RBACPolicy(principal="user1", role=MigrationRole.OPERATOR, scope="*"),
            ],
        )
        assert config.authorize("user1", "deploy", package="anything")

    def test_get_role(self):
        config = RBACConfig(
            enabled=True,
            policies=[RBACPolicy(principal="user1", role=MigrationRole.ADMIN)],
            default_role=MigrationRole.VIEWER,
        )
        assert config.get_role("user1") == MigrationRole.ADMIN
        assert config.get_role("unknown") == MigrationRole.VIEWER

    def test_to_dict_roundtrip(self):
        config = RBACConfig(
            enabled=True,
            policies=[RBACPolicy(principal="p1", role=MigrationRole.OPERATOR)],
        )
        d = config.to_dict()
        restored = RBACConfig.from_dict(d)
        assert restored.enabled is True
        assert len(restored.policies) == 1
        assert restored.policies[0].role == MigrationRole.OPERATOR

    def test_all_roles(self):
        assert set(MigrationRole) == {
            MigrationRole.ADMIN,
            MigrationRole.OPERATOR,
            MigrationRole.VIEWER,
            MigrationRole.DEPLOYER,
        }


# ── Multi-Tenant ──────────────────────────────────────────────────


class TestMultiTenant:
    def test_tenant_target(self):
        t = TenantTarget(tenant_id="t1", workspace_id="ws1", workspace_name="Dev")
        assert t.tenant_id == "t1"
        assert t.environment == "production"

    def test_multi_tenant_config(self):
        config = MultiTenantConfig(
            tenants=[
                TenantTarget(tenant_id="t1", workspace_id="ws1"),
                TenantTarget(tenant_id="t2", workspace_id="ws2"),
            ],
            parallel_tenants=2,
        )
        assert config.tenant_ids() == ["t1", "t2"]

    def test_plan_batches(self, tmp_path: Path):
        config = MultiTenantConfig(
            tenants=[
                TenantTarget(tenant_id=f"t{i}", workspace_id=f"ws{i}")
                for i in range(5)
            ],
            parallel_tenants=2,
        )
        batches = plan_multi_tenant_deployment(config, tmp_path)
        assert len(batches) == 3  # 5 tenants / 2 per batch = 3 batches
        assert len(batches[0]["tenants"]) == 2
        assert len(batches[2]["tenants"]) == 1

    def test_plan_single_tenant(self, tmp_path: Path):
        config = MultiTenantConfig(
            tenants=[TenantTarget(tenant_id="t1", workspace_id="ws1")],
        )
        batches = plan_multi_tenant_deployment(config, tmp_path)
        assert len(batches) == 1

    def test_plan_empty(self, tmp_path: Path):
        config = MultiTenantConfig()
        batches = plan_multi_tenant_deployment(config, tmp_path)
        assert batches == []

    def test_deployment_result(self):
        result = TenantDeploymentResult(
            tenant_id="t1",
            workspace_id="ws1",
            success=True,
            items_deployed=5,
        )
        assert result.success
        assert result.items_deployed == 5


# ── Queue-Based Orchestration ────────────────────────────────────


class TestMigrationQueue:
    def test_submit(self):
        q = MigrationQueue()
        job = q.submit("package.dtsx")
        assert job.status == JobStatus.PENDING
        assert job.package_path == "package.dtsx"
        assert q.pending_count == 1

    def test_priority_ordering(self):
        q = MigrationQueue()
        q.submit("low.dtsx", priority=10)
        q.submit("high.dtsx", priority=1)
        q.submit("medium.dtsx", priority=5)

        first = q.next_pending()
        assert first is not None
        assert first.package_path == "high.dtsx"

    def test_claim(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        claimed = q.claim(job.job_id, "worker-1")
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.worker_id == "worker-1"
        assert claimed.attempt == 1

    def test_claim_non_pending_fails(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        q.claim(job.job_id, "w1")
        # Already running — can't claim again
        assert q.claim(job.job_id, "w2") is None

    def test_complete(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        q.claim(job.job_id, "w1")
        q.complete(job.job_id, {"artifacts": 3})
        assert job.status == JobStatus.COMPLETED
        assert job.result == {"artifacts": 3}

    def test_fail(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        q.claim(job.job_id, "w1")
        q.fail(job.job_id, "parse error")
        assert job.status == JobStatus.FAILED
        assert job.error == "parse error"

    def test_retry_failed(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        q.claim(job.job_id, "w1")
        q.fail(job.job_id, "error")
        assert job.can_retry()

        retried = q.retry_failed()
        assert len(retried) == 1
        assert retried[0].status == JobStatus.PENDING

    def test_max_attempts_exceeded(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        job.max_attempts = 2

        for _ in range(2):
            q.claim(job.job_id, "w1")
            q.fail(job.job_id, "error")
            if job.can_retry():
                job.status = JobStatus.PENDING

        assert not job.can_retry()

    def test_cancel(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        assert q.cancel(job.job_id)
        assert job.status == JobStatus.CANCELLED

    def test_cancel_running_fails(self):
        q = MigrationQueue()
        job = q.submit("pkg.dtsx")
        q.claim(job.job_id, "w1")
        assert not q.cancel(job.job_id)

    def test_summary(self):
        q = MigrationQueue()
        q.submit("a.dtsx")
        q.submit("b.dtsx")
        job = q.submit("c.dtsx")
        q.claim(job.job_id, "w1")

        summary = q.summary()
        assert summary["pending"] == 2
        assert summary["running"] == 1

    def test_save_and_load(self, tmp_path: Path):
        q = MigrationQueue()
        q.submit("a.dtsx", priority=1)
        q.submit("b.dtsx", priority=2)

        path = tmp_path / "queue.json"
        q.save(path)

        q2 = MigrationQueue()
        q2.load(path)
        assert len(q2.all_jobs()) == 2
        assert q2.all_jobs()[0].package_path == "a.dtsx"

    def test_get_nonexistent(self):
        q = MigrationQueue()
        assert q.get("nonexistent") is None

    def test_next_pending_empty(self):
        q = MigrationQueue()
        assert q.next_pending() is None


class TestMigrationJob:
    def test_auto_id(self):
        job = MigrationJob(package_path="test.dtsx")
        assert job.job_id != ""
        assert job.created_at != ""

    def test_roundtrip(self):
        job = MigrationJob(package_path="test.dtsx", priority=5)
        d = job.to_dict()
        restored = MigrationJob.from_dict(d)
        assert restored.package_path == "test.dtsx"
        assert restored.priority == 5

    def test_all_statuses(self):
        assert set(JobStatus) == {
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }


# ── Cost Estimation ──────────────────────────────────────────────


class TestCostEstimation:
    def test_estimate_pipelines(self, tmp_path: Path):
        pipelines = tmp_path / "pipelines"
        pipelines.mkdir()
        (pipelines / "P1.json").write_text('{"name": "P1", "activities": [{"type": "Copy"}, {"type": "Copy"}]}')

        estimates = estimate_migration_cost(pipelines, tmp_path / "notebooks")
        assert len(estimates) == 1
        assert estimates[0].artifact_type == "pipeline"
        assert estimates[0].estimated_cu_seconds > 0

    def test_estimate_notebooks(self, tmp_path: Path):
        notebooks = tmp_path / "notebooks"
        notebooks.mkdir()
        (notebooks / "NB1.py").write_text("# notebook\n" * 100)

        estimates = estimate_migration_cost(tmp_path / "pipelines", notebooks)
        assert len(estimates) == 1
        assert estimates[0].artifact_type == "notebook"

    def test_estimate_daily_executions(self, tmp_path: Path):
        pipelines = tmp_path / "pipelines"
        pipelines.mkdir()
        (pipelines / "P1.json").write_text('{"name": "P1"}')

        est1 = estimate_migration_cost(pipelines, tmp_path / "nb", daily_executions=1)
        est5 = estimate_migration_cost(pipelines, tmp_path / "nb", daily_executions=5)
        assert est5[0].estimated_cu_seconds > est1[0].estimated_cu_seconds

    def test_estimate_empty(self, tmp_path: Path):
        estimates = estimate_migration_cost(tmp_path / "p", tmp_path / "n")
        assert estimates == []

    def test_write_cost_report(self, tmp_path: Path):
        estimates = [
            CostEstimate(artifact_name="P1", artifact_type="pipeline", estimated_cu_seconds=1.5),
        ]
        path = write_cost_report(estimates, tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["total_estimated_cu_seconds"] == 1.5
        assert len(data["estimates"]) == 1

    def test_cost_estimate_dataclass(self):
        e = CostEstimate(
            artifact_name="test",
            artifact_type="notebook",
            estimated_cu_seconds=5.0,
            complexity_factor=2.5,
        )
        assert e.complexity_factor == 2.5


# ── Compliance Tracking ──────────────────────────────────────────


class TestComplianceTracker:
    def test_record(self):
        tracker = ComplianceTracker()
        entry = tracker.record("analyze", principal="user1", package="pkg1")
        assert entry.action == "analyze"
        assert tracker.entry_count == 1

    def test_entries_for_package(self):
        tracker = ComplianceTracker()
        tracker.record("analyze", package="pkg1")
        tracker.record("migrate", package="pkg1")
        tracker.record("analyze", package="pkg2")

        entries = tracker.entries_for_package("pkg1")
        assert len(entries) == 2

    def test_entries_for_principal(self):
        tracker = ComplianceTracker()
        tracker.record("analyze", principal="user1")
        tracker.record("deploy", principal="user2")
        tracker.record("migrate", principal="user1")

        entries = tracker.entries_for_principal("user1")
        assert len(entries) == 2

    def test_data_lineage_report(self):
        tracker = ComplianceTracker()
        tracker.record("analyze", principal="u1", package="p1")
        tracker.record("deploy", principal="u2", package="p2")

        report = tracker.data_lineage_report()
        assert report["total_events"] == 2
        assert report["unique_packages"] == 2
        assert report["unique_principals"] == 2

    def test_write_audit_log(self, tmp_path: Path):
        tracker = ComplianceTracker()
        tracker.record("analyze", principal="u1", package="p1")
        path = tracker.write_audit_log(tmp_path / "audit.json")
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data["audit_log"]) == 1
        assert "summary" in data

    def test_to_dict(self):
        tracker = ComplianceTracker()
        tracker.record("x")
        d = tracker.to_dict()
        assert d["total_events"] == 1


class TestAuditEntry:
    def test_auto_timestamp(self):
        entry = AuditEntry(action="test")
        assert entry.timestamp != ""

    def test_explicit_fields(self):
        entry = AuditEntry(
            action="deploy",
            principal="admin",
            package="pkg1",
            correlation_id="corr-123",
        )
        assert entry.principal == "admin"
        assert entry.correlation_id == "corr-123"


# ── Worker Identity ──────────────────────────────────────────────


class TestWorkerIdentity:
    def test_generate_worker_id(self):
        wid = generate_worker_id()
        assert wid.startswith("worker-")
        assert len(wid) > 7

    def test_unique_ids(self):
        ids = {generate_worker_id() for _ in range(10)}
        assert len(ids) > 1  # should generate different IDs


# ── CLI Commands ─────────────────────────────────────────────────


class TestCLIEnterprise:
    def test_cost_estimate_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["cost-estimate", "--help"])
        assert result.exit_code == 0
        assert "Estimate Fabric CU" in result.output

    def test_cost_estimate_with_artifacts(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "P1.json").write_text('{"name": "P1", "activities": []}')
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "NB1.py").write_text("x = 1\n")

        runner = CliRunner()
        result = runner.invoke(main, ["cost-estimate", str(tmp_path)])
        assert result.exit_code == 0
        assert "CU-seconds" in result.output

    def test_cost_estimate_empty(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["cost-estimate", str(tmp_path)])
        assert result.exit_code == 0
        assert "No artifacts" in result.output

    def test_compliance_report_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["compliance-report", "--help"])
        assert result.exit_code == 0
        assert "compliance" in result.output.lower()

    def test_compliance_report_with_artifacts(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "P1.json").write_text('{"a":1}')

        runner = CliRunner()
        result = runner.invoke(main, ["compliance-report", str(tmp_path)])
        assert result.exit_code == 0
        assert "Compliance" in result.output
