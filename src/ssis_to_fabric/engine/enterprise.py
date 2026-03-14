"""
Enterprise Scale
=================
Phase 15 features for large-scale, multi-tenant SSIS migrations:

- **RBAC**: role-based access control with Azure AD group enforcement
- **Multi-tenant**: parallel deployment to multiple workspace targets
- **Queue-based orchestration**: async job processing with progress tracking
- **Horizontal scaling**: distribute migration work across workers
- **Cost estimation**: Fabric CU consumption projections
- **Compliance reporting**: audit trail for SOC2/GDPR
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path  # noqa: TC003 (used at runtime)
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# =====================================================================
# RBAC — Role-Based Access Control
# =====================================================================


class MigrationRole(str, Enum):
    """Predefined roles for migration access control."""

    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"
    DEPLOYER = "deployer"


# Permissions for each role
_ROLE_PERMISSIONS: dict[MigrationRole, frozenset[str]] = {
    MigrationRole.ADMIN: frozenset({
        "analyze", "plan", "migrate", "deploy", "rollback",
        "configure", "manage_users", "view_reports", "delete",
    }),
    MigrationRole.OPERATOR: frozenset({
        "analyze", "plan", "migrate", "deploy", "rollback", "view_reports",
    }),
    MigrationRole.DEPLOYER: frozenset({
        "deploy", "rollback", "view_reports",
    }),
    MigrationRole.VIEWER: frozenset({
        "analyze", "view_reports",
    }),
}


@dataclass
class RBACPolicy:
    """A role-based access control policy."""

    principal: str  # Azure AD group ID or user principal
    role: MigrationRole
    scope: str = "*"  # package pattern or "*" for all

    def has_permission(self, action: str) -> bool:
        return action in _ROLE_PERMISSIONS.get(self.role, frozenset())


@dataclass
class RBACConfig:
    """RBAC configuration for a migration project."""

    enabled: bool = False
    policies: list[RBACPolicy] = field(default_factory=list)
    default_role: MigrationRole = MigrationRole.VIEWER

    def authorize(self, principal: str, action: str, package: str = "*") -> bool:
        """Check if a principal is authorized for an action on a package."""
        if not self.enabled:
            return True

        for policy in self.policies:
            if policy.principal != principal:
                continue
            if policy.scope != "*" and policy.scope != package:
                continue
            if policy.has_permission(action):
                return True

        # Fall back to default role
        return action in _ROLE_PERMISSIONS.get(self.default_role, frozenset())

    def get_role(self, principal: str) -> MigrationRole:
        """Get the effective role for a principal."""
        for policy in self.policies:
            if policy.principal == principal:
                return policy.role
        return self.default_role

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "default_role": self.default_role.value,
            "policies": [
                {
                    "principal": p.principal,
                    "role": p.role.value,
                    "scope": p.scope,
                }
                for p in self.policies
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RBACConfig:
        return cls(
            enabled=data.get("enabled", False),
            default_role=MigrationRole(data.get("default_role", "viewer")),
            policies=[
                RBACPolicy(
                    principal=p["principal"],
                    role=MigrationRole(p["role"]),
                    scope=p.get("scope", "*"),
                )
                for p in data.get("policies", [])
            ],
        )


# =====================================================================
# Multi-Tenant Migration
# =====================================================================


@dataclass
class TenantTarget:
    """A single tenant/workspace deployment target."""

    tenant_id: str
    workspace_id: str
    workspace_name: str = ""
    connection_mappings: dict[str, str] = field(default_factory=dict)
    environment: str = "production"


@dataclass
class MultiTenantConfig:
    """Configuration for multi-tenant parallel deployment."""

    tenants: list[TenantTarget] = field(default_factory=list)
    parallel_tenants: int = 2
    fail_fast: bool = False  # stop all if one tenant fails

    def tenant_ids(self) -> list[str]:
        return [t.tenant_id for t in self.tenants]


@dataclass
class TenantDeploymentResult:
    """Result of deploying to a single tenant."""

    tenant_id: str
    workspace_id: str
    success: bool
    items_deployed: int = 0
    error: str = ""
    elapsed_seconds: float = 0.0


def plan_multi_tenant_deployment(
    config: MultiTenantConfig,
    artifacts_dir: Path,
) -> list[dict[str, Any]]:
    """Plan deployment batches for multi-tenant execution.

    Groups tenants into parallel batches based on ``parallel_tenants``.
    """
    batches: list[dict[str, Any]] = []
    tenants = config.tenants

    for i in range(0, len(tenants), config.parallel_tenants):
        batch = tenants[i : i + config.parallel_tenants]
        batches.append({
            "batch_index": i // config.parallel_tenants,
            "tenants": [
                {
                    "tenant_id": t.tenant_id,
                    "workspace_id": t.workspace_id,
                    "workspace_name": t.workspace_name,
                    "environment": t.environment,
                }
                for t in batch
            ],
            "artifact_source": str(artifacts_dir),
        })

    logger.info(
        "multi_tenant_plan",
        total_tenants=len(tenants),
        batches=len(batches),
        parallel=config.parallel_tenants,
    )
    return batches


# =====================================================================
# Queue-Based Migration Orchestration
# =====================================================================


class JobStatus(str, Enum):
    """Status of a migration job in the queue."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class MigrationJob:
    """A unit of work in the migration queue."""

    job_id: str = ""
    package_path: str = ""
    status: JobStatus = JobStatus.PENDING
    priority: int = 0  # lower = higher priority
    created_at: str = ""
    started_at: str = ""
    completed_at: str = ""
    worker_id: str = ""
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    attempt: int = 0
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if not self.job_id:
            self.job_id = str(uuid.uuid4())[:8]
        if not self.created_at:
            self.created_at = datetime.now(tz=timezone.utc).isoformat()

    def can_retry(self) -> bool:
        return self.attempt < self.max_attempts and self.status == JobStatus.FAILED

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "package_path": self.package_path,
            "status": self.status.value,
            "priority": self.priority,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "worker_id": self.worker_id,
            "result": self.result,
            "error": self.error,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationJob:
        return cls(
            job_id=data["job_id"],
            package_path=data.get("package_path", ""),
            status=JobStatus(data.get("status", "pending")),
            priority=data.get("priority", 0),
            created_at=data.get("created_at", ""),
            started_at=data.get("started_at", ""),
            completed_at=data.get("completed_at", ""),
            worker_id=data.get("worker_id", ""),
            result=data.get("result", {}),
            error=data.get("error", ""),
            attempt=data.get("attempt", 0),
            max_attempts=data.get("max_attempts", 3),
        )


class MigrationQueue:
    """In-process migration job queue with priority ordering.

    Provides a local queue for migration orchestration.
    For distributed scenarios, this can be backed by Redis or Azure Queue.
    """

    def __init__(self) -> None:
        self._jobs: list[MigrationJob] = []

    def submit(self, package_path: str, priority: int = 0) -> MigrationJob:
        """Submit a new migration job to the queue."""
        job = MigrationJob(package_path=package_path, priority=priority)
        self._jobs.append(job)
        self._jobs.sort(key=lambda j: j.priority)
        logger.info("job_submitted", job_id=job.job_id, package=package_path)
        return job

    def next_pending(self) -> MigrationJob | None:
        """Get the next pending job (highest priority)."""
        for job in self._jobs:
            if job.status == JobStatus.PENDING:
                return job
        return None

    def claim(self, job_id: str, worker_id: str) -> MigrationJob | None:
        """Claim a job for processing by a worker."""
        job = self.get(job_id)
        if not job or job.status != JobStatus.PENDING:
            return None
        job.status = JobStatus.RUNNING
        job.worker_id = worker_id
        job.started_at = datetime.now(tz=timezone.utc).isoformat()
        job.attempt += 1
        return job

    def complete(self, job_id: str, result: dict[str, Any] | None = None) -> None:
        """Mark a job as completed."""
        job = self.get(job_id)
        if job:
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(tz=timezone.utc).isoformat()
            job.result = result or {}

    def fail(self, job_id: str, error: str) -> None:
        """Mark a job as failed."""
        job = self.get(job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error = error
            job.completed_at = datetime.now(tz=timezone.utc).isoformat()

    def retry_failed(self) -> list[MigrationJob]:
        """Re-queue failed jobs that haven't exceeded max attempts."""
        retried = []
        for job in self._jobs:
            if job.can_retry():
                job.status = JobStatus.PENDING
                job.error = ""
                retried.append(job)
        return retried

    def cancel(self, job_id: str) -> bool:
        """Cancel a pending job."""
        job = self.get(job_id)
        if job and job.status == JobStatus.PENDING:
            job.status = JobStatus.CANCELLED
            return True
        return False

    def get(self, job_id: str) -> MigrationJob | None:
        return next((j for j in self._jobs if j.job_id == job_id), None)

    @property
    def pending_count(self) -> int:
        return sum(1 for j in self._jobs if j.status == JobStatus.PENDING)

    @property
    def running_count(self) -> int:
        return sum(1 for j in self._jobs if j.status == JobStatus.RUNNING)

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for job in self._jobs:
            counts[job.status.value] = counts.get(job.status.value, 0) + 1
        return counts

    def all_jobs(self) -> list[MigrationJob]:
        return list(self._jobs)

    def save(self, path: Path) -> None:
        """Persist queue state to JSON."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = [j.to_dict() for j in self._jobs]
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def load(self, path: Path) -> None:
        """Load queue state from JSON."""
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            self._jobs = [MigrationJob.from_dict(d) for d in data]


# =====================================================================
# Cost Estimation
# =====================================================================


@dataclass
class CostEstimate:
    """Estimated Fabric CU consumption for a migration artifact."""

    artifact_name: str
    artifact_type: str  # "pipeline", "notebook", "dataflow"
    estimated_cu_seconds: float = 0.0
    complexity_factor: float = 1.0
    notes: str = ""


# Base CU costs per artifact type (approximate)
_CU_BASE_COSTS: dict[str, float] = {
    "pipeline": 0.5,    # per execution
    "notebook": 2.0,    # per execution (Spark startup + processing)
    "dataflow": 1.5,    # per execution
    "connection": 0.1,  # per reference
}


def estimate_migration_cost(
    pipelines_dir: Path,
    notebooks_dir: Path,
    daily_executions: int = 1,
) -> list[CostEstimate]:
    """Estimate Fabric CU consumption for generated artifacts.

    These are rough order-of-magnitude projections based on
    artifact count and type. Actual costs depend on data volume,
    Spark cluster size, and query complexity.
    """
    estimates: list[CostEstimate] = []

    if pipelines_dir.exists():
        for f in pipelines_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                activities = len(data.get("activities", []))
                complexity = max(1.0, activities * 0.3)
            except (json.JSONDecodeError, KeyError):
                complexity = 1.0

            estimates.append(CostEstimate(
                artifact_name=f.stem,
                artifact_type="pipeline",
                estimated_cu_seconds=_CU_BASE_COSTS["pipeline"] * complexity * daily_executions,
                complexity_factor=complexity,
            ))

    if notebooks_dir.exists():
        for f in notebooks_dir.glob("*.py"):
            content = f.read_text(encoding="utf-8")
            lines = content.count("\n")
            complexity = max(1.0, lines / 50)
            estimates.append(CostEstimate(
                artifact_name=f.stem,
                artifact_type="notebook",
                estimated_cu_seconds=_CU_BASE_COSTS["notebook"] * complexity * daily_executions,
                complexity_factor=complexity,
            ))

    total_cu = sum(e.estimated_cu_seconds for e in estimates)
    logger.info("cost_estimation_complete", artifacts=len(estimates), total_cu_seconds=total_cu)
    return estimates


def write_cost_report(estimates: list[CostEstimate], output_path: Path) -> Path:
    """Write cost estimation report to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = sum(e.estimated_cu_seconds for e in estimates)
    data = {
        "total_estimated_cu_seconds": total,
        "total_estimated_cu_hours": total / 3600,
        "artifact_count": len(estimates),
        "estimates": [
            {
                "name": e.artifact_name,
                "type": e.artifact_type,
                "cu_seconds": e.estimated_cu_seconds,
                "complexity": e.complexity_factor,
                "notes": e.notes,
            }
            for e in estimates
        ],
    }
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return output_path


# =====================================================================
# Compliance Reporting
# =====================================================================


@dataclass
class AuditEntry:
    """A single entry in the compliance audit trail."""

    timestamp: str = ""
    action: str = ""
    principal: str = ""
    package: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    correlation_id: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(tz=timezone.utc).isoformat()


class ComplianceTracker:
    """Tracks data flow audit trail for SOC2/GDPR compliance.

    Records who accessed what data, when, and through which
    migration artifacts.
    """

    def __init__(self) -> None:
        self._entries: list[AuditEntry] = []

    def record(
        self,
        action: str,
        principal: str = "system",
        package: str = "",
        correlation_id: str = "",
        **details: Any,
    ) -> AuditEntry:
        entry = AuditEntry(
            action=action,
            principal=principal,
            package=package,
            details=dict(details),
            correlation_id=correlation_id,
        )
        self._entries.append(entry)
        logger.info(
            "compliance_audit",
            action=action,
            principal=principal,
            package=package,
        )
        return entry

    def entries_for_package(self, package: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.package == package]

    def entries_for_principal(self, principal: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.principal == principal]

    def data_lineage_report(self) -> dict[str, Any]:
        """Generate a data-lineage focused compliance report."""
        packages = set()
        principals = set()
        actions: dict[str, int] = {}
        for entry in self._entries:
            if entry.package:
                packages.add(entry.package)
            principals.add(entry.principal)
            actions[entry.action] = actions.get(entry.action, 0) + 1

        return {
            "report_type": "data_lineage_compliance",
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "total_events": len(self._entries),
            "unique_packages": len(packages),
            "unique_principals": len(principals),
            "action_counts": actions,
            "packages_accessed": sorted(packages),
            "principals_active": sorted(principals),
        }

    def write_audit_log(self, path: Path) -> Path:
        """Persist audit entries to a JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "audit_log": [
                {
                    "timestamp": e.timestamp,
                    "action": e.action,
                    "principal": e.principal,
                    "package": e.package,
                    "details": e.details,
                    "correlation_id": e.correlation_id,
                }
                for e in self._entries
            ],
            "summary": self.data_lineage_report(),
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return path

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def to_dict(self) -> dict[str, Any]:
        return self.data_lineage_report()


# =====================================================================
# Worker Identity
# =====================================================================


def generate_worker_id() -> str:
    """Generate a unique worker identifier for distributed orchestration."""
    host_hash = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:6]
    return f"worker-{host_hash}"
