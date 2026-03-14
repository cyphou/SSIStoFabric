"""
Deployment Hardening
=====================
Adds production-grade deployment features on top of the core ``FabricDeployer``:

- **Deployment state machine**: ``PENDING → QUEUED → IN_PROGRESS → COMMITTED / ROLLED_BACK``
- **Blue-green deployment**: deploy to a staging folder, validate, then swap
- **Deployment snapshots**: persist state to enable ``ssis2fabric rollback``
- **Pre-deploy validation**: naming conflict detection, workspace readiness
- **Adaptive rate limiting**: dynamically throttle on 429 responses
- **Pipeline scheduling**: configure triggers after deployment
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path  # noqa: TC003 (used at runtime)
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# =====================================================================
# Deployment State Machine
# =====================================================================


class DeploymentState(str, Enum):
    """State machine for a deployment lifecycle."""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    VALIDATING = "VALIDATING"
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


# Valid state transitions
_TRANSITIONS: dict[DeploymentState, set[DeploymentState]] = {
    DeploymentState.PENDING: {DeploymentState.QUEUED, DeploymentState.FAILED},
    DeploymentState.QUEUED: {DeploymentState.IN_PROGRESS, DeploymentState.FAILED},
    DeploymentState.IN_PROGRESS: {
        DeploymentState.VALIDATING,
        DeploymentState.COMMITTED,
        DeploymentState.FAILED,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.VALIDATING: {
        DeploymentState.COMMITTED,
        DeploymentState.ROLLED_BACK,
        DeploymentState.FAILED,
    },
    DeploymentState.COMMITTED: set(),
    DeploymentState.ROLLED_BACK: set(),
    DeploymentState.FAILED: {DeploymentState.ROLLED_BACK},
}


@dataclass
class DeploymentSnapshot:
    """Persistent record of a deployment for rollback and auditing."""

    deployment_id: str
    state: DeploymentState = DeploymentState.PENDING
    workspace_id: str = ""
    started_at: str = ""
    completed_at: str = ""
    items: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, str]] = field(default_factory=list)
    error: str = ""

    def transition(self, new_state: DeploymentState) -> None:
        """Transition to a new state, enforcing valid transitions."""
        valid = _TRANSITIONS.get(self.state, set())
        if new_state not in valid:
            raise InvalidStateTransitionError(
                f"Cannot transition from {self.state.value} to {new_state.value}"
            )
        self.history.append({
            "from": self.state.value,
            "to": new_state.value,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        })
        self.state = new_state
        if new_state in (DeploymentState.COMMITTED, DeploymentState.ROLLED_BACK, DeploymentState.FAILED):
            self.completed_at = datetime.now(tz=timezone.utc).isoformat()
        logger.info("deployment_state_changed", state=new_state.value, deployment_id=self.deployment_id)

    def add_item(self, name: str, item_type: str, item_id: str = "") -> None:
        self.items.append({
            "name": name,
            "item_type": item_type,
            "item_id": item_id,
            "deployed_at": datetime.now(tz=timezone.utc).isoformat(),
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "state": self.state.value,
            "workspace_id": self.workspace_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "items": self.items,
            "history": self.history,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeploymentSnapshot:
        return cls(
            deployment_id=data["deployment_id"],
            state=DeploymentState(data["state"]),
            workspace_id=data.get("workspace_id", ""),
            started_at=data.get("started_at", ""),
            completed_at=data.get("completed_at", ""),
            items=data.get("items", []),
            history=data.get("history", []),
            error=data.get("error", ""),
        )


class InvalidStateTransitionError(Exception):
    """Raised when an invalid deployment state transition is attempted."""


# =====================================================================
# Snapshot Persistence
# =====================================================================


_SNAPSHOT_DIR = ".ssis2fabric"
_SNAPSHOT_FILE = "deployments.json"


def save_snapshot(snapshot: DeploymentSnapshot, base_dir: Path) -> Path:
    """Persist a deployment snapshot to disk."""
    snap_dir = base_dir / _SNAPSHOT_DIR
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_file = snap_dir / _SNAPSHOT_FILE

    snapshots: list[dict[str, Any]] = []
    if snap_file.exists():
        snapshots = json.loads(snap_file.read_text(encoding="utf-8"))

    # Update existing or append new
    replaced = False
    for i, s in enumerate(snapshots):
        if s["deployment_id"] == snapshot.deployment_id:
            snapshots[i] = snapshot.to_dict()
            replaced = True
            break
    if not replaced:
        snapshots.append(snapshot.to_dict())

    snap_file.write_text(json.dumps(snapshots, indent=2), encoding="utf-8")
    return snap_file


def load_snapshots(base_dir: Path) -> list[DeploymentSnapshot]:
    """Load all deployment snapshots from disk."""
    snap_file = base_dir / _SNAPSHOT_DIR / _SNAPSHOT_FILE
    if not snap_file.exists():
        return []
    data = json.loads(snap_file.read_text(encoding="utf-8"))
    return [DeploymentSnapshot.from_dict(d) for d in data]


def load_latest_snapshot(base_dir: Path) -> DeploymentSnapshot | None:
    """Load the most recent deployment snapshot."""
    snapshots = load_snapshots(base_dir)
    return snapshots[-1] if snapshots else None


# =====================================================================
# Pre-Deploy Validation
# =====================================================================


@dataclass
class ValidationIssue:
    """A single pre-deploy validation finding."""

    severity: str  # "error", "warning", "info"
    message: str
    suggestion: str = ""


@dataclass
class ValidationResult:
    """Result of pre-deployment workspace validation."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]


def pre_deploy_validate(
    output_dir: Path,
    workspace_items: list[dict[str, str]] | None = None,
) -> ValidationResult:
    """Validate deployment readiness before deploying.

    Checks:
    - Pipeline JSON files are valid JSON
    - Notebook Python files exist
    - No naming conflicts with existing workspace items
    """
    result = ValidationResult()

    # Check pipeline JSONs
    pipelines_dir = output_dir / "pipelines"
    if pipelines_dir.exists():
        for f in pipelines_dir.glob("*.json"):
            try:
                json.loads(f.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                result.issues.append(ValidationIssue(
                    severity="error",
                    message=f"Invalid JSON in {f.name}: {e}",
                    suggestion="Regenerate the pipeline with ssis2fabric migrate",
                ))

    # Check notebooks
    notebooks_dir = output_dir / "notebooks"
    if notebooks_dir.exists():
        py_files = list(notebooks_dir.glob("*.py"))
        if not py_files:
            result.issues.append(ValidationIssue(
                severity="warning",
                message="No notebook files found in notebooks/ directory",
            ))
        for f in py_files:
            content = f.read_text(encoding="utf-8")
            if not content.strip():
                result.issues.append(ValidationIssue(
                    severity="error",
                    message=f"Empty notebook file: {f.name}",
                    suggestion="Check migration output for errors",
                ))

    # Check for output directory structure
    if not pipelines_dir.exists() and not (notebooks_dir and notebooks_dir.exists()):
        result.issues.append(ValidationIssue(
            severity="error",
            message="No pipelines/ or notebooks/ directory found",
            suggestion="Run ssis2fabric migrate first",
        ))

    # Naming conflict check
    if workspace_items:
        existing_names = {item.get("displayName", "") for item in workspace_items}
        if pipelines_dir.exists():
            for f in pipelines_dir.glob("*.json"):
                pipeline_name = f.stem
                if pipeline_name in existing_names:
                    result.issues.append(ValidationIssue(
                        severity="warning",
                        message=f"Pipeline '{pipeline_name}' already exists in workspace",
                        suggestion="Use --skip-existing or --clean to handle conflicts",
                    ))

    if result.is_valid:
        logger.info("pre_deploy_validation_passed", issues=len(result.issues))
    else:
        logger.warning("pre_deploy_validation_failed", errors=len(result.errors))

    return result


# =====================================================================
# Blue-Green Deployment
# =====================================================================


@dataclass
class BlueGreenConfig:
    """Configuration for blue-green deployment."""

    staging_suffix: str = "_staging"
    auto_swap: bool = True
    validate_before_swap: bool = True


class BlueGreenDeployment:
    """Manages blue-green deployment via staging folders.

    Deploys artifacts to a staging folder, optionally validates,
    then swaps staging → production by renaming folders.
    """

    def __init__(self, config: BlueGreenConfig | None = None) -> None:
        self.config = config or BlueGreenConfig()
        self._staging_items: list[dict[str, str]] = []

    def staging_name(self, original_name: str) -> str:
        """Get the staging folder/item name for a given original name."""
        return f"{original_name}{self.config.staging_suffix}"

    def register_staged_item(
        self, name: str, item_type: str, item_id: str,
    ) -> None:
        """Register an item deployed to staging."""
        self._staging_items.append({
            "name": name,
            "item_type": item_type,
            "item_id": item_id,
            "staged_at": datetime.now(tz=timezone.utc).isoformat(),
        })

    @property
    def staged_items(self) -> list[dict[str, str]]:
        return list(self._staging_items)

    def can_swap(self) -> bool:
        """Check if staging has items ready to swap."""
        return len(self._staging_items) > 0

    def swap_plan(self) -> list[dict[str, str]]:
        """Generate a swap plan: rename staging items to production names."""
        plan = []
        for item in self._staging_items:
            staging_name = item["name"]
            prod_name = staging_name.removesuffix(self.config.staging_suffix)
            plan.append({
                "action": "rename",
                "from": staging_name,
                "to": prod_name,
                "item_id": item["item_id"],
                "item_type": item["item_type"],
            })
        return plan

    def to_dict(self) -> dict[str, Any]:
        return {
            "staging_suffix": self.config.staging_suffix,
            "staged_items": self._staging_items,
            "can_swap": self.can_swap(),
        }


# =====================================================================
# Adaptive Rate Limiter
# =====================================================================


class AdaptiveRateLimiter:
    """Dynamically adjusts request rate based on API responses.

    Tracks 429 responses and exponentially backs off concurrency.
    Recovers concurrency slowly when requests succeed.
    """

    def __init__(self, initial_concurrency: int = 4, min_concurrency: int = 1) -> None:
        self._concurrency = initial_concurrency
        self._max_concurrency = initial_concurrency
        self._min_concurrency = min_concurrency
        self._throttle_count = 0
        self._success_count = 0
        self._last_retry_after: float = 0

    @property
    def concurrency(self) -> int:
        return self._concurrency

    def on_success(self) -> None:
        """Record a successful API call — slowly recover concurrency."""
        self._success_count += 1
        if self._success_count >= 10 and self._concurrency < self._max_concurrency:
            self._concurrency = min(self._concurrency + 1, self._max_concurrency)
            self._success_count = 0
            logger.debug("rate_limiter_recovered", concurrency=self._concurrency)

    def on_throttle(self, retry_after: float = 0) -> float:
        """Record a 429 throttle — reduce concurrency and return wait time."""
        self._throttle_count += 1
        self._success_count = 0
        self._concurrency = max(self._min_concurrency, self._concurrency // 2)
        wait = max(retry_after, 2.0 ** min(self._throttle_count, 6))
        self._last_retry_after = wait
        logger.warning(
            "rate_limiter_throttled",
            concurrency=self._concurrency,
            wait_seconds=wait,
            throttle_count=self._throttle_count,
        )
        return wait

    def reset(self) -> None:
        """Reset throttle state."""
        self._throttle_count = 0
        self._success_count = 0
        self._concurrency = self._max_concurrency

    def to_dict(self) -> dict[str, Any]:
        return {
            "concurrency": self._concurrency,
            "max_concurrency": self._max_concurrency,
            "throttle_count": self._throttle_count,
            "success_count": self._success_count,
        }


# =====================================================================
# Pipeline Scheduling
# =====================================================================


@dataclass
class ScheduleTrigger:
    """Configuration for a pipeline schedule trigger."""

    pipeline_name: str
    schedule_type: str = "daily"  # "daily", "hourly", "weekly", "cron"
    interval: int = 1
    cron_expression: str = ""
    start_time: str = ""
    enabled: bool = True

    def to_trigger_payload(self) -> dict[str, Any]:
        """Generate a Fabric-compatible trigger payload."""
        props: dict[str, Any] = {
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": self._frequency(),
                    "interval": self.interval,
                },
            },
        }
        if self.start_time:
            props["typeProperties"]["recurrence"]["startTime"] = self.start_time
        return props

    def _frequency(self) -> str:
        freq_map = {
            "daily": "Day",
            "hourly": "Hour",
            "weekly": "Week",
            "cron": "Day",
        }
        return freq_map.get(self.schedule_type, "Day")


def generate_schedule_config(
    triggers: list[ScheduleTrigger],
    output_dir: Path,
) -> Path:
    """Write schedule trigger configurations to a JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "schedule_triggers.json"
    data = [
        {
            "pipeline": t.pipeline_name,
            "trigger": t.to_trigger_payload(),
            "enabled": t.enabled,
        }
        for t in triggers
    ]
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.info("schedule_config_generated", count=len(triggers), path=str(path))
    return path
