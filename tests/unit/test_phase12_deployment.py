"""Phase 12 – Deployment Hardening tests.

Tests cover:
- DeploymentState machine transitions
- DeploymentSnapshot lifecycle
- Snapshot persistence (save / load)
- Pre-deploy validation
- Blue-green deployment
- Adaptive rate limiter
- Schedule trigger generation
- CLI rollback / validate-deploy commands
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from ssis_to_fabric.engine.deployment_hardening import (
    AdaptiveRateLimiter,
    BlueGreenConfig,
    BlueGreenDeployment,
    DeploymentSnapshot,
    DeploymentState,
    InvalidStateTransitionError,
    ScheduleTrigger,
    ValidationResult,
    generate_schedule_config,
    load_latest_snapshot,
    load_snapshots,
    pre_deploy_validate,
    save_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path


# ── State Machine ──────────────────────────────────────────────────


class TestDeploymentState:
    def test_valid_transition_pending_to_queued(self):
        snap = DeploymentSnapshot(deployment_id="d1")
        snap.transition(DeploymentState.QUEUED)
        assert snap.state == DeploymentState.QUEUED

    def test_valid_transition_queued_to_in_progress(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.QUEUED)
        snap.transition(DeploymentState.IN_PROGRESS)
        assert snap.state == DeploymentState.IN_PROGRESS

    def test_valid_transition_in_progress_to_committed(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.IN_PROGRESS)
        snap.transition(DeploymentState.COMMITTED)
        assert snap.state == DeploymentState.COMMITTED
        assert snap.completed_at != ""

    def test_valid_transition_in_progress_to_rolled_back(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.IN_PROGRESS)
        snap.transition(DeploymentState.ROLLED_BACK)
        assert snap.state == DeploymentState.ROLLED_BACK

    def test_valid_transition_in_progress_to_validating(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.IN_PROGRESS)
        snap.transition(DeploymentState.VALIDATING)
        assert snap.state == DeploymentState.VALIDATING

    def test_valid_transition_validating_to_committed(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.VALIDATING)
        snap.transition(DeploymentState.COMMITTED)
        assert snap.state == DeploymentState.COMMITTED

    def test_valid_transition_pending_to_failed(self):
        snap = DeploymentSnapshot(deployment_id="d1")
        snap.transition(DeploymentState.FAILED)
        assert snap.state == DeploymentState.FAILED

    def test_valid_transition_failed_to_rolled_back(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.FAILED)
        snap.transition(DeploymentState.ROLLED_BACK)
        assert snap.state == DeploymentState.ROLLED_BACK

    def test_invalid_transition_pending_to_committed(self):
        snap = DeploymentSnapshot(deployment_id="d1")
        with pytest.raises(InvalidStateTransitionError):
            snap.transition(DeploymentState.COMMITTED)

    def test_invalid_transition_committed_terminal(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.COMMITTED)
        with pytest.raises(InvalidStateTransitionError):
            snap.transition(DeploymentState.ROLLED_BACK)

    def test_invalid_transition_rolled_back_terminal(self):
        snap = DeploymentSnapshot(deployment_id="d1", state=DeploymentState.ROLLED_BACK)
        with pytest.raises(InvalidStateTransitionError):
            snap.transition(DeploymentState.COMMITTED)

    def test_history_recorded(self):
        snap = DeploymentSnapshot(deployment_id="d1")
        snap.transition(DeploymentState.QUEUED)
        snap.transition(DeploymentState.IN_PROGRESS)
        assert len(snap.history) == 2
        assert snap.history[0]["from"] == "PENDING"
        assert snap.history[0]["to"] == "QUEUED"
        assert snap.history[1]["from"] == "QUEUED"
        assert snap.history[1]["to"] == "IN_PROGRESS"

    def test_all_state_values(self):
        assert set(DeploymentState) == {
            DeploymentState.PENDING,
            DeploymentState.QUEUED,
            DeploymentState.IN_PROGRESS,
            DeploymentState.VALIDATING,
            DeploymentState.COMMITTED,
            DeploymentState.ROLLED_BACK,
            DeploymentState.FAILED,
        }


# ── DeploymentSnapshot ─────────────────────────────────────────────


class TestDeploymentSnapshot:
    def test_add_item(self):
        snap = DeploymentSnapshot(deployment_id="d1")
        snap.add_item("Pipeline1", "Pipeline", "id-123")
        assert len(snap.items) == 1
        assert snap.items[0]["name"] == "Pipeline1"
        assert snap.items[0]["item_type"] == "Pipeline"
        assert snap.items[0]["item_id"] == "id-123"

    def test_to_dict_roundtrip(self):
        snap = DeploymentSnapshot(
            deployment_id="d1",
            workspace_id="ws1",
        )
        snap.transition(DeploymentState.QUEUED)
        snap.add_item("NB1", "Notebook")
        d = snap.to_dict()
        restored = DeploymentSnapshot.from_dict(d)
        assert restored.deployment_id == "d1"
        assert restored.state == DeploymentState.QUEUED
        assert restored.workspace_id == "ws1"
        assert len(restored.items) == 1
        assert len(restored.history) == 1

    def test_from_dict_defaults(self):
        d = {"deployment_id": "x", "state": "PENDING"}
        snap = DeploymentSnapshot.from_dict(d)
        assert snap.workspace_id == ""
        assert snap.items == []


# ── Snapshot Persistence ───────────────────────────────────────────


class TestSnapshotPersistence:
    def test_save_and_load(self, tmp_path: Path):
        snap = DeploymentSnapshot(deployment_id="d1", workspace_id="ws1")
        snap.add_item("P1", "Pipeline")
        save_snapshot(snap, tmp_path)

        loaded = load_snapshots(tmp_path)
        assert len(loaded) == 1
        assert loaded[0].deployment_id == "d1"

    def test_save_multiple(self, tmp_path: Path):
        for i in range(3):
            save_snapshot(DeploymentSnapshot(deployment_id=f"d{i}"), tmp_path)
        assert len(load_snapshots(tmp_path)) == 3

    def test_save_updates_existing(self, tmp_path: Path):
        snap = DeploymentSnapshot(deployment_id="d1")
        save_snapshot(snap, tmp_path)

        snap.transition(DeploymentState.QUEUED)
        save_snapshot(snap, tmp_path)

        loaded = load_snapshots(tmp_path)
        assert len(loaded) == 1
        assert loaded[0].state == DeploymentState.QUEUED

    def test_load_latest_snapshot(self, tmp_path: Path):
        save_snapshot(DeploymentSnapshot(deployment_id="d1"), tmp_path)
        save_snapshot(DeploymentSnapshot(deployment_id="d2"), tmp_path)
        latest = load_latest_snapshot(tmp_path)
        assert latest is not None
        assert latest.deployment_id == "d2"

    def test_load_latest_empty(self, tmp_path: Path):
        assert load_latest_snapshot(tmp_path) is None

    def test_load_snapshots_empty(self, tmp_path: Path):
        assert load_snapshots(tmp_path) == []


# ── Pre-Deploy Validation ─────────────────────────────────────────


class TestPreDeployValidation:
    def test_valid_output(self, tmp_path: Path):
        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "P1.json").write_text('{"name": "P1"}')
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "NB1.py").write_text("# notebook")

        result = pre_deploy_validate(tmp_path)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_invalid_json(self, tmp_path: Path):
        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "bad.json").write_text("{invalid json}")
        (tmp_path / "notebooks").mkdir()

        result = pre_deploy_validate(tmp_path)
        assert not result.is_valid
        assert any("Invalid JSON" in e.message for e in result.errors)

    def test_empty_notebook(self, tmp_path: Path):
        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "P1.json").write_text('{"a":1}')
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "empty.py").write_text("")

        result = pre_deploy_validate(tmp_path)
        assert not result.is_valid
        assert any("Empty notebook" in e.message for e in result.errors)

    def test_no_output_dirs(self, tmp_path: Path):
        result = pre_deploy_validate(tmp_path)
        assert not result.is_valid

    def test_naming_conflict(self, tmp_path: Path):
        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "Existing.json").write_text('{"ok": true}')
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "NB.py").write_text("# ok")

        workspace_items = [{"displayName": "Existing"}]
        result = pre_deploy_validate(tmp_path, workspace_items)
        assert result.is_valid  # conflicts are warnings, not errors
        assert len(result.warnings) == 1
        assert "already exists" in result.warnings[0].message

    def test_validation_result_properties(self):
        from ssis_to_fabric.engine.deployment_hardening import ValidationIssue

        r = ValidationResult(issues=[
            ValidationIssue(severity="error", message="e1"),
            ValidationIssue(severity="warning", message="w1"),
            ValidationIssue(severity="warning", message="w2"),
        ])
        assert not r.is_valid
        assert len(r.errors) == 1
        assert len(r.warnings) == 2

    def test_empty_result_is_valid(self):
        assert ValidationResult().is_valid


# ── Blue-Green Deployment ──────────────────────────────────────────


class TestBlueGreenDeployment:
    def test_staging_name(self):
        bg = BlueGreenDeployment()
        assert bg.staging_name("MyPipeline") == "MyPipeline_staging"

    def test_custom_suffix(self):
        bg = BlueGreenDeployment(BlueGreenConfig(staging_suffix="_v2"))
        assert bg.staging_name("P") == "P_v2"

    def test_register_staged_item(self):
        bg = BlueGreenDeployment()
        bg.register_staged_item("P_staging", "Pipeline", "id1")
        assert len(bg.staged_items) == 1
        assert bg.staged_items[0]["name"] == "P_staging"

    def test_can_swap_empty(self):
        bg = BlueGreenDeployment()
        assert not bg.can_swap()

    def test_can_swap_with_items(self):
        bg = BlueGreenDeployment()
        bg.register_staged_item("P_staging", "Pipeline", "id1")
        assert bg.can_swap()

    def test_swap_plan(self):
        bg = BlueGreenDeployment()
        bg.register_staged_item("P1_staging", "Pipeline", "id1")
        bg.register_staged_item("NB1_staging", "Notebook", "id2")

        plan = bg.swap_plan()
        assert len(plan) == 2
        assert plan[0]["from"] == "P1_staging"
        assert plan[0]["to"] == "P1"
        assert plan[0]["action"] == "rename"
        assert plan[1]["from"] == "NB1_staging"
        assert plan[1]["to"] == "NB1"

    def test_to_dict(self):
        bg = BlueGreenDeployment()
        bg.register_staged_item("X_staging", "Pipeline", "id")
        d = bg.to_dict()
        assert d["can_swap"] is True
        assert d["staging_suffix"] == "_staging"
        assert len(d["staged_items"]) == 1

    def test_staged_items_returns_copy(self):
        bg = BlueGreenDeployment()
        bg.register_staged_item("X_staging", "Pipeline", "id")
        items = bg.staged_items
        items.clear()
        assert len(bg.staged_items) == 1  # original unchanged


# ── Adaptive Rate Limiter ──────────────────────────────────────────


class TestAdaptiveRateLimiter:
    def test_initial_concurrency(self):
        rl = AdaptiveRateLimiter(initial_concurrency=8)
        assert rl.concurrency == 8

    def test_on_throttle_halves(self):
        rl = AdaptiveRateLimiter(initial_concurrency=4)
        rl.on_throttle()
        assert rl.concurrency == 2

    def test_on_throttle_respects_minimum(self):
        rl = AdaptiveRateLimiter(initial_concurrency=2, min_concurrency=1)
        rl.on_throttle()
        assert rl.concurrency == 1
        rl.on_throttle()
        assert rl.concurrency == 1

    def test_on_throttle_returns_wait(self):
        rl = AdaptiveRateLimiter()
        wait = rl.on_throttle(retry_after=5.0)
        assert wait >= 5.0

    def test_on_throttle_exponential_backoff(self):
        rl = AdaptiveRateLimiter()
        w1 = rl.on_throttle()
        w2 = rl.on_throttle()
        assert w2 > w1

    def test_on_success_recovery(self):
        rl = AdaptiveRateLimiter(initial_concurrency=4)
        rl.on_throttle()
        assert rl.concurrency == 2

        for _ in range(10):
            rl.on_success()
        assert rl.concurrency == 3  # recovered by 1

    def test_on_success_no_overflow(self):
        rl = AdaptiveRateLimiter(initial_concurrency=4)
        for _ in range(100):
            rl.on_success()
        assert rl.concurrency == 4  # never exceeds max

    def test_reset(self):
        rl = AdaptiveRateLimiter(initial_concurrency=4)
        rl.on_throttle()
        rl.on_throttle()
        rl.reset()
        assert rl.concurrency == 4

    def test_to_dict(self):
        rl = AdaptiveRateLimiter(initial_concurrency=4)
        rl.on_throttle()
        d = rl.to_dict()
        assert d["concurrency"] == 2
        assert d["max_concurrency"] == 4
        assert d["throttle_count"] == 1


# ── Schedule Triggers ──────────────────────────────────────────────


class TestScheduleTrigger:
    def test_daily_payload(self):
        t = ScheduleTrigger(pipeline_name="P1", schedule_type="daily", interval=1)
        payload = t.to_trigger_payload()
        assert payload["type"] == "ScheduleTrigger"
        assert payload["typeProperties"]["recurrence"]["frequency"] == "Day"
        assert payload["typeProperties"]["recurrence"]["interval"] == 1

    def test_hourly_payload(self):
        t = ScheduleTrigger(pipeline_name="P1", schedule_type="hourly", interval=2)
        payload = t.to_trigger_payload()
        assert payload["typeProperties"]["recurrence"]["frequency"] == "Hour"

    def test_weekly_payload(self):
        t = ScheduleTrigger(pipeline_name="P1", schedule_type="weekly")
        payload = t.to_trigger_payload()
        assert payload["typeProperties"]["recurrence"]["frequency"] == "Week"

    def test_start_time(self):
        t = ScheduleTrigger(
            pipeline_name="P1",
            start_time="2025-01-01T06:00:00Z",
        )
        payload = t.to_trigger_payload()
        assert payload["typeProperties"]["recurrence"]["startTime"] == "2025-01-01T06:00:00Z"

    def test_no_start_time(self):
        t = ScheduleTrigger(pipeline_name="P1")
        payload = t.to_trigger_payload()
        assert "startTime" not in payload["typeProperties"]["recurrence"]

    def test_generate_schedule_config(self, tmp_path: Path):
        triggers = [
            ScheduleTrigger(pipeline_name="P1", schedule_type="daily"),
            ScheduleTrigger(pipeline_name="P2", schedule_type="hourly", interval=4),
        ]
        path = generate_schedule_config(triggers, tmp_path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data) == 2
        assert data[0]["pipeline"] == "P1"
        assert data[1]["pipeline"] == "P2"

    def test_generate_schedule_config_creates_dir(self, tmp_path: Path):
        out = tmp_path / "sub" / "dir"
        generate_schedule_config(
            [ScheduleTrigger(pipeline_name="P")], out,
        )
        assert (out / "schedule_triggers.json").exists()


# ── CLI Commands ───────────────────────────────────────────────────


class TestCLICommands:
    def test_rollback_command_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", "--help"])
        assert result.exit_code == 0
        assert "Roll back" in result.output

    def test_validate_deploy_command_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["validate-deploy", "--help"])
        assert result.exit_code == 0
        assert "Validate migration output" in result.output

    def test_rollback_no_snapshots(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", str(tmp_path)])
        assert result.exit_code == 1
        assert "No deployment snapshots found" in result.output

    def test_rollback_marks_rolled_back(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        snap = DeploymentSnapshot(
            deployment_id="test-deploy",
            state=DeploymentState.IN_PROGRESS,
        )
        snap.add_item("P1", "Pipeline")
        save_snapshot(snap, tmp_path)

        # The rollback command transitions from IN_PROGRESS → ROLLED_BACK
        runner = CliRunner()
        result = runner.invoke(main, ["rollback", str(tmp_path)])
        assert result.exit_code == 0
        assert "ROLLED_BACK" in result.output

        # Verify persisted state
        loaded = load_latest_snapshot(tmp_path)
        assert loaded is not None
        assert loaded.state == DeploymentState.ROLLED_BACK

    def test_rollback_already_rolled_back(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        snap = DeploymentSnapshot(
            deployment_id="d1",
            state=DeploymentState.ROLLED_BACK,
        )
        save_snapshot(snap, tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", str(tmp_path)])
        assert result.exit_code == 0
        assert "already rolled back" in result.output

    def test_rollback_specific_id(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        save_snapshot(DeploymentSnapshot(deployment_id="d1", state=DeploymentState.IN_PROGRESS), tmp_path)
        save_snapshot(DeploymentSnapshot(deployment_id="d2", state=DeploymentState.IN_PROGRESS), tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", str(tmp_path), "--deployment-id", "d1"])
        assert result.exit_code == 0
        assert "d1" in result.output

    def test_rollback_nonexistent_id(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        save_snapshot(DeploymentSnapshot(deployment_id="d1"), tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", str(tmp_path), "--deployment-id", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_validate_deploy_valid(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "P1.json").write_text('{"name": "P1"}')
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "NB1.py").write_text("# notebook")

        runner = CliRunner()
        result = runner.invoke(main, ["validate-deploy", str(tmp_path)])
        assert result.exit_code == 0
        assert "passed" in result.output

    def test_validate_deploy_invalid(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        (tmp_path / "pipelines").mkdir()
        (tmp_path / "pipelines" / "bad.json").write_text("{broken")

        runner = CliRunner()
        result = runner.invoke(main, ["validate-deploy", str(tmp_path)])
        assert result.exit_code == 1
        assert "error" in result.output.lower()
