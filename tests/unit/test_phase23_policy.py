"""Tests for Phase 23 — Policy Engine & Governance."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.policy_engine import (
    DataClassification,
    PolicyCategory,
    PolicyEngine,
    PolicyRule,
    PolicySeverity,
    PolicyViolation,
    PromotionStage,
    auto_classify_columns,
    create_promotion,
    write_policy_report,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestPolicyRule:
    def test_to_dict(self):
        r = PolicyRule(name="r1", description="desc", category=PolicyCategory.NAMING, pattern=r"^[a-z]+$")
        d = r.to_dict()
        assert d["name"] == "r1"
        assert d["category"] == "naming"

    def test_from_dict(self):
        d = {"name": "r", "description": "d", "category": "security", "severity": "warning"}
        r = PolicyRule.from_dict(d)
        assert r.category == PolicyCategory.SECURITY
        assert r.severity == PolicySeverity.WARNING


class TestPolicyEngine:
    def test_naming_pass(self):
        rule = PolicyRule(
            name="naming", description="",
            category=PolicyCategory.NAMING, pattern=r"^[a-z_]+$",
        )
        engine = PolicyEngine([rule])
        v = engine.check_naming("my_pipeline", "pipeline")
        assert len(v) == 0

    def test_naming_fail(self):
        rule = PolicyRule(
            name="naming", description="",
            category=PolicyCategory.NAMING, pattern=r"^[a-z_]+$",
        )
        engine = PolicyEngine([rule])
        v = engine.check_naming("MyPipeline", "pipeline")
        assert len(v) == 1
        assert v[0].severity == PolicySeverity.ERROR

    def test_forbidden_pattern(self):
        engine = PolicyEngine([PolicyRule(
            name="no_secrets", description="", category=PolicyCategory.SECURITY,
            forbidden_patterns=[r"password\s*="],
        )])
        v = engine.check_forbidden('password = "secret123"', "config", "file")
        assert len(v) == 1

    def test_required_tags(self):
        engine = PolicyEngine([PolicyRule(
            name="tags", description="", category=PolicyCategory.COMPLIANCE,
            required_tags=["owner", "cost-center"],
        )])
        v = engine.check_required_tags(["owner"], "art", "pipeline")
        assert len(v) == 1  # missing cost-center

    def test_evaluate_artifact(self):
        engine = PolicyEngine([
            PolicyRule(name="naming", description="", category=PolicyCategory.NAMING, pattern=r"^[a-z_]+$"),
            PolicyRule(name="no_todo", description="", category=PolicyCategory.QUALITY, forbidden_patterns=["TODO"]),
        ])
        v = engine.evaluate_artifact("MyBadName", "pipeline", content="# TODO: fix this")
        assert len(v) == 2

    def test_has_errors(self):
        engine = PolicyEngine()
        assert not engine.has_errors
        engine.violations.append(PolicyViolation(
            rule_name="r", artifact_name="a", artifact_type="t",
            severity=PolicySeverity.ERROR, message="fail",
        ))
        assert engine.has_errors

    def test_summary(self):
        engine = PolicyEngine([PolicyRule(name="r", description="", category=PolicyCategory.NAMING)])
        s = engine.summary()
        assert s["total_rules"] == 1
        assert s["passed"]


class TestPromotion:
    def test_create_approved(self):
        engine = PolicyEngine()
        req = create_promotion("my_pipeline", PromotionStage.DEV, PromotionStage.STAGING, engine)
        assert req.status == "approved"
        assert req.policy_check_passed

    def test_prod_rejected_with_errors(self):
        engine = PolicyEngine([PolicyRule(
            name="naming", description="", category=PolicyCategory.NAMING, pattern=r"^[a-z]+$",
        )])
        # This will create a naming violation for "my_pipeline" (has underscore!)
        req = create_promotion("my_pipeline", PromotionStage.STAGING, PromotionStage.PROD, engine)
        assert req.status == "rejected"


class TestDataClassification:
    def test_detect_pii(self):
        tags = auto_classify_columns(["customer_email", "phone_number", "order_id"])
        assert any(t.classification == DataClassification.PII for t in tags)
        assert len(tags) >= 2

    def test_detect_financial(self):
        tags = auto_classify_columns(["employee_salary", "credit_card_number"])
        assert any(t.classification == DataClassification.FINANCIAL for t in tags)

    def test_detect_phi(self):
        tags = auto_classify_columns(["patient_diagnosis"])
        assert any(t.classification == DataClassification.PHI for t in tags)

    def test_no_classification(self):
        tags = auto_classify_columns(["order_id", "quantity", "status"])
        assert len(tags) == 0

    def test_auto_detected_flag(self):
        tags = auto_classify_columns(["ssn"])
        assert all(t.auto_detected for t in tags)


class TestPolicyReport:
    def test_write_report(self, tmp_path: Path):
        engine = PolicyEngine([PolicyRule(name="r", description="d", category=PolicyCategory.NAMING)])
        path = write_policy_report(engine, tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert "summary" in data


class TestCLI:
    def test_policy_check_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["policy-check", "--help"])
        assert result.exit_code == 0
