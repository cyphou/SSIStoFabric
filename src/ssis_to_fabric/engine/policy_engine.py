"""
Policy Engine & Governance
===========================
Phase 23: Declarative governance rules, deployment gates,
environment promotion, and data classification tagging.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Policy Rules
# =====================================================================


class PolicySeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class PolicyCategory(str, Enum):
    NAMING = "naming"
    SECURITY = "security"
    QUALITY = "quality"
    COMPLIANCE = "compliance"
    PERFORMANCE = "performance"
    CUSTOM = "custom"


@dataclass
class PolicyRule:
    """A declarative governance policy rule."""

    name: str
    description: str
    category: PolicyCategory
    severity: PolicySeverity = PolicySeverity.ERROR
    pattern: str = ""  # regex pattern for naming rules
    forbidden_patterns: list[str] = field(default_factory=list)
    required_tags: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "severity": self.severity.value,
            "pattern": self.pattern,
            "forbidden_patterns": self.forbidden_patterns,
            "required_tags": self.required_tags,
            "params": self.params,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PolicyRule:
        d = dict(data)
        d["category"] = PolicyCategory(d["category"])
        d["severity"] = PolicySeverity(d.get("severity", "error"))
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in valid})


@dataclass
class PolicyViolation:
    """A detected policy violation."""

    rule_name: str
    artifact_name: str
    artifact_type: str
    severity: PolicySeverity
    message: str
    line_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "artifact_name": self.artifact_name,
            "artifact_type": self.artifact_type,
            "severity": self.severity.value,
            "message": self.message,
            "line_number": self.line_number,
        }


# =====================================================================
# Policy Engine
# =====================================================================


class PolicyEngine:
    """Evaluates governance policies against migration artifacts."""

    def __init__(self, rules: list[PolicyRule] | None = None) -> None:
        self.rules = rules or []
        self.violations: list[PolicyViolation] = []

    def add_rule(self, rule: PolicyRule) -> None:
        self.rules.append(rule)

    def check_naming(self, artifact_name: str, artifact_type: str) -> list[PolicyViolation]:
        """Check artifact name against naming convention rules."""
        violations: list[PolicyViolation] = []
        for rule in self.rules:
            if rule.category != PolicyCategory.NAMING:
                continue
            if rule.pattern and not re.match(rule.pattern, artifact_name):
                v = PolicyViolation(
                    rule_name=rule.name,
                    artifact_name=artifact_name,
                    artifact_type=artifact_type,
                    severity=rule.severity,
                    message=f"Name '{artifact_name}' does not match pattern '{rule.pattern}'",
                )
                violations.append(v)
                self.violations.append(v)
        return violations

    def check_forbidden(self, content: str, artifact_name: str, artifact_type: str) -> list[PolicyViolation]:
        """Check content for forbidden patterns."""
        violations: list[PolicyViolation] = []
        for rule in self.rules:
            for pattern in rule.forbidden_patterns:
                matches = list(re.finditer(pattern, content, re.IGNORECASE))
                for m in matches:
                    line_num = content[:m.start()].count("\n") + 1
                    v = PolicyViolation(
                        rule_name=rule.name,
                        artifact_name=artifact_name,
                        artifact_type=artifact_type,
                        severity=rule.severity,
                        message=f"Forbidden pattern '{pattern}' found",
                        line_number=line_num,
                    )
                    violations.append(v)
                    self.violations.append(v)
        return violations

    def check_required_tags(self, tags: list[str], artifact_name: str, artifact_type: str) -> list[PolicyViolation]:
        """Check that required tags are present."""
        violations: list[PolicyViolation] = []
        for rule in self.rules:
            for req_tag in rule.required_tags:
                if req_tag not in tags:
                    v = PolicyViolation(
                        rule_name=rule.name,
                        artifact_name=artifact_name,
                        artifact_type=artifact_type,
                        severity=rule.severity,
                        message=f"Required tag '{req_tag}' missing",
                    )
                    violations.append(v)
                    self.violations.append(v)
        return violations

    def evaluate_artifact(
        self,
        name: str,
        artifact_type: str,
        content: str = "",
        tags: list[str] | None = None,
    ) -> list[PolicyViolation]:
        """Run all applicable policy checks on an artifact."""
        violations: list[PolicyViolation] = []
        violations.extend(self.check_naming(name, artifact_type))
        if content:
            violations.extend(self.check_forbidden(content, name, artifact_type))
        if tags is not None:
            violations.extend(self.check_required_tags(tags, name, artifact_type))
        return violations

    @property
    def has_errors(self) -> bool:
        return any(v.severity == PolicySeverity.ERROR for v in self.violations)

    def summary(self) -> dict[str, Any]:
        return {
            "total_rules": len(self.rules),
            "total_violations": len(self.violations),
            "errors": sum(1 for v in self.violations if v.severity == PolicySeverity.ERROR),
            "warnings": sum(1 for v in self.violations if v.severity == PolicySeverity.WARNING),
            "passed": not self.has_errors,
        }


# =====================================================================
# Environment Promotion
# =====================================================================


class PromotionStage(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class PromotionRequest:
    """Request to promote artifacts between environments."""

    artifact_name: str
    from_stage: PromotionStage
    to_stage: PromotionStage
    requested_by: str = ""
    approved_by: str = ""
    status: str = "pending"  # pending, approved, rejected, deployed
    timestamp: str = ""
    policy_check_passed: bool = False

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(tz=timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_name": self.artifact_name,
            "from_stage": self.from_stage.value,
            "to_stage": self.to_stage.value,
            "requested_by": self.requested_by,
            "approved_by": self.approved_by,
            "status": self.status,
            "timestamp": self.timestamp,
            "policy_check_passed": self.policy_check_passed,
        }


def create_promotion(
    artifact_name: str,
    from_stage: PromotionStage,
    to_stage: PromotionStage,
    engine: PolicyEngine,
    requested_by: str = "system",
) -> PromotionRequest:
    """Create a promotion request with policy gate check."""
    req = PromotionRequest(
        artifact_name=artifact_name,
        from_stage=from_stage,
        to_stage=to_stage,
        requested_by=requested_by,
    )

    # Run policy checks
    engine.evaluate_artifact(artifact_name, "promotion")
    req.policy_check_passed = not engine.has_errors

    if to_stage == PromotionStage.PROD and engine.has_errors:
        req.status = "rejected"
    elif not engine.has_errors:
        req.status = "approved"

    return req


# =====================================================================
# Data Classification
# =====================================================================


class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"
    PHI = "phi"
    FINANCIAL = "financial"
    RESTRICTED = "restricted"


@dataclass
class ClassificationTag:
    """Data classification tag for an artifact or column."""

    entity_name: str  # table or column name
    classification: DataClassification
    reason: str = ""
    auto_detected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_name": self.entity_name,
            "classification": self.classification.value,
            "reason": self.reason,
            "auto_detected": self.auto_detected,
        }


# PII detection patterns
_PII_PATTERNS: dict[str, DataClassification] = {
    r"(?i)(ssn|social.?security)": DataClassification.PII,
    r"(?i)(email|e.?mail)": DataClassification.PII,
    r"(?i)(phone|mobile|cell)": DataClassification.PII,
    r"(?i)(credit.?card|card.?number|ccn)": DataClassification.FINANCIAL,
    r"(?i)(salary|income|compensation|wage)": DataClassification.FINANCIAL,
    r"(?i)(diagnosis|medical|health|patient)": DataClassification.PHI,
    r"(?i)(password|secret|token|api.?key)": DataClassification.RESTRICTED,
    r"(?i)(address|street|zip.?code|postal)": DataClassification.PII,
    r"(?i)(birth.?date|dob|date.?of.?birth)": DataClassification.PII,
}


def auto_classify_columns(column_names: list[str]) -> list[ClassificationTag]:
    """Auto-detect data classifications from column names."""
    tags: list[ClassificationTag] = []
    for col in column_names:
        for pattern, classification in _PII_PATTERNS.items():
            if re.search(pattern, col):
                tags.append(ClassificationTag(
                    entity_name=col,
                    classification=classification,
                    reason=f"Matched pattern: {pattern}",
                    auto_detected=True,
                ))
                break
    return tags


# =====================================================================
# Report
# =====================================================================


def write_policy_report(engine: PolicyEngine, output_path: Path) -> Path:
    """Write policy check report to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "summary": engine.summary(),
        "rules": [r.to_dict() for r in engine.rules],
        "violations": [v.to_dict() for v in engine.violations],
    }
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("policy_report_written", path=str(output_path))
    return output_path
