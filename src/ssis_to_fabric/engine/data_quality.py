"""
Data Quality Framework
=======================
Phase 17: Column-level profiling, validation rules, pre/post-migration
reconciliation, and risk scoring.
"""

from __future__ import annotations

import hashlib
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
# Column Profiling
# =====================================================================


@dataclass
class ColumnProfile:
    """Statistical profile for a single column."""

    column_name: str
    data_type: str = "unknown"
    total_rows: int = 0
    null_count: int = 0
    distinct_count: int = 0
    min_value: str | None = None
    max_value: str | None = None
    avg_length: float = 0.0
    pattern_sample: str | None = None  # regex pattern detected

    @property
    def null_percentage(self) -> float:
        return (self.null_count / self.total_rows * 100) if self.total_rows > 0 else 0.0

    @property
    def cardinality_ratio(self) -> float:
        return (self.distinct_count / self.total_rows) if self.total_rows > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "null_percentage": round(self.null_percentage, 2),
            "distinct_count": self.distinct_count,
            "cardinality_ratio": round(self.cardinality_ratio, 4),
            "min_value": self.min_value,
            "max_value": self.max_value,
            "avg_length": round(self.avg_length, 2),
            "pattern_sample": self.pattern_sample,
        }


@dataclass
class TableProfile:
    """Profile for an entire table."""

    table_name: str
    row_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    profiled_at: str = ""

    def __post_init__(self) -> None:
        if not self.profiled_at:
            self.profiled_at = datetime.now(tz=timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "column_count": len(self.columns),
            "profiled_at": self.profiled_at,
            "columns": [c.to_dict() for c in self.columns],
        }


def profile_from_sample(table_name: str, rows: list[dict[str, Any]]) -> TableProfile:
    """Build a table profile from in-memory sample rows."""
    if not rows:
        return TableProfile(table_name=table_name)

    columns: dict[str, ColumnProfile] = {}
    for col in rows[0]:
        columns[col] = ColumnProfile(column_name=col, total_rows=len(rows))

    for row in rows:
        for col, val in row.items():
            cp = columns[col]
            if val is None:
                cp.null_count += 1
            else:
                s = str(val)
                cp.avg_length += len(s)
                if cp.min_value is None or s < cp.min_value:
                    cp.min_value = s
                if cp.max_value is None or s > cp.max_value:
                    cp.max_value = s

    for cp in columns.values():
        non_nulls = cp.total_rows - cp.null_count
        if non_nulls > 0:
            cp.avg_length = cp.avg_length / non_nulls
        cp.distinct_count = len({str(r.get(cp.column_name)) for r in rows if r.get(cp.column_name) is not None})

    return TableProfile(table_name=table_name, row_count=len(rows), columns=list(columns.values()))


# =====================================================================
# Validation Rules
# =====================================================================


class RuleType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    REGEX = "regex"
    REFERENTIAL = "referential"
    CUSTOM = "custom"


@dataclass
class ValidationRule:
    """A data quality validation rule."""

    name: str
    rule_type: RuleType
    column: str
    params: dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # error, warning, info

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "rule_type": self.rule_type.value,
            "column": self.column,
            "params": self.params,
            "severity": self.severity,
        }


@dataclass
class RuleResult:
    """Result of applying a validation rule."""

    rule: ValidationRule
    passed: bool
    violations: int = 0
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule.name,
            "rule_type": self.rule.rule_type.value,
            "column": self.rule.column,
            "passed": self.passed,
            "violations": self.violations,
            "message": self.message,
            "severity": self.rule.severity,
        }


def apply_rule(rule: ValidationRule, profile: ColumnProfile) -> RuleResult:
    """Apply a validation rule against a column profile."""
    if rule.rule_type == RuleType.NOT_NULL:
        passed = profile.null_count == 0
        return RuleResult(
            rule=rule, passed=passed, violations=profile.null_count,
            message="" if passed else f"{profile.null_count} null values found",
        )
    elif rule.rule_type == RuleType.UNIQUE:
        passed = profile.distinct_count == profile.total_rows - profile.null_count
        non_null = profile.total_rows - profile.null_count
        dups = max(0, non_null - profile.distinct_count)
        return RuleResult(
            rule=rule, passed=passed, violations=dups,
            message="" if passed else f"{dups} duplicate values detected",
        )
    elif rule.rule_type == RuleType.RANGE:
        min_val = rule.params.get("min")
        max_val = rule.params.get("max")
        issues = []
        try:
            if min_val is not None and profile.min_value is not None and float(profile.min_value) < float(min_val):
                issues.append(f"min {profile.min_value} < {min_val}")
            if max_val is not None and profile.max_value is not None and float(profile.max_value) > float(max_val):
                issues.append(f"max {profile.max_value} > {max_val}")
        except (ValueError, TypeError):
            pass
        return RuleResult(
            rule=rule, passed=len(issues) == 0, violations=len(issues),
            message="; ".join(issues),
        )
    elif rule.rule_type == RuleType.REGEX:
        pattern = rule.params.get("pattern", ".*")
        try:
            re.compile(pattern)
            if profile.pattern_sample and not re.match(pattern, profile.pattern_sample):
                return RuleResult(rule=rule, passed=False, violations=1, message=f"Sample does not match {pattern}")
        except re.error:
            return RuleResult(rule=rule, passed=False, violations=1, message=f"Invalid regex: {pattern}")
        return RuleResult(rule=rule, passed=True)
    return RuleResult(rule=rule, passed=True, message="Rule type not profiled")


# =====================================================================
# Risk Scoring
# =====================================================================


@dataclass
class QualityRiskScore:
    """Aggregate risk score for data quality."""

    table_name: str
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    warnings: int = 0
    score: float = 100.0  # 0-100, 100 = best

    @property
    def risk_level(self) -> str:
        if self.score >= 90:
            return "LOW"
        elif self.score >= 70:
            return "MEDIUM"
        elif self.score >= 50:
            return "HIGH"
        return "CRITICAL"

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_rules": self.total_rules,
            "passed": self.passed_rules,
            "failed": self.failed_rules,
            "warnings": self.warnings,
            "score": round(self.score, 1),
            "risk_level": self.risk_level,
        }


def compute_risk_score(results: list[RuleResult], table_name: str = "") -> QualityRiskScore:
    """Compute a quality risk score from validation results."""
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed_errors = sum(1 for r in results if not r.passed and r.rule.severity == "error")
    warnings = sum(1 for r in results if not r.passed and r.rule.severity == "warning")

    score = (passed / total * 100) if total > 0 else 100.0
    # Errors penalize more than warnings
    penalty = failed_errors * 10 + warnings * 3
    score = max(0, score - penalty)

    return QualityRiskScore(
        table_name=table_name,
        total_rules=total,
        passed_rules=passed,
        failed_rules=total - passed,
        warnings=warnings,
        score=score,
    )


# =====================================================================
# Reconciliation
# =====================================================================


@dataclass
class ReconciliationResult:
    """Result of comparing source and target data."""

    table_name: str
    source_row_count: int = 0
    target_row_count: int = 0
    row_count_match: bool = True
    checksum_match: bool = True
    column_mismatches: list[str] = field(default_factory=list)
    source_checksum: str = ""
    target_checksum: str = ""

    @property
    def passed(self) -> bool:
        return self.row_count_match and self.checksum_match and len(self.column_mismatches) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "row_count_match": self.row_count_match,
            "checksum_match": self.checksum_match,
            "column_mismatches": self.column_mismatches,
            "passed": self.passed,
        }


def reconcile_profiles(source: TableProfile, target: TableProfile) -> ReconciliationResult:
    """Compare source and target profiles for reconciliation."""
    result = ReconciliationResult(table_name=source.table_name)
    result.source_row_count = source.row_count
    result.target_row_count = target.row_count
    result.row_count_match = source.row_count == target.row_count

    src_cols = {c.column_name for c in source.columns}
    tgt_cols = {c.column_name for c in target.columns}
    missing_in_target = src_cols - tgt_cols
    extra_in_target = tgt_cols - src_cols

    for col in missing_in_target:
        result.column_mismatches.append(f"missing_in_target:{col}")
    for col in extra_in_target:
        result.column_mismatches.append(f"extra_in_target:{col}")

    # Compare checksums
    src_hash = hashlib.sha256(json.dumps(source.to_dict(), sort_keys=True).encode()).hexdigest()[:16]
    tgt_hash = hashlib.sha256(json.dumps(target.to_dict(), sort_keys=True).encode()).hexdigest()[:16]
    result.source_checksum = src_hash
    result.target_checksum = tgt_hash
    result.checksum_match = src_hash == tgt_hash

    return result


# =====================================================================
# Report Writer
# =====================================================================


def write_quality_report(
    profiles: list[TableProfile],
    rule_results: list[RuleResult],
    reconciliations: list[ReconciliationResult],
    output_path: Path,
) -> Path:
    """Write a JSON data quality report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "profiles": [p.to_dict() for p in profiles],
        "validation_results": [r.to_dict() for r in rule_results],
        "reconciliation": [r.to_dict() for r in reconciliations],
        "summary": {
            "tables_profiled": len(profiles),
            "rules_evaluated": len(rule_results),
            "rules_passed": sum(1 for r in rule_results if r.passed),
            "rules_failed": sum(1 for r in rule_results if not r.passed),
            "reconciliations_passed": sum(1 for r in reconciliations if r.passed),
        },
    }
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("quality_report_written", path=str(output_path))
    return output_path
