"""Tests for Phase 17 — Data Quality Framework."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.data_quality import (
    ColumnProfile,
    RuleResult,
    RuleType,
    TableProfile,
    ValidationRule,
    apply_rule,
    compute_risk_score,
    profile_from_sample,
    reconcile_profiles,
    write_quality_report,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestColumnProfile:
    def test_null_percentage(self):
        cp = ColumnProfile(column_name="a", total_rows=100, null_count=25)
        assert cp.null_percentage == 25.0

    def test_cardinality(self):
        cp = ColumnProfile(column_name="a", total_rows=100, distinct_count=50)
        assert cp.cardinality_ratio == 0.5

    def test_to_dict(self):
        cp = ColumnProfile(column_name="id", total_rows=10, null_count=0, distinct_count=10)
        d = cp.to_dict()
        assert d["column_name"] == "id"
        assert d["null_percentage"] == 0.0


class TestTableProfile:
    def test_from_sample(self):
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": None}]
        tp = profile_from_sample("test_table", rows)
        assert tp.row_count == 3
        assert len(tp.columns) == 2

    def test_empty_sample(self):
        tp = profile_from_sample("empty", [])
        assert tp.row_count == 0

    def test_to_dict(self):
        tp = TableProfile(table_name="t", row_count=5)
        d = tp.to_dict()
        assert d["table_name"] == "t"


class TestValidationRules:
    def test_not_null_pass(self):
        rule = ValidationRule(name="r", rule_type=RuleType.NOT_NULL, column="a")
        cp = ColumnProfile(column_name="a", total_rows=10, null_count=0)
        result = apply_rule(rule, cp)
        assert result.passed

    def test_not_null_fail(self):
        rule = ValidationRule(name="r", rule_type=RuleType.NOT_NULL, column="a")
        cp = ColumnProfile(column_name="a", total_rows=10, null_count=3)
        result = apply_rule(rule, cp)
        assert not result.passed
        assert result.violations == 3

    def test_unique_pass(self):
        rule = ValidationRule(name="r", rule_type=RuleType.UNIQUE, column="a")
        cp = ColumnProfile(column_name="a", total_rows=10, null_count=0, distinct_count=10)
        assert apply_rule(rule, cp).passed

    def test_unique_fail(self):
        rule = ValidationRule(name="r", rule_type=RuleType.UNIQUE, column="a")
        cp = ColumnProfile(column_name="a", total_rows=10, null_count=0, distinct_count=5)
        result = apply_rule(rule, cp)
        assert not result.passed

    def test_range_pass(self):
        rule = ValidationRule(name="r", rule_type=RuleType.RANGE, column="a", params={"min": "0", "max": "100"})
        cp = ColumnProfile(column_name="a", total_rows=10, min_value="5", max_value="50")
        assert apply_rule(rule, cp).passed

    def test_range_fail(self):
        rule = ValidationRule(name="r", rule_type=RuleType.RANGE, column="a", params={"max": "50"})
        cp = ColumnProfile(column_name="a", total_rows=10, min_value="0", max_value="999")
        assert not apply_rule(rule, cp).passed

    def test_regex_pass(self):
        rule = ValidationRule(name="r", rule_type=RuleType.REGEX, column="a", params={"pattern": r"\d+"})
        cp = ColumnProfile(column_name="a", total_rows=10, pattern_sample="123")
        assert apply_rule(rule, cp).passed


class TestRiskScore:
    def test_perfect_score(self):
        results = [RuleResult(rule=ValidationRule(name="r", rule_type=RuleType.NOT_NULL, column="a"), passed=True)]
        score = compute_risk_score(results, "t")
        assert score.score == 100.0
        assert score.risk_level == "LOW"

    def test_failed_score(self):
        results = [
            RuleResult(
                rule=ValidationRule(name="r", rule_type=RuleType.NOT_NULL, column="a", severity="error"),
                passed=False,
            ),
        ]
        score = compute_risk_score(results, "t")
        assert score.risk_level in ("MEDIUM", "HIGH", "CRITICAL")


class TestReconciliation:
    def test_matching_profiles(self):
        src = TableProfile(table_name="t", row_count=100, columns=[ColumnProfile(column_name="id")])
        tgt = TableProfile(table_name="t", row_count=100, columns=[ColumnProfile(column_name="id")])
        result = reconcile_profiles(src, tgt)
        assert result.row_count_match

    def test_row_count_mismatch(self):
        src = TableProfile(table_name="t", row_count=100)
        tgt = TableProfile(table_name="t", row_count=90)
        result = reconcile_profiles(src, tgt)
        assert not result.row_count_match

    def test_column_mismatch(self):
        src = TableProfile(table_name="t", columns=[ColumnProfile(column_name="a"), ColumnProfile(column_name="b")])
        tgt = TableProfile(table_name="t", columns=[ColumnProfile(column_name="a")])
        result = reconcile_profiles(src, tgt)
        assert len(result.column_mismatches) > 0


class TestQualityReport:
    def test_write_report(self, tmp_path: Path):
        path = write_quality_report([], [], [], tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert "summary" in data


class TestCLI:
    def test_data_quality_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["data-quality", "--help"])
        assert result.exit_code == 0
