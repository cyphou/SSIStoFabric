"""Tests for Phase 33 — Auto-Remediation & Self-Healing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_to_fabric.engine.remediation import (
    Confidence,
    RemediationEngine,
    RemediationOpportunity,
    RemediationReport,
    RemediationResult,
    RemediationTemplate,
    TodoCategory,
    _classify_todo,
    _levenshtein,
    fuzzy_match_connections,
    get_template_by_id,
    get_templates,
    scan_directory,
    scan_file,
)


# =====================================================================
# Template Library
# =====================================================================


class TestTemplateLibrary:
    """Tests for the built-in remediation template library."""

    def test_templates_not_empty(self):
        templates = get_templates()
        assert len(templates) >= 30

    def test_template_ids_unique(self):
        templates = get_templates()
        ids = [t.id for t in templates]
        assert len(ids) == len(set(ids)), f"Duplicate IDs: {[x for x in ids if ids.count(x) > 1]}"

    def test_get_template_by_id(self):
        t = get_template_by_id("conn_name")
        assert t is not None
        assert t.category == TodoCategory.CONFIGURATION

    def test_get_template_by_id_missing(self):
        assert get_template_by_id("nonexistent_xyz") is None

    def test_all_templates_have_required_fields(self):
        for t in get_templates():
            assert t.id
            assert t.pattern
            assert isinstance(t.category, TodoCategory)
            assert isinstance(t.confidence, Confidence)
            assert t.replacement

    def test_template_to_dict(self):
        t = get_template_by_id("conn_name")
        d = t.to_dict()
        assert d["id"] == "conn_name"
        assert d["confidence"] == "HIGH"
        assert "pattern" in d

    @pytest.mark.parametrize(
        "template_id,expected_cat",
        [
            ("conn_name", TodoCategory.CONFIGURATION),
            ("derived_expr", TodoCategory.EXPRESSION),
            ("script_manual", TodoCategory.MANUAL),
            ("lookup_verify", TodoCategory.VERIFICATION),
            ("raw_file_reader", TodoCategory.CONNECTION),
        ],
    )
    def test_template_categories(self, template_id, expected_cat):
        t = get_template_by_id(template_id)
        assert t is not None
        assert t.category == expected_cat


# =====================================================================
# TODO Classification
# =====================================================================


class TestTodoClassification:

    def test_connection_classification(self):
        assert _classify_todo("Set connection name for gateway") == TodoCategory.CONNECTION

    def test_manual_classification(self):
        assert _classify_todo("Manually translate the script logic") == TodoCategory.MANUAL

    def test_verification_classification(self):
        assert _classify_todo("verify lookup query") == TodoCategory.VERIFICATION

    def test_expression_classification(self):
        assert _classify_todo("Replace with actual condition expression") == TodoCategory.EXPRESSION

    def test_default_classification(self):
        assert _classify_todo("Set BK column(s)") == TodoCategory.CONFIGURATION


# =====================================================================
# Levenshtein Distance
# =====================================================================


class TestLevenshtein:

    def test_identical(self):
        assert _levenshtein("abc", "abc") == 0

    def test_empty(self):
        assert _levenshtein("abc", "") == 3
        assert _levenshtein("", "abc") == 3

    def test_single_edit(self):
        assert _levenshtein("abc", "ab") == 1

    def test_completely_different(self):
        assert _levenshtein("abc", "xyz") == 3

    def test_case_sensitive(self):
        assert _levenshtein("A", "a") == 1


# =====================================================================
# Fuzzy Connection Matching
# =====================================================================


class TestFuzzyMatchConnections:

    def test_exact_match(self):
        results = fuzzy_match_connections(
            ["OLEDB_Source"], ["OLEDB_Source"], threshold=0.5,
        )
        assert len(results) == 1
        assert results[0]["similarity"] == 1.0
        assert results[0]["confidence"] == "HIGH"

    def test_close_match(self):
        results = fuzzy_match_connections(
            ["OLEDB-Source"], ["OLEDB_Source"], threshold=0.5,
        )
        assert len(results) == 1
        assert results[0]["similarity"] >= 0.8

    def test_no_match_below_threshold(self):
        results = fuzzy_match_connections(
            ["CompleteDifferent"], ["OLEDB_Source"], threshold=0.9,
        )
        assert len(results) == 0

    def test_multiple_sources(self):
        ssis = ["Source_A", "Source_B"]
        fabric = ["Source_A", "Source_B", "Source_C"]
        results = fuzzy_match_connections(ssis, fabric, threshold=0.5)
        assert len(results) == 2

    def test_confidence_levels(self):
        # Exact match should be HIGH
        r = fuzzy_match_connections(["X"], ["X"], threshold=0.5)
        assert r[0]["confidence"] == "HIGH"

        # Similar match should be MEDIUM
        r = fuzzy_match_connections(["Source_Name"], ["Source-Name"], threshold=0.5)
        assert r[0]["confidence"] in ("HIGH", "MEDIUM")

    def test_empty_lists(self):
        assert fuzzy_match_connections([], ["X"], threshold=0.5) == []
        assert fuzzy_match_connections(["X"], [], threshold=0.5) == []


# =====================================================================
# Scanner
# =====================================================================


class TestScanner:

    def test_scan_file_with_todos(self, tmp_path):
        nb = tmp_path / "notebooks" / "test.py"
        nb.parent.mkdir()
        nb.write_text(
            "# cell 1\n"
            "df = spark.read.table('source')\n"
            "# TODO: set connection name\n"
            "# TODO: Manually translate the script logic\n"
            "df.write.mode('overwrite').saveAsTable('dest')\n",
            encoding="utf-8",
        )
        opps = scan_file(nb)
        assert len(opps) == 2
        assert opps[0].line_number == 3
        assert opps[0].matched_template is not None
        assert opps[0].matched_template.id == "conn_name"

    def test_scan_file_no_todos(self, tmp_path):
        nb = tmp_path / "notebooks" / "clean.py"
        nb.parent.mkdir()
        nb.write_text("# no todos here\nprint('hello')\n", encoding="utf-8")
        assert scan_file(nb) == []

    def test_scan_directory(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "a.py").write_text("# TODO: set connection name\n", encoding="utf-8")
        (nb_dir / "b.py").write_text("# TODO: verify lookup query\n", encoding="utf-8")
        pipe_dir = tmp_path / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "c.json").write_text('{"comment": "# TODO: unknown op"}\n', encoding="utf-8")

        opps = scan_directory(tmp_path)
        assert len(opps) == 3

    def test_scan_nonexistent_file(self, tmp_path):
        assert scan_file(tmp_path / "nonexistent.py") == []

    def test_context_lines_captured(self, tmp_path):
        nb = tmp_path / "notebooks" / "ctx.py"
        nb.parent.mkdir()
        nb.write_text("line1\nline2\n# TODO: Set sort column(s)\nline4\nline5\n", encoding="utf-8")
        opps = scan_file(nb)
        assert len(opps) == 1
        # 2 lines before + TODO line + 2 lines after = up to 5
        assert len(opps[0].context_lines) >= 3

    def test_opportunity_to_dict(self, tmp_path):
        nb = tmp_path / "notebooks" / "ser.py"
        nb.parent.mkdir()
        nb.write_text("# TODO: set connection name\n", encoding="utf-8")
        opps = scan_file(nb)
        d = opps[0].to_dict()
        assert "file_path" in d
        assert "matched_template" in d


# =====================================================================
# Remediation Engine
# =====================================================================


class TestRemediationEngine:

    def _make_notebook(self, tmp_path, content: str) -> Path:
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir(exist_ok=True)
        nb = nb_dir / "test_notebook.py"
        nb.write_text(content, encoding="utf-8")
        return nb

    def test_dry_run_does_not_modify(self, tmp_path):
        content = "# TODO: set connection name\ndf = spark.read.table('t')\n"
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(dry_run=True)
        results = engine.remediate_file(nb)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].patch_preview
        # File should be unchanged
        assert nb.read_text(encoding="utf-8") == content

    def test_apply_remediation(self, tmp_path):
        content = "# TODO: set connection name\ndf = spark.read.table('t')\n"
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(dry_run=False)
        results = engine.remediate_file(nb)
        assert len(results) == 1
        assert results[0].status == "applied"
        new_content = nb.read_text(encoding="utf-8")
        assert "# TODO" not in new_content
        assert "auto-resolved" in new_content.lower() or "connection" in new_content.lower()

    def test_min_confidence_filter(self, tmp_path):
        # script_manual is LOW confidence
        content = "# TODO: Manually translate the script logic\n"
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(min_confidence=Confidence.HIGH, dry_run=False)
        results = engine.remediate_file(nb)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Below min confidence" in results[0].error

    def test_no_matching_template(self, tmp_path):
        content = "# TODO: some entirely custom thing nobody ever wrote a template for\n"
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(dry_run=False)
        results = engine.remediate_file(nb)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].error == "No matching template"

    def test_remediate_directory(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "a.py").write_text("# TODO: set connection name\n", encoding="utf-8")
        (nb_dir / "b.py").write_text("# TODO: Set sort column(s)\n", encoding="utf-8")

        engine = RemediationEngine(dry_run=True)
        report = engine.remediate_directory(tmp_path)
        assert isinstance(report, RemediationReport)
        assert report.total_scanned == 2
        assert report.total_todos == 2

    def test_report_to_dict(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "a.py").write_text("# TODO: set connection name\n", encoding="utf-8")

        engine = RemediationEngine(dry_run=True)
        report = engine.remediate_directory(tmp_path)
        d = report.to_dict()
        assert "total_scanned" in d
        assert "results" in d
        assert "by_category" in d

    def test_write_report_json(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "a.py").write_text("# TODO: set connection name\n", encoding="utf-8")

        engine = RemediationEngine(dry_run=True)
        report = engine.remediate_directory(tmp_path)
        report_path = engine.write_report(report, tmp_path)
        assert report_path.exists()
        data = json.loads(report_path.read_text(encoding="utf-8"))
        assert data["total_scanned"] == 1

    def test_multiple_todos_same_file(self, tmp_path):
        content = (
            "# TODO: set connection name\n"
            "df = spark.read.table('t')\n"
            "# TODO: Set sort column(s)\n"
            "# TODO: verify lookup query\n"
        )
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(dry_run=False)
        results = engine.remediate_file(nb)
        assert len(results) == 3
        applied_count = sum(1 for r in results if r.status == "applied")
        assert applied_count == 3

    def test_indentation_preserved(self, tmp_path):
        content = "    # TODO: set connection name\n"
        nb = self._make_notebook(tmp_path, content)
        engine = RemediationEngine(dry_run=False)
        engine.remediate_file(nb)
        new_content = nb.read_text(encoding="utf-8")
        # Replacement should maintain 4-space indent
        for line in new_content.splitlines():
            if line.strip():
                assert line.startswith("    ")


# =====================================================================
# Result & Report Serialization
# =====================================================================


class TestSerialization:

    def test_result_to_dict(self):
        opp = RemediationOpportunity(
            file_path="test.py", line_number=5, todo_text="Fix me",
            category=TodoCategory.MANUAL,
        )
        result = RemediationResult(
            opportunity=opp, status="applied", confidence=Confidence.HIGH,
        )
        d = result.to_dict()
        assert d["status"] == "applied"
        assert d["confidence"] == "HIGH"
        assert d["file"] == "test.py"

    def test_report_by_category_and_confidence(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "a.py").write_text(
            "# TODO: set connection name\n"
            "# TODO: Manually translate the script logic\n"
            "# TODO: verify lookup query\n",
            encoding="utf-8",
        )
        engine = RemediationEngine(dry_run=True)
        report = engine.remediate_directory(tmp_path)
        assert len(report.by_category) >= 2
        assert len(report.by_confidence) >= 1


# =====================================================================
# Template Pattern Matching (regression for each template)
# =====================================================================


class TestTemplatePatternMatching:
    """Verify that each template's regex matches the expected TODO text."""

    @pytest.mark.parametrize(
        "todo_line,expected_id",
        [
            ("# TODO: set connection name", "conn_name"),
            ("# TODO: set BK column(s)", "bk_columns"),
            ("# TODO: Set sort column(s)", "sort_columns"),
            ("# TODO: Set group-by column(s)", "group_by"),
            ("# TODO: Set aggregate functions", "aggregate_functions"),
            ("# TODO: Set correct join key(s)", "join_keys"),
            ("# TODO: Specify columns to copy", "copy_columns"),
            ("# TODO: Configure source data read", "configure_source"),
            ("# TODO: Configure pivot operation", "configure_pivot"),
            ("# TODO: Configure unpivot", "configure_unpivot"),
            ("# TODO: Adjust stack parameters", "configure_unpivot"),
            ("# TODO: Set derived expression", "derived_expr"),
            ("# TODO: Replace with actual condition", "split_condition"),
            ("# TODO: Adjust threshold", "fuzzy_threshold"),
            ("# TODO: verify lookup query", "lookup_verify"),
            ("# TODO: verify sort column", "sort_verify"),
            ("# TODO: Manually translate the script logic", "script_manual"),
            ("# TODO: Translate row-by-row command execution", "oledb_command"),
            ("# TODO: Translate external process execution", "process_task"),
            ("# TODO: Translate SSIS Web Service Task", "webservice_task"),
            ("# TODO: Translate SSIS XML Task", "xml_task"),
            ("# TODO: WMI queries are Windows-specific", "wmi_task"),
            ("# TODO: Manually implement this transformation", "unknown_transform"),
            ("# TODO: Fabric Copy does not support Excel sink", "excel_sink"),
            ("# TODO: unknown op", "unknown_fs_op"),
        ],
    )
    def test_pattern_matches(self, todo_line, expected_id, tmp_path):
        nb = tmp_path / "notebooks" / "test.py"
        nb.parent.mkdir(exist_ok=True)
        nb.write_text(todo_line + "\n", encoding="utf-8")
        opps = scan_file(nb)
        assert len(opps) == 1, f"Expected 1 TODO match for: {todo_line}"
        assert opps[0].matched_template is not None, f"No template matched for: {todo_line}"
        assert opps[0].matched_template.id == expected_id


# =====================================================================
# Edge Cases
# =====================================================================


class TestEdgeCases:

    def test_empty_directory(self, tmp_path):
        (tmp_path / "notebooks").mkdir()
        engine = RemediationEngine(dry_run=True)
        report = engine.remediate_directory(tmp_path)
        assert report.total_scanned == 0
        assert report.total_todos == 0

    def test_binary_file_skipped(self, tmp_path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "binary.py").write_bytes(b"\x00\x01\x02\xff" * 100)
        opps = scan_file(nb_dir / "binary.py")
        assert opps == []

    def test_scan_empty_file(self, tmp_path):
        nb = tmp_path / "notebooks" / "empty.py"
        nb.parent.mkdir(exist_ok=True)
        nb.write_text("", encoding="utf-8")
        assert scan_file(nb) == []
