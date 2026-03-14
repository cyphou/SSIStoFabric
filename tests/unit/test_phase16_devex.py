"""Tests for Phase 16 — Developer Experience."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.developer_experience import (
    ADR,
    CookbookEntry,
    generate_adrs,
    generate_config_json_schema,
    generate_cookbook,
    generate_decision_tree_html,
    generate_sphinx_conf,
    get_cookbook_entries,
    write_decision_tree,
    write_json_schema,
    write_sphinx_conf,
)

# ── JSON Schema ─────────────────────────────────────────────────────


class TestJsonSchema:
    def test_generate_returns_dict(self):
        schema = generate_config_json_schema()
        assert isinstance(schema, dict)
        assert "$schema" in schema

    def test_schema_has_title(self):
        schema = generate_config_json_schema()
        assert "SSIS to Fabric" in schema["title"]

    def test_schema_has_properties(self):
        schema = generate_config_json_schema()
        assert "properties" in schema

    def test_write_json_schema(self, tmp_path: Path):
        out = tmp_path / "sub" / "schema.json"
        result = write_json_schema(out)
        assert result.exists()
        data = json.loads(result.read_text(encoding="utf-8"))
        assert data["$schema"] == "https://json-schema.org/draft/2020-12/schema"

    def test_schema_roundtrip(self, tmp_path: Path):
        write_json_schema(tmp_path / "s.json")
        loaded = json.loads((tmp_path / "s.json").read_text(encoding="utf-8"))
        assert "properties" in loaded


# ── ADRs ─────────────────────────────────────────────────────────────


class TestADR:
    def test_adr_markdown(self):
        adr = ADR(number=1, title="Test Decision", context="ctx", decision="dec", consequences="cons")
        md = adr.to_markdown()
        assert "# ADR-0001: Test Decision" in md
        assert "## Context" in md
        assert "ctx" in md

    def test_adr_filename(self):
        adr = ADR(number=42, title="Use Pydantic For Config")
        assert adr.filename == "adr-0042-use-pydantic-for-config.md"

    def test_adr_default_date(self):
        adr = ADR(number=1, title="t")
        assert len(adr.date) == 10  # YYYY-MM-DD

    def test_adr_explicit_date(self):
        adr = ADR(number=1, title="t", date="2025-01-01")
        assert adr.date == "2025-01-01"

    def test_generate_adrs(self, tmp_path: Path):
        paths = generate_adrs(tmp_path)
        assert len(paths) >= 6  # 5 ADRs + README
        adr_dir = tmp_path / "docs" / "adr"
        assert adr_dir.exists()
        readme = adr_dir / "README.md"
        assert readme.exists()
        content = readme.read_text(encoding="utf-8")
        assert "ADR-0001" in content

    def test_adr_files_are_markdown(self, tmp_path: Path):
        paths = generate_adrs(tmp_path)
        md_files = [p for p in paths if p.name.startswith("adr-")]
        assert len(md_files) >= 5
        for f in md_files:
            assert f.suffix == ".md"


# ── Cookbook ──────────────────────────────────────────────────────────


class TestCookbook:
    def test_get_entries(self):
        entries = get_cookbook_entries()
        assert len(entries) >= 10
        assert all(isinstance(e, CookbookEntry) for e in entries)

    def test_entry_fields(self):
        entries = get_cookbook_entries()
        for e in entries:
            assert e.title
            assert e.ssis_pattern
            assert e.fabric_pattern

    def test_generate_cookbook(self, tmp_path: Path):
        path = generate_cookbook(tmp_path)
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "Migration Cookbook" in content
        assert "SSIS Pattern" in content
        assert "Fabric Equivalent" in content

    def test_cookbook_contains_all_entries(self, tmp_path: Path):
        path = generate_cookbook(tmp_path)
        content = path.read_text(encoding="utf-8")
        for entry in get_cookbook_entries():
            assert entry.title in content

    def test_cookbook_complexity_levels(self):
        entries = get_cookbook_entries()
        complexities = {e.complexity for e in entries}
        assert "low" in complexities
        assert "medium" in complexities
        assert "high" in complexities


# ── Decision Tree ────────────────────────────────────────────────────


class TestDecisionTree:
    def test_html_structure(self):
        html = generate_decision_tree_html()
        assert "<!DOCTYPE html>" in html
        assert "Migration Strategy Wizard" in html
        assert "<script>" in html

    def test_has_strategies(self):
        html = generate_decision_tree_html()
        assert "result-spark" in html
        assert "result-data_factory" in html
        assert "result-hybrid" in html

    def test_write_decision_tree(self, tmp_path: Path):
        path = write_decision_tree(tmp_path)
        assert path.exists()
        assert path.suffix == ".html"

    def test_html_no_external_deps(self):
        html = generate_decision_tree_html()
        # Should be self-contained
        assert "http" not in html.split("<body>")[1]


# ── Sphinx ───────────────────────────────────────────────────────────


class TestSphinx:
    def test_generate_conf(self):
        conf = generate_sphinx_conf()
        assert "sphinx.ext.autodoc" in conf
        assert "sphinx.ext.napoleon" in conf
        assert 'html_theme = "furo"' in conf

    def test_custom_project_name(self):
        conf = generate_sphinx_conf("My Project")
        assert 'project = "My Project"' in conf

    def test_write_sphinx_conf(self, tmp_path: Path):
        path = write_sphinx_conf(tmp_path)
        assert path.exists()
        assert (tmp_path / "docs" / "index.rst").exists()

    def test_index_rst_content(self, tmp_path: Path):
        write_sphinx_conf(tmp_path, "TestProj")
        content = (tmp_path / "docs" / "index.rst").read_text(encoding="utf-8")
        assert "TestProj" in content
        assert "api/modules" in content


# ── CLI Commands ─────────────────────────────────────────────────────


class TestCLICommands:
    def test_schema_export_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["schema-export", "--help"])
        assert result.exit_code == 0
        assert "JSON Schema" in result.output

    def test_generate_docs_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["generate-docs", "--help"])
        assert result.exit_code == 0
        assert "cookbook" in result.output.lower()

    def test_schema_export_creates_file(self, tmp_path: Path):
        runner = CliRunner()
        out = str(tmp_path / "schema.json")
        result = runner.invoke(main, ["schema-export", "-o", out])
        assert result.exit_code == 0
        assert Path(out).exists()

    def test_generate_docs_creates_artifacts(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(main, ["generate-docs", str(tmp_path)])
        assert result.exit_code == 0
        assert (tmp_path / "docs" / "migration_cookbook.md").exists()
        assert (tmp_path / "docs" / "adr").exists()
        assert (tmp_path / "docs" / "strategy_wizard.html").exists()
        assert (tmp_path / "docs" / "conf.py").exists()

    def test_generate_docs_selective(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(main, ["generate-docs", str(tmp_path), "--no-sphinx", "--no-wizard"])
        assert result.exit_code == 0
        assert (tmp_path / "docs" / "migration_cookbook.md").exists()
        assert not (tmp_path / "docs" / "conf.py").exists()
        assert not (tmp_path / "docs" / "strategy_wizard.html").exists()
