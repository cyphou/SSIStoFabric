"""Tests for Phase 18 — GitOps & Artifact Versioning."""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.gitops import (
    ChangeLog,
    ChangeRecord,
    diff_artifacts,
    generate_default_fabricignore,
    parse_fabricignore,
    should_ignore,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestChangeRecord:
    def test_to_dict(self):
        r = ChangeRecord(artifact_path="p.json", artifact_type="pipeline", action="created")
        d = r.to_dict()
        assert d["artifact_path"] == "p.json"
        assert d["action"] == "created"
        assert d["timestamp"]

    def test_roundtrip(self):
        r = ChangeRecord(artifact_path="n.py", artifact_type="notebook", action="modified")
        r2 = ChangeRecord.from_dict(r.to_dict())
        assert r2.artifact_path == r.artifact_path


class TestChangeLog:
    def test_add_and_save(self, tmp_path: Path):
        cl = ChangeLog(run_id="run1")
        cl.add(ChangeRecord(artifact_path="a.json", artifact_type="pipeline", action="created"))
        path = cl.save(tmp_path / "changelog.json")
        assert path.exists()

    def test_load(self, tmp_path: Path):
        cl = ChangeLog(run_id="r2")
        cl.add(ChangeRecord(artifact_path="b.py", artifact_type="notebook", action="created"))
        cl.save(tmp_path / "cl.json")
        loaded = ChangeLog.load(tmp_path / "cl.json")
        assert loaded.run_id == "r2"
        assert len(loaded.records) == 1


class TestFabricignore:
    def test_parse_empty(self, tmp_path: Path):
        assert parse_fabricignore(tmp_path) == []

    def test_parse_file(self, tmp_path: Path):
        (tmp_path / ".fabricignore").write_text("*.tmp\n# comment\n*.log\n", encoding="utf-8")
        patterns = parse_fabricignore(tmp_path)
        assert "*.tmp" in patterns
        assert "*.log" in patterns
        assert len(patterns) == 2

    def test_should_ignore(self):
        assert should_ignore("file.tmp", ["*.tmp"])
        assert not should_ignore("file.json", ["*.tmp"])

    def test_default_template(self):
        template = generate_default_fabricignore()
        assert "*.tmp" in template
        assert ".fabricignore" in template or "migration_config" in template


class TestArtifactDiff:
    def test_diff_added(self, tmp_path: Path):
        old = tmp_path / "old"
        old.mkdir()
        new = tmp_path / "new"
        new.mkdir()
        (new / "added.json").write_text("{}", encoding="utf-8")
        diffs = diff_artifacts(old, new)
        assert len(diffs) == 1
        assert diffs[0].diff_type == "added"

    def test_diff_removed(self, tmp_path: Path):
        old = tmp_path / "old"
        old.mkdir()
        (old / "removed.json").write_text("{}", encoding="utf-8")
        new = tmp_path / "new"
        new.mkdir()
        diffs = diff_artifacts(old, new)
        assert any(d.diff_type == "removed" for d in diffs)

    def test_diff_modified(self, tmp_path: Path):
        old = tmp_path / "old"
        old.mkdir()
        (old / "f.json").write_text('{"v":1}', encoding="utf-8")
        new = tmp_path / "new"
        new.mkdir()
        (new / "f.json").write_text('{"v":2}', encoding="utf-8")
        diffs = diff_artifacts(old, new)
        assert any(d.diff_type == "modified" for d in diffs)

    def test_diff_unchanged(self, tmp_path: Path):
        old = tmp_path / "old"
        old.mkdir()
        (old / "f.json").write_text("{}", encoding="utf-8")
        new = tmp_path / "new"
        new.mkdir()
        (new / "f.json").write_text("{}", encoding="utf-8")
        diffs = diff_artifacts(old, new)
        assert len(diffs) == 0


class TestCLI:
    def test_git_sync_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["git-sync", "--help"])
        assert result.exit_code == 0
        assert "Git" in result.output or "artifact" in result.output.lower()
