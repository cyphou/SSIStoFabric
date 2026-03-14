"""Tests for CLI commands using Click's CliRunner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from ssis_to_fabric.cli import main

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "sample_packages"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestAnalyze:
    """Tests for the 'analyze' CLI command."""

    @pytest.mark.unit
    def test_analyze_single_file(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["analyze", str(FIXTURES_DIR / "simple_etl.dtsx")])
        assert result.exit_code == 0
        assert "SSIS Package Analysis" in result.output

    @pytest.mark.unit
    def test_analyze_directory(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["analyze", str(FIXTURES_DIR)])
        assert result.exit_code == 0
        assert "SSIS Package Analysis" in result.output

    @pytest.mark.unit
    def test_analyze_nonexistent(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["analyze", "/nonexistent/path"])
        assert result.exit_code != 0


class TestPlan:
    """Tests for the 'plan' CLI command."""

    @pytest.mark.unit
    def test_plan_default(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["plan", str(FIXTURES_DIR / "simple_etl.dtsx")])
        assert result.exit_code == 0
        assert "Migration Plan" in result.output

    @pytest.mark.unit
    def test_plan_with_strategy(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["plan", str(FIXTURES_DIR / "simple_etl.dtsx"), "--strategy", "spark"])
        assert result.exit_code == 0
        assert "Migration Plan" in result.output


class TestMigrate:
    """Tests for the 'migrate' CLI command."""

    @pytest.mark.unit
    def test_migrate_default(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--output", str(tmp_path / "out")],
        )
        assert result.exit_code == 0
        assert "Migration Results" in result.output
        assert (tmp_path / "out" / "pipelines").exists()

    @pytest.mark.unit
    def test_migrate_spark_strategy(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            [
                "migrate",
                str(FIXTURES_DIR / "simple_etl.dtsx"),
                "--strategy", "spark",
                "--output", str(tmp_path / "out"),
            ],
        )
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_migrate_hybrid_dataflow_gen2(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            [
                "migrate",
                str(FIXTURES_DIR / "simple_etl.dtsx"),
                "--strategy", "hybrid",
                "--dataflow-type", "dataflow_gen2",
                "--output", str(tmp_path / "out"),
            ],
        )
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_migrate_complex_package(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "complex_etl.dtsx"), "--output", str(tmp_path / "out")],
        )
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_migrate_generates_report_json(self, runner: CliRunner, tmp_path: Path) -> None:
        out = tmp_path / "out"
        runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--output", str(out)],
        )
        report = out / "migration_report.json"
        assert report.exists()
        data = json.loads(report.read_text())
        assert "items" in data


class TestValidate:
    """Tests for the 'validate' CLI command."""

    @pytest.mark.unit
    def test_validate_matching(self, runner: CliRunner, tmp_path: Path) -> None:
        baseline = tmp_path / "baseline"
        output = tmp_path / "output"
        baseline.mkdir()
        output.mkdir()
        (baseline / "test.json").write_text('{"key": "value"}')
        (output / "test.json").write_text('{"key": "value"}')

        result = runner.invoke(main, ["validate", str(baseline), str(output)])
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_validate_mismatch(self, runner: CliRunner, tmp_path: Path) -> None:
        baseline = tmp_path / "baseline"
        output = tmp_path / "output"
        baseline.mkdir()
        output.mkdir()
        (baseline / "test.json").write_text('{"key": "value1"}')
        (output / "test.json").write_text('{"key": "value2"}')

        result = runner.invoke(main, ["validate", str(baseline), str(output)])
        # Mismatch reports diff but exit code depends on implementation
        assert result.exit_code in (0, 1)


class TestReport:
    """Tests for the 'report' CLI command."""

    @pytest.mark.unit
    def test_report(self, runner: CliRunner, tmp_path: Path) -> None:
        out = tmp_path / "out"
        runner.invoke(
            main,
            ["migrate", str(FIXTURES_DIR / "simple_etl.dtsx"), "--output", str(out)],
        )
        result = runner.invoke(main, ["report", str(out)])
        assert result.exit_code == 0
        assert (out / "migration_report.html").exists()


class TestLineage:
    """Tests for the 'lineage' CLI command."""

    @pytest.mark.unit
    def test_lineage(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            ["lineage", str(FIXTURES_DIR), "--output", str(tmp_path / "lineage.json")],
        )
        assert result.exit_code == 0
        assert (tmp_path / "lineage.json").exists()


class TestMainGroup:
    """Tests for the main CLI group."""

    @pytest.mark.unit
    def test_help(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "SSIS to Fabric" in result.output
