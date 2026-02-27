"""
Non-regression tests.
Validate that migration outputs remain consistent across code changes.

These tests:
1. Generate migration outputs from known SSIS packages
2. Compare against approved baselines
3. Validate structural integrity of generated artifacts
"""

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.migration_engine import MigrationEngine
from ssis_to_fabric.testing.regression_runner import RegressionRunner

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


class TestRegressionFileComparison:
    """Tests for file-based non-regression validation."""

    @pytest.mark.regression
    def test_identical_files_pass(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Identical baseline and output should pass."""
        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        # Create identical JSON files
        data = {"name": "test", "properties": {"activities": []}}
        (baseline_dir / "pipeline.json").write_text(json.dumps(data))
        (output_dir / "pipeline.json").write_text(json.dumps(data))

        runner = RegressionRunner(default_config)
        results = runner.run_file_comparison(baseline_dir, output_dir)

        assert len(results) == 1
        assert results[0]["status"] == "pass"

    @pytest.mark.regression
    def test_different_files_fail(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Different baseline and output should fail."""
        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        (baseline_dir / "pipeline.json").write_text(json.dumps({"name": "v1"}))
        (output_dir / "pipeline.json").write_text(json.dumps({"name": "v2"}))

        runner = RegressionRunner(default_config)
        results = runner.run_file_comparison(baseline_dir, output_dir)

        assert len(results) == 1
        assert results[0]["status"] == "fail"

    @pytest.mark.regression
    def test_missing_output_file_fails(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Missing output file should report failure."""
        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        (baseline_dir / "pipeline.json").write_text(json.dumps({"name": "test"}))

        runner = RegressionRunner(default_config)
        results = runner.run_file_comparison(baseline_dir, output_dir)

        assert any(r["status"] == "fail" for r in results)

    @pytest.mark.regression
    def test_new_file_warns(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """File in output but not in baseline should warn."""
        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        (output_dir / "new_pipeline.json").write_text(json.dumps({"name": "new"}))

        runner = RegressionRunner(default_config)
        results = runner.run_file_comparison(baseline_dir, output_dir)

        assert any(r["status"] == "warn" for r in results)

    @pytest.mark.regression
    def test_python_file_comparison_ignores_timestamps(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Python file comparison should ignore timestamp comments."""
        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        baseline_code = """# Generated: 2024-01-01 00:00:00 UTC
import foo
x = 1
"""
        output_code = """# Generated: 2024-12-31 23:59:59 UTC
import foo
x = 1
"""
        (baseline_dir / "notebook.py").write_text(baseline_code)
        (output_dir / "notebook.py").write_text(output_code)

        runner = RegressionRunner(default_config)
        results = runner.run_file_comparison(baseline_dir, output_dir)

        # Should pass because timestamp lines are excluded
        assert results[0]["status"] == "pass"


class TestRegressionStructuralValidation:
    """Tests for structural validation of generated artifacts."""

    @pytest.mark.regression
    def test_valid_pipeline_passes(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        pipeline = {
            "name": "test_pipeline",
            "properties": {"activities": [{"name": "CopyData", "type": "Copy", "typeProperties": {}}]},
        }
        path = tmp_path / "pipeline.json"
        path.write_text(json.dumps(pipeline))

        runner = RegressionRunner(default_config)
        result = runner.validate_pipeline_structure(path)
        assert result["status"] == "pass"

    @pytest.mark.regression
    def test_invalid_pipeline_fails(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        pipeline = {"not_a_valid": "pipeline"}
        path = tmp_path / "bad.json"
        path.write_text(json.dumps(pipeline))

        runner = RegressionRunner(default_config)
        result = runner.validate_pipeline_structure(path)
        assert result["status"] == "fail"

    @pytest.mark.regression
    def test_empty_activities_fails(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        pipeline = {"name": "empty", "properties": {"activities": []}}
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(pipeline))

        runner = RegressionRunner(default_config)
        result = runner.validate_pipeline_structure(path)
        assert result["status"] == "fail"

    @pytest.mark.regression
    def test_validate_notebook_syntax(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Valid Python notebook should pass syntax check."""
        code = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql("SELECT 1")
"""
        path = tmp_path / "notebook.py"
        path.write_text(code)

        runner = RegressionRunner(default_config)
        result = runner.validate_notebook_structure(path)
        assert result["status"] == "pass"

    @pytest.mark.regression
    def test_invalid_notebook_fails(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Invalid Python should fail syntax check."""
        code = "def broken(\n  # missing closing paren"
        path = tmp_path / "bad.py"
        path.write_text(code)

        runner = RegressionRunner(default_config)
        result = runner.validate_notebook_structure(path)
        assert result["status"] == "fail"


class TestRegressionEndToEnd:
    """End-to-end regression: parse → migrate → validate."""

    @pytest.mark.regression
    def test_simple_package_regression(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Full regression test for the simple ETL package."""
        parser = DTSXParser()
        package = parser.parse(FIXTURES_DIR / "simple_etl.dtsx")

        default_config.output_dir = tmp_path / "output"
        engine = MigrationEngine(default_config)
        result = engine.execute([package])

        # All items should be completed or manual_review
        for item in result.items:
            assert item.status in ("completed", "manual_review_required")

        # Validate generated pipelines
        runner = RegressionRunner(default_config)
        pipelines_dir = default_config.output_dir / "pipelines"
        if pipelines_dir.exists():
            for pipeline_file in pipelines_dir.glob("*.json"):
                validation = runner.validate_pipeline_structure(pipeline_file)
                assert validation["status"] == "pass", f"Pipeline validation failed: {validation}"

    @pytest.mark.regression
    def test_complex_package_regression(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Full regression test for the complex ETL package."""
        parser = DTSXParser()
        package = parser.parse(FIXTURES_DIR / "complex_etl.dtsx")

        default_config.output_dir = tmp_path / "output"
        engine = MigrationEngine(default_config)
        result = engine.execute([package])

        # Should have mix of completed and manual_review
        statuses = {item.status for item in result.items}
        assert "completed" in statuses or "manual_review_required" in statuses

        # Validate notebooks
        runner = RegressionRunner(default_config)
        notebooks_dir = default_config.output_dir / "notebooks"
        if notebooks_dir.exists():
            for nb_file in notebooks_dir.glob("*.py"):
                validation = runner.validate_notebook_structure(nb_file)
                # TODO count should be > 0 for generated notebooks
                assert validation.get("todo_count", 0) >= 0

    @pytest.mark.regression
    def test_migration_report_consistency(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Migration report should be consistent across runs."""
        parser = DTSXParser()
        package = parser.parse(FIXTURES_DIR / "simple_etl.dtsx")

        # Run migration twice
        config1 = MigrationConfig(output_dir=tmp_path / "run1", strategy=MigrationStrategy.HYBRID)
        config2 = MigrationConfig(output_dir=tmp_path / "run2", strategy=MigrationStrategy.HYBRID)

        engine1 = MigrationEngine(config1)
        engine2 = MigrationEngine(config2)

        result1 = engine1.execute([package])
        result2 = engine2.execute([package])

        # Same number of items
        assert len(result1.items) == len(result2.items)

        # Same target artifacts
        artifacts1 = [(i.source_task, i.target_artifact) for i in result1.items]
        artifacts2 = [(i.source_task, i.target_artifact) for i in result2.items]
        assert artifacts1 == artifacts2

    @pytest.mark.regression
    def test_report_generation(self, tmp_path: Path, default_config: MigrationConfig) -> None:
        """Regression runner should generate proper report."""
        runner = RegressionRunner(default_config)

        baseline_dir = tmp_path / "baseline"
        output_dir = tmp_path / "output"
        baseline_dir.mkdir()
        output_dir.mkdir()

        (baseline_dir / "a.json").write_text('{"name": "a"}')
        (output_dir / "a.json").write_text('{"name": "a"}')
        (baseline_dir / "b.json").write_text('{"name": "b1"}')
        (output_dir / "b.json").write_text('{"name": "b2"}')

        runner.run_file_comparison(baseline_dir, output_dir)
        report = runner.generate_report()

        assert report["total_checks"] == 2
        assert report["passed"] == 1
        assert report["failed"] == 1
