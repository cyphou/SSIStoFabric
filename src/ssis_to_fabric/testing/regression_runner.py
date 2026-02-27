"""
Non-Regression Test Runner
============================
Validates that migration outputs are consistent and correct by:
1. Comparing generated artifacts against approved baselines (file-level)
2. Comparing data outputs between SSIS and Fabric (data-level)
3. Structural validation of generated pipelines and notebooks
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from deepdiff import DeepDiff

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from ssis_to_fabric.config import MigrationConfig

logger = get_logger(__name__)


class RegressionRunner:
    """
    Non-regression test harness that validates migration outputs.

    Three validation levels:
    1. FILE COMPARISON: Compare generated artifacts against baselines
    2. STRUCTURAL VALIDATION: Validate pipeline/notebook structure
    3. DATA VALIDATION: Compare row counts and data between source/target
    """

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config
        self.results: list[dict[str, Any]] = []

    # =========================================================================
    # Level 1: File-Based Comparison
    # =========================================================================

    def run_file_comparison(self, baseline_dir: Path, output_dir: Path) -> list[dict[str, Any]]:
        """
        Compare generated files against approved baselines.

        For JSON files: deep structural comparison (ignoring timestamps).
        For .py files: line-by-line diff (ignoring comments/blanks).
        """
        results = []

        baseline_files = {f.relative_to(baseline_dir): f for f in baseline_dir.rglob("*") if f.is_file()}
        output_files = {f.relative_to(output_dir): f for f in output_dir.rglob("*") if f.is_file()}

        # Check all baselines have corresponding outputs
        for rel_path, baseline_file in baseline_files.items():
            output_file = output_dir / rel_path

            if not output_file.exists():
                results.append(
                    {
                        "file": str(rel_path),
                        "status": "fail",
                        "details": "Missing in output — expected file not generated",
                    }
                )
                continue

            if baseline_file.suffix == ".json":
                result = self._compare_json_files(baseline_file, output_file, str(rel_path))
            elif baseline_file.suffix == ".py":
                result = self._compare_python_files(baseline_file, output_file, str(rel_path))
            else:
                result = self._compare_text_files(baseline_file, output_file, str(rel_path))

            results.append(result)

        # Check for unexpected new files
        for rel_path in output_files:
            if rel_path not in baseline_files:
                results.append(
                    {
                        "file": str(rel_path),
                        "status": "warn",
                        "details": "New file not in baseline — review and approve if correct",
                    }
                )

        self.results = results
        return results

    def _compare_json_files(self, baseline: Path, output: Path, rel_path: str) -> dict[str, Any]:
        """Deep-compare two JSON files, ignoring timestamps and generated dates."""
        try:
            with open(baseline) as f:
                baseline_data = json.load(f)
            with open(output) as f:
                output_data = json.load(f)

            # Exclude volatile fields from comparison
            exclude_paths = {
                "root['properties']['annotations']",  # Contains generation dates
            }

            diff = DeepDiff(
                baseline_data,
                output_data,
                ignore_order=True,
                exclude_paths=exclude_paths,
                significant_digits=6,
            )

            if diff:
                return {
                    "file": rel_path,
                    "status": "fail",
                    "details": f"Differences found: {self._summarize_diff(diff)}",
                    "diff": diff.to_dict(),
                }
            return {"file": rel_path, "status": "pass", "details": "Identical"}

        except Exception as e:
            return {"file": rel_path, "status": "fail", "details": f"Comparison error: {str(e)}"}

    def _compare_python_files(self, baseline: Path, output: Path, rel_path: str) -> dict[str, Any]:
        """Compare Python files ignoring comments and blank lines."""
        try:
            baseline_lines = self._normalize_python(baseline.read_text(encoding="utf-8"))
            output_lines = self._normalize_python(output.read_text(encoding="utf-8"))

            if baseline_lines == output_lines:
                return {"file": rel_path, "status": "pass", "details": "Identical (excluding comments)"}

            # Find differences
            diffs = []
            max_lines = max(len(baseline_lines), len(output_lines))
            for i in range(max_lines):
                bl = baseline_lines[i] if i < len(baseline_lines) else "<missing>"
                ol = output_lines[i] if i < len(output_lines) else "<missing>"
                if bl != ol:
                    diffs.append(f"Line {i + 1}: baseline='{bl}' vs output='{ol}'")
                    if len(diffs) >= 5:
                        diffs.append("... and more differences")
                        break

            return {
                "file": rel_path,
                "status": "fail",
                "details": "; ".join(diffs),
            }

        except Exception as e:
            return {"file": rel_path, "status": "fail", "details": f"Comparison error: {str(e)}"}

    def _compare_text_files(self, baseline: Path, output: Path, rel_path: str) -> dict[str, Any]:
        """Simple text comparison."""
        try:
            b_text = baseline.read_text(encoding="utf-8").strip()
            o_text = output.read_text(encoding="utf-8").strip()

            if b_text == o_text:
                return {"file": rel_path, "status": "pass", "details": "Identical"}
            return {"file": rel_path, "status": "fail", "details": "Content differs"}

        except Exception as e:
            return {"file": rel_path, "status": "fail", "details": f"Error: {str(e)}"}

    # =========================================================================
    # Level 2: Structural Validation
    # =========================================================================

    def validate_pipeline_structure(self, pipeline_path: Path) -> dict[str, Any]:
        """
        Validate that a generated Data Factory pipeline has proper structure.
        Checks required fields, activity types, and references.
        """
        try:
            with open(pipeline_path) as f:
                pipeline = json.load(f)

            errors = []

            # Check top-level structure
            if "name" not in pipeline:
                errors.append("Missing 'name' field")
            if "properties" not in pipeline:
                errors.append("Missing 'properties' field")
                return {"file": str(pipeline_path), "status": "fail", "errors": errors}

            props = pipeline["properties"]

            # Check activities
            activities = props.get("activities", [])
            if not activities:
                errors.append("No activities defined in pipeline")

            for i, activity in enumerate(activities):
                act_errors = self._validate_activity(activity, i)
                errors.extend(act_errors)

            status = "pass" if not errors else "fail"
            return {
                "file": str(pipeline_path),
                "status": status,
                "errors": errors,
                "activity_count": len(activities),
            }

        except json.JSONDecodeError as e:
            return {"file": str(pipeline_path), "status": "fail", "errors": [f"Invalid JSON: {e}"]}

    def _validate_activity(self, activity: dict, index: int) -> list[str]:
        """Validate a single pipeline activity."""
        errors = []
        prefix = f"Activity[{index}]"

        if "name" not in activity:
            errors.append(f"{prefix}: Missing 'name'")
        if "type" not in activity:
            errors.append(f"{prefix}: Missing 'type'")

        valid_types = {
            "Copy",
            "Script",
            "ExecutePipeline",
            "Office365Outlook",
            "ForEach",
            "Until",
            "IfCondition",
            "SetVariable",
            "Wait",
            "WebActivity",
            "Notebook",
            "Dataflow",
            "TridentNotebook",
        }
        act_type = activity.get("type", "")
        if act_type and act_type not in valid_types:
            errors.append(f"{prefix}: Unknown activity type '{act_type}'")

        return errors

    def validate_notebook_structure(self, notebook_path: Path) -> dict[str, Any]:
        """Validate that a generated Spark notebook is syntactically correct."""
        try:
            content = notebook_path.read_text(encoding="utf-8")
            errors = []

            # Basic syntax check
            try:
                compile(content, str(notebook_path), "exec")
            except SyntaxError as e:
                errors.append(f"Python syntax error at line {e.lineno}: {e.msg}")

            # Check for required patterns
            if "spark" not in content.lower() and "pyspark" not in content.lower():
                errors.append("No Spark/PySpark references found")

            # Count TODO markers
            todo_count = content.lower().count("todo")

            status = "pass" if not errors else "fail"
            return {
                "file": str(notebook_path),
                "status": status,
                "errors": errors,
                "todo_count": todo_count,
            }

        except Exception as e:
            return {"file": str(notebook_path), "status": "fail", "errors": [str(e)]}

    # =========================================================================
    # Level 3: Data Validation
    # =========================================================================

    def validate_data_consistency(
        self,
        source_connection: str,
        target_connection: str,
        table_name: str,
        key_columns: list[str],
    ) -> dict[str, Any]:
        """
        Compare data between source (SSIS processed) and target (Fabric processed).

        Compares:
        - Row counts
        - Schema match
        - Sample data comparison
        """
        import pandas as pd
        from sqlalchemy import create_engine

        result: dict[str, Any] = {
            "table": table_name,
            "checks": {},
        }

        try:
            src_engine = create_engine(source_connection)
            tgt_engine = create_engine(target_connection)

            # Row count comparison
            src_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", src_engine).iloc[0]["cnt"]
            tgt_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", tgt_engine).iloc[0]["cnt"]

            count_match = src_count == tgt_count
            tolerance = self.config.regression.tolerance_row_count
            if not count_match and tolerance > 0:
                diff_pct = abs(src_count - tgt_count) / max(src_count, 1)
                count_match = diff_pct <= tolerance

            result["checks"]["row_count"] = {
                "status": "pass" if count_match else "fail",
                "source_count": int(src_count),
                "target_count": int(tgt_count),
            }

            # Schema comparison
            src_cols = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", src_engine).columns.tolist()
            tgt_cols = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", tgt_engine).columns.tolist()

            missing_cols = set(src_cols) - set(tgt_cols)
            extra_cols = set(tgt_cols) - set(src_cols)

            result["checks"]["schema"] = {
                "status": "pass" if not missing_cols and not extra_cols else "fail",
                "missing_columns": list(missing_cols),
                "extra_columns": list(extra_cols),
            }

            # Sample data comparison
            sample_size = self.config.regression.sample_size
            order_clause = ", ".join(key_columns) if key_columns else "1"

            src_sample = pd.read_sql(
                f"SELECT TOP {sample_size} * FROM {table_name} ORDER BY {order_clause}",
                src_engine,
            )
            tgt_sample = pd.read_sql(
                f"SELECT TOP {sample_size} * FROM {table_name} ORDER BY {order_clause}",
                tgt_engine,
            )

            # Compare common columns
            common_cols = sorted(set(src_cols) & set(tgt_cols))
            if common_cols:
                src_compare = src_sample[common_cols].reset_index(drop=True)
                tgt_compare = tgt_sample[common_cols].reset_index(drop=True)

                matches = src_compare.equals(tgt_compare)
                result["checks"]["sample_data"] = {
                    "status": "pass" if matches else "fail",
                    "sample_size": len(src_compare),
                    "columns_compared": len(common_cols),
                }

                if not matches:
                    # Find specific mismatches
                    diff_mask = src_compare != tgt_compare
                    mismatch_cols = diff_mask.any().index[diff_mask.any()].tolist()
                    result["checks"]["sample_data"]["mismatch_columns"] = mismatch_cols[:10]

            result["status"] = "pass" if all(c.get("status") == "pass" for c in result["checks"].values()) else "fail"

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    # =========================================================================
    # Helpers
    # =========================================================================

    def _normalize_python(self, content: str) -> list[str]:
        """Normalize Python content for comparison (ignore comments, blanks, timestamps)."""
        lines = []
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#") and any(kw in stripped.lower() for kw in ["generated:", "date:", "timestamp"]):
                continue
            lines.append(line.rstrip())
        return lines

    def _summarize_diff(self, diff: DeepDiff) -> str:
        """Create a human-readable summary of a DeepDiff."""
        parts = []
        if "values_changed" in diff:
            parts.append(f"{len(diff['values_changed'])} value(s) changed")
        if "dictionary_item_added" in diff:
            parts.append(f"{len(diff['dictionary_item_added'])} item(s) added")
        if "dictionary_item_removed" in diff:
            parts.append(f"{len(diff['dictionary_item_removed'])} item(s) removed")
        if "iterable_item_added" in diff:
            parts.append(f"{len(diff['iterable_item_added'])} list item(s) added")
        if "iterable_item_removed" in diff:
            parts.append(f"{len(diff['iterable_item_removed'])} list item(s) removed")
        return ", ".join(parts) if parts else "Unknown differences"

    def generate_report(self) -> dict:
        """Generate a summary report of all regression results."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["status"] == "pass")
        failed = sum(1 for r in self.results if r["status"] == "fail")
        warnings = sum(1 for r in self.results if r["status"] == "warn")

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "pass_rate": f"{(passed / max(total, 1)) * 100:.1f}%",
            "results": self.results,
        }
