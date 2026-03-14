"""
Testing & Quality Utilities
============================
Provides validation, benchmarking, and quality-assurance helpers for the
generated migration artifacts:

- **Generated code validation**: ``ast.parse()`` for PySpark notebooks
- **Expression fuzzer**: random SSIS expression generator for round-trip tests
- **Report validator**: structural checks on migration report HTML / JSON
- **Benchmark helpers**: timing wrappers for migration steps
"""

from __future__ import annotations

import ast
import json
import random
import re
import string
import time
from dataclasses import dataclass, field
from pathlib import Path  # noqa: TC003 (used at runtime)
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# =====================================================================
# Generated Code Validation
# =====================================================================


@dataclass
class CodeValidationResult:
    """Result of validating generated code."""

    file_path: str
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    line_count: int = 0


def validate_python_syntax(source: str, filename: str = "<generated>") -> CodeValidationResult:
    """Validate Python source code using ``ast.parse()``.

    Returns a ``CodeValidationResult`` indicating whether the code is
    syntactically valid.
    """
    result = CodeValidationResult(file_path=filename, is_valid=True)
    result.line_count = source.count("\n") + 1

    try:
        ast.parse(source, filename=filename)
    except SyntaxError as e:
        result.is_valid = False
        result.errors.append(f"Line {e.lineno}: {e.msg}")

    # Heuristic warnings
    if "TODO" in source or "FIXME" in source:
        result.warnings.append("Contains TODO/FIXME markers")
    if "F.expr(" in source:
        result.warnings.append("Contains fallback F.expr() — expression may need manual review")

    return result


def validate_notebook_dir(notebooks_dir: Path) -> list[CodeValidationResult]:
    """Validate all ``.py`` notebooks in a directory."""
    results = []
    if not notebooks_dir.exists():
        return results

    for f in sorted(notebooks_dir.glob("*.py")):
        source = f.read_text(encoding="utf-8")
        result = validate_python_syntax(source, filename=f.name)
        results.append(result)

    valid = sum(1 for r in results if r.is_valid)
    logger.info("notebook_validation_complete", total=len(results), valid=valid)
    return results


def validate_pipeline_json(pipelines_dir: Path) -> list[CodeValidationResult]:
    """Validate all pipeline JSON files for structure."""
    results = []
    if not pipelines_dir.exists():
        return results

    for f in sorted(pipelines_dir.glob("*.json")):
        result = CodeValidationResult(file_path=f.name, is_valid=True)
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                result.is_valid = False
                result.errors.append("Pipeline JSON must be an object, not a list or scalar")
            elif "name" not in data and "activities" not in data:
                result.warnings.append("Pipeline JSON has no 'name' or 'activities' key")
        except json.JSONDecodeError as e:
            result.is_valid = False
            result.errors.append(f"Invalid JSON: {e}")
        results.append(result)

    return results


# =====================================================================
# Expression Fuzzer
# =====================================================================

# SSIS expression building blocks
_SSIS_FUNCTIONS = [
    "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "LEN", "SUBSTRING",
    "REPLACE", "REVERSE", "RIGHT", "LEFT", "FINDSTRING",
    "DATEADD", "DATEDIFF", "DATEPART", "YEAR", "MONTH", "DAY",
    "GETDATE", "GETUTCDATE", "NULL", "ISNULL",
    "ABS", "CEILING", "FLOOR", "ROUND", "POWER", "SQRT", "SIGN",
    "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN",
]

_SSIS_DATE_PARTS = ["dd", "mm", "yy", "hh", "mi", "ss", "ms"]

_SSIS_CAST_TYPES = [
    "(DT_WSTR,50)", "(DT_I4)", "(DT_I8)", "(DT_R8)",
    "(DT_BOOL)", "(DT_DATE)", "(DT_NUMERIC,10,2)",
]


def generate_random_ssis_expression(max_depth: int = 3) -> str:
    """Generate a random SSIS expression for fuzz testing.

    Produces syntactically plausible SSIS expressions that can be fed
    into the expression transpiler.  Does *not* guarantee semantic
    validity — the goal is to exercise parser edge-cases.
    """
    return _random_expr(max_depth)


def _random_expr(depth: int) -> str:
    if depth <= 0:
        return _random_leaf()

    choice = random.randint(0, 7)
    if choice == 0:
        return _random_leaf()
    if choice == 1:
        func = random.choice(["UPPER", "LOWER", "TRIM", "LEN", "REVERSE"])
        return f"{func}({_random_expr(depth - 1)})"
    if choice == 2:
        return f"SUBSTRING({_random_expr(depth - 1)}, 1, 5)"
    if choice == 3:
        dp = random.choice(_SSIS_DATE_PARTS)
        return f'DATEADD("{dp}", {random.randint(1, 30)}, GETDATE())'
    if choice == 4:
        cast = random.choice(_SSIS_CAST_TYPES)
        return f"{cast}{_random_expr(depth - 1)}"
    if choice == 5:
        op = random.choice(["+", "-", "*"])
        return f"({_random_expr(depth - 1)} {op} {_random_expr(depth - 1)})"
    if choice == 6:
        return f'{_random_expr(depth - 1)} == "test" ? "yes" : "no"'
    # choice == 7
    return f"ISNULL({_random_expr(depth - 1)}) ? {_random_expr(depth - 1)} : {_random_expr(depth - 1)}"


def _random_leaf() -> str:
    choice = random.randint(0, 4)
    if choice == 0:
        col = "".join(random.choices(string.ascii_letters, k=5))
        return f"@[User::{col}]"
    if choice == 1:
        return str(random.randint(-100, 100))
    if choice == 2:
        s = "".join(random.choices(string.ascii_letters + " ", k=random.randint(1, 10)))
        return f'"{s}"'
    if choice == 3:
        return "GETDATE()"
    return "NULL(DT_WSTR, 50)"


# =====================================================================
# Report Validation
# =====================================================================


def validate_report_json(report_path: Path) -> CodeValidationResult:
    """Validate a migration report JSON file for expected structure."""
    result = CodeValidationResult(file_path=str(report_path), is_valid=True)

    if not report_path.exists():
        result.is_valid = False
        result.errors.append("Report file does not exist")
        return result

    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        result.is_valid = False
        result.errors.append(f"Invalid JSON: {e}")
        return result

    expected_keys = {"summary", "packages"}
    missing = expected_keys - set(data.keys())
    if missing:
        result.warnings.append(f"Missing expected keys: {missing}")

    if "summary" in data:
        summary = data["summary"]
        if not isinstance(summary, dict):
            result.errors.append("'summary' must be an object")
            result.is_valid = False

    return result


def validate_report_html(html_path: Path) -> CodeValidationResult:
    """Validate a migration report HTML file for expected structure."""
    result = CodeValidationResult(file_path=str(html_path), is_valid=True)

    if not html_path.exists():
        result.is_valid = False
        result.errors.append("HTML report file does not exist")
        return result

    content = html_path.read_text(encoding="utf-8")
    result.line_count = content.count("\n") + 1

    if "<html" not in content:
        result.is_valid = False
        result.errors.append("Missing <html> tag")
    if "<title>" not in content:
        result.warnings.append("Missing <title> tag")
    if "Migration Report" not in content:
        result.warnings.append("Missing 'Migration Report' text")

    return result


# =====================================================================
# Benchmark Helpers
# =====================================================================


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    elapsed_seconds: float
    iterations: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def ops_per_second(self) -> float:
        return self.iterations / self.elapsed_seconds if self.elapsed_seconds > 0 else 0


def benchmark(name: str, func: Any, *args: Any, iterations: int = 1, **kwargs: Any) -> BenchmarkResult:
    """Run a function and record its execution time."""
    start = time.perf_counter()
    for _ in range(iterations):
        func(*args, **kwargs)
    elapsed = time.perf_counter() - start

    result = BenchmarkResult(
        name=name,
        elapsed_seconds=elapsed,
        iterations=iterations,
    )
    logger.info(
        "benchmark_complete",
        name=name,
        elapsed=f"{elapsed:.4f}s",
        ops_per_sec=f"{result.ops_per_second:.1f}",
    )
    return result


def write_benchmark_results(results: list[BenchmarkResult], output_path: Path) -> None:
    """Write benchmark results to a JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = [
        {
            "name": r.name,
            "elapsed_seconds": r.elapsed_seconds,
            "iterations": r.iterations,
            "ops_per_second": r.ops_per_second,
            "metadata": r.metadata,
        }
        for r in results
    ]
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# =====================================================================
# M Syntax Checker (basic)
# =====================================================================


def validate_m_syntax(source: str) -> CodeValidationResult:
    """Basic structural validation of Power Query M code.

    Not a full M parser — checks for balanced constructs, known
    keywords, and common syntax issues.
    """
    result = CodeValidationResult(file_path="<m-expression>", is_valid=True)
    result.line_count = source.count("\n") + 1

    # Check balanced constructs
    opens = source.count("(") + source.count("[") + source.count("{")
    closes = source.count(")") + source.count("]") + source.count("}")
    if opens != closes:
        result.is_valid = False
        result.errors.append(
            f"Unbalanced brackets: {opens} openers vs {closes} closers"
        )

    # Check for required 'let ... in' structure
    if "let" in source.lower() and "in" not in source.lower():
        result.warnings.append("Found 'let' without matching 'in'")

    # Check for common M keywords
    m_keywords = re.findall(r"\b(let|in|if|then|else|each|try|otherwise|error)\b", source, re.IGNORECASE)
    if not m_keywords:
        result.warnings.append("No M keywords found — may not be M code")

    return result
