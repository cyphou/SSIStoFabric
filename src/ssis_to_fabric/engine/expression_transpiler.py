"""
Unified SSIS Expression Transpiler
====================================
Provides a single entry point for converting SSIS expressions to PySpark
or Power Query M, delegating to the existing per-generator transpilers.

Includes expression validation that detects unsupported patterns and
reports issues before migration.

Usage::

    from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

    transpiler = ExpressionTranspiler()
    pyspark = transpiler.to_pyspark("UPPER(ColumnA)")
    m_expr = transpiler.to_m("UPPER(ColumnA)")
    issues = transpiler.validate("UNKNOWN_FUNC(x)")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class TargetLanguage(str, Enum):
    PYSPARK = "pyspark"
    POWER_QUERY_M = "m"


@dataclass
class ExpressionIssue:
    """A single validation finding for an SSIS expression."""

    severity: str  # "error", "warning", "info"
    message: str
    expression: str
    position: int = 0  # char offset where the issue was detected


@dataclass
class ValidationResult:
    """Result of validating an SSIS expression."""

    expression: str
    issues: list[ExpressionIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)


# Functions known to both transpilers
_KNOWN_FUNCTIONS = {
    # String
    "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "LEN", "LEFT", "RIGHT",
    "REPLACE", "SUBSTRING", "FINDSTRING", "TOKEN", "TOKENCOUNT",
    "REVERSE", "CODEPOINT", "HEX", "CONCATENATE",
    # Null
    "ISNULL", "REPLACENULL", "NULL",
    # Date
    "GETDATE", "GETUTCDATE", "DATEADD", "DATEDIFF", "DATEPART",
    "YEAR", "MONTH", "DAY",
    # Math
    "ABS", "CEILING", "FLOOR", "ROUND", "POWER", "SIGN", "SQUARE",
    "SQRT", "EXP", "LOG", "LOG10", "LOG2",
    # Trig
    "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2",
    # Constants
    "PI", "RAND",
    # Bitwise
    "BITAND", "BITOR", "BITXOR",
}


class ExpressionTranspiler:
    """Unified SSIS expression → target-language converter."""

    @staticmethod
    def to_pyspark(expr: str) -> str:
        """Convert an SSIS expression to PySpark (``pyspark.sql.functions``)."""
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        return SparkNotebookGenerator._ssis_expr_to_pyspark(expr)

    @staticmethod
    def to_m(expr: str) -> str:
        """Convert an SSIS expression to Power Query M."""
        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        return DataflowGen2Generator._ssis_expr_to_m(expr)

    def transpile(self, expr: str, target: TargetLanguage) -> str:
        """Convert *expr* to the specified *target* language."""
        if target == TargetLanguage.PYSPARK:
            return self.to_pyspark(expr)
        return self.to_m(expr)

    @staticmethod
    def validate(expr: str) -> ValidationResult:
        """Validate an SSIS expression and report issues.

        Checks for:
        - Unknown function calls
        - Unbalanced parentheses
        - Empty/null expressions
        - Unrecognised type cast prefixes
        """
        result = ValidationResult(expression=expr)

        if not expr or not expr.strip():
            result.issues.append(
                ExpressionIssue("warning", "Empty expression", expr),
            )
            return result

        expr_s = expr.strip()

        # Check balanced parentheses
        depth = 0
        for i, ch in enumerate(expr_s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if depth < 0:
                result.issues.append(
                    ExpressionIssue("error", "Unbalanced parenthesis: extra ')'", expr, i),
                )
                break
        if depth > 0:
            result.issues.append(
                ExpressionIssue("error", f"Unbalanced parentheses: {depth} unclosed '('", expr),
            )

        # Check for unknown functions
        for m in re.finditer(r"\b([A-Z_][A-Z0-9_]*)\s*\(", expr_s, re.IGNORECASE):
            func = m.group(1).upper()
            if func not in _KNOWN_FUNCTIONS:
                result.issues.append(
                    ExpressionIssue(
                        "warning",
                        f"Unknown function '{func}' — may require manual conversion",
                        expr,
                        m.start(),
                    ),
                )

        # Check for unrecognised DT_ casts
        for m in re.finditer(r"\(DT_([A-Z0-9_]+)", expr_s, re.IGNORECASE):
            dtype = f"DT_{m.group(1).upper()}"
            known_types = {
                "DT_STR", "DT_WSTR", "DT_I1", "DT_UI1", "DT_I2", "DT_UI2",
                "DT_I4", "DT_UI4", "DT_I8", "DT_UI8", "DT_R4", "DT_R8",
                "DT_DECIMAL", "DT_NUMERIC", "DT_CY", "DT_BOOL",
                "DT_DATE", "DT_DBDATE", "DT_DBTIMESTAMP", "DT_DBTIMESTAMP2",
                "DT_GUID", "DT_BYTES", "DT_FLOAT",
            }
            if dtype not in known_types:
                result.issues.append(
                    ExpressionIssue(
                        "warning",
                        f"Unknown SSIS type cast '{dtype}'",
                        expr,
                        m.start(),
                    ),
                )

        return result
