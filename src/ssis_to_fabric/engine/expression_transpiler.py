"""
Unified SSIS Expression Transpiler
====================================
Provides a single entry point for converting SSIS expressions to PySpark
or Power Query M, delegating to the existing per-generator transpilers.

Usage::

    from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

    transpiler = ExpressionTranspiler()
    pyspark = transpiler.to_pyspark("UPPER(ColumnA)")
    m_expr = transpiler.to_m("UPPER(ColumnA)")
"""

from __future__ import annotations

from enum import Enum


class TargetLanguage(str, Enum):
    PYSPARK = "pyspark"
    POWER_QUERY_M = "m"


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
