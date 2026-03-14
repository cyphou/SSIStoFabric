"""
Intelligent Migration
======================
Phase 26: AI-assisted pattern recognition, LLM transpilation,
smart strategy recommendation, NL queries, and auto-test generation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Pattern Recognition
# =====================================================================


class PackagePattern(str, Enum):
    """Recognized SSIS package patterns."""

    SIMPLE_COPY = "simple_copy"
    INCREMENTAL_LOAD = "incremental_load"
    SCD_DIMENSION = "scd_dimension"
    FACT_LOAD = "fact_load"
    ETL_PIPELINE = "etl_pipeline"
    DATA_WAREHOUSE = "data_warehouse"
    MASTER_DATA = "master_data"
    CDC_STREAMING = "cdc_streaming"
    ORCHESTRATOR = "orchestrator"
    FILE_PROCESSING = "file_processing"
    CUSTOM = "custom"


@dataclass
class PatternMatch:
    """Result of pattern recognition on a package."""

    package_name: str
    pattern: PackagePattern
    confidence: float = 0.0  # 0-1
    indicators: list[str] = field(default_factory=list)
    suggested_strategy: str = ""
    complexity_estimate: str = "MEDIUM"

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "pattern": self.pattern.value,
            "confidence": round(self.confidence, 2),
            "indicators": self.indicators,
            "suggested_strategy": self.suggested_strategy,
            "complexity_estimate": self.complexity_estimate,
        }


def classify_package(package: Any) -> PatternMatch:
    """Auto-classify an SSIS package by its migration complexity pattern.

    Analyzes task types, data flow components, variables, and connections
    to determine the most likely pattern.
    """
    pkg_name = getattr(package, "name", "unknown")
    tasks = getattr(package, "control_flow_tasks", [])
    data_flows = getattr(package, "data_flow_components", [])
    connections = getattr(package, "connection_managers", [])
    variables = getattr(package, "variables", [])
    task_types = [getattr(t, "task_type", "").lower() for t in tasks]
    task_names = [getattr(t, "name", "").lower() for t in tasks]

    indicators: list[str] = []
    scores: dict[PackagePattern, float] = {p: 0.0 for p in PackagePattern}

    # Orchestrator pattern
    exec_pkg_count = sum(1 for t in task_types if "executepackage" in t)
    if exec_pkg_count > 0:
        scores[PackagePattern.ORCHESTRATOR] += 0.4 + min(exec_pkg_count * 0.1, 0.4)
        indicators.append(f"{exec_pkg_count} Execute Package tasks")

    # CDC pattern
    cdc_hints = sum(1 for n in task_names if "cdc" in n)
    if cdc_hints > 0:
        scores[PackagePattern.CDC_STREAMING] += 0.6 + min(cdc_hints * 0.1, 0.3)
        indicators.append(f"{cdc_hints} CDC-related tasks")

    # SCD pattern
    scd_hints = sum(1 for n in task_names if "scd" in n or "dimension" in n or "slowly" in n)
    if scd_hints > 0:
        scores[PackagePattern.SCD_DIMENSION] += 0.5 + min(scd_hints * 0.15, 0.4)
        indicators.append(f"{scd_hints} SCD/dimension tasks")

    # Fact load pattern
    fact_hints = sum(1 for n in task_names if "fact" in n or "measure" in n)
    if fact_hints > 0:
        scores[PackagePattern.FACT_LOAD] += 0.5 + min(fact_hints * 0.15, 0.4)
        indicators.append(f"{fact_hints} fact-related tasks")

    # File processing
    file_hints = sum(1 for t in task_types if "file" in t)
    file_conn = sum(1 for c in connections if "file" in getattr(c, "connection_type", "").lower())
    if file_hints + file_conn > 0:
        scores[PackagePattern.FILE_PROCESSING] += 0.4 + min((file_hints + file_conn) * 0.1, 0.4)
        indicators.append(f"{file_hints + file_conn} file-related items")

    # Simple copy (few tasks, one data flow, no complex logic)
    if len(tasks) <= 3 and len(data_flows) <= 2:
        scores[PackagePattern.SIMPLE_COPY] += 0.5
        indicators.append("Simple structure (≤3 tasks)")

    # Incremental load (variables for watermark, date filters)
    watermark_kws = ("watermark", "lastrun", "maxdate", "incremental")
    watermark_hints = sum(
        1 for v in variables
        if any(kw in getattr(v, "name", "").lower() for kw in watermark_kws)
    )
    if watermark_hints > 0:
        scores[PackagePattern.INCREMENTAL_LOAD] += 0.5 + min(watermark_hints * 0.2, 0.4)
        indicators.append(f"{watermark_hints} watermark/incremental variables")

    # Data warehouse (many tasks, multiple data flows)
    if len(tasks) > 8 and len(data_flows) > 3:
        scores[PackagePattern.DATA_WAREHOUSE] += 0.6
        indicators.append(f"Complex: {len(tasks)} tasks, {len(data_flows)} data flows")

    # ETL pipeline (moderate complexity)
    if 3 < len(tasks) <= 8:
        scores[PackagePattern.ETL_PIPELINE] += 0.3
        indicators.append(f"Standard ETL: {len(tasks)} tasks")

    # Find best match
    best_pattern = max(scores, key=scores.get)  # type: ignore[arg-type]
    best_score = scores[best_pattern]

    if best_score < 0.2:
        best_pattern = PackagePattern.CUSTOM
        best_score = 0.5

    # Strategy recommendation
    strategy_map = {
        PackagePattern.SIMPLE_COPY: "data_factory",
        PackagePattern.INCREMENTAL_LOAD: "data_factory",
        PackagePattern.SCD_DIMENSION: "spark",
        PackagePattern.FACT_LOAD: "hybrid",
        PackagePattern.ETL_PIPELINE: "hybrid",
        PackagePattern.DATA_WAREHOUSE: "hybrid",
        PackagePattern.MASTER_DATA: "spark",
        PackagePattern.CDC_STREAMING: "spark",
        PackagePattern.ORCHESTRATOR: "data_factory",
        PackagePattern.FILE_PROCESSING: "hybrid",
        PackagePattern.CUSTOM: "hybrid",
    }

    complexity_map = {
        PackagePattern.SIMPLE_COPY: "LOW",
        PackagePattern.INCREMENTAL_LOAD: "LOW",
        PackagePattern.ORCHESTRATOR: "MEDIUM",
        PackagePattern.FILE_PROCESSING: "MEDIUM",
        PackagePattern.ETL_PIPELINE: "MEDIUM",
        PackagePattern.FACT_LOAD: "MEDIUM",
        PackagePattern.SCD_DIMENSION: "HIGH",
        PackagePattern.MASTER_DATA: "HIGH",
        PackagePattern.CDC_STREAMING: "HIGH",
        PackagePattern.DATA_WAREHOUSE: "HIGH",
        PackagePattern.CUSTOM: "MEDIUM",
    }

    return PatternMatch(
        package_name=pkg_name,
        pattern=best_pattern,
        confidence=min(best_score, 1.0),
        indicators=indicators,
        suggested_strategy=strategy_map.get(best_pattern, "hybrid"),
        complexity_estimate=complexity_map.get(best_pattern, "MEDIUM"),
    )


# =====================================================================
# Natural Language Query
# =====================================================================


@dataclass
class NLQueryResult:
    """Result of a natural language query against migration data."""

    query: str
    interpreted_as: str
    results: list[dict[str, Any]] = field(default_factory=list)
    match_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "interpreted_as": self.interpreted_as,
            "match_count": self.match_count,
            "results": self.results,
        }


def natural_language_query(query: str, packages: list[Any]) -> NLQueryResult:
    """Process a natural language query against package metadata.

    Supports queries like:
    - "show all packages that write to DimCustomer"
    - "packages with more than 5 tasks"
    - "find CDC packages"
    """
    query_lower = query.lower()
    results: list[dict[str, Any]] = []
    interpretation = ""

    # Pattern: write to / read from table
    table_match = re.search(r"(?:write|read|access|use|reference)\s+(?:to|from)?\s*(\w+)", query_lower)
    if table_match:
        table_name = table_match.group(1)
        interpretation = f"Find packages referencing '{table_name}'"
        for pkg in packages:
            pkg_name = getattr(pkg, "name", "")
            # Check in task names and data flow components
            all_names = [getattr(t, "name", "").lower() for t in getattr(pkg, "control_flow_tasks", [])]
            if any(table_name in n for n in all_names) or table_name in pkg_name.lower():
                results.append({"package": pkg_name, "match": "name contains reference"})

    # Pattern: more/less than N tasks
    count_match = re.search(r"(?:more|greater|less|fewer)\s+than\s+(\d+)\s+(task|flow|connection)", query_lower)
    if count_match:
        threshold = int(count_match.group(1))
        entity = count_match.group(2)
        op = ">" if "more" in query_lower or "greater" in query_lower else "<"
        interpretation = f"Find packages with {op} {threshold} {entity}s"
        for pkg in packages:
            count = 0
            if "task" in entity:
                count = getattr(pkg, "total_tasks", 0)
            elif "flow" in entity:
                count = getattr(pkg, "total_data_flows", 0)
            elif "connection" in entity:
                count = len(getattr(pkg, "connection_managers", []))
            if (op == ">" and count > threshold) or (op == "<" and count < threshold):
                results.append({"package": getattr(pkg, "name", ""), entity + "_count": count})

    # Pattern: find type (CDC, SCD, ETL, etc.)
    type_match = re.search(r"(?:find|show|list|get)\s+(\w+)\s+package", query_lower)
    if type_match and not results:
        keyword = type_match.group(1)
        interpretation = f"Find packages matching '{keyword}'"
        for pkg in packages:
            pkg_name = getattr(pkg, "name", "")
            task_names = " ".join(getattr(t, "name", "") for t in getattr(pkg, "control_flow_tasks", []))
            if keyword in pkg_name.lower() or keyword in task_names.lower():
                results.append({"package": pkg_name, "match": f"contains '{keyword}'"})

    # Fallback: keyword search
    if not results and not interpretation:
        keywords = [w for w in query_lower.split() if len(w) > 3]
        interpretation = f"Keyword search: {', '.join(keywords)}"
        for pkg in packages:
            pkg_name = getattr(pkg, "name", "")
            if any(kw in pkg_name.lower() for kw in keywords):
                results.append({"package": pkg_name, "match": "keyword match"})

    return NLQueryResult(
        query=query,
        interpreted_as=interpretation,
        results=results,
        match_count=len(results),
    )


# =====================================================================
# Auto Test Generation
# =====================================================================


@dataclass
class GeneratedTest:
    """An automatically generated validation test."""

    test_name: str
    test_type: str  # row_count, column_check, not_null, type_check
    target_table: str
    expected: Any = None
    code: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_name": self.test_name,
            "test_type": self.test_type,
            "target_table": self.target_table,
            "expected": self.expected,
        }


def generate_validation_tests(package: Any) -> list[GeneratedTest]:
    """Generate validation tests from SSIS package semantics.

    Creates tests based on:
    - Destination tables → row count checks
    - Column mappings → column existence checks
    - Not-null constraints → null checks
    """
    tests: list[GeneratedTest] = []
    pkg_name = getattr(package, "name", "unknown")

    # Generate table existence tests from tasks
    for task in getattr(package, "control_flow_tasks", []):
        task_name = getattr(task, "name", "")
        table_match = re.search(r"(?:Load|Insert|Update|Merge)\s*_?\s*(\w+)", task_name)
        if table_match:
            table = table_match.group(1)
            tests.append(GeneratedTest(
                test_name=f"test_{pkg_name}_{table}_exists",
                test_type="table_exists",
                target_table=table,
                code=f"assert spark.catalog.tableExists('{table}'), 'Table {table} should exist after migration'",
            ))
            tests.append(GeneratedTest(
                test_name=f"test_{pkg_name}_{table}_not_empty",
                test_type="row_count",
                target_table=table,
                expected="> 0",
                code=f"assert spark.table('{table}').count() > 0, 'Table {table} should not be empty'",
            ))

    # Generate column check from data flow components
    for comp in getattr(package, "data_flow_components", []):
        comp_name = getattr(comp, "name", "")
        columns = getattr(comp, "output_columns", [])
        if columns:
            table = re.sub(r"[^a-zA-Z0-9_]", "_", comp_name)
            for col in columns[:5]:  # Limit to first 5 columns
                col_name = getattr(col, "name", str(col))
                tests.append(GeneratedTest(
                    test_name=f"test_{table}_{col_name}_exists",
                    test_type="column_check",
                    target_table=table,
                    expected=col_name,
                    code=f"assert '{col_name}' in spark.table('{table}').columns",
                ))

    return tests


def write_test_file(tests: list[GeneratedTest], output_path: Path) -> Path:
    """Write generated tests as a Python test file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        '"""Auto-generated validation tests from SSIS package semantics."""',
        "",
        "import pytest",
        "",
        "",
    ]

    for test in tests:
        lines.append(f"def {test.test_name}():")
        lines.append(f'    """{test.test_type}: {test.target_table}"""')
        if test.code:
            lines.append(f"    {test.code}")
        else:
            lines.append("    pass  # TODO: implement")
        lines.append("")
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("test_file_generated", path=str(output_path), tests=len(tests))
    return output_path


# =====================================================================
# Strategy Recommendation
# =====================================================================


@dataclass
class StrategyRecommendation:
    """AI-generated strategy recommendation."""

    package_name: str
    recommended_strategy: str
    confidence: float = 0.0
    reasoning: list[str] = field(default_factory=list)
    alternatives: list[str] = field(default_factory=list)
    estimated_effort_hours: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "recommended_strategy": self.recommended_strategy,
            "confidence": round(self.confidence, 2),
            "reasoning": self.reasoning,
            "alternatives": self.alternatives,
            "estimated_effort_hours": round(self.estimated_effort_hours, 1),
        }


def recommend_strategy(package: Any) -> StrategyRecommendation:
    """Generate a smart strategy recommendation for a package."""
    pattern = classify_package(package)
    pkg_name = getattr(package, "name", "unknown")
    task_count = getattr(package, "total_tasks", 0)
    df_count = getattr(package, "total_data_flows", 0)

    reasoning = list(pattern.indicators)
    alternatives = []

    if pattern.suggested_strategy == "data_factory":
        reasoning.append("Package is control-flow dominant; Data Factory is the natural fit.")
        alternatives = ["hybrid"]
    elif pattern.suggested_strategy == "spark":
        reasoning.append("Complex transformations require PySpark notebook processing.")
        alternatives = ["hybrid", "data_factory"]
    else:
        reasoning.append("Mixed workload benefits from hybrid approach (pipelines + notebooks).")
        alternatives = ["data_factory", "spark"]

    # Effort estimation
    base_hours = {
        "LOW": 2,
        "MEDIUM": 8,
        "HIGH": 20,
    }
    effort = base_hours.get(pattern.complexity_estimate, 8)
    effort += task_count * 0.5 + df_count * 1.5

    return StrategyRecommendation(
        package_name=pkg_name,
        recommended_strategy=pattern.suggested_strategy,
        confidence=pattern.confidence,
        reasoning=reasoning,
        alternatives=alternatives,
        estimated_effort_hours=effort,
    )


# =====================================================================
# Knowledge Base
# =====================================================================


@dataclass
class KnowledgeEntry:
    """An entry in the migration knowledge base."""

    pattern: str
    source_description: str
    target_description: str
    success_rate: float = 0.0
    tips: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern,
            "source_description": self.source_description,
            "target_description": self.target_description,
            "success_rate": round(self.success_rate, 2),
            "tips": self.tips,
        }


# Pre-built knowledge base
KNOWLEDGE_BASE: list[KnowledgeEntry] = [
    KnowledgeEntry(
        pattern="Execute SQL Task",
        source_description="Runs ad-hoc SQL (DDL/DML/stored procs)",
        target_description="Data Factory Script activity or Spark spark.sql()",
        success_rate=0.95,
        tips=["Map connection managers first", "Test stored proc compatibility"],
    ),
    KnowledgeEntry(
        pattern="Data Flow - OLE DB Source → Destination",
        source_description="Bulk data copy between databases",
        target_description="Data Factory Copy Activity with source/sink",
        success_rate=0.98,
        tips=["Use Copy Activity for simple moves", "Enable staging for large datasets"],
    ),
    KnowledgeEntry(
        pattern="Derived Column Transform",
        source_description="SSIS expression-based column derivation",
        target_description="df.withColumn() in PySpark",
        success_rate=0.90,
        tips=["Most expressions transpile automatically", "Review date format expressions"],
    ),
    KnowledgeEntry(
        pattern="Lookup Transform",
        source_description="Reference table join for enrichment",
        target_description="df.join(lookup_df, ...) in PySpark",
        success_rate=0.92,
        tips=["Use broadcast for small lookup tables", "Handle no-match rows explicitly"],
    ),
    KnowledgeEntry(
        pattern="SCD Type 2",
        source_description="Slowly Changing Dimension with history tracking",
        target_description="Delta Lake MERGE with date range tracking",
        success_rate=0.75,
        tips=["Test merge logic thoroughly", "Verify surrogate key generation"],
    ),
    KnowledgeEntry(
        pattern="For Each Loop",
        source_description="Iterates over collection (files, rows)",
        target_description="Data Factory ForEach activity",
        success_rate=0.93,
        tips=["Check batch size limits", "Consider parallel execution settings"],
    ),
    KnowledgeEntry(
        pattern="Script Task (C#)",
        source_description="Custom C# logic in Script Task",
        target_description="Python function in Spark notebook",
        success_rate=0.60,
        tips=["Review auto-transpilation carefully", "Complex .NET code needs manual review"],
    ),
    KnowledgeEntry(
        pattern="Execute Package Task",
        source_description="Calls child SSIS packages",
        target_description="Data Factory Invoke Pipeline activity",
        success_rate=0.96,
        tips=["Migrate child packages first", "Map parameter passing correctly"],
    ),
]


def search_knowledge_base(query: str) -> list[KnowledgeEntry]:
    """Search the knowledge base by keyword."""
    query_lower = query.lower()
    return [
        entry for entry in KNOWLEDGE_BASE
        if query_lower in entry.pattern.lower()
        or query_lower in entry.source_description.lower()
        or query_lower in entry.target_description.lower()
    ]


def write_intelligence_report(
    classifications: list[PatternMatch],
    recommendations: list[StrategyRecommendation],
    output_path: Path,
) -> Path:
    """Write the intelligent migration analysis report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_packages": len(classifications),
        "pattern_distribution": {},
        "classifications": [c.to_dict() for c in classifications],
        "strategy_recommendations": [r.to_dict() for r in recommendations],
        "knowledge_base_entries": len(KNOWLEDGE_BASE),
    }
    for c in classifications:
        p = c.pattern.value
        report["pattern_distribution"][p] = report["pattern_distribution"].get(p, 0) + 1

    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("intelligence_report_written", path=str(output_path))
    return output_path
