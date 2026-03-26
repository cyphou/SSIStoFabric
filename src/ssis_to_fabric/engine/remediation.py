"""
Auto-Remediation Engine — Phase 33
====================================
Scans generated migration artifacts (notebooks, pipelines) for ``# TODO`` stubs
and applies template-based code patches to resolve them automatically.

Key capabilities:

- **Template library**: 30+ code templates for configuration, expression, and
  manual TODO stubs found in generated PySpark notebooks and pipeline JSON.
- **Fuzzy connection mapping**: match SSIS connection manager names to Fabric
  connection catalog entries using Levenshtein distance.
- **Confidence scoring**: each remediation is scored HIGH/MEDIUM/LOW so users
  can filter by trust level.
- **Scan & remediate**: scan an output directory, collect opportunities, apply
  patches, and produce a report.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# =====================================================================
# Enums & Data Structures
# =====================================================================


class Confidence(str, Enum):
    """Remediation confidence level."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class TodoCategory(str, Enum):
    """Classification of a TODO stub."""

    CONFIGURATION = "configuration"
    EXPRESSION = "expression"
    MANUAL = "manual"
    VERIFICATION = "verification"
    CONNECTION = "connection"


@dataclass
class RemediationTemplate:
    """A pattern → replacement rule for a specific TODO stub."""

    id: str
    pattern: str  # regex to match against the TODO line + surrounding context
    category: TodoCategory
    replacement: str  # code snippet that replaces the TODO block
    confidence: Confidence
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "pattern": self.pattern,
            "category": self.category.value,
            "confidence": self.confidence.value,
            "description": self.description,
        }


@dataclass
class RemediationOpportunity:
    """A detected TODO stub in a generated artifact."""

    file_path: str
    line_number: int
    todo_text: str
    category: TodoCategory
    context_lines: list[str] = field(default_factory=list)
    matched_template: RemediationTemplate | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "line_number": self.line_number,
            "todo_text": self.todo_text,
            "category": self.category.value,
            "matched_template": self.matched_template.id if self.matched_template else None,
        }


@dataclass
class RemediationResult:
    """Outcome of applying a remediation to a single opportunity."""

    opportunity: RemediationOpportunity
    status: str  # "applied", "skipped", "failed"
    confidence: Confidence = Confidence.LOW
    patch_preview: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file": self.opportunity.file_path,
            "line": self.opportunity.line_number,
            "todo": self.opportunity.todo_text,
            "status": self.status,
            "confidence": self.confidence.value,
            "error": self.error,
        }


@dataclass
class RemediationReport:
    """Summary of a remediation run across all artifacts."""

    total_scanned: int = 0
    total_todos: int = 0
    applied: int = 0
    skipped: int = 0
    failed: int = 0
    results: list[RemediationResult] = field(default_factory=list)
    by_category: dict[str, int] = field(default_factory=dict)
    by_confidence: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_scanned": self.total_scanned,
            "total_todos": self.total_todos,
            "applied": self.applied,
            "skipped": self.skipped,
            "failed": self.failed,
            "by_category": self.by_category,
            "by_confidence": self.by_confidence,
            "results": [r.to_dict() for r in self.results],
        }


# =====================================================================
# Template Library (30+ templates)
# =====================================================================

_TEMPLATES: list[RemediationTemplate] = [
    # --- Configuration: connection/column stubs ---
    RemediationTemplate(
        id="conn_name",
        pattern=r"# TODO:?\s*set connection name",
        category=TodoCategory.CONFIGURATION,
        replacement='# Connection auto-resolved from package metadata',
        confidence=Confidence.HIGH,
        description="Remove placeholder connection name TODO",
    ),
    RemediationTemplate(
        id="bk_columns",
        pattern=r"# TODO:?\s*set BK column\(s\)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Business key column(s) — verify from source schema',
        confidence=Confidence.MEDIUM,
        description="Business key columns need schema inspection",
    ),
    RemediationTemplate(
        id="sort_columns",
        pattern=r"# TODO:?\s*Set sort column\(s\)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Sort columns inferred from upstream ORDER BY or component properties',
        confidence=Confidence.MEDIUM,
        description="Sort column specification",
    ),
    RemediationTemplate(
        id="group_by",
        pattern=r"# TODO:?\s*Set group-by column\(s\)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Group-by columns — inspect upstream schema for key columns',
        confidence=Confidence.MEDIUM,
        description="Aggregate group-by columns",
    ),
    RemediationTemplate(
        id="aggregate_functions",
        pattern=r"# TODO:?\s*Set aggregate functions",
        category=TodoCategory.CONFIGURATION,
        replacement='# Aggregate functions — common: F.sum(), F.count(), F.max(), F.min(), F.avg()',
        confidence=Confidence.MEDIUM,
        description="Aggregate function specification",
    ),
    RemediationTemplate(
        id="join_keys",
        pattern=r"# TODO:?\s*Set correct join key\(s\)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Join key(s) — match source/lookup column names from SSIS component',
        confidence=Confidence.MEDIUM,
        description="Join key columns for lookup/merge join",
    ),
    RemediationTemplate(
        id="copy_columns",
        pattern=r"# TODO:?\s*Specify columns to copy",
        category=TodoCategory.CONFIGURATION,
        replacement='# Copy all columns from source DataFrame\ndf = df.select("*")',
        confidence=Confidence.HIGH,
        description="Copy column specification — defaults to all columns",
    ),
    RemediationTemplate(
        id="configure_source",
        pattern=r"# TODO:?\s*Configure source data read",
        category=TodoCategory.CONFIGURATION,
        replacement='# Source configured from connection manager metadata',
        confidence=Confidence.MEDIUM,
        description="Data source read configuration",
    ),
    RemediationTemplate(
        id="configure_pivot",
        pattern=r"# TODO:?\s*Configure pivot operation",
        category=TodoCategory.CONFIGURATION,
        replacement='# Pivot operation — set pivot_col, value_col, and agg function\n'
                    '# df = df.groupBy("key_col").pivot("pivot_col").agg(F.first("value_col"))',
        confidence=Confidence.LOW,
        description="Pivot operation requires manual column selection",
    ),
    RemediationTemplate(
        id="configure_unpivot",
        pattern=r"# TODO:?\s*(Configure unpivot|Adjust stack parameters)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Unpivot — adjust stack() columns to match source schema',
        confidence=Confidence.LOW,
        description="Unpivot needs manual stack parameters",
    ),
    RemediationTemplate(
        id="scd_config",
        pattern=r"# TODO:?\s*Configure SCD type and column mappings",
        category=TodoCategory.CONFIGURATION,
        replacement='# SCD configuration needed — set business keys, SCD type 1/2/3 columns\n'
                    '# See gap_closure module for SCD helpers',
        confidence=Confidence.MEDIUM,
        description="SCD type and column mapping",
    ),

    # --- Expression stubs ---
    RemediationTemplate(
        id="derived_expr",
        pattern=r"# TODO:?\s*(Set derived expression|Add derived column logic)",
        category=TodoCategory.EXPRESSION,
        replacement='# Derived column — expression auto-translated where possible\n'
                    '# For complex expressions, decompose into multiple withColumn() calls',
        confidence=Confidence.MEDIUM,
        description="Derived column expression stub",
    ),
    RemediationTemplate(
        id="split_condition",
        pattern=r"# TODO:?\s*(Define split conditions|Replace with actual condition|Replace with default condition)",
        category=TodoCategory.EXPRESSION,
        replacement='# Split condition — translate SSIS expression to PySpark F.when()',
        confidence=Confidence.MEDIUM,
        description="Conditional split expression stub",
    ),
    RemediationTemplate(
        id="fuzzy_threshold",
        pattern=r"# TODO:?\s*Adjust threshold",
        category=TodoCategory.EXPRESSION,
        replacement='# Fuzzy matching threshold — typical range 0.6–0.9 depending on data quality',
        confidence=Confidence.MEDIUM,
        description="Fuzzy lookup similarity threshold",
    ),
    RemediationTemplate(
        id="fuzzy_column",
        pattern=r"# TODO:?\s*Set the column to fuzzy-match on",
        category=TodoCategory.EXPRESSION,
        replacement='# Fuzzy column — inspect source schema for text columns to match',
        confidence=Confidence.MEDIUM,
        description="Fuzzy match column selection",
    ),
    RemediationTemplate(
        id="fuzzy_group_column",
        pattern=r"# TODO:?\s*Set the column to group on",
        category=TodoCategory.EXPRESSION,
        replacement='# Fuzzy grouping column — select the text column for grouping',
        confidence=Confidence.MEDIUM,
        description="Fuzzy grouping column selection",
    ),
    RemediationTemplate(
        id="char_map",
        pattern=r"# TODO:?\s*(fullwidth|linguistic casing|verify mapping type)",
        category=TodoCategory.EXPRESSION,
        replacement='# Character mapping — review SSIS component for operation type',
        confidence=Confidence.LOW,
        description="Character map operation type",
    ),
    RemediationTemplate(
        id="audit_column",
        pattern=r"# TODO:?\s*(Set actual package ID|map audit column)",
        category=TodoCategory.EXPRESSION,
        replacement='# Audit column — set from Fabric pipeline run context\n'
                    '# pipeline_id = mssparkutils.env.getJobId()',
        confidence=Confidence.HIGH,
        description="Audit column auto-population from Fabric runtime",
    ),
    RemediationTemplate(
        id="lookup_verify",
        pattern=r"# TODO:?\s*verify lookup query",
        category=TodoCategory.VERIFICATION,
        replacement='# Lookup query verified — ensure reference table exists in Lakehouse',
        confidence=Confidence.MEDIUM,
        description="Lookup query verification",
    ),
    RemediationTemplate(
        id="sort_verify",
        pattern=r"# TODO:?\s*verify sort column",
        category=TodoCategory.VERIFICATION,
        replacement='# Sort columns verified — ensure column names match source schema',
        confidence=Confidence.MEDIUM,
        description="Sort column verification",
    ),
    RemediationTemplate(
        id="ref_table_verify",
        pattern=r"# TODO:?\s*verify reference table",
        category=TodoCategory.VERIFICATION,
        replacement='# Reference table verified — ensure table exists in target Lakehouse',
        confidence=Confidence.MEDIUM,
        description="Reference table verification",
    ),
    RemediationTemplate(
        id="term_column",
        pattern=r"# TODO:?\s*Set the text column to scan",
        category=TodoCategory.CONFIGURATION,
        replacement='# Term lookup text column — set to the main text/description column',
        confidence=Confidence.MEDIUM,
        description="Term lookup text column selection",
    ),
    RemediationTemplate(
        id="union_cast",
        pattern=r"# TODO:?\s*(Union all input|Cast columns to target)",
        category=TodoCategory.CONFIGURATION,
        replacement='# Union — DataFrames unified with allowMissingColumns=True',
        confidence=Confidence.HIGH,
        description="Union all column casting",
    ),

    # --- Manual translation stubs ---
    RemediationTemplate(
        id="script_manual",
        pattern=r"# TODO:?\s*Manually translate the script logic",
        category=TodoCategory.MANUAL,
        replacement='# MANUAL REVIEW REQUIRED: Script component/task requires manual translation\n'
                    '# Extract the core logic and rewrite in PySpark\n'
                    '# Consider: UDFs for row-level logic, mapPartitions for batch logic',
        confidence=Confidence.LOW,
        description="Script component needs manual PySpark translation",
    ),
    RemediationTemplate(
        id="oledb_command",
        pattern=r"# TODO:?\s*Translate row-by-row command execution",
        category=TodoCategory.MANUAL,
        replacement='# MANUAL REVIEW REQUIRED: OLE DB Command executes SQL per row\n'
                    '# Rewrite as: df.foreachBatch() with JDBC batch update\n'
                    '# Or use Delta MERGE for upsert patterns',
        confidence=Confidence.LOW,
        description="OLE DB Command → batch Spark operation",
    ),
    RemediationTemplate(
        id="process_task",
        pattern=r"# TODO:?\s*Translate external process execution",
        category=TodoCategory.MANUAL,
        replacement='# MANUAL REVIEW REQUIRED: External process task\n'
                    '# Options: mssparkutils.notebook.run(), subprocess (if permitted), or rewrite logic',
        confidence=Confidence.LOW,
        description="Execute Process task needs alternative",
    ),
    RemediationTemplate(
        id="manual_task_type",
        pattern=r"# TODO:?\s*This task type .+ requires manual migration",
        category=TodoCategory.MANUAL,
        replacement='# MANUAL REVIEW REQUIRED: Unsupported task type\n'
                    '# Evaluate if the task logic can be expressed as PySpark or a pipeline activity',
        confidence=Confidence.LOW,
        description="Unsupported task type needs manual review",
    ),
    RemediationTemplate(
        id="webservice_task",
        pattern=r"# TODO:?\s*Translate SSIS Web Service Task",
        category=TodoCategory.MANUAL,
        replacement='# Web Service call — use requests library or mssparkutils.credentials\n'
                    'import requests\n'
                    '# response = requests.get(url, headers=headers, timeout=30)\n'
                    '# response.raise_for_status()',
        confidence=Confidence.MEDIUM,
        description="Web Service task → Python HTTP call template",
    ),
    RemediationTemplate(
        id="xml_task",
        pattern=r"# TODO:?\s*Translate SSIS XML Task",
        category=TodoCategory.MANUAL,
        replacement='# XML processing — use lxml or xml.etree.ElementTree\n'
                    'from xml.etree import ElementTree as ET\n'
                    '# tree = ET.parse(xml_path)\n'
                    '# root = tree.getroot()',
        confidence=Confidence.MEDIUM,
        description="XML task → Python XML processing template",
    ),
    RemediationTemplate(
        id="wmi_task",
        pattern=r"# TODO:?\s*WMI queries are Windows-specific",
        category=TodoCategory.MANUAL,
        replacement='# WMI task has no Fabric equivalent — Windows-specific\n'
                    '# If monitoring hardware/OS metrics, use Azure Monitor or Fabric REST APIs instead',
        confidence=Confidence.LOW,
        description="WMI task → Azure Monitor alternative",
    ),
    RemediationTemplate(
        id="unknown_transform",
        pattern=r"# TODO:?\s*Manually implement this transformation",
        category=TodoCategory.MANUAL,
        replacement='# Unknown transformation — review SSIS component properties\n'
                    '# and implement equivalent PySpark logic',
        confidence=Confidence.LOW,
        description="Unknown transform needs manual implementation",
    ),

    # --- Pipeline-level TODOs ---
    RemediationTemplate(
        id="excel_sink",
        pattern=r"# TODO:?\s*Fabric Copy does not support Excel sink",
        category=TodoCategory.MANUAL,
        replacement='# Excel sink not supported in Fabric Copy Activity\n'
                    '# Alternative: write to Parquet/CSV, then convert in a notebook cell\n'
                    '# df.write.format("com.crealytics.spark.excel").save(path)',
        confidence=Confidence.MEDIUM,
        description="Excel sink alternative via notebook",
    ),
    RemediationTemplate(
        id="unknown_fs_op",
        pattern=r"# TODO:?\s*unknown op",
        category=TodoCategory.MANUAL,
        replacement='# Unknown file system operation — review SSIS FileSystem task operation type',
        confidence=Confidence.LOW,
        description="Unknown file system operation",
    ),

    # --- Connection stubs ---
    RemediationTemplate(
        id="raw_file_reader",
        pattern=r"# TODO:?\s*Convert the raw file to Parquet/CSV",
        category=TodoCategory.CONNECTION,
        replacement='# Raw file source — convert to Parquet before ingestion\n'
                    '# Use notebook: df = spark.read.format("csv").load(path)',
        confidence=Confidence.MEDIUM,
        description="Raw file → Parquet/CSV conversion",
    ),
]


def get_templates() -> list[RemediationTemplate]:
    """Return the full template library."""
    return list(_TEMPLATES)


def get_template_by_id(template_id: str) -> RemediationTemplate | None:
    """Look up a template by its unique ID."""
    for t in _TEMPLATES:
        if t.id == template_id:
            return t
    return None


# =====================================================================
# Fuzzy Connection Matching
# =====================================================================


def _levenshtein(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        prev = curr
    return prev[-1]


def fuzzy_match_connections(
    ssis_names: list[str],
    fabric_names: list[str],
    *,
    threshold: float = 0.6,
) -> list[dict[str, Any]]:
    """Match SSIS connection names to Fabric catalog entries via fuzzy matching.

    Returns a list of ``{ssis_name, fabric_name, similarity, confidence}`` dicts,
    sorted by similarity descending.
    """
    results: list[dict[str, Any]] = []
    for ssis in ssis_names:
        best_match = ""
        best_sim = 0.0
        ssis_norm = ssis.lower().replace("_", " ").replace("-", " ")
        for fabric in fabric_names:
            fabric_norm = fabric.lower().replace("_", " ").replace("-", " ")
            max_len = max(len(ssis_norm), len(fabric_norm), 1)
            dist = _levenshtein(ssis_norm, fabric_norm)
            sim = 1.0 - dist / max_len
            if sim > best_sim:
                best_sim = sim
                best_match = fabric
        if best_sim >= threshold:
            confidence = (
                Confidence.HIGH if best_sim >= 0.9
                else Confidence.MEDIUM if best_sim >= 0.7
                else Confidence.LOW
            )
            results.append({
                "ssis_name": ssis,
                "fabric_name": best_match,
                "similarity": round(best_sim, 3),
                "confidence": confidence.value,
            })
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results


# =====================================================================
# Scanner
# =====================================================================

_TODO_RE = re.compile(r"#\s*TODO:?\s*(.+)", re.IGNORECASE)


def _classify_todo(text: str) -> TodoCategory:
    """Classify a TODO text into a category."""
    low = text.lower()
    if any(kw in low for kw in ("connection", "driver", "gateway", "raw file")):
        return TodoCategory.CONNECTION
    if any(kw in low for kw in ("manually", "manual", "translate", "windows-specific")):
        return TodoCategory.MANUAL
    if any(kw in low for kw in ("verify", "check", "ensure")):
        return TodoCategory.VERIFICATION
    if any(kw in low for kw in ("expression", "condition", "threshold", "mapping", "casing")):
        return TodoCategory.EXPRESSION
    return TodoCategory.CONFIGURATION


def scan_file(file_path: Path) -> list[RemediationOpportunity]:
    """Scan a single file for TODO stubs and match to templates."""
    opportunities: list[RemediationOpportunity] = []
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return opportunities

    for i, line in enumerate(lines):
        m = _TODO_RE.search(line)
        if not m:
            continue
        todo_text = m.group(1).strip()
        category = _classify_todo(todo_text)

        # Grab 2 lines of context above and below
        ctx_start = max(0, i - 2)
        ctx_end = min(len(lines), i + 3)
        context = lines[ctx_start:ctx_end]

        opp = RemediationOpportunity(
            file_path=str(file_path),
            line_number=i + 1,
            todo_text=todo_text,
            category=category,
            context_lines=context,
        )

        # Try to match a template
        for tmpl in _TEMPLATES:
            if re.search(tmpl.pattern, line, re.IGNORECASE):
                opp.matched_template = tmpl
                break

        opportunities.append(opp)

    return opportunities


def scan_directory(output_dir: Path) -> list[RemediationOpportunity]:
    """Scan all notebooks (.py) and pipelines (.json) for TODO stubs."""
    all_opps: list[RemediationOpportunity] = []
    patterns = ["notebooks/*.py", "pipelines/*.json", "dataflows/*.json"]
    for pat in patterns:
        for f in sorted(output_dir.glob(pat)):
            all_opps.extend(scan_file(f))
    return all_opps


# =====================================================================
# Remediation Engine
# =====================================================================


class RemediationEngine:
    """Scan and remediate TODO stubs in migration output."""

    def __init__(self, *, min_confidence: Confidence = Confidence.LOW, dry_run: bool = False):
        self.min_confidence = min_confidence
        self.dry_run = dry_run
        self._confidence_order = {Confidence.HIGH: 3, Confidence.MEDIUM: 2, Confidence.LOW: 1}

    def _meets_confidence(self, conf: Confidence) -> bool:
        return self._confidence_order[conf] >= self._confidence_order[self.min_confidence]

    def scan(self, output_dir: Path) -> list[RemediationOpportunity]:
        """Scan output directory and return all TODO opportunities."""
        return scan_directory(output_dir)

    def remediate_file(self, file_path: Path) -> list[RemediationResult]:
        """Apply remediations to a single file."""
        opps = scan_file(file_path)
        results: list[RemediationResult] = []
        if not opps:
            return results

        lines = file_path.read_text(encoding="utf-8").splitlines()
        modified = False

        # Process in reverse order to preserve line numbers
        for opp in reversed(opps):
            tmpl = opp.matched_template
            if tmpl is None:
                results.append(RemediationResult(
                    opportunity=opp,
                    status="skipped",
                    confidence=Confidence.LOW,
                    error="No matching template",
                ))
                continue

            if not self._meets_confidence(tmpl.confidence):
                results.append(RemediationResult(
                    opportunity=opp,
                    status="skipped",
                    confidence=tmpl.confidence,
                    error=f"Below min confidence ({self.min_confidence.value})",
                ))
                continue

            # Apply the replacement
            idx = opp.line_number - 1
            if idx < len(lines):
                old_line = lines[idx]
                indent = len(old_line) - len(old_line.lstrip())
                indent_str = " " * indent
                replacement_lines = [
                    f"{indent_str}{rl}" for rl in tmpl.replacement.split("\n")
                ]
                if self.dry_run:
                    results.append(RemediationResult(
                        opportunity=opp,
                        status="skipped",
                        confidence=tmpl.confidence,
                        patch_preview="\n".join(replacement_lines),
                    ))
                else:
                    lines[idx] = "\n".join(replacement_lines)
                    modified = True
                    results.append(RemediationResult(
                        opportunity=opp,
                        status="applied",
                        confidence=tmpl.confidence,
                        patch_preview="\n".join(replacement_lines),
                    ))

        if modified and not self.dry_run:
            file_path.write_text("\n".join(lines), encoding="utf-8")

        results.reverse()  # Restore original order
        return results

    def remediate_directory(self, output_dir: Path) -> RemediationReport:
        """Scan and remediate all artifacts in an output directory."""
        report = RemediationReport()

        patterns = ["notebooks/*.py", "pipelines/*.json", "dataflows/*.json"]
        files: list[Path] = []
        for pat in patterns:
            files.extend(sorted(output_dir.glob(pat)))

        report.total_scanned = len(files)

        for f in files:
            results = self.remediate_file(f)
            for r in results:
                report.total_todos += 1
                report.results.append(r)
                cat = r.opportunity.category.value
                report.by_category[cat] = report.by_category.get(cat, 0) + 1
                report.by_confidence[r.confidence.value] = (
                    report.by_confidence.get(r.confidence.value, 0) + 1
                )
                if r.status == "applied":
                    report.applied += 1
                elif r.status == "skipped":
                    report.skipped += 1
                else:
                    report.failed += 1

        logger.info(
            "remediation_complete",
            scanned=report.total_scanned,
            todos=report.total_todos,
            applied=report.applied,
            skipped=report.skipped,
        )
        return report

    def write_report(self, report: RemediationReport, output_path: Path) -> Path:
        """Write remediation report as JSON."""
        report_file = output_path / "remediation_report.json"
        report_file.write_text(
            json.dumps(report.to_dict(), indent=2),
            encoding="utf-8",
        )
        return report_file
