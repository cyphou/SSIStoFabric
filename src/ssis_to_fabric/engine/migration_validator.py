"""
Migration Validation & Testing Framework
==========================================
Phase 28: Generated test harnesses, schema drift detection, hash-based
record comparison, execution trace replay, and golden dataset management.
"""

from __future__ import annotations

import hashlib
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
# Schema Models
# =====================================================================


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""

    name: str
    data_type: str = "string"
    nullable: bool = True
    max_length: int = 0
    precision: int = 0
    scale: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "max_length": self.max_length,
            "precision": self.precision,
            "scale": self.scale,
        }

    def signature(self) -> str:
        """Stable string representation for comparison."""
        return f"{self.name}:{self.data_type}:n={self.nullable}:l={self.max_length}"


@dataclass
class TableSchema:
    """Schema definition for a table."""

    table_name: str
    columns: list[ColumnSchema] = field(default_factory=list)
    source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "columns": [c.to_dict() for c in self.columns],
            "source": self.source,
        }

    def column_names(self) -> set[str]:
        return {c.name for c in self.columns}


# =====================================================================
# Schema Drift Detection
# =====================================================================


class DriftType(Enum):
    """Type of schema drift detected."""

    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    TYPE_CHANGED = "type_changed"
    NULLABILITY_CHANGED = "nullability_changed"
    LENGTH_CHANGED = "length_changed"


@dataclass
class SchemaDrift:
    """A single schema drift finding."""

    table_name: str
    drift_type: DriftType
    column_name: str
    detail: str = ""
    severity: str = "WARNING"

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "drift_type": self.drift_type.value,
            "column_name": self.column_name,
            "detail": self.detail,
            "severity": self.severity,
        }


def detect_schema_drift(
    source: TableSchema, target: TableSchema,
) -> list[SchemaDrift]:
    """Compare two table schemas and return drift findings."""
    drifts: list[SchemaDrift] = []
    src_cols = {c.name: c for c in source.columns}
    tgt_cols = {c.name: c for c in target.columns}

    # Columns added in target
    for name in tgt_cols:
        if name not in src_cols:
            drifts.append(SchemaDrift(
                table_name=source.table_name,
                drift_type=DriftType.COLUMN_ADDED,
                column_name=name,
                detail=f"Column '{name}' exists in target but not in source",
                severity="INFO",
            ))

    # Columns removed from target
    for name in src_cols:
        if name not in tgt_cols:
            drifts.append(SchemaDrift(
                table_name=source.table_name,
                drift_type=DriftType.COLUMN_REMOVED,
                column_name=name,
                detail=f"Column '{name}' exists in source but not in target",
                severity="ERROR",
            ))

    # Column changes
    for name in src_cols:
        if name not in tgt_cols:
            continue
        src_col = src_cols[name]
        tgt_col = tgt_cols[name]

        if src_col.data_type != tgt_col.data_type:
            drifts.append(SchemaDrift(
                table_name=source.table_name,
                drift_type=DriftType.TYPE_CHANGED,
                column_name=name,
                detail=f"Type: {src_col.data_type} → {tgt_col.data_type}",
                severity="ERROR",
            ))
        if src_col.nullable != tgt_col.nullable:
            drifts.append(SchemaDrift(
                table_name=source.table_name,
                drift_type=DriftType.NULLABILITY_CHANGED,
                column_name=name,
                detail=f"Nullable: {src_col.nullable} → {tgt_col.nullable}",
                severity="WARNING",
            ))
        if src_col.max_length != tgt_col.max_length and src_col.max_length > 0:
            drifts.append(SchemaDrift(
                table_name=source.table_name,
                drift_type=DriftType.LENGTH_CHANGED,
                column_name=name,
                detail=f"Length: {src_col.max_length} → {tgt_col.max_length}",
                severity="WARNING",
            ))

    return drifts


# =====================================================================
# Schema Extraction from SSIS Artifacts
# =====================================================================


def extract_schemas_from_package(package: Any) -> list[TableSchema]:
    """Extract destination table schemas from an SSIS package."""
    schemas: list[TableSchema] = []
    tasks = getattr(package, "control_flow_tasks", [])

    for task in tasks:
        components = getattr(task, "data_flow_components", [])
        for comp in components:
            comp_type = str(getattr(comp, "component_type", "")).upper()
            if "DEST" not in comp_type and "SINK" not in comp_type:
                # Also check the name pattern
                name = getattr(comp, "name", "")
                if not any(kw in name.lower() for kw in ("dest", "target", "sink", "load")):
                    continue

            table_name = (
                getattr(comp, "destination_table", "")
                or getattr(comp, "table_name", "")
                or getattr(comp, "name", "unknown")
            )

            columns: list[ColumnSchema] = []
            for col in getattr(comp, "output_columns", getattr(comp, "columns", [])):
                col_name = getattr(col, "name", str(col))
                col_type = getattr(col, "data_type", "string")
                columns.append(ColumnSchema(
                    name=col_name,
                    data_type=str(col_type),
                    max_length=getattr(col, "length", 0),
                    precision=getattr(col, "precision", 0),
                    scale=getattr(col, "scale", 0),
                ))

            if columns:
                schemas.append(TableSchema(
                    table_name=table_name,
                    columns=columns,
                    source=getattr(package, "name", ""),
                ))

    return schemas


def extract_schemas_from_sidecar(sidecar_path: Path) -> list[TableSchema]:
    """Extract schemas from a .destinations.json sidecar file."""
    content = json.loads(sidecar_path.read_text(encoding="utf-8"))
    schemas: list[TableSchema] = []

    destinations = content if isinstance(content, list) else content.get("destinations", [])
    for dest in destinations:
        table_name = dest.get("table", dest.get("table_name", "unknown"))
        columns = []
        for col in dest.get("columns", []):
            columns.append(ColumnSchema(
                name=col.get("name", ""),
                data_type=col.get("dataType", col.get("data_type", "string")),
                nullable=col.get("nullable", True),
                max_length=col.get("length", col.get("max_length", 0)),
                precision=col.get("precision", 0),
                scale=col.get("scale", 0),
            ))
        schemas.append(TableSchema(table_name=table_name, columns=columns))

    return schemas


# =====================================================================
# Hash-Based Record Comparison
# =====================================================================


@dataclass
class RecordHash:
    """Hash of a data record for comparison."""

    record_id: str
    hash_value: str
    source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "hash_value": self.hash_value,
            "source": self.source,
        }


@dataclass
class RecordComparisonResult:
    """Result of comparing records between source and target."""

    table_name: str
    source_count: int = 0
    target_count: int = 0
    matched: int = 0
    mismatched: int = 0
    missing_in_target: int = 0
    extra_in_target: int = 0

    @property
    def match_rate(self) -> float:
        if self.source_count == 0:
            return 1.0
        return self.matched / self.source_count

    @property
    def passed(self) -> bool:
        return self.mismatched == 0 and self.missing_in_target == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "matched": self.matched,
            "mismatched": self.mismatched,
            "missing_in_target": self.missing_in_target,
            "extra_in_target": self.extra_in_target,
            "match_rate": round(self.match_rate, 4),
            "passed": self.passed,
        }


def hash_record(record: dict[str, Any], columns: list[str] | None = None) -> str:
    """Compute a stable hash for a data record."""
    data = {k: record.get(k) for k in sorted(columns)} if columns else dict(sorted(record.items()))
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def compare_record_sets(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    key_columns: list[str],
    compare_columns: list[str] | None = None,
    table_name: str = "",
) -> RecordComparisonResult:
    """Compare two sets of records using hash-based comparison."""
    def _get_key(record: dict[str, Any]) -> str:
        return "|".join(str(record.get(k, "")) for k in key_columns)

    src_map: dict[str, str] = {}
    for rec in source_records:
        key = _get_key(rec)
        src_map[key] = hash_record(rec, compare_columns)

    tgt_map: dict[str, str] = {}
    for rec in target_records:
        key = _get_key(rec)
        tgt_map[key] = hash_record(rec, compare_columns)

    matched = 0
    mismatched = 0
    missing = 0

    for key, src_hash in src_map.items():
        tgt_hash = tgt_map.get(key)
        if tgt_hash is None:
            missing += 1
        elif src_hash == tgt_hash:
            matched += 1
        else:
            mismatched += 1

    extra = len(tgt_map) - (matched + mismatched)

    return RecordComparisonResult(
        table_name=table_name,
        source_count=len(source_records),
        target_count=len(target_records),
        matched=matched,
        mismatched=mismatched,
        missing_in_target=missing,
        extra_in_target=max(0, extra),
    )


# =====================================================================
# Execution Trace Replay
# =====================================================================


@dataclass
class ExecutionTrace:
    """A recorded execution trace for comparison."""

    pipeline_name: str
    tasks_executed: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    status: str = "success"
    error_message: str = ""
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(tz=timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "tasks_executed": self.tasks_executed,
            "row_counts": self.row_counts,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "error_message": self.error_message,
            "timestamp": self.timestamp,
        }


@dataclass
class TraceComparison:
    """Result of comparing two execution traces."""

    pipeline_name: str
    tasks_match: bool = True
    row_count_diffs: dict[str, tuple[int, int]] = field(default_factory=dict)
    missing_tasks: list[str] = field(default_factory=list)
    extra_tasks: list[str] = field(default_factory=list)
    status_match: bool = True

    @property
    def passed(self) -> bool:
        return (
            self.tasks_match
            and not self.row_count_diffs
            and not self.missing_tasks
            and self.status_match
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "passed": self.passed,
            "tasks_match": self.tasks_match,
            "row_count_diffs": {
                k: {"expected": v[0], "actual": v[1]}
                for k, v in self.row_count_diffs.items()
            },
            "missing_tasks": self.missing_tasks,
            "extra_tasks": self.extra_tasks,
            "status_match": self.status_match,
        }


def compare_traces(
    expected: ExecutionTrace, actual: ExecutionTrace,
    row_count_tolerance: float = 0.0,
) -> TraceComparison:
    """Compare two execution traces."""
    expected_tasks = set(expected.tasks_executed)
    actual_tasks = set(actual.tasks_executed)

    missing = sorted(expected_tasks - actual_tasks)
    extra = sorted(actual_tasks - expected_tasks)
    tasks_match = not missing and not extra

    # Compare row counts
    row_diffs: dict[str, tuple[int, int]] = {}
    for table, expected_count in expected.row_counts.items():
        actual_count = actual.row_counts.get(table, 0)
        if expected_count == 0:
            continue
        diff_pct = abs(actual_count - expected_count) / expected_count
        if diff_pct > row_count_tolerance:
            row_diffs[table] = (expected_count, actual_count)

    return TraceComparison(
        pipeline_name=expected.pipeline_name,
        tasks_match=tasks_match,
        row_count_diffs=row_diffs,
        missing_tasks=missing,
        extra_tasks=extra,
        status_match=expected.status == actual.status,
    )


# =====================================================================
# Golden Dataset Management
# =====================================================================


@dataclass
class GoldenDataset:
    """A golden (reference) dataset for migration validation."""

    name: str
    table_name: str
    records: list[dict[str, Any]] = field(default_factory=list)
    schema: TableSchema | None = None
    created_at: str = ""
    version: int = 1

    def __post_init__(self) -> None:
        if not self.created_at:
            self.created_at = datetime.now(tz=timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "table_name": self.table_name,
            "record_count": len(self.records),
            "records": self.records,
            "schema": self.schema.to_dict() if self.schema else None,
            "created_at": self.created_at,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GoldenDataset:
        schema = None
        if data.get("schema"):
            s = data["schema"]
            schema = TableSchema(
                table_name=s.get("table_name", ""),
                columns=[
                    ColumnSchema(**c) for c in s.get("columns", [])
                ],
            )
        return cls(
            name=data.get("name", ""),
            table_name=data.get("table_name", ""),
            records=data.get("records", []),
            schema=schema,
            created_at=data.get("created_at", ""),
            version=data.get("version", 1),
        )


def save_golden_dataset(dataset: GoldenDataset, output_path: Path) -> Path:
    """Save a golden dataset to a JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(dataset.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("golden_dataset_saved", path=str(output_path), name=dataset.name)
    return output_path


def load_golden_dataset(path: Path) -> GoldenDataset:
    """Load a golden dataset from a JSON file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return GoldenDataset.from_dict(data)


# =====================================================================
# Test Harness Generation
# =====================================================================


@dataclass
class GeneratedTestCase:
    """A generated test case for a migrated pipeline."""

    test_name: str
    test_type: str  # schema_check, record_compare, row_count, trace_replay
    target_table: str = ""
    pipeline_name: str = ""
    code: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_name": self.test_name,
            "test_type": self.test_type,
            "target_table": self.target_table,
            "pipeline_name": self.pipeline_name,
        }


def generate_test_harness(
    package: Any,
    output_schemas: list[TableSchema] | None = None,
) -> list[GeneratedTestCase]:
    """Generate test cases for a migrated package."""
    pkg_name = getattr(package, "name", "unknown")
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", pkg_name.lower())
    tests: list[GeneratedTestCase] = []

    # Extract schemas from the package if not provided
    schemas = output_schemas or extract_schemas_from_package(package)

    for schema in schemas:
        tbl_safe = re.sub(r"[^a-zA-Z0-9_]", "_", schema.table_name.lower())

        # 1. Schema existence test
        tests.append(GeneratedTestCase(
            test_name=f"test_{safe_name}_{tbl_safe}_schema_exists",
            test_type="schema_check",
            target_table=schema.table_name,
            pipeline_name=pkg_name,
            code=_gen_schema_test(schema),
        ))

        # 2. Row count test
        tests.append(GeneratedTestCase(
            test_name=f"test_{safe_name}_{tbl_safe}_row_count",
            test_type="row_count",
            target_table=schema.table_name,
            pipeline_name=pkg_name,
            code=_gen_row_count_test(schema),
        ))

        # 3. Not-null check for non-nullable columns
        non_nullable = [c for c in schema.columns if not c.nullable]
        if non_nullable:
            tests.append(GeneratedTestCase(
                test_name=f"test_{safe_name}_{tbl_safe}_not_null",
                test_type="not_null_check",
                target_table=schema.table_name,
                pipeline_name=pkg_name,
                code=_gen_not_null_test(schema, non_nullable),
            ))

    # 4. Pipeline-level trace test
    tasks = getattr(package, "control_flow_tasks", [])
    if tasks:
        tests.append(GeneratedTestCase(
            test_name=f"test_{safe_name}_execution_trace",
            test_type="trace_replay",
            pipeline_name=pkg_name,
            code=_gen_trace_test(pkg_name, tasks),
        ))

    return tests


def _gen_schema_test(schema: TableSchema) -> str:
    cols = ", ".join(f'"{c.name}"' for c in schema.columns)
    return (
        f'def test_{_safe(schema.table_name)}_schema_exists():\n'
        f'    """Verify table {schema.table_name} has expected columns."""\n'
        f'    expected_columns = {{{cols}}}\n'
        f'    # TODO: Query actual table schema from Fabric lakehouse\n'
        f'    actual_columns = set()  # Replace with actual query\n'
        f'    missing = expected_columns - actual_columns\n'
        f'    assert not missing, f"Missing columns: {{missing}}"\n'
    )


def _gen_row_count_test(schema: TableSchema) -> str:
    return (
        f'def test_{_safe(schema.table_name)}_row_count():\n'
        f'    """Verify table {schema.table_name} has rows after migration."""\n'
        f'    # TODO: Query actual row count from Fabric lakehouse\n'
        f'    row_count = 0  # Replace with actual query\n'
        f'    assert row_count > 0, "Table {schema.table_name} is empty"\n'
    )


def _gen_not_null_test(schema: TableSchema, non_nullable: list[ColumnSchema]) -> str:
    cols = ", ".join(f'"{c.name}"' for c in non_nullable)
    return (
        f'def test_{_safe(schema.table_name)}_not_null():\n'
        f'    """Verify non-nullable columns in {schema.table_name}."""\n'
        f'    non_nullable_columns = [{cols}]\n'
        f'    for col in non_nullable_columns:\n'
        f'        # TODO: Query null count from Fabric lakehouse\n'
        f'        null_count = 0  # Replace with actual query\n'
        f'        assert null_count == 0, f"Column {{col}} has {{null_count}} nulls"\n'
    )


def _gen_trace_test(pkg_name: str, tasks: list[Any]) -> str:
    task_names = [getattr(t, "name", str(t)) for t in tasks]
    task_list = ", ".join(f'"{t}"' for t in task_names)
    return (
        f'def test_{_safe(pkg_name)}_execution_trace():\n'
        f'    """Verify all tasks from {pkg_name} are executed."""\n'
        f'    expected_tasks = [{task_list}]\n'
        f'    # TODO: Query actual execution trace from Fabric\n'
        f'    actual_tasks = []  # Replace with actual trace\n'
        f'    missing = set(expected_tasks) - set(actual_tasks)\n'
        f'    assert not missing, f"Tasks not executed: {{missing}}"\n'
    )


def _safe(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())


# =====================================================================
# Test File Writer
# =====================================================================


def write_test_file(
    tests: list[GeneratedTestCase],
    output_path: Path,
    package_name: str = "",
) -> Path:
    """Write generated test cases to a pytest file."""
    lines = [
        '"""',
        f"Auto-generated validation tests for {package_name or 'migration'}.",
        f"Generated at {datetime.now(tz=timezone.utc).isoformat()}",
        '"""',
        "",
        "import pytest",
        "",
        "",
    ]

    for tc in tests:
        lines.append(tc.code)
        lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("test_file_written", path=str(output_path), tests=len(tests))
    return output_path


# =====================================================================
# Validation Report
# =====================================================================


def write_validation_report(
    schema_drifts: list[SchemaDrift],
    record_results: list[RecordComparisonResult],
    trace_comparisons: list[TraceComparison],
    generated_tests: list[GeneratedTestCase],
    output_path: Path,
) -> Path:
    """Write a comprehensive validation report."""
    report = {
        "summary": {
            "schema_drifts": len(schema_drifts),
            "schema_errors": sum(1 for d in schema_drifts if d.severity == "ERROR"),
            "record_comparisons": len(record_results),
            "record_failures": sum(1 for r in record_results if not r.passed),
            "trace_comparisons": len(trace_comparisons),
            "trace_failures": sum(1 for t in trace_comparisons if not t.passed),
            "generated_tests": len(generated_tests),
            "all_passed": (
                all(d.severity != "ERROR" for d in schema_drifts)
                and all(r.passed for r in record_results)
                and all(t.passed for t in trace_comparisons)
            ),
        },
        "schema_drifts": [d.to_dict() for d in schema_drifts],
        "record_comparisons": [r.to_dict() for r in record_results],
        "trace_comparisons": [t.to_dict() for t in trace_comparisons],
        "generated_tests": [t.to_dict() for t in generated_tests],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("validation_report_written", path=str(output_path))
    return output_path
