"""Tests for Phase 28 — Migration Validation & Testing Framework."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from ssis_to_fabric.engine.migration_validator import (
    ColumnSchema,
    DriftType,
    ExecutionTrace,
    GoldenDataset,
    RecordComparisonResult,
    SchemaDrift,
    TableSchema,
    TraceComparison,
    compare_record_sets,
    compare_traces,
    detect_schema_drift,
    extract_schemas_from_package,
    generate_test_harness,
    hash_record,
    load_golden_dataset,
    save_golden_dataset,
    write_test_file,
    write_validation_report,
)

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def sample_schema():
    return TableSchema(
        table_name="Customers",
        columns=[
            ColumnSchema(name="CustomerID", data_type="int", nullable=False, max_length=0),
            ColumnSchema(name="Name", data_type="nvarchar", nullable=False, max_length=100),
            ColumnSchema(name="Email", data_type="nvarchar", nullable=True, max_length=255),
        ],
        source="TestPackage",
    )


@pytest.fixture
def sample_package():
    col1 = SimpleNamespace(name="ID", data_type="int", length=0, precision=0, scale=0)
    col2 = SimpleNamespace(name="Value", data_type="nvarchar", length=50, precision=0, scale=0)
    comp = SimpleNamespace(
        name="OLE DB Destination",
        component_type="OLE_DB_DEST",
        destination_table="Orders",
        output_columns=[col1, col2],
    )
    task = SimpleNamespace(name="Load Orders", data_flow_components=[comp])
    return SimpleNamespace(name="TestPkg", control_flow_tasks=[task])


@pytest.fixture
def source_records():
    return [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300},
    ]


@pytest.fixture
def target_records():
    return [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 250},  # Different value
        {"id": 4, "name": "Diana", "value": 400},  # Extra record
    ]


# =====================================================================
# Column & Table Schema Tests
# =====================================================================


class TestColumnSchema:
    def test_to_dict(self):
        col = ColumnSchema(name="ID", data_type="int", nullable=False, max_length=4)
        d = col.to_dict()
        assert d["name"] == "ID"
        assert d["data_type"] == "int"
        assert d["nullable"] is False

    def test_signature(self):
        col = ColumnSchema(name="Name", data_type="nvarchar", max_length=100)
        sig = col.signature()
        assert "Name" in sig
        assert "nvarchar" in sig


class TestTableSchema:
    def test_column_names(self, sample_schema):
        assert sample_schema.column_names() == {"CustomerID", "Name", "Email"}

    def test_to_dict(self, sample_schema):
        d = sample_schema.to_dict()
        assert d["table_name"] == "Customers"
        assert len(d["columns"]) == 3


# =====================================================================
# Schema Drift Detection Tests
# =====================================================================


class TestSchemaDriftDetection:
    def test_no_drift(self, sample_schema):
        drifts = detect_schema_drift(sample_schema, sample_schema)
        assert len(drifts) == 0

    def test_column_added(self, sample_schema):
        target = TableSchema(
            table_name="Customers",
            columns=sample_schema.columns + [
                ColumnSchema(name="Phone", data_type="nvarchar"),
            ],
        )
        drifts = detect_schema_drift(sample_schema, target)
        added = [d for d in drifts if d.drift_type == DriftType.COLUMN_ADDED]
        assert len(added) == 1
        assert added[0].column_name == "Phone"
        assert added[0].severity == "INFO"

    def test_column_removed(self, sample_schema):
        target = TableSchema(
            table_name="Customers",
            columns=[sample_schema.columns[0]],
        )
        drifts = detect_schema_drift(sample_schema, target)
        removed = [d for d in drifts if d.drift_type == DriftType.COLUMN_REMOVED]
        assert len(removed) == 2  # Name and Email removed

    def test_type_changed(self, sample_schema):
        target = TableSchema(
            table_name="Customers",
            columns=[
                ColumnSchema(name="CustomerID", data_type="bigint", nullable=False),
                ColumnSchema(name="Name", data_type="nvarchar", nullable=False, max_length=100),
                ColumnSchema(name="Email", data_type="nvarchar", nullable=True, max_length=255),
            ],
        )
        drifts = detect_schema_drift(sample_schema, target)
        type_changes = [d for d in drifts if d.drift_type == DriftType.TYPE_CHANGED]
        assert len(type_changes) == 1
        assert type_changes[0].severity == "ERROR"

    def test_nullability_changed(self, sample_schema):
        target = TableSchema(
            table_name="Customers",
            columns=[
                ColumnSchema(name="CustomerID", data_type="int", nullable=True),
                ColumnSchema(name="Name", data_type="nvarchar", nullable=False, max_length=100),
                ColumnSchema(name="Email", data_type="nvarchar", nullable=True, max_length=255),
            ],
        )
        drifts = detect_schema_drift(sample_schema, target)
        null_changes = [d for d in drifts if d.drift_type == DriftType.NULLABILITY_CHANGED]
        assert len(null_changes) == 1

    def test_length_changed(self, sample_schema):
        target = TableSchema(
            table_name="Customers",
            columns=[
                ColumnSchema(name="CustomerID", data_type="int", nullable=False),
                ColumnSchema(name="Name", data_type="nvarchar", nullable=False, max_length=200),
                ColumnSchema(name="Email", data_type="nvarchar", nullable=True, max_length=255),
            ],
        )
        drifts = detect_schema_drift(sample_schema, target)
        len_changes = [d for d in drifts if d.drift_type == DriftType.LENGTH_CHANGED]
        assert len(len_changes) == 1

    def test_drift_to_dict(self):
        d = SchemaDrift(
            table_name="T1", drift_type=DriftType.COLUMN_ADDED,
            column_name="col1", detail="added", severity="INFO",
        )
        result = d.to_dict()
        assert result["drift_type"] == "column_added"


# =====================================================================
# Hash-Based Record Comparison Tests
# =====================================================================


class TestHashRecordComparison:
    def test_hash_deterministic(self):
        rec = {"a": 1, "b": "hello"}
        assert hash_record(rec) == hash_record(rec)

    def test_hash_order_independent(self):
        rec1 = {"a": 1, "b": 2}
        rec2 = {"b": 2, "a": 1}
        assert hash_record(rec1) == hash_record(rec2)

    def test_hash_with_columns(self):
        rec = {"a": 1, "b": 2, "c": 3}
        h1 = hash_record(rec, columns=["a", "b"])
        h2 = hash_record(rec, columns=["a", "b", "c"])
        assert h1 != h2

    def test_compare_matching_records(self, source_records):
        result = compare_record_sets(
            source_records, source_records,
            key_columns=["id"], table_name="test",
        )
        assert result.matched == 3
        assert result.mismatched == 0
        assert result.passed is True

    def test_compare_with_differences(self, source_records, target_records):
        result = compare_record_sets(
            source_records, target_records,
            key_columns=["id"], table_name="test",
        )
        assert result.matched == 1  # id=1 matches
        assert result.mismatched == 1  # id=2 differs
        assert result.missing_in_target == 1  # id=3 missing
        assert result.extra_in_target == 1  # id=4 extra
        assert result.passed is False

    def test_empty_source(self):
        result = compare_record_sets([], [], key_columns=["id"], table_name="test")
        assert result.match_rate == 1.0
        assert result.passed is True

    def test_match_rate(self, source_records, target_records):
        result = compare_record_sets(
            source_records, target_records,
            key_columns=["id"], table_name="test",
        )
        assert 0 < result.match_rate < 1.0

    def test_comparison_result_to_dict(self):
        r = RecordComparisonResult(table_name="T", matched=5, source_count=5, target_count=5)
        d = r.to_dict()
        assert d["passed"] is True
        assert d["match_rate"] == 1.0


# =====================================================================
# Execution Trace Tests
# =====================================================================


class TestExecutionTrace:
    def test_trace_creation(self):
        trace = ExecutionTrace(
            pipeline_name="Pipe1",
            tasks_executed=["T1", "T2"],
            row_counts={"Orders": 100},
            status="success",
        )
        assert trace.timestamp  # auto-populated

    def test_trace_to_dict(self):
        trace = ExecutionTrace(pipeline_name="Pipe1", tasks_executed=["T1"])
        d = trace.to_dict()
        assert d["pipeline_name"] == "Pipe1"

    def test_compare_identical_traces(self):
        t1 = ExecutionTrace(
            pipeline_name="P1",
            tasks_executed=["T1", "T2"],
            row_counts={"Orders": 100},
        )
        result = compare_traces(t1, t1)
        assert result.passed is True
        assert result.tasks_match is True

    def test_compare_missing_tasks(self):
        expected = ExecutionTrace(pipeline_name="P1", tasks_executed=["T1", "T2", "T3"])
        actual = ExecutionTrace(pipeline_name="P1", tasks_executed=["T1", "T2"])
        result = compare_traces(expected, actual)
        assert result.passed is False
        assert "T3" in result.missing_tasks

    def test_compare_extra_tasks(self):
        expected = ExecutionTrace(pipeline_name="P1", tasks_executed=["T1"])
        actual = ExecutionTrace(pipeline_name="P1", tasks_executed=["T1", "T2"])
        result = compare_traces(expected, actual)
        assert "T2" in result.extra_tasks

    def test_compare_row_count_diff(self):
        expected = ExecutionTrace(
            pipeline_name="P1", tasks_executed=["T1"],
            row_counts={"Orders": 100},
        )
        actual = ExecutionTrace(
            pipeline_name="P1", tasks_executed=["T1"],
            row_counts={"Orders": 50},
        )
        result = compare_traces(expected, actual)
        assert "Orders" in result.row_count_diffs
        assert result.passed is False

    def test_compare_with_tolerance(self):
        expected = ExecutionTrace(
            pipeline_name="P1", tasks_executed=["T1"],
            row_counts={"Orders": 100},
        )
        actual = ExecutionTrace(
            pipeline_name="P1", tasks_executed=["T1"],
            row_counts={"Orders": 95},
        )
        result = compare_traces(expected, actual, row_count_tolerance=0.1)
        assert not result.row_count_diffs  # Within 10% tolerance
        assert result.passed is True

    def test_compare_status_mismatch(self):
        expected = ExecutionTrace(pipeline_name="P1", status="success")
        actual = ExecutionTrace(pipeline_name="P1", status="failed")
        result = compare_traces(expected, actual)
        assert result.status_match is False
        assert result.passed is False

    def test_trace_comparison_to_dict(self):
        tc = TraceComparison(
            pipeline_name="P1",
            row_count_diffs={"Orders": (100, 50)},
        )
        d = tc.to_dict()
        assert d["row_count_diffs"]["Orders"]["expected"] == 100


# =====================================================================
# Golden Dataset Tests
# =====================================================================


class TestGoldenDataset:
    def test_create_golden_dataset(self):
        gd = GoldenDataset(
            name="orders_golden",
            table_name="Orders",
            records=[{"id": 1, "value": 100}],
        )
        assert gd.version == 1
        assert gd.created_at

    def test_to_dict_and_from_dict(self, sample_schema):
        gd = GoldenDataset(
            name="customers_golden",
            table_name="Customers",
            records=[{"id": 1}],
            schema=sample_schema,
        )
        d = gd.to_dict()
        restored = GoldenDataset.from_dict(d)
        assert restored.name == "customers_golden"
        assert restored.schema is not None
        assert len(restored.schema.columns) == 3

    def test_save_and_load(self, tmp_path):
        gd = GoldenDataset(
            name="test_golden",
            table_name="TestTable",
            records=[{"k": 1, "v": "a"}, {"k": 2, "v": "b"}],
        )
        path = tmp_path / "datasets" / "test_golden.json"
        save_golden_dataset(gd, path)
        assert path.exists()

        loaded = load_golden_dataset(path)
        assert loaded.name == "test_golden"
        assert len(loaded.records) == 2

    def test_from_dict_without_schema(self):
        gd = GoldenDataset.from_dict({
            "name": "simple", "table_name": "T", "records": [],
        })
        assert gd.schema is None


# =====================================================================
# Test Harness Generation Tests
# =====================================================================


class TestTestHarness:
    def test_generate_tests(self, sample_package):
        tests = generate_test_harness(sample_package)
        assert len(tests) >= 2  # At least schema + row count tests
        test_names = [t.test_name for t in tests]
        assert any("schema" in n for n in test_names)
        assert any("row_count" in n for n in test_names)

    def test_generated_tests_have_code(self, sample_package):
        tests = generate_test_harness(sample_package)
        for t in tests:
            assert t.code
            assert "def test_" in t.code

    def test_trace_test_generated(self, sample_package):
        tests = generate_test_harness(sample_package)
        trace_tests = [t for t in tests if t.test_type == "trace_replay"]
        assert len(trace_tests) == 1

    def test_with_explicit_schemas(self, sample_package, sample_schema):
        tests = generate_test_harness(sample_package, output_schemas=[sample_schema])
        table_tests = [t for t in tests if t.target_table == "Customers"]
        assert len(table_tests) >= 2

    def test_non_nullable_check(self, sample_package, sample_schema):
        # sample_schema has CustomerID and Name as non-nullable
        tests = generate_test_harness(sample_package, output_schemas=[sample_schema])
        null_tests = [t for t in tests if t.test_type == "not_null_check"]
        assert len(null_tests) == 1

    def test_to_dict_generated_test(self, sample_package):
        tests = generate_test_harness(sample_package)
        d = tests[0].to_dict()
        assert "test_name" in d
        assert "test_type" in d


# =====================================================================
# Write Test File
# =====================================================================


class TestWriteTestFile:
    def test_write_file(self, tmp_path, sample_package):
        tests = generate_test_harness(sample_package)
        output_path = tmp_path / "test_output.py"
        result = write_test_file(tests, output_path, package_name="TestPkg")
        assert result.exists()
        content = result.read_text()
        assert "import pytest" in content
        assert "def test_" in content


# =====================================================================
# Schema Extraction Tests
# =====================================================================


class TestSchemaExtraction:
    def test_extract_from_package(self, sample_package):
        schemas = extract_schemas_from_package(sample_package)
        assert len(schemas) == 1
        assert schemas[0].table_name == "Orders"
        assert len(schemas[0].columns) == 2

    def test_extract_empty_package(self):
        pkg = SimpleNamespace(name="Empty", control_flow_tasks=[])
        schemas = extract_schemas_from_package(pkg)
        assert schemas == []

    def test_extract_non_destination_skipped(self):
        comp = SimpleNamespace(
            name="Sort Transform",
            component_type="SORT",
            output_columns=[],
        )
        task = SimpleNamespace(name="Sort", data_flow_components=[comp])
        pkg = SimpleNamespace(name="Pkg", control_flow_tasks=[task])
        schemas = extract_schemas_from_package(pkg)
        assert schemas == []


# =====================================================================
# Validation Report Tests
# =====================================================================


class TestValidationReport:
    def test_write_report(self, tmp_path):
        drifts = [
            SchemaDrift("T1", DriftType.COLUMN_REMOVED, "col1", severity="ERROR"),
        ]
        records = [
            RecordComparisonResult("T1", source_count=10, target_count=10, matched=10),
        ]
        traces = [
            TraceComparison("P1"),
        ]
        tests = []
        path = tmp_path / "report.json"
        write_validation_report(drifts, records, traces, tests, path)
        assert path.exists()
        report = json.loads(path.read_text())
        assert report["summary"]["schema_errors"] == 1
        assert report["summary"]["all_passed"] is False

    def test_all_passed_report(self, tmp_path):
        path = tmp_path / "report_ok.json"
        write_validation_report([], [], [], [], path)
        report = json.loads(path.read_text())
        assert report["summary"]["all_passed"] is True


# =====================================================================
# CLI Registration Test
# =====================================================================


class TestCLI:
    def test_test_gen_command_registered(self):
        from ssis_to_fabric.cli import main

        command_names = [cmd for cmd in main.commands]
        assert "test-gen" in command_names
