"""
Tests for all new Phase 1–3 features added in v1.2+.

Covers:
- Complete Power Query M expression transpiler parity
- Graceful handling of malformed .dtsx files (partial parse)
- Structured errors in MigrationPlan
- RetryConfig in MigrationConfig
- Multi-workspace EnvironmentProfile in MigrationConfig
- LakehouseProvisioner DDL generation
- ReportGenerator HTML output
- LineageGraph build/query/export
- CSharpTranspiler basic patterns
- Incremental migration helpers
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import EnvironmentProfile, MigrationConfig, RetryConfig
from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator
from ssis_to_fabric.engine.migration_engine import (
    MigrationError,
    MigrationPlan,
    _file_sha256,
    _load_migration_state,
    _save_migration_state,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


# ===========================================================================
# 1. Power Query M expression transpiler — new patterns
# ===========================================================================


class TestMExpressionTranspilerParity:
    """Tests for the extended _ssis_expr_to_m recursive transpiler."""

    # --- Date functions ---

    @pytest.mark.unit
    def test_dateadd_day(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEADD("DAY", 7, MyDate)')
        assert "Date.AddDays" in result

    @pytest.mark.unit
    def test_dateadd_month(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEADD("MONTH", 1, MyDate)')
        assert "Date.AddMonths" in result

    @pytest.mark.unit
    def test_dateadd_year(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEADD("YEAR", -1, MyDate)')
        assert "Date.AddYears" in result

    @pytest.mark.unit
    def test_datediff_day(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEDIFF("DAY", StartDate, EndDate)')
        assert "Duration.Days" in result

    @pytest.mark.unit
    def test_datepart_year(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEPART("YEAR", MyDate)')
        assert "Date.Year" in result

    @pytest.mark.unit
    def test_datepart_month(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEPART("MONTH", MyDate)')
        assert "Date.Month" in result

    @pytest.mark.unit
    def test_datepart_day(self):
        result = DataflowGen2Generator._ssis_expr_to_m('DATEPART("DAY", MyDate)')
        assert "Date.Day" in result

    @pytest.mark.unit
    def test_year_function(self):
        result = DataflowGen2Generator._ssis_expr_to_m("YEAR(MyDate)")
        assert "Date.Year" in result

    @pytest.mark.unit
    def test_month_function(self):
        result = DataflowGen2Generator._ssis_expr_to_m("MONTH(MyDate)")
        assert "Date.Month" in result

    @pytest.mark.unit
    def test_day_function(self):
        result = DataflowGen2Generator._ssis_expr_to_m("DAY(MyDate)")
        assert "Date.Day" in result

    # --- String functions ---

    @pytest.mark.unit
    def test_replacenull(self):
        result = DataflowGen2Generator._ssis_expr_to_m('REPLACENULL(MyCol, "default")')
        assert "if" in result and "null" in result

    @pytest.mark.unit
    def test_left(self):
        result = DataflowGen2Generator._ssis_expr_to_m("LEFT(Name, 3)")
        assert "Text.Start" in result

    @pytest.mark.unit
    def test_right(self):
        result = DataflowGen2Generator._ssis_expr_to_m("RIGHT(Name, 3)")
        assert "Text.End" in result

    @pytest.mark.unit
    def test_findstring(self):
        result = DataflowGen2Generator._ssis_expr_to_m('FINDSTRING(Name, "Smith", 1)')
        assert "Text.PositionOf" in result

    @pytest.mark.unit
    def test_reverse(self):
        result = DataflowGen2Generator._ssis_expr_to_m("REVERSE(Name)")
        assert "Text.Reverse" in result

    @pytest.mark.unit
    def test_token(self):
        result = DataflowGen2Generator._ssis_expr_to_m('TOKEN(Name, ",", 1)')
        assert "Text.Split" in result or "List.ItemAt" in result

    @pytest.mark.unit
    def test_ltrim(self):
        result = DataflowGen2Generator._ssis_expr_to_m("LTRIM(Name)")
        assert "TrimStart" in result

    @pytest.mark.unit
    def test_rtrim(self):
        result = DataflowGen2Generator._ssis_expr_to_m("RTRIM(Name)")
        assert "TrimEnd" in result

    # --- Math functions ---

    @pytest.mark.unit
    def test_abs(self):
        result = DataflowGen2Generator._ssis_expr_to_m("ABS(Amount)")
        assert "Number.Abs" in result

    @pytest.mark.unit
    def test_ceiling(self):
        result = DataflowGen2Generator._ssis_expr_to_m("CEILING(Amount)")
        assert "Number.RoundUp" in result

    @pytest.mark.unit
    def test_floor(self):
        result = DataflowGen2Generator._ssis_expr_to_m("FLOOR(Amount)")
        assert "Number.RoundDown" in result

    @pytest.mark.unit
    def test_round(self):
        result = DataflowGen2Generator._ssis_expr_to_m("ROUND(Amount, 2)")
        assert "Number.Round" in result

    @pytest.mark.unit
    def test_power(self):
        result = DataflowGen2Generator._ssis_expr_to_m("POWER(Base, Exp)")
        assert "Number.Power" in result

    @pytest.mark.unit
    def test_sqrt(self):
        result = DataflowGen2Generator._ssis_expr_to_m("SQRT(Value)")
        assert "Number.Sqrt" in result

    @pytest.mark.unit
    def test_sign(self):
        result = DataflowGen2Generator._ssis_expr_to_m("SIGN(Amount)")
        assert "Number.Sign" in result

    # --- Type casts ---

    @pytest.mark.unit
    def test_dt_bool_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_BOOL) Flag")
        assert "Logical.From" in result

    @pytest.mark.unit
    def test_dt_decimal_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_DECIMAL, 4) Amount")
        assert "Decimal.From" in result

    @pytest.mark.unit
    def test_dt_numeric_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_NUMERIC, 18, 4) Amount")
        assert "Decimal.From" in result

    @pytest.mark.unit
    def test_dt_cy_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_CY) Amount")
        assert "Currency.From" in result

    @pytest.mark.unit
    def test_dt_guid_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_GUID) IdCol")
        assert "Text.From" in result

    @pytest.mark.unit
    def test_dt_bytes_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_BYTES, 16) HashCol")
        assert "Binary.From" in result

    # --- Nested expressions ---

    @pytest.mark.unit
    def test_nested_upper_trim(self):
        result = DataflowGen2Generator._ssis_expr_to_m("UPPER(TRIM(Name))")
        assert "Text.Upper" in result
        assert "Text.Trim" in result

    @pytest.mark.unit
    def test_nested_upper_trim_substring(self):
        result = DataflowGen2Generator._ssis_expr_to_m("UPPER(TRIM(SUBSTRING(Name, 1, 5)))")
        assert "Text.Upper" in result
        assert "Text.Trim" in result
        assert "Text.Middle" in result

    @pytest.mark.unit
    def test_nested_abs_round(self):
        result = DataflowGen2Generator._ssis_expr_to_m("ABS(ROUND(Amount, 2))")
        assert "Number.Abs" in result
        assert "Number.Round" in result

    # --- Regression: existing patterns still work ---

    @pytest.mark.unit
    def test_existing_upper(self):
        result = DataflowGen2Generator._ssis_expr_to_m("UPPER(Name)")
        assert "Text.Upper" in result

    @pytest.mark.unit
    def test_existing_substring(self):
        result = DataflowGen2Generator._ssis_expr_to_m("SUBSTRING(Name, 1, 3)")
        assert "Text.Middle" in result
        assert "0" in result  # 1-based → 0-based

    @pytest.mark.unit
    def test_existing_getdate(self):
        result = DataflowGen2Generator._ssis_expr_to_m("GETDATE()")
        assert "DateTime.LocalNow()" in result

    @pytest.mark.unit
    def test_existing_null(self):
        assert DataflowGen2Generator._ssis_expr_to_m("null") == "null"
        assert DataflowGen2Generator._ssis_expr_to_m("") == "null"


# ===========================================================================
# 2. Graceful handling of malformed .dtsx files
# ===========================================================================


class TestMalformedDtsxHandling:
    """Parser should return partial SSISPackage instead of raising."""

    @pytest.mark.unit
    def test_invalid_xml_returns_partial(self, tmp_path: Path):
        bad = tmp_path / "bad.dtsx"
        bad.write_text("<DTS:Executable THIS IS NOT VALID XML", encoding="utf-8")
        parser = DTSXParser()
        pkg = parser.parse(bad)
        assert pkg.status == "partial"
        assert len(pkg.warnings) > 0

    @pytest.mark.unit
    def test_missing_file_returns_partial(self, tmp_path: Path):
        parser = DTSXParser()
        pkg = parser.parse(tmp_path / "nonexistent.dtsx")
        assert pkg.status == "partial"
        assert len(pkg.warnings) > 0

    @pytest.mark.unit
    def test_malformed_file_name_preserved(self, tmp_path: Path):
        bad = tmp_path / "broken_pkg.dtsx"
        bad.write_text("<<< invalid >>>", encoding="utf-8")
        parser = DTSXParser()
        pkg = parser.parse(bad)
        assert pkg.name == "broken_pkg"

    @pytest.mark.unit
    def test_valid_dtsx_has_ok_status(self):
        parser = DTSXParser()
        pkg = parser.parse(FIXTURES_DIR / "simple_etl.dtsx")
        assert pkg.status == "ok"
        assert len(pkg.warnings) == 0

    @pytest.mark.unit
    def test_parse_directory_does_not_crash_on_bad_file(self, tmp_path: Path):
        """parse_directory should skip malformed files and return valid ones."""
        # Write one bad file
        bad = tmp_path / "bad.dtsx"
        bad.write_text("<bad>", encoding="utf-8")
        # parse_directory returns whatever it can
        parser = DTSXParser()
        packages = parser.parse_directory(tmp_path)
        # Should include the partial package, not raise
        names = [p.name for p in packages]
        assert "bad" in names


# ===========================================================================
# 3. Structured errors in MigrationPlan
# ===========================================================================


class TestMigrationPlanErrors:
    """MigrationPlan should collect and serialise structured errors."""

    @pytest.mark.unit
    def test_add_error(self):
        plan = MigrationPlan(project_name="test")
        plan.add_error(source="pkg.dtsx", severity="error", message="boom", suggested_fix="fix it")
        assert len(plan.errors) == 1
        err = plan.errors[0]
        assert err.source == "pkg.dtsx"
        assert err.severity == "error"
        assert err.message == "boom"
        assert err.suggested_fix == "fix it"

    @pytest.mark.unit
    def test_to_dict_includes_errors(self):
        plan = MigrationPlan(project_name="test")
        plan.add_error(source="x", severity="warning", message="w")
        d = plan.to_dict()
        assert "errors" in d
        assert len(d["errors"]) == 1
        assert d["errors"][0]["severity"] == "warning"

    @pytest.mark.unit
    def test_migration_error_dataclass(self):
        err = MigrationError(source="a", severity="info", message="msg")
        assert err.source == "a"
        assert err.suggested_fix == ""


# ===========================================================================
# 4. RetryConfig in MigrationConfig
# ===========================================================================


class TestRetryConfig:
    @pytest.mark.unit
    def test_default_values(self):
        cfg = MigrationConfig()
        assert cfg.retry.max_retries == 5
        assert cfg.retry.base_delay == 2.0
        assert cfg.retry.max_delay == 60.0
        assert cfg.retry.default_retry_after == 30

    @pytest.mark.unit
    def test_custom_values(self):
        cfg = MigrationConfig(retry=RetryConfig(max_retries=3, base_delay=1.0))
        assert cfg.retry.max_retries == 3
        assert cfg.retry.base_delay == 1.0


# ===========================================================================
# 5. Multi-workspace environment support
# ===========================================================================


class TestEnvironmentProfiles:
    @pytest.mark.unit
    def test_get_environment_present(self):
        cfg = MigrationConfig(
            environments={
                "dev": EnvironmentProfile(workspace_id="dev-ws"),
                "prod": EnvironmentProfile(workspace_id="prod-ws"),
            }
        )
        dev = cfg.get_environment("dev")
        assert dev.workspace_id == "dev-ws"
        prod = cfg.get_environment("prod")
        assert prod.workspace_id == "prod-ws"

    @pytest.mark.unit
    def test_get_environment_fallback(self):
        cfg = MigrationConfig()
        cfg.fabric.workspace_id = "fallback-ws"
        env = cfg.get_environment("staging")
        assert env.workspace_id == "fallback-ws"

    @pytest.mark.unit
    def test_environment_connection_mappings(self):
        cfg = MigrationConfig(
            environments={
                "dev": EnvironmentProfile(
                    workspace_id="dev-ws",
                    connection_mappings={"OLEDB": "dev-conn-id"},
                )
            }
        )
        dev = cfg.get_environment("dev")
        assert dev.connection_mappings["OLEDB"] == "dev-conn-id"


# ===========================================================================
# 6. LakehouseProvisioner
# ===========================================================================


class TestLakehouseProvisioner:
    @pytest.mark.unit
    def test_generate_ddl_spark(self):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        p = LakehouseProvisioner(dialect="spark")
        manifest = {
            "table_name": "dbo.FactSales",
            "columns": [
                {"name": "SalesKey", "data_type": "DT_I4"},
                {"name": "Amount", "data_type": "DT_DECIMAL,10,2"},
                {"name": "LoadDate", "data_type": "DT_DBDATE"},
            ],
        }
        ddl = p.generate_ddl(manifest)
        assert "CREATE TABLE IF NOT EXISTS dbo.FactSales" in ddl
        assert "USING DELTA" in ddl
        assert "SalesKey INT" in ddl
        assert "DECIMAL(10,2)" in ddl

    @pytest.mark.unit
    def test_generate_ddl_sql(self):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        p = LakehouseProvisioner(dialect="sql")
        manifest = {
            "table_name": "dbo.DimCustomer",
            "columns": [
                {"name": "CustomerKey", "data_type": "DT_I4"},
                {"name": "Name", "data_type": "DT_WSTR,100"},
            ],
        }
        ddl = p.generate_ddl(manifest)
        assert "CREATE TABLE dbo.DimCustomer" in ddl
        assert "CustomerKey INT" in ddl
        assert "NVARCHAR(100)" in ddl

    @pytest.mark.unit
    def test_provision_no_manifests(self, tmp_path: Path):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        p = LakehouseProvisioner()
        result = p.provision(tmp_path)
        assert result == []

    @pytest.mark.unit
    def test_provision_with_manifest(self, tmp_path: Path):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        manifest = {
            "table_name": "dbo.FactTest",
            "columns": [{"name": "Id", "data_type": "DT_I4"}],
        }
        (tmp_path / "test.destinations.json").write_text(json.dumps(manifest))
        p = LakehouseProvisioner(dialect="spark")
        result = p.provision(tmp_path)
        assert len(result) == 1
        assert result[0].suffix == ".sql"
        assert "CREATE TABLE" in result[0].read_text()

    @pytest.mark.unit
    def test_type_map_wstr_with_length(self):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        p = LakehouseProvisioner(dialect="sql")
        assert p._map_type("DT_WSTR,255") == "NVARCHAR(255)"

    @pytest.mark.unit
    def test_type_map_unknown(self):
        from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner

        p = LakehouseProvisioner(dialect="spark")
        assert p._map_type("UNKNOWN") == "STRING"


# ===========================================================================
# 7. ReportGenerator
# ===========================================================================


class TestReportGenerator:
    @pytest.mark.unit
    def test_generate_html(self, tmp_path: Path):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        report = {
            "project_name": "test-project",
            "strategy": "hybrid",
            "items": [
                {
                    "source_package": "pkg1",
                    "source_task": "task1",
                    "task_type": "DATA_FLOW",
                    "target_artifact": "spark_notebook",
                    "complexity": "LOW",
                    "status": "completed",
                    "output_path": "/out/pkg1.py",
                    "notes": [],
                }
            ],
            "summary": {"total": 1, "spark_notebook": 1, "complexity_breakdown": {"LOW": 1}},
            "errors": [],
        }
        report_path = tmp_path / "migration_report.json"
        report_path.write_text(json.dumps(report))
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        assert html_path.exists()
        assert html_path.suffix == ".html"
        content = html_path.read_text()
        assert "test-project" in content
        assert "hybrid" in content
        assert "spark_notebook" in content

    @pytest.mark.unit
    def test_generate_html_with_errors(self, tmp_path: Path):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        report = {
            "project_name": "proj",
            "strategy": "spark",
            "items": [],
            "summary": {"total": 0, "complexity_breakdown": {}},
            "errors": [
                {
                    "source": "pkg.dtsx",
                    "severity": "error",
                    "message": "XML error",
                    "suggested_fix": "Fix the XML",
                }
            ],
        }
        (tmp_path / "migration_report.json").write_text(json.dumps(report))
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        content = html_path.read_text()
        assert "XML error" in content
        assert "Fix the XML" in content

    @pytest.mark.unit
    def test_missing_report_raises(self, tmp_path: Path):
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        with pytest.raises(FileNotFoundError):
            gen.generate(tmp_path)


# ===========================================================================
# 8. LineageGraph
# ===========================================================================


class TestLineageGraph:
    @pytest.mark.unit
    def test_build_from_packages(self):
        from ssis_to_fabric.analyzer.models import (
            ControlFlowTask,
            DataFlowComponent,
            DataFlowComponentType,
            SSISPackage,
            TaskType,
        )
        from ssis_to_fabric.engine.lineage import LineageGraph

        pkg = SSISPackage(name="TestPkg")
        task = ControlFlowTask(name="DataFlow1", task_type=TaskType.DATA_FLOW)
        src = DataFlowComponent(
            name="Source",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            table_name="dbo.SourceTable",
        )
        dst = DataFlowComponent(
            name="Dest",
            component_type=DataFlowComponentType.OLE_DB_DESTINATION,
            table_name="dbo.DestTable",
        )
        task.data_flow_components = [src, dst]
        pkg.control_flow_tasks = [task]

        graph = LineageGraph()
        graph.build([pkg])

        assert "dbo.SourceTable" in graph.all_tables()
        assert "dbo.DestTable" in graph.all_tables()

    @pytest.mark.unit
    def test_impact_analysis(self):
        from ssis_to_fabric.analyzer.models import (
            ControlFlowTask,
            DataFlowComponent,
            DataFlowComponentType,
            SSISPackage,
            TaskType,
        )
        from ssis_to_fabric.engine.lineage import LineageGraph

        pkg = SSISPackage(name="PkgA")
        task = ControlFlowTask(name="DFT", task_type=TaskType.DATA_FLOW)
        src = DataFlowComponent(
            name="S",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            table_name="dbo.FactSales",
        )
        task.data_flow_components = [src]
        pkg.control_flow_tasks = [task]

        graph = LineageGraph()
        graph.build([pkg])

        impact = graph.impact("dbo.FactSales")
        assert "PkgA" in impact["readers"]
        assert impact["writers"] == []

    @pytest.mark.unit
    def test_mermaid_export(self):
        from ssis_to_fabric.analyzer.models import (
            ControlFlowTask,
            DataFlowComponent,
            DataFlowComponentType,
            SSISPackage,
            TaskType,
        )
        from ssis_to_fabric.engine.lineage import LineageGraph

        pkg = SSISPackage(name="Pkg1")
        task = ControlFlowTask(name="DFT", task_type=TaskType.DATA_FLOW)
        src = DataFlowComponent(
            name="S",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            table_name="dbo.Orders",
        )
        dst = DataFlowComponent(
            name="D",
            component_type=DataFlowComponentType.OLE_DB_DESTINATION,
            table_name="dbo.OrdersStaging",
        )
        task.data_flow_components = [src, dst]
        pkg.control_flow_tasks = [task]

        graph = LineageGraph()
        graph.build([pkg])
        mermaid = graph.to_mermaid()
        assert "flowchart LR" in mermaid
        assert "read" in mermaid
        assert "write" in mermaid

    @pytest.mark.unit
    def test_write_json(self, tmp_path: Path):
        from ssis_to_fabric.engine.lineage import LineageGraph

        graph = LineageGraph()
        graph.build([])
        json_path = graph.write_json(tmp_path)
        assert json_path.exists()
        data = json.loads(json_path.read_text())
        assert "tables" in data


# ===========================================================================
# 9. CSharpTranspiler
# ===========================================================================


class TestCSharpTranspiler:
    @pytest.mark.unit
    def test_simple_string_ops(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile("var x = name.ToUpper();")
        assert ".upper()" in result

    @pytest.mark.unit
    def test_null_to_none(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile("if (x == null) {}")
        assert "None" in result

    @pytest.mark.unit
    def test_console_writeline(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile('Console.WriteLine("hello");')
        assert "print(" in result

    @pytest.mark.unit
    def test_dts_variable_access(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile('var val = (string)Dts.Variables["User::MyVar"].Value;')
        assert "dts_variables" in result
        assert "MyVar" in result

    @pytest.mark.unit
    def test_empty_input(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile("")
        assert "Empty Script Task" in result

    @pytest.mark.unit
    def test_unsupported_pattern_emits_todo(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile("var client = new HttpClient();")
        assert "TODO" in result

    @pytest.mark.unit
    def test_bool_conversion(self):
        from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler

        t = CSharpTranspiler()
        result = t.transpile("bool flag = true;")
        assert "True" in result


# ===========================================================================
# 10. Incremental migration helpers
# ===========================================================================


class TestIncrementalMigration:
    @pytest.mark.unit
    def test_file_sha256(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        sha = _file_sha256(f)
        assert len(sha) == 64  # SHA-256 hex digest

    @pytest.mark.unit
    def test_file_sha256_deterministic(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        assert _file_sha256(f) == _file_sha256(f)

    @pytest.mark.unit
    def test_save_and_load_state(self, tmp_path: Path):
        state = {"pkg1": {"hash": "abc123", "migrated_at": "2026-01-01T00:00:00Z"}}
        _save_migration_state(tmp_path, state)
        loaded = _load_migration_state(tmp_path)
        assert loaded["pkg1"]["hash"] == "abc123"

    @pytest.mark.unit
    def test_load_state_empty_dir(self, tmp_path: Path):
        state = _load_migration_state(tmp_path / "nonexistent")
        assert state == {}

    @pytest.mark.unit
    def test_save_state_creates_dir(self, tmp_path: Path):
        state_dir = tmp_path / "newdir"
        _save_migration_state(state_dir, {"x": 1})
        assert (state_dir / "state.json").exists()


# ===========================================================================
# 11. SSISPackage status field
# ===========================================================================


class TestSSISPackageStatus:
    @pytest.mark.unit
    def test_valid_package_has_ok_status(self):
        from ssis_to_fabric.analyzer.models import SSISPackage

        pkg = SSISPackage(name="test")
        assert pkg.status == "ok"

    @pytest.mark.unit
    def test_partial_package(self):
        from ssis_to_fabric.analyzer.models import SSISPackage

        pkg = SSISPackage(name="broken", status="partial", warnings=["XML error"])
        assert pkg.status == "partial"
        assert len(pkg.warnings) == 1
