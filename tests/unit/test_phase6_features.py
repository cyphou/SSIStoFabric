"""
Phase 6 — Fidelity & Scale Tests
==================================
Tests for:
- FabricDeployer (mocked REST) — deploy, retry, rollback, connection resolution
- LineageGraph — build, impact, Mermaid, SQL ref extraction
- ReportGenerator — HTML output, edge cases
- Shared generator utilities (engine/utils.py)
- Expression transpiler (unified facade + new functions)
- DAG-aware Spark generation (DataFlowPath routing)
- Incremental mode selective cleanup
- SSISDBExtractor parameterised queries
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    DataFlowPath,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig

# =====================================================================
# Helpers
# =====================================================================


def _make_config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(
        project_name="Phase6Test",
        source_dir=str(tmp_path),
        output_dir=tmp_path / "output",
    )


def _simple_package(**overrides: Any) -> SSISPackage:
    defaults: dict[str, Any] = dict(
        name="Pkg1",
        file_path="/fake/Pkg1.dtsx",
        control_flow_tasks=[
            ControlFlowTask(
                name="SQL1",
                task_type=TaskType.EXECUTE_SQL,
                sql_statement="SELECT 1",
            )
        ],
        total_tasks=1,
        total_data_flows=0,
        overall_complexity=MigrationComplexity.LOW,
    )
    defaults.update(overrides)
    return SSISPackage(**defaults)


# =====================================================================
# 1. Shared Generator Utilities (engine/utils.py)
# =====================================================================


class TestSharedUtils:
    """Test centralized component classification and sanitization."""

    def test_sanitize_name_basic(self) -> None:
        from ssis_to_fabric.engine.utils import sanitize_name

        assert sanitize_name("Hello World!") == "Hello_World"
        assert sanitize_name("a--b__c") == "a_b_c"

    def test_sanitize_name_max_length(self) -> None:
        from ssis_to_fabric.engine.utils import sanitize_name

        assert len(sanitize_name("x" * 500, max_length=100)) == 100

    def test_is_source(self) -> None:
        from ssis_to_fabric.engine.utils import is_source

        comp = DataFlowComponent(
            name="src", component_type=DataFlowComponentType.OLE_DB_SOURCE
        )
        assert is_source(comp) is True

    def test_is_destination(self) -> None:
        from ssis_to_fabric.engine.utils import is_destination

        comp = DataFlowComponent(
            name="dst", component_type=DataFlowComponentType.OLE_DB_DESTINATION
        )
        assert is_destination(comp) is True

    def test_is_transform(self) -> None:
        from ssis_to_fabric.engine.utils import is_transform

        comp = DataFlowComponent(
            name="xform", component_type=DataFlowComponentType.DERIVED_COLUMN
        )
        assert is_transform(comp) is True
        # Sources are not transforms
        src = DataFlowComponent(
            name="s", component_type=DataFlowComponentType.OLE_DB_SOURCE
        )
        assert is_transform(src) is False

    def test_filter_error_columns(self) -> None:
        from ssis_to_fabric.engine.utils import filter_error_columns

        class _Col:
            def __init__(self, name: str) -> None:
                self.name = name

        cols = [_Col("ID"), _Col("ErrorCode"), _Col("ErrorColumn"), _Col(""), _Col("Name")]
        result = filter_error_columns(cols)
        assert [c.name for c in result] == ["ID", "Name"]

    def test_source_dest_type_sets_are_frozenset(self) -> None:
        from ssis_to_fabric.engine.utils import (
            DEST_COMPONENT_TYPES,
            SOURCE_COMPONENT_TYPES,
        )

        assert isinstance(SOURCE_COMPONENT_TYPES, frozenset)
        assert isinstance(DEST_COMPONENT_TYPES, frozenset)
        assert DataFlowComponentType.OLE_DB_SOURCE in SOURCE_COMPONENT_TYPES
        assert DataFlowComponentType.OLE_DB_DESTINATION in DEST_COMPONENT_TYPES


# =====================================================================
# 2. Lineage Graph Tests
# =====================================================================


class TestLineageGraph:
    """Test LineageGraph build, impact, Mermaid, and SQL extraction."""

    def _make_pkg_with_dataflow(self, name: str, src_table: str, dst_table: str) -> SSISPackage:
        return SSISPackage(
            name=name,
            file_path=f"/fake/{name}.dtsx",
            control_flow_tasks=[
                ControlFlowTask(
                    name="DFT",
                    task_type=TaskType.DATA_FLOW,
                    data_flow_components=[
                        DataFlowComponent(
                            name="OLE Source",
                            component_type=DataFlowComponentType.OLE_DB_SOURCE,
                            table_name=src_table,
                        ),
                        DataFlowComponent(
                            name="OLE Dest",
                            component_type=DataFlowComponentType.OLE_DB_DESTINATION,
                            table_name=dst_table,
                        ),
                    ],
                )
            ],
            total_tasks=1,
            total_data_flows=1,
            overall_complexity=MigrationComplexity.LOW,
        )

    def test_build_populates_tables(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("P1", "dbo.Src", "dbo.Dst")])
        assert "dbo.Src" in g.all_tables()
        assert "dbo.Dst" in g.all_tables()

    def test_impact_readers_writers(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("PkgA", "dbo.Raw", "dbo.Staging")])
        impact = g.impact("dbo.Raw")
        assert "PkgA" in impact["readers"]
        impact2 = g.impact("dbo.Staging")
        assert "PkgA" in impact2["writers"]

    def test_impact_unknown_table(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([])
        result = g.impact("nonexistent")
        assert result == {"readers": [], "writers": []}

    def test_package_tables(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("P1", "Sales", "Staging")])
        pt = g.package_tables("P1")
        assert "Sales" in pt["sources"]
        assert "Staging" in pt["destinations"]

    def test_to_mermaid(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("ETL1", "dbo.Orders", "dbo.FactOrders")])
        mermaid = g.to_mermaid()
        assert "flowchart LR" in mermaid
        assert "ETL1" in mermaid
        assert "dbo.Orders" in mermaid

    def test_to_dict(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("P1", "A", "B")])
        d = g.to_dict()
        assert "tables" in d
        assert "A" in d["tables"]

    def test_write_json(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        g = LineageGraph()
        g.build([self._make_pkg_with_dataflow("P1", "X", "Y")])
        path = g.write_json(tmp_path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert "X" in data["tables"]

    def test_sql_table_extraction(self) -> None:
        from ssis_to_fabric.engine.lineage import _extract_sql_table_refs

        refs = _extract_sql_table_refs("SELECT * FROM dbo.Orders o JOIN dbo.Customers c ON o.id = c.id")
        assert "dbo.Orders" in refs
        assert "dbo.Customers" in refs

    def test_normalise_table(self) -> None:
        from ssis_to_fabric.engine.lineage import _normalise_table

        assert _normalise_table("  tablename  ") == "tablename"
        assert _normalise_table("dbo.Sales") == "dbo.Sales"

    def test_execute_sql_adds_readers(self) -> None:
        from ssis_to_fabric.engine.lineage import LineageGraph

        pkg = SSISPackage(
            name="SqlPkg",
            file_path="/fake/SqlPkg.dtsx",
            control_flow_tasks=[
                ControlFlowTask(
                    name="RunSQL",
                    task_type=TaskType.EXECUTE_SQL,
                    sql_statement="INSERT INTO dbo.Target SELECT * FROM dbo.Source",
                )
            ],
            total_tasks=1,
            total_data_flows=0,
            overall_complexity=MigrationComplexity.LOW,
        )
        g = LineageGraph()
        g.build([pkg])
        assert "SqlPkg" in g.impact("dbo.Source")["readers"]


# =====================================================================
# 3. Report Generator Tests
# =====================================================================


class TestReportGenerator:
    """Test HTML report generation from migration_report.json."""

    def _write_report(self, output_dir: Path, data: dict[str, Any]) -> None:
        (output_dir / "migration_report.json").write_text(json.dumps(data))

    def test_generate_creates_html(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        self._write_report(
            tmp_path,
            {
                "project_name": "TestProject",
                "strategy": "hybrid",
                "items": [
                    {
                        "source_package": "Pkg1",
                        "source_task": "Task1",
                        "task_type": "EXECUTE_SQL",
                        "target_artifact": "data_factory_pipeline",
                        "complexity": "LOW",
                        "status": "completed",
                        "notes": [],
                    }
                ],
                "errors": [],
                "summary": {
                    "total": 1,
                    "data_factory_pipeline": 1,
                    "complexity_breakdown": {"LOW": 1},
                },
            },
        )
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        assert html_path.exists()
        html = html_path.read_text()
        assert "TestProject" in html
        assert "<!DOCTYPE html>" in html

    def test_generate_with_errors(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        self._write_report(
            tmp_path,
            {
                "project_name": "ErrProject",
                "items": [],
                "errors": [
                    {
                        "source": "pkg.dtsx",
                        "severity": "error",
                        "message": "Parse failed",
                        "suggested_fix": "Check XML",
                    }
                ],
                "summary": {"total": 0, "complexity_breakdown": {}},
            },
        )
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        html = html_path.read_text()
        assert "Parse failed" in html
        assert "Errors" in html

    def test_generate_empty_items(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        self._write_report(
            tmp_path,
            {
                "project_name": "Empty",
                "items": [],
                "errors": [],
                "summary": {"total": 0, "complexity_breakdown": {}},
            },
        )
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        assert html_path.exists()
        assert "No items" in html_path.read_text()

    def test_generate_missing_report_raises(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        with pytest.raises(FileNotFoundError):
            gen.generate(tmp_path)

    def test_html_escapes_special_chars(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        self._write_report(
            tmp_path,
            {
                "project_name": "Test<Project>&",
                "items": [],
                "errors": [],
                "summary": {"total": 0, "complexity_breakdown": {}},
            },
        )
        gen = ReportGenerator()
        html_path = gen.generate(tmp_path)
        html = html_path.read_text()
        assert "Test&lt;Project&gt;&amp;" in html


# =====================================================================
# 4. FabricDeployer Tests (mocked REST)
# =====================================================================


class TestFabricDeployer:
    """Test FabricDeployer with mocked HTTP requests."""

    def _make_deployer(self, **kwargs: Any) -> Any:
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        deployer = FabricDeployer(
            workspace_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            **kwargs,
        )
        deployer._token = "mock-token"
        return deployer

    def test_headers_require_auth(self) -> None:
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        deployer = FabricDeployer(workspace_id="test-ws")
        with pytest.raises(RuntimeError, match="Not authenticated"):
            deployer._headers()

    def test_headers_with_token(self) -> None:
        deployer = self._make_deployer()
        headers = deployer._headers()
        assert headers["Authorization"] == "Bearer mock-token"
        assert "Content-Type" in headers

    def test_looks_like_guid(self) -> None:
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        assert FabricDeployer._looks_like_guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        assert not FabricDeployer._looks_like_guid("not-a-guid")

    @patch("ssis_to_fabric.engine.fabric_deployer.requests")
    def test_api_call_success(self, mock_requests: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_requests.request.return_value = mock_resp

        deployer = self._make_deployer()
        resp = deployer._api_call("GET", "https://example.com/api")
        assert resp.status_code == 200

    @patch("ssis_to_fabric.engine.fabric_deployer.requests")
    def test_api_call_retry_on_429(self, mock_requests: MagicMock) -> None:
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "0"}
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        mock_requests.request.side_effect = [rate_resp, ok_resp]

        deployer = self._make_deployer()
        resp = deployer._api_call("GET", "https://example.com/api", retries=2)
        assert resp.status_code == 200

    @patch("ssis_to_fabric.engine.fabric_deployer.requests")
    def test_api_call_retry_on_500(self, mock_requests: MagicMock) -> None:
        err_resp = MagicMock()
        err_resp.status_code = 500
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        mock_requests.request.side_effect = [err_resp, ok_resp]

        deployer = self._make_deployer()
        resp = deployer._api_call("GET", "https://example.com/api", retries=2)
        assert resp.status_code == 200

    def test_delete_item_dry_run(self) -> None:
        deployer = self._make_deployer(dry_run=True)
        result = deployer.delete_item("item-1", "DataPipeline", "test")
        assert result is True

    @patch("ssis_to_fabric.engine.fabric_deployer.requests")
    def test_delete_item_success(self, mock_requests: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_requests.request.return_value = mock_resp

        deployer = self._make_deployer()
        result = deployer.delete_item("item-1", "DataPipeline", "test")
        assert result is True

    @patch("ssis_to_fabric.engine.fabric_deployer.requests")
    def test_delete_item_404_is_success(self, mock_requests: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_requests.request.return_value = mock_resp

        deployer = self._make_deployer()
        result = deployer.delete_item("item-1", "DataPipeline", "test")
        assert result is True

    def test_deployment_report_properties(self) -> None:
        from ssis_to_fabric.engine.fabric_deployer import (
            DeploymentReport,
            DeploymentResult,
        )

        report = DeploymentReport(
            workspace_id="ws-1",
            results=[
                DeploymentResult(name="p1", item_type="DataPipeline", status="success", item_id="id1"),
                DeploymentResult(name="p2", item_type="DataPipeline", status="error", error="fail"),
                DeploymentResult(name="p3", item_type="Notebook", status="skipped"),
            ],
        )
        assert report.total == 3
        assert report.succeeded == 1
        assert report.failed == 1
        assert report.skipped == 1
        assert len(report.created_item_ids) == 1

    def test_py_to_ipynb(self) -> None:
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        py = "# --- Imports ---\nimport os\n# --- Main ---\nprint('hello')"
        nb = FabricDeployer._py_to_ipynb(py)
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 1
        assert nb["cells"][0]["cell_type"] == "code"

    def test_discover_connections_dry_run(self) -> None:
        deployer = self._make_deployer(dry_run=True)
        result = deployer._discover_connections()
        assert result == {}

    def test_resolve_connection_for_activity(self) -> None:
        deployer = self._make_deployer()
        deployer._connection_map = {"SourceDB": "guid-123"}
        assert deployer._resolve_connection_for_activity("SourceDB") == "guid-123"

    def test_resolve_connection_fallback(self) -> None:
        deployer = self._make_deployer(default_connection_id="default-guid")
        assert deployer._resolve_connection_for_activity("unknown") == "default-guid"

    def test_folder_for_item_longest_prefix_match(self) -> None:
        deployer = self._make_deployer()
        deployer._folder_map = {
            "StgProduct": "folder-1",
            "StgProductCategory": "folder-2",
        }
        deployer._folder_names_sorted = sorted(
            deployer._folder_map.keys(), key=len, reverse=True
        )
        assert deployer._folder_for_item("StgProductCategory_DFT") == "folder-2"
        assert deployer._folder_for_item("StgProduct_DFT") == "folder-1"


# =====================================================================
# 5. Expression Transpiler Tests
# =====================================================================


class TestExpressionTranspiler:
    """Test the unified expression transpiler and new functions."""

    def test_pyspark_codepoint(self) -> None:
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        result = SparkNotebookGenerator._ssis_expr_to_pyspark("CODEPOINT(MyCol)")
        assert "ascii" in result
        assert "MyCol" in result

    def test_pyspark_tokencount(self) -> None:
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        result = SparkNotebookGenerator._ssis_expr_to_pyspark('TOKENCOUNT(Data, ",")')
        assert "size" in result
        assert "split" in result

    def test_pyspark_hex(self) -> None:
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        result = SparkNotebookGenerator._ssis_expr_to_pyspark("HEX(ColA)")
        assert "hex" in result

    def test_m_tokencount(self) -> None:
        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        result = DataflowGen2Generator._ssis_expr_to_m('TOKENCOUNT(Col, "|")')
        assert "List.Count" in result
        assert "Text.Split" in result

    def test_m_codepoint(self) -> None:
        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        result = DataflowGen2Generator._ssis_expr_to_m("CODEPOINT(Col)")
        assert "Character.ToNumber" in result

    def test_m_hex(self) -> None:
        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        result = DataflowGen2Generator._ssis_expr_to_m("HEX(Col)")
        assert "Number.ToText" in result

    def test_unified_facade_pyspark(self) -> None:
        from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

        result = ExpressionTranspiler.to_pyspark("UPPER(Name)")
        assert "upper" in result.lower()

    def test_unified_facade_m(self) -> None:
        from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

        result = ExpressionTranspiler.to_m("UPPER(Name)")
        assert "Text.Upper" in result

    def test_unified_transpile_method(self) -> None:
        from ssis_to_fabric.engine.expression_transpiler import (
            ExpressionTranspiler,
            TargetLanguage,
        )

        t = ExpressionTranspiler()
        ps = t.transpile("LOWER(X)", TargetLanguage.PYSPARK)
        assert "lower" in ps.lower()
        m = t.transpile("LOWER(X)", TargetLanguage.POWER_QUERY_M)
        assert "Text.Lower" in m

    def test_pyspark_dt_date_cast(self) -> None:
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        result = SparkNotebookGenerator._ssis_expr_to_pyspark("(DT_DBDATE) OrderDate")
        assert 'cast("date")' in result

    def test_pyspark_dt_timestamp_cast(self) -> None:
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        result = SparkNotebookGenerator._ssis_expr_to_pyspark("(DT_DBTIMESTAMP) CreatedAt")
        assert 'cast("timestamp")' in result


# =====================================================================
# 6. DAG-aware Spark Generation Tests
# =====================================================================


class TestDAGAwareSpark:
    """Test that DataFlowPath routing generates correct DAG-ordered code."""

    def test_linear_fallback_no_paths(self, tmp_path: Path) -> None:
        """Without data_flow_paths, linear code generation should still work."""
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        config = _make_config(tmp_path)
        gen = SparkNotebookGenerator(config)
        pkg = SSISPackage(
            name="Linear",
            file_path="/fake/Linear.dtsx",
            control_flow_tasks=[
                ControlFlowTask(
                    name="DFT",
                    task_type=TaskType.DATA_FLOW,
                    data_flow_components=[
                        DataFlowComponent(name="Src", component_type=DataFlowComponentType.OLE_DB_SOURCE, table_name="T1"),
                        DataFlowComponent(name="Dst", component_type=DataFlowComponentType.OLE_DB_DESTINATION, table_name="T2"),
                    ],
                    data_flow_paths=[],
                )
            ],
            total_tasks=1,
            total_data_flows=1,
            overall_complexity=MigrationComplexity.LOW,
        )
        path = gen.generate(pkg, pkg.control_flow_tasks[0], tmp_path)
        assert path.exists()
        code = path.read_text()
        assert "df_source_src" in code

    def test_dag_multicast_two_paths(self, tmp_path: Path) -> None:
        """With multiple paths from one source, DAG mode should activate."""
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        config = _make_config(tmp_path)
        gen = SparkNotebookGenerator(config)
        pkg = SSISPackage(
            name="Multi",
            file_path="/fake/Multi.dtsx",
            control_flow_tasks=[
                ControlFlowTask(
                    name="DFT",
                    task_type=TaskType.DATA_FLOW,
                    data_flow_components=[
                        DataFlowComponent(name="Src", component_type=DataFlowComponentType.OLE_DB_SOURCE, table_name="Raw"),
                        DataFlowComponent(name="Dest1", component_type=DataFlowComponentType.OLE_DB_DESTINATION, table_name="Out1"),
                        DataFlowComponent(name="Dest2", component_type=DataFlowComponentType.FLAT_FILE_DESTINATION),
                    ],
                    data_flow_paths=[
                        DataFlowPath(source_component="Src", destination_component="Dest1"),
                        DataFlowPath(source_component="Src", destination_component="Dest2"),
                    ],
                )
            ],
            total_tasks=1,
            total_data_flows=1,
            overall_complexity=MigrationComplexity.MEDIUM,
        )
        path = gen.generate(pkg, pkg.control_flow_tasks[0], tmp_path)
        code = path.read_text()
        assert "DAG-aware" in code
        assert "df_source_src" in code

    def test_dag_with_transform(self, tmp_path: Path) -> None:
        """DAG with source → transform → destination follows paths."""
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        config = _make_config(tmp_path)
        gen = SparkNotebookGenerator(config)
        pkg = SSISPackage(
            name="DAGXform",
            file_path="/fake/DAGXform.dtsx",
            control_flow_tasks=[
                ControlFlowTask(
                    name="DFT",
                    task_type=TaskType.DATA_FLOW,
                    data_flow_components=[
                        DataFlowComponent(name="Src", component_type=DataFlowComponentType.OLE_DB_SOURCE, table_name="Raw"),
                        DataFlowComponent(name="DerivedCol", component_type=DataFlowComponentType.DERIVED_COLUMN),
                        DataFlowComponent(name="Dest", component_type=DataFlowComponentType.OLE_DB_DESTINATION, table_name="Final"),
                    ],
                    data_flow_paths=[
                        DataFlowPath(source_component="Src", destination_component="DerivedCol"),
                        DataFlowPath(source_component="DerivedCol", destination_component="Dest"),
                    ],
                )
            ],
            total_tasks=1,
            total_data_flows=1,
            overall_complexity=MigrationComplexity.MEDIUM,
        )
        path = gen.generate(pkg, pkg.control_flow_tasks[0], tmp_path)
        code = path.read_text()
        assert "DAG-aware" in code


# =====================================================================
# 7. Incremental Mode Selective Cleanup Tests
# =====================================================================


class TestIncrementalCleanup:
    """Test that incremental mode only deletes changed packages' artifacts."""

    def test_incremental_preserves_unchanged_artifacts(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = _make_config(tmp_path)
        engine = MigrationEngine(config)
        output_dir = config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Pre-create a "notebook" that belongs to an unchanged package
        notebooks_dir = output_dir / "notebooks"
        notebooks_dir.mkdir()
        preserved = notebooks_dir / "UnchangedPkg_Task.py"
        preserved.write_text("# unchanged")

        # Create a notebook for the changed package
        changed = notebooks_dir / "Pkg1_SQL1.py"
        changed.write_text("# old")

        # Write a dtsx file so hashing works
        dtsx_path = tmp_path / "Pkg1.dtsx"
        dtsx_path.write_text("<xml>test</xml>")

        pkg = _simple_package(file_path=str(dtsx_path))

        # First run (non-incremental) to establish state
        state_dir = tmp_path / ".ssis2fabric"
        engine.execute([pkg], incremental=True, state_dir=state_dir)

        # unchanged file should still be there (name doesn't match "Pkg1")
        assert preserved.exists()

    def test_non_incremental_cleans_all(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = _make_config(tmp_path)
        engine = MigrationEngine(config)
        output_dir = config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        notebooks_dir = output_dir / "notebooks"
        notebooks_dir.mkdir()
        old_file = notebooks_dir / "OldPkg_Task.py"
        old_file.write_text("# old")

        dtsx_path = tmp_path / "Pkg1.dtsx"
        dtsx_path.write_text("<xml>test</xml>")
        pkg = _simple_package(file_path=str(dtsx_path))

        engine.execute([pkg], incremental=False)

        # Non-incremental should have removed the old notebooks directory
        assert not old_file.exists()


# =====================================================================
# 8. SSISDBExtractor Parameterised Queries Tests
# =====================================================================


class TestSSISDBExtractorSQL:
    """Test that SSISDBExtractor uses parameterised queries."""

    def test_list_packages_no_filters(self) -> None:
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        extractor = SSISDBExtractor("fake_conn_string")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        extractor._conn = mock_conn

        extractor.list_packages()

        # Should call execute with the SQL and empty params list
        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "?" not in sql
        assert params == []

    def test_list_packages_with_folder_uses_param(self) -> None:
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        extractor = SSISDBExtractor("fake_conn_string")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        extractor._conn = mock_conn

        extractor.list_packages(folder_name="MyFolder")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "f.name = ?" in sql
        assert params == ["MyFolder"]
        # Must NOT contain the folder name directly in SQL
        assert "MyFolder" not in sql

    def test_list_packages_with_both_filters(self) -> None:
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        extractor = SSISDBExtractor("fake_conn_string")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        extractor._conn = mock_conn

        extractor.list_packages(folder_name="F1", project_name="P1")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "f.name = ?" in sql
        assert "pj.name = ?" in sql
        assert params == ["F1", "P1"]

    def test_list_packages_sql_injection_safe(self) -> None:
        """Verify that SQL injection payloads are passed as params, not interpolated."""
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        extractor = SSISDBExtractor("fake_conn_string")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        extractor._conn = mock_conn

        payload = "'; DROP TABLE packages; --"
        extractor.list_packages(folder_name=payload)

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        # The payload should be a parameter, not in the SQL text
        assert payload not in sql
        assert payload in params


# =====================================================================
# 9. Assess O(N) Performance Fix Tests
# =====================================================================


class TestAssessPerformance:
    """Verify that assess() creates the plan only once (not N+1 times)."""

    def test_assess_calls_create_plan_once(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = _make_config(tmp_path)
        engine = MigrationEngine(config)

        pkgs = [_simple_package(name=f"Pkg{i}") for i in range(5)]

        with patch.object(engine, "create_plan", wraps=engine.create_plan) as mock_plan:
            engine.assess(pkgs)
            # Should be called exactly once (not 5 or 10 times)
            assert mock_plan.call_count == 1
