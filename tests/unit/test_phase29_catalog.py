"""Tests for Phase 29 — Metadata Catalog & Discovery."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.metadata_catalog import (
    CatalogEntry,
    CatalogEntryType,
    CatalogSummary,
    CatalogTag,
    DependencyEdge,
    DependencyMatrix,
    ExportFormat,
    MetadataCatalog,
    SearchResult,
    build_catalog,
    write_catalog_report,
    _auto_tag,
    _extract_sql_table_refs,
    _normalise_table,
    _purview_type,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _col(name="col1", expression=""):
    return SimpleNamespace(name=name, data_type="String", length=0, precision=0, scale=0,
                           expression=expression, source_column="")


def _comp(name="src", comp_type="OLE_DB_SOURCE", table="dbo.Customers", conn_ref="conn1", expressions=None):
    from ssis_to_fabric.analyzer.models import DataFlowComponentType, MigrationComplexity
    ct = DataFlowComponentType(comp_type)
    return SimpleNamespace(
        id="", name=name, component_type=ct, connection_manager_ref=conn_ref,
        sql_command="", table_name=table, columns=[_col()],
        properties={}, expressions=expressions or {},
        migration_complexity=MigrationComplexity.LOW,
    )


def _task(name="t1", task_type="DATA_FLOW", components=None, sql="", child_tasks=None,
          disabled=False, conn_ref=""):
    from ssis_to_fabric.analyzer.models import MigrationComplexity, TaskType
    return SimpleNamespace(
        id="", ref_id="", name=name, task_type=TaskType(task_type),
        description="", connection_manager_ref=conn_ref,
        sql_statement=sql, expression="", disabled=disabled,
        transaction_option="Supported", isolation_level="",
        properties={}, sql_parameters=[], parameter_bindings={},
        data_flow_components=components or [],
        data_flow_paths=[], child_tasks=child_tasks or [],
        child_precedence_constraints=[],
        migration_complexity=MigrationComplexity.LOW,
    )


def _conn(name="conn1", conn_type="OLEDB", server="srv", database="db"):
    from ssis_to_fabric.analyzer.models import ConnectionType
    return SimpleNamespace(
        id="", name=name, connection_type=ConnectionType(conn_type),
        connection_string=f"Server={server};Database={database}",
        server=server, database=database, provider="SQLOLEDB", properties={},
    )


def _var(name="var1", value="val", expression=""):
    return SimpleNamespace(
        name=name, namespace="User", data_type="String",
        value=value, expression=expression, scope="PACKAGE",
        is_parameter=False, sensitive=False, required=False, description="",
    )


def _param(name="p1", data_type="String", required=False, sensitive=False, description=""):
    return SimpleNamespace(
        name=name, namespace="Package", data_type=data_type,
        value="", expression="", scope="PACKAGE",
        is_parameter=True, sensitive=sensitive, required=required, description=description,
    )


def _make_pkg(name="TestPkg", tasks=None, connections=None, variables=None,
              parameters=None, project_parameters=None, description=""):
    from ssis_to_fabric.analyzer.models import MigrationComplexity
    return SimpleNamespace(
        name=name, file_path=f"/ssis/{name}.dtsx", description=description,
        creation_date="2025-01-01", creator_name="tester", format_version="8",
        connection_managers=connections or [],
        variables=variables or [], parameters=parameters or [],
        project_parameters=project_parameters or [],
        control_flow_tasks=tasks or [],
        precedence_constraints=[], event_handlers=[], logging_providers=[],
        annotations=[],
        total_tasks=len(tasks or []), total_data_flows=0,
        overall_complexity=MigrationComplexity.LOW,
        warnings=[], status="ok",
    )


def _pkg_with_data_flow(name="StgCustomer"):
    """Create a package with a data flow containing source + destination."""
    src = _comp("OLE DB Source", "OLE_DB_SOURCE", "dbo.Customers", "conn_src")
    dst = _comp("OLE DB Dest", "OLE_DB_DESTINATION", "staging.StgCustomer", "conn_dst")
    dft = _task("Load Staging Customer", "DATA_FLOW", [src, dst])
    return _make_pkg(
        name=name,
        tasks=[dft],
        connections=[_conn("conn_src", "OLEDB", "source_srv", "source_db"),
                     _conn("conn_dst", "OLEDB", "dest_srv", "dest_db")],
    )


# ── Tests ────────────────────────────────────────────────────────────


class TestAutoTag:
    def test_staging_tag(self):
        tags = _auto_tag("StgCustomer")
        assert CatalogTag.STAGING.value in tags

    def test_dimension_tag(self):
        tags = _auto_tag("DimProduct")
        assert CatalogTag.DIMENSION.value in tags

    def test_fact_tag(self):
        tags = _auto_tag("FactSales")
        assert CatalogTag.FACT.value in tags

    def test_scd_tag(self):
        tags = _auto_tag("Load_SCD_Dimension")
        assert CatalogTag.SCD.value in tags
        assert CatalogTag.DIMENSION.value in tags

    def test_cdc_tag(self):
        tags = _auto_tag("CDC_Incremental_Load")
        assert CatalogTag.CDC.value in tags
        assert CatalogTag.INCREMENTAL.value in tags

    def test_lookup_tag(self):
        tags = _auto_tag("Lookup_Customer")
        assert CatalogTag.LOOKUP.value in tags

    def test_no_match(self):
        tags = _auto_tag("MyPackage")
        assert tags == []

    def test_watermark_tag(self):
        tags = _auto_tag("watermark_date")
        assert CatalogTag.INCREMENTAL.value in tags


class TestHelpers:
    def test_normalise_table_brackets(self):
        assert _normalise_table("[dbo].[Customers]") == "dbo.Customers"

    def test_normalise_table_quotes(self):
        assert _normalise_table('"dbo"."Customers"') == "dbo.Customers"

    def test_normalise_table_plain(self):
        assert _normalise_table("Products") == "Products"

    def test_extract_sql_tables(self):
        sql = "SELECT * FROM dbo.Orders JOIN dbo.Customers ON 1=1"
        tables = _extract_sql_table_refs(sql)
        assert "dbo.Orders" in tables
        assert "dbo.Customers" in tables

    def test_extract_sql_insert(self):
        sql = "INSERT INTO staging.StgOrders SELECT * FROM source.Orders"
        tables = _extract_sql_table_refs(sql)
        assert "staging.StgOrders" in tables
        assert "source.Orders" in tables

    def test_extract_sql_update(self):
        sql = "UPDATE dbo.Products SET Price = 0"
        tables = _extract_sql_table_refs(sql)
        assert "dbo.Products" in tables

    def test_purview_type_table(self):
        assert _purview_type(CatalogEntryType.TABLE) == "rdbms_table"

    def test_purview_type_package(self):
        assert _purview_type(CatalogEntryType.PACKAGE) == "ssis_package"


class TestCatalogBuild:
    def test_build_empty(self):
        cat = MetadataCatalog()
        summary = cat.build([])
        assert summary.total_entries == 0
        assert summary.total_packages == 0

    def test_build_single_package(self):
        pkg = _make_pkg("Pkg1", tasks=[_task("t1", "EXECUTE_SQL", sql="SELECT 1")])
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_packages == 1
        assert summary.total_tasks >= 1
        assert summary.total_entries > 0

    def test_build_with_connections(self):
        pkg = _make_pkg("Pkg1", connections=[_conn("c1"), _conn("c2")])
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_connections == 2

    def test_build_with_parameters(self):
        pkg = _make_pkg("Pkg1", parameters=[_param("p1"), _param("p2")])
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_parameters == 2

    def test_build_with_variables(self):
        pkg = _make_pkg("Pkg1", variables=[_var("v1"), _var("v2")])
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_variables == 2

    def test_build_data_flow(self):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_data_flows >= 1
        assert summary.total_tables >= 2  # source + destination

    def test_build_indexes_tables(self):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        table_entries = cat.browse(entry_type=CatalogEntryType.TABLE)
        table_names = [e.name for e in table_entries]
        assert "dbo.Customers" in table_names
        assert "staging.StgCustomer" in table_names

    def test_build_dedup_connections(self):
        """Same connection name in a package should only appear once."""
        pkg = _make_pkg("Pkg1", connections=[_conn("c1"), _conn("c1")])
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert summary.total_connections == 1

    def test_build_sql_table_extraction(self):
        sql_task = _task("Load", "EXECUTE_SQL", sql="INSERT INTO dbo.Target SELECT * FROM dbo.Source")
        pkg = _make_pkg("Pkg1", tasks=[sql_task])
        cat = MetadataCatalog()
        cat.build([pkg])
        table_entries = cat.browse(entry_type=CatalogEntryType.TABLE)
        names = [e.name for e in table_entries]
        assert "dbo.Target" in names
        assert "dbo.Source" in names

    def test_build_with_expressions(self):
        comp = _comp("DC1", "DERIVED_COLUMN", "", expressions={"NewCol": "UPPER([Name])"})
        dft = _task("Transform", "DATA_FLOW", [comp])
        pkg = _make_pkg("Pkg1", tasks=[dft])
        cat = MetadataCatalog()
        cat.build([pkg])
        expr_entries = cat.browse(entry_type=CatalogEntryType.EXPRESSION)
        assert len(expr_entries) >= 1

    def test_build_child_tasks(self):
        """Container children should be recursively indexed."""
        child = _task("Inner SQL", "EXECUTE_SQL", sql="SELECT 1")
        seq = _task("Seq Container", "SEQUENCE_CONTAINER", child_tasks=[child])
        pkg = _make_pkg("Pkg1", tasks=[seq])
        cat = MetadataCatalog()
        cat.build([pkg])
        task_entries = cat.browse(entry_type=CatalogEntryType.TASK)
        names = [e.name for e in task_entries]
        assert "Inner SQL" in names
        assert "Seq Container" in names

    def test_build_auto_tags_package(self):
        pkg = _make_pkg("StgCustomer_Load")
        cat = MetadataCatalog()
        cat.build([pkg])
        pkg_entries = cat.browse(entry_type=CatalogEntryType.PACKAGE)
        assert any(CatalogTag.STAGING.value in e.tags for e in pkg_entries)

    def test_summary_complexity_distribution(self):
        pkg = _make_pkg("Pkg1")
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert "LOW" in summary.complexity_distribution

    def test_summary_tag_counts(self):
        pkg = _make_pkg("StgCustomer")
        cat = MetadataCatalog()
        summary = cat.build([pkg])
        assert CatalogTag.STAGING.value in summary.tag_counts

    def test_rebuild_clears_previous(self):
        pkg1 = _make_pkg("A")
        pkg2 = _make_pkg("B")
        cat = MetadataCatalog()
        cat.build([pkg1])
        cat.build([pkg2])
        assert cat.summary.total_packages == 1
        assert cat.entries[0].package_name == "B"


class TestCatalogSearch:
    def test_search_by_name(self):
        pkg = _pkg_with_data_flow("CustomerETL")
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("customer")
        assert result.total_matches > 0
        assert result.query == "customer"

    def test_search_case_insensitive(self):
        pkg = _pkg_with_data_flow("StgProduct")
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("STGPRODUCT")
        assert result.total_matches > 0

    def test_search_no_match(self):
        pkg = _make_pkg("Pkg1")
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("nonexistent_xyz_999")
        assert result.total_matches == 0

    def test_search_multi_term(self):
        """Space-separated terms use AND semantics."""
        pkg = _pkg_with_data_flow("StgCustomer")
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("staging customer")
        assert result.total_matches >= 1

    def test_search_by_type(self):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("customer", entry_type=CatalogEntryType.TABLE)
        assert all(e.entry_type == CatalogEntryType.TABLE for e in result.entries)

    def test_search_in_sql(self):
        sql_task = _task("RunSQL", "EXECUTE_SQL", sql="SELECT * FROM reporting.SalesReport")
        pkg = _make_pkg("Pkg1", tasks=[sql_task])
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("salesreport")
        assert result.total_matches > 0

    def test_search_in_connection_string(self):
        pkg = _make_pkg("Pkg1", connections=[_conn("prod_conn", "OLEDB", "prod-server.db.com", "ProdDB")])
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("prod-server")
        assert result.total_matches > 0

    def test_search_result_serialization(self):
        pkg = _make_pkg("Pkg1")
        cat = MetadataCatalog()
        cat.build([pkg])
        result = cat.search("pkg1")
        d = result.to_dict()
        assert "query" in d
        assert "total_matches" in d
        assert "entries" in d
        assert "elapsed_ms" in d


class TestCatalogBrowse:
    def test_browse_by_tag(self):
        pkg = _make_pkg("StgCustomer")
        cat = MetadataCatalog()
        cat.build([pkg])
        entries = cat.browse(tags=[CatalogTag.STAGING.value])
        assert len(entries) > 0
        assert all(CatalogTag.STAGING.value in e.tags for e in entries)

    def test_browse_by_type(self):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        entries = cat.browse(entry_type=CatalogEntryType.CONNECTION)
        assert all(e.entry_type == CatalogEntryType.CONNECTION for e in entries)

    def test_browse_by_package(self):
        pkg1 = _make_pkg("A", connections=[_conn("c1")])
        pkg2 = _make_pkg("B", connections=[_conn("c2")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        entries = cat.browse(package_name="A")
        assert all(e.package_name == "A" for e in entries)

    def test_browse_combined_filters(self):
        pkg = _make_pkg("StgCustomer", connections=[_conn("staging_conn")])
        cat = MetadataCatalog()
        cat.build([pkg])
        entries = cat.browse(
            tags=[CatalogTag.STAGING.value],
            entry_type=CatalogEntryType.CONNECTION,
            package_name="StgCustomer",
        )
        for e in entries:
            assert e.entry_type == CatalogEntryType.CONNECTION
            assert e.package_name == "StgCustomer"
            assert CatalogTag.STAGING.value in e.tags

    def test_browse_no_results(self):
        pkg = _make_pkg("Pkg1")
        cat = MetadataCatalog()
        cat.build([pkg])
        entries = cat.browse(tags=["NONEXISTENT_TAG_XYZ"])
        assert entries == []


class TestDependencyMatrix:
    def test_no_shared_resources(self):
        pkg1 = _make_pkg("A", connections=[_conn("c1")])
        pkg2 = _make_pkg("B", connections=[_conn("c2")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        assert len(matrix.edges) == 0

    def test_shared_connection(self):
        pkg1 = _make_pkg("A", connections=[_conn("shared_conn")])
        pkg2 = _make_pkg("B", connections=[_conn("shared_conn")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        assert len(matrix.edges) == 1
        assert matrix.edges[0].shared_resource == "shared_conn"
        assert matrix.edges[0].resource_type == "connection"

    def test_shared_table(self):
        src1 = _comp("src1", "OLE_DB_SOURCE", "dbo.SharedTable")
        src2 = _comp("src2", "OLE_DB_SOURCE", "dbo.SharedTable")
        pkg1 = _make_pkg("A", tasks=[_task("dft1", "DATA_FLOW", [src1])])
        pkg2 = _make_pkg("B", tasks=[_task("dft2", "DATA_FLOW", [src2])])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        shared_tables = [e for e in matrix.edges if e.resource_type == "table"]
        assert len(shared_tables) >= 1
        assert any(e.shared_resource == "dbo.SharedTable" for e in shared_tables)

    def test_shared_parameter(self):
        pkg1 = _make_pkg("A", parameters=[_param("ServerName")])
        pkg2 = _make_pkg("B", parameters=[_param("ServerName")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        shared_params = [e for e in matrix.edges if e.resource_type == "parameter"]
        assert len(shared_params) >= 1

    def test_matrix_serialization(self):
        pkg1 = _make_pkg("A", connections=[_conn("c1")])
        pkg2 = _make_pkg("B", connections=[_conn("c1")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        d = matrix.to_dict()
        assert "packages" in d
        assert "edges" in d
        assert "summary" in d
        assert d["summary"]["shared_connections"] >= 1

    def test_matrix_multiple_packages(self):
        pkg1 = _make_pkg("A", connections=[_conn("c1"), _conn("c2")])
        pkg2 = _make_pkg("B", connections=[_conn("c1")])
        pkg3 = _make_pkg("C", connections=[_conn("c2")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2, pkg3])
        matrix = cat.dependency_matrix()
        assert len(matrix.packages) == 3
        # A-B share c1, A-C share c2
        assert len(matrix.edges) >= 2


class TestCatalogExport:
    def test_export_json(self, tmp_path):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        path = cat.write_catalog(tmp_path, fmt=ExportFormat.JSON)
        assert path.exists()
        data = json.loads(path.read_text())
        assert "summary" in data
        assert "entries" in data
        assert "dependency_matrix" in data

    def test_export_purview(self, tmp_path):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        path = cat.write_catalog(tmp_path, fmt=ExportFormat.PURVIEW)
        assert path.exists()
        assert path.name == "purview_catalog.json"
        data = json.loads(path.read_text())
        assert "entities" in data
        # Check entity structure
        for entity in data["entities"]:
            assert "typeName" in entity
            assert "attributes" in entity
            assert "qualifiedName" in entity["attributes"]

    def test_export_data_catalog(self, tmp_path):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        path = cat.write_catalog(tmp_path, fmt=ExportFormat.DATA_CATALOG)
        assert path.exists()
        assert path.name == "data_catalog_export.json"
        data = json.loads(path.read_text())
        assert "assets" in data
        assert "totalAssets" in data

    def test_to_dict(self):
        pkg = _make_pkg("Pkg1")
        cat = MetadataCatalog()
        cat.build([pkg])
        d = cat.to_dict()
        assert "summary" in d
        assert "entries" in d
        assert isinstance(d["entries"], list)


class TestConvenienceFunctions:
    def test_build_catalog(self):
        pkg = _make_pkg("Pkg1")
        cat = build_catalog([pkg])
        assert isinstance(cat, MetadataCatalog)
        assert cat.summary.total_packages == 1

    def test_write_catalog_report(self, tmp_path):
        pkg = _make_pkg("Pkg1")
        cat = build_catalog([pkg])
        path = write_catalog_report(cat, tmp_path)
        assert path.exists()


class TestDataclasses:
    def test_catalog_entry_to_dict(self):
        entry = CatalogEntry(
            entry_id="pkg_0001", entry_type=CatalogEntryType.PACKAGE,
            name="Test", package_name="Test", tags=["ETL"],
            properties={"complexity": "LOW"},
        )
        d = entry.to_dict()
        assert d["entry_id"] == "pkg_0001"
        assert d["entry_type"] == "package"
        assert d["tags"] == ["ETL"]

    def test_search_result_to_dict(self):
        sr = SearchResult(query="test", total_matches=0)
        d = sr.to_dict()
        assert d["query"] == "test"
        assert d["total_matches"] == 0

    def test_dependency_edge_to_dict(self):
        edge = DependencyEdge("A", "B", "dbo.Table", "table")
        d = edge.to_dict()
        assert d["package_a"] == "A"
        assert d["shared_resource"] == "dbo.Table"

    def test_catalog_summary_to_dict(self):
        s = CatalogSummary(total_entries=10, total_packages=2)
        d = s.to_dict()
        assert d["total_entries"] == 10
        assert d["total_packages"] == 2


class TestClassifyPackageTags:
    def test_orchestrator_tag(self):
        from ssis_to_fabric.analyzer.models import TaskType
        tasks = [
            _task("Run A", "EXECUTE_PACKAGE"),
            _task("Run B", "EXECUTE_PACKAGE"),
            _task("Init", "EXECUTE_SQL", sql="SELECT 1"),
        ]
        pkg = _make_pkg("Master", tasks=tasks)
        cat = MetadataCatalog()
        cat.build([pkg])
        pkg_entry = cat.browse(entry_type=CatalogEntryType.PACKAGE)[0]
        assert CatalogTag.ORCHESTRATOR.value in pkg_entry.tags

    def test_etl_tag_from_data_flow(self):
        pkg = _pkg_with_data_flow()
        cat = MetadataCatalog()
        cat.build([pkg])
        pkg_entry = cat.browse(entry_type=CatalogEntryType.PACKAGE)[0]
        assert CatalogTag.ETL.value in pkg_entry.tags

    def test_scd_tag_from_component(self):
        scd_comp = _comp("SCD Transform", "SCD", "dbo.DimCustomer")
        dft = _task("SCD Load", "DATA_FLOW", [scd_comp])
        pkg = _make_pkg("DimCustomer", tasks=[dft])
        cat = MetadataCatalog()
        cat.build([pkg])
        pkg_entry = cat.browse(entry_type=CatalogEntryType.PACKAGE)[0]
        assert CatalogTag.SCD.value in pkg_entry.tags

    def test_lookup_tag_from_component(self):
        lkp = _comp("LKP Customer", "LOOKUP", "dbo.LookupCustomers")
        dft = _task("Transform", "DATA_FLOW", [lkp])
        pkg = _make_pkg("Pkg1", tasks=[dft])
        cat = MetadataCatalog()
        cat.build([pkg])
        pkg_entry = cat.browse(entry_type=CatalogEntryType.PACKAGE)[0]
        assert CatalogTag.LOOKUP.value in pkg_entry.tags


class TestCLI:
    """Test the CLI catalog command with real dtsx examples if available."""

    def test_catalog_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["catalog", "--help"])
        assert result.exit_code == 0
        assert "metadata catalog" in result.output.lower() or "catalog" in result.output.lower()

    def test_catalog_with_examples(self):
        """Run catalog against example packages if available."""
        example_path = Path(__file__).parent.parent / "fixtures"
        if not example_path.exists():
            example_path = Path(__file__).parent.parent.parent / "examples" / "01_simple_copy"
        if not example_path.exists():
            pytest.skip("No example packages available")

        dtsx_files = list(example_path.glob("*.dtsx"))
        if not dtsx_files:
            pytest.skip("No .dtsx files found")

        runner = CliRunner()
        result = runner.invoke(main, ["catalog", str(dtsx_files[0])])
        assert result.exit_code == 0


class TestMultiPackageCatalog:
    """Integration tests with multiple packages."""

    def test_multi_package_summary(self):
        pkgs = [
            _pkg_with_data_flow("StgCustomer"),
            _pkg_with_data_flow("StgProduct"),
            _make_pkg("EP_Master", tasks=[
                _task("Run StgCustomer", "EXECUTE_PACKAGE"),
                _task("Run StgProduct", "EXECUTE_PACKAGE"),
                _task("Init", "EXECUTE_SQL", sql="TRUNCATE TABLE staging.StgCustomer"),
            ]),
        ]
        cat = MetadataCatalog()
        summary = cat.build(pkgs)
        assert summary.total_packages == 3
        assert summary.total_entries > 10

    def test_multi_package_search(self):
        pkgs = [
            _pkg_with_data_flow("StgCustomer"),
            _pkg_with_data_flow("StgProduct"),
        ]
        cat = MetadataCatalog()
        cat.build(pkgs)

        # Search for a specific table
        result = cat.search("dbo.Customers")
        assert result.total_matches >= 1

    def test_multi_package_dependency_matrix(self):
        # Both packages use the same source connection
        src1 = _comp("src1", "OLE_DB_SOURCE", "dbo.Customers", "shared_conn")
        src2 = _comp("src2", "OLE_DB_SOURCE", "dbo.Products", "shared_conn")
        pkg1 = _make_pkg("A",
                         tasks=[_task("dft1", "DATA_FLOW", [src1])],
                         connections=[_conn("shared_conn")])
        pkg2 = _make_pkg("B",
                         tasks=[_task("dft2", "DATA_FLOW", [src2])],
                         connections=[_conn("shared_conn")])
        cat = MetadataCatalog()
        cat.build([pkg1, pkg2])
        matrix = cat.dependency_matrix()
        conn_edges = [e for e in matrix.edges if e.resource_type == "connection"]
        assert len(conn_edges) >= 1

    def test_full_export_round_trip(self, tmp_path):
        pkgs = [_pkg_with_data_flow("A"), _pkg_with_data_flow("B")]
        cat = MetadataCatalog()
        cat.build(pkgs)
        path = cat.write_catalog(tmp_path)
        data = json.loads(path.read_text())
        assert data["summary"]["total_packages"] == 2
        assert len(data["entries"]) == cat.summary.total_entries
