"""
Unit tests for the data lineage graph & impact analysis module.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.lineage import (
    LineageBuilder,
    LineageEdge,
    LineageEdgeType,
    LineageGraph,
    LineageNode,
    LineageNodeType,
    _extract_tables_from_sql,
)
from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    DataFlowPath,
    PrecedenceConstraint,
    SSISPackage,
    TaskType,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


# =============================================================================
# LineageGraph unit tests
# =============================================================================


class TestLineageGraph:
    """Tests for LineageGraph traversal, export, and helpers."""

    @pytest.fixture
    def simple_graph(self) -> LineageGraph:
        """A→B→C linear graph for traversal tests."""
        graph = LineageGraph()
        graph.add_node(LineageNode(id="A", name="Source", node_type=LineageNodeType.TABLE))
        graph.add_node(LineageNode(id="B", name="Transform", node_type=LineageNodeType.TRANSFORMATION))
        graph.add_node(LineageNode(id="C", name="Dest", node_type=LineageNodeType.TABLE))
        graph.add_edge(
            LineageEdge(source_id="A", target_id="B", edge_type=LineageEdgeType.DATA_FLOW)
        )
        graph.add_edge(
            LineageEdge(source_id="B", target_id="C", edge_type=LineageEdgeType.DATA_FLOW)
        )
        return graph

    @pytest.mark.unit
    def test_add_node_dedup(self) -> None:
        graph = LineageGraph()
        node = LineageNode(id="X", name="X", node_type=LineageNodeType.TABLE)
        graph.add_node(node)
        graph.add_node(node)
        assert len(graph.nodes) == 1

    @pytest.mark.unit
    def test_add_edge_dedup(self) -> None:
        graph = LineageGraph()
        graph.add_node(LineageNode(id="A", name="A", node_type=LineageNodeType.TABLE))
        graph.add_node(LineageNode(id="B", name="B", node_type=LineageNodeType.TABLE))
        edge = LineageEdge(source_id="A", target_id="B", edge_type=LineageEdgeType.DATA_FLOW)
        graph.add_edge(edge)
        graph.add_edge(edge)
        assert len(graph.edges) == 1

    @pytest.mark.unit
    def test_downstream_traversal(self, simple_graph: LineageGraph) -> None:
        downstream = simple_graph.get_downstream("A")
        names = {n.name for n in downstream}
        assert "Transform" in names
        assert "Dest" in names

    @pytest.mark.unit
    def test_upstream_traversal(self, simple_graph: LineageGraph) -> None:
        upstream = simple_graph.get_upstream("C")
        names = {n.name for n in upstream}
        assert "Transform" in names
        assert "Source" in names

    @pytest.mark.unit
    def test_downstream_from_leaf(self, simple_graph: LineageGraph) -> None:
        downstream = simple_graph.get_downstream("C")
        assert len(downstream) == 0

    @pytest.mark.unit
    def test_upstream_from_root(self, simple_graph: LineageGraph) -> None:
        upstream = simple_graph.get_upstream("A")
        assert len(upstream) == 0

    @pytest.mark.unit
    def test_find_node(self, simple_graph: LineageGraph) -> None:
        node = simple_graph.find_node("source")
        assert node is not None
        assert node.id == "A"

    @pytest.mark.unit
    def test_find_node_not_found(self, simple_graph: LineageGraph) -> None:
        assert simple_graph.find_node("nonexistent") is None

    @pytest.mark.unit
    def test_to_mermaid_output(self, simple_graph: LineageGraph) -> None:
        mermaid = simple_graph.to_mermaid()
        assert mermaid.startswith("graph LR")
        assert "A" in mermaid
        assert "B" in mermaid
        assert "C" in mermaid
        assert "-->" in mermaid

    @pytest.mark.unit
    def test_to_d3_json_output(self, simple_graph: LineageGraph) -> None:
        import json

        d3 = simple_graph.to_d3_json()
        data = json.loads(d3)
        assert "nodes" in data
        assert "links" in data
        assert len(data["nodes"]) == 3
        assert len(data["links"]) == 2

    @pytest.mark.unit
    def test_mermaid_table_shape(self) -> None:
        graph = LineageGraph()
        graph.add_node(LineageNode(id="t1", name="MyTable", node_type=LineageNodeType.TABLE))
        mermaid = graph.to_mermaid()
        # Tables use stadium shape: id[("label")]
        assert '("MyTable")' in mermaid

    @pytest.mark.unit
    def test_mermaid_edge_label(self) -> None:
        graph = LineageGraph()
        graph.add_node(LineageNode(id="A", name="A", node_type=LineageNodeType.TABLE))
        graph.add_node(LineageNode(id="B", name="B", node_type=LineageNodeType.TABLE))
        graph.add_edge(
            LineageEdge(
                source_id="A", target_id="B", edge_type=LineageEdgeType.DATA_FLOW, label="read"
            )
        )
        mermaid = graph.to_mermaid()
        assert "-->|read|" in mermaid


# =============================================================================
# SQL extraction tests
# =============================================================================


class TestSqlExtraction:
    """Tests for the _extract_tables_from_sql helper."""

    @pytest.mark.unit
    def test_select_from(self) -> None:
        tables = _extract_tables_from_sql("SELECT * FROM dbo.Customers")
        assert ("dbo.Customers", "source") in tables

    @pytest.mark.unit
    def test_insert_into(self) -> None:
        tables = _extract_tables_from_sql("INSERT INTO Staging.Orders SELECT * FROM Sales.Orders")
        assert ("Staging.Orders", "target") in tables
        assert ("Sales.Orders", "source") in tables

    @pytest.mark.unit
    def test_truncate_table(self) -> None:
        tables = _extract_tables_from_sql("TRUNCATE TABLE [dbo].[StagingTable]")
        assert ("dbo.StagingTable", "target") in tables

    @pytest.mark.unit
    def test_update_statement(self) -> None:
        tables = _extract_tables_from_sql("UPDATE dbo.Products SET Price = 0")
        assert ("dbo.Products", "target") in tables

    @pytest.mark.unit
    def test_delete_statement(self) -> None:
        tables = _extract_tables_from_sql("DELETE FROM dbo.Logs WHERE Date < '2020-01-01'")
        assert ("dbo.Logs", "target") in tables

    @pytest.mark.unit
    def test_join(self) -> None:
        sql = "SELECT * FROM Orders o JOIN Customers c ON o.CustId = c.Id"
        tables = _extract_tables_from_sql(sql)
        assert ("Orders", "source") in tables
        assert ("Customers", "source") in tables

    @pytest.mark.unit
    def test_exec_procedure(self) -> None:
        tables = _extract_tables_from_sql("EXEC dbo.sp_AuditLog @status='done'")
        assert ("dbo.sp_AuditLog", "target") in tables

    @pytest.mark.unit
    def test_empty_sql(self) -> None:
        assert _extract_tables_from_sql("") == []


# =============================================================================
# LineageBuilder with SSISPackage models
# =============================================================================


class TestLineageBuilder:
    """Tests for building lineage from SSISPackage models."""

    @pytest.fixture
    def simple_package(self) -> SSISPackage:
        """A minimal SSISPackage with one data flow and one SQL task."""
        return SSISPackage(
            name="TestPkg",
            control_flow_tasks=[
                ControlFlowTask(
                    name="Truncate Target",
                    task_type=TaskType.EXECUTE_SQL,
                    sql_statement="TRUNCATE TABLE dbo.Target",
                ),
                ControlFlowTask(
                    name="Load Data",
                    task_type=TaskType.DATA_FLOW,
                    data_flow_components=[
                        DataFlowComponent(
                            name="Source Query",
                            component_type=DataFlowComponentType.OLE_DB_SOURCE,
                            table_name="dbo.SourceTable",
                        ),
                        DataFlowComponent(
                            name="Dest Table",
                            component_type=DataFlowComponentType.OLE_DB_DESTINATION,
                            table_name="dbo.Target",
                        ),
                    ],
                    data_flow_paths=[
                        DataFlowPath(
                            source_component="Source Query",
                            destination_component="Dest Table",
                        ),
                    ],
                ),
            ],
            precedence_constraints=[
                PrecedenceConstraint(
                    source_task="Truncate Target",
                    destination_task="Load Data",
                ),
            ],
        )

    @pytest.mark.unit
    def test_build_creates_package_node(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        pkg_node = next((n for n in graph.nodes if n.node_type == LineageNodeType.PACKAGE), None)
        assert pkg_node is not None
        assert pkg_node.name == "TestPkg"

    @pytest.mark.unit
    def test_build_creates_task_nodes(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        task_nodes = [n for n in graph.nodes if n.node_type == LineageNodeType.TASK]
        names = {n.name for n in task_nodes}
        assert "Truncate Target" in names
        assert "Load Data" in names

    @pytest.mark.unit
    def test_build_creates_table_nodes(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        table_nodes = [n for n in graph.nodes if n.node_type == LineageNodeType.TABLE]
        names = {n.name for n in table_nodes}
        assert "dbo.SourceTable" in names
        assert "dbo.Target" in names

    @pytest.mark.unit
    def test_build_creates_precedence_edge(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        prec_edges = [e for e in graph.edges if e.edge_type == LineageEdgeType.PRECEDENCE]
        assert len(prec_edges) >= 1

    @pytest.mark.unit
    def test_build_creates_data_flow_edges(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        df_edges = [e for e in graph.edges if e.edge_type == LineageEdgeType.DATA_FLOW]
        assert len(df_edges) >= 2

    @pytest.mark.unit
    def test_impact_downstream_from_source(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        source_node = graph.find_node("SourceTable")
        assert source_node is not None
        downstream = graph.get_downstream(source_node.id)
        downstream_names = {n.name for n in downstream}
        assert "dbo.Target" in downstream_names

    @pytest.mark.unit
    def test_impact_upstream_from_dest(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        dest_node = graph.find_node("dbo.Target")
        assert dest_node is not None
        upstream = graph.get_upstream(dest_node.id)
        # dbo.SourceTable should be upstream (through the task node)
        upstream_names = {n.name for n in upstream}
        assert "dbo.SourceTable" in upstream_names

    @pytest.mark.unit
    def test_mermaid_export(self, simple_package: SSISPackage) -> None:
        graph = LineageBuilder().build([simple_package])
        mermaid = graph.to_mermaid()
        assert "graph LR" in mermaid
        assert "SourceTable" in mermaid

    @pytest.mark.unit
    def test_d3_export(self, simple_package: SSISPackage) -> None:
        import json

        graph = LineageBuilder().build([simple_package])
        d3 = graph.to_d3_json()
        data = json.loads(d3)
        assert len(data["nodes"]) > 0
        assert len(data["links"]) > 0

    @pytest.mark.unit
    def test_multiple_packages(self) -> None:
        pkg1 = SSISPackage(
            name="PkgA",
            control_flow_tasks=[
                ControlFlowTask(
                    name="Load",
                    task_type=TaskType.EXECUTE_SQL,
                    sql_statement="INSERT INTO Staging SELECT * FROM Raw",
                ),
            ],
        )
        pkg2 = SSISPackage(
            name="PkgB",
            control_flow_tasks=[
                ControlFlowTask(
                    name="Transform",
                    task_type=TaskType.EXECUTE_SQL,
                    sql_statement="INSERT INTO Final SELECT * FROM Staging",
                ),
            ],
        )
        graph = LineageBuilder().build([pkg1, pkg2])
        pkg_nodes = [n for n in graph.nodes if n.node_type == LineageNodeType.PACKAGE]
        assert len(pkg_nodes) == 2

        # Staging is shared between the two packages
        staging_node = graph.find_node("Staging")
        assert staging_node is not None


# =============================================================================
# Integration with real fixture files
# =============================================================================


class TestLineageWithFixtures:
    """Build lineage from the actual sample .dtsx fixture files."""

    @pytest.fixture
    def packages(self) -> list[SSISPackage]:
        parser = DTSXParser()
        return parser.parse_directory(FIXTURES_DIR)

    @pytest.mark.unit
    def test_build_from_fixtures(self, packages: list[SSISPackage]) -> None:
        graph = LineageBuilder().build(packages)
        assert len(graph.nodes) > 0
        assert len(graph.edges) > 0

    @pytest.mark.unit
    def test_mermaid_export_from_fixtures(self, packages: list[SSISPackage]) -> None:
        graph = LineageBuilder().build(packages)
        mermaid = graph.to_mermaid()
        assert mermaid.startswith("graph LR")
        assert "-->" in mermaid

    @pytest.mark.unit
    def test_d3_export_from_fixtures(self, packages: list[SSISPackage]) -> None:
        import json

        graph = LineageBuilder().build(packages)
        d3 = graph.to_d3_json()
        data = json.loads(d3)
        assert "nodes" in data
        assert "links" in data
        assert len(data["nodes"]) > 0

    @pytest.mark.unit
    def test_simple_etl_lineage(self) -> None:
        parser = DTSXParser()
        pkg = parser.parse(FIXTURES_DIR / "simple_etl.dtsx")
        graph = LineageBuilder().build([pkg])
        # The simple ETL has source/dest tables and SQL tasks
        table_nodes = [n for n in graph.nodes if n.node_type == LineageNodeType.TABLE]
        assert len(table_nodes) >= 1
