"""Phase 11 — Column-Level Lineage tests.

Covers:
- TransformationType enum values
- ColumnEdge data model
- ColumnLineageGraph build from packages
- Column flow through source → transform → destination
- Derived Column transformation tracking
- Column-level impact analysis (upstream + downstream)
- Cross-package parameter tracing
- Sankey data export (nodes + links)
- Mermaid export
- JSON export
- CLI --column option
- Report generator Sankey section
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from ssis_to_fabric.analyzer.models import (
    Column,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    DataFlowPath,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.engine.column_lineage import (
    ColumnEdge,
    ColumnLineageGraph,
    TransformationType,
)

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(
    name: str = "OLE_Source",
    table: str = "dbo.Sales",
    columns: list[str] | None = None,
) -> DataFlowComponent:
    cols = [Column(name=c) for c in (columns or ["ID", "Amount", "Date"])]
    return DataFlowComponent(
        name=name,
        component_type=DataFlowComponentType.OLE_DB_SOURCE,
        table_name=table,
        columns=cols,
    )


def _make_dest(name: str = "OLE_Dest", table: str = "stg.Sales", columns: list[str] | None = None) -> DataFlowComponent:
    cols = [Column(name=c) for c in (columns or ["ID", "Amount", "Date"])]
    return DataFlowComponent(
        name=name,
        component_type=DataFlowComponentType.OLE_DB_DESTINATION,
        table_name=table,
        columns=cols,
    )


def _make_derived(name: str = "Derive", columns: list[Column] | None = None) -> DataFlowComponent:
    return DataFlowComponent(
        name=name,
        component_type=DataFlowComponentType.DERIVED_COLUMN,
        columns=columns or [],
    )


def _make_lookup(name: str = "Lookup") -> DataFlowComponent:
    return DataFlowComponent(
        name=name,
        component_type=DataFlowComponentType.LOOKUP,
        columns=[Column(name="LookupValue")],
    )


def _simple_df_task(
    task_name: str = "DFT",
    components: list[DataFlowComponent] | None = None,
    paths: list[DataFlowPath] | None = None,
) -> ControlFlowTask:
    return ControlFlowTask(
        name=task_name,
        task_type=TaskType.DATA_FLOW,
        data_flow_components=components or [],
        data_flow_paths=paths or [],
    )


def _simple_package(
    name: str = "TestPkg",
    tasks: list[ControlFlowTask] | None = None,
) -> SSISPackage:
    return SSISPackage(name=name, control_flow_tasks=tasks or [])


# =====================================================================
# Enum tests
# =====================================================================


class TestTransformationType:
    @pytest.mark.unit
    def test_passthrough(self) -> None:
        assert TransformationType.PASSTHROUGH.value == "PASSTHROUGH"

    @pytest.mark.unit
    def test_derive(self) -> None:
        assert TransformationType.DERIVE.value == "DERIVE"

    @pytest.mark.unit
    def test_join(self) -> None:
        assert TransformationType.JOIN.value == "JOIN"

    @pytest.mark.unit
    def test_aggregate(self) -> None:
        assert TransformationType.AGGREGATE.value == "AGGREGATE"

    @pytest.mark.unit
    def test_filter(self) -> None:
        assert TransformationType.FILTER.value == "FILTER"

    @pytest.mark.unit
    def test_lookup(self) -> None:
        assert TransformationType.LOOKUP.value == "LOOKUP"


# =====================================================================
# ColumnEdge tests
# =====================================================================


class TestColumnEdge:
    @pytest.mark.unit
    def test_fqn(self) -> None:
        edge = ColumnEdge(
            source_table="dbo.Sales",
            source_column="Amount",
            destination_table="stg.Sales",
            destination_column="Amount",
            transformation=TransformationType.PASSTHROUGH,
        )
        assert edge.source_fqn == "dbo.Sales.Amount"
        assert edge.destination_fqn == "stg.Sales.Amount"

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        edge = ColumnEdge(
            source_table="dbo.Sales",
            source_column="Amount",
            destination_table="stg.Sales",
            destination_column="Amount",
            transformation=TransformationType.DERIVE,
            expression="Amount * 1.1",
            package_name="Pkg1",
        )
        d = edge.to_dict()
        assert d["transformation"] == "DERIVE"
        assert d["expression"] == "Amount * 1.1"
        assert d["package"] == "Pkg1"


# =====================================================================
# ColumnLineageGraph build tests
# =====================================================================


class TestGraphBuild:
    @pytest.mark.unit
    def test_simple_source_to_dest(self) -> None:
        """Source → Destination: columns pass through."""
        src = _make_source(columns=["ID", "Name"])
        dst = _make_dest(columns=["ID", "Name"])
        task = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        pkg = _simple_package(tasks=[task])

        graph = ColumnLineageGraph()
        graph.build([pkg])

        assert len(graph.edges) == 2
        col_names = {e.destination_column for e in graph.edges}
        assert "ID" in col_names
        assert "Name" in col_names

    @pytest.mark.unit
    def test_source_columns_are_passthrough(self) -> None:
        src = _make_source(columns=["A"])
        dst = _make_dest(columns=["A"])
        task = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])
        assert graph.edges[0].transformation == TransformationType.PASSTHROUGH

    @pytest.mark.unit
    def test_derived_column_tracked(self) -> None:
        """Source → Derived Column → Destination: derived columns marked DERIVE."""
        src = _make_source(columns=["Price", "Qty"])
        derived = _make_derived(columns=[
            Column(name="Total", expression="[Price] * [Qty]"),
        ])
        dst = _make_dest(columns=["Price", "Qty", "Total"])
        task = _simple_df_task(
            components=[src, derived, dst],
            paths=[
                DataFlowPath(source_component="OLE_Source", destination_component="Derive"),
                DataFlowPath(source_component="Derive", destination_component="OLE_Dest"),
            ],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])

        total_edges = [e for e in graph.edges if e.destination_column == "Total"]
        assert len(total_edges) >= 1
        assert total_edges[0].transformation == TransformationType.DERIVE
        assert "Price" in total_edges[0].expression

    @pytest.mark.unit
    def test_lookup_tracked(self) -> None:
        """Lookup transform changes type to LOOKUP."""
        src = _make_source(columns=["ID"])
        lookup = _make_lookup()
        dst = _make_dest(columns=["ID", "LookupValue"])
        task = _simple_df_task(
            components=[src, lookup, dst],
            paths=[
                DataFlowPath(source_component="OLE_Source", destination_component="Lookup"),
                DataFlowPath(source_component="Lookup", destination_component="OLE_Dest"),
            ],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])

        id_edges = [e for e in graph.edges if e.destination_column == "ID"]
        assert len(id_edges) >= 1
        assert id_edges[0].transformation == TransformationType.LOOKUP

    @pytest.mark.unit
    def test_no_data_flow_no_edges(self) -> None:
        """Non-data-flow tasks produce no column edges."""
        task = ControlFlowTask(name="SQL", task_type=TaskType.EXECUTE_SQL)
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])
        assert len(graph.edges) == 0

    @pytest.mark.unit
    def test_multiple_packages(self) -> None:
        src1 = _make_source(table="A", columns=["X"])
        dst1 = _make_dest(table="B", columns=["X"])
        t1 = _simple_df_task(
            components=[src1, dst1],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )

        src2 = _make_source(table="B", columns=["X"])
        dst2 = _make_dest(table="C", columns=["X"])
        t2 = _simple_df_task(
            components=[src2, dst2],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )

        graph = ColumnLineageGraph()
        graph.build([_simple_package("P1", [t1]), _simple_package("P2", [t2])])
        assert len(graph.edges) == 2

    @pytest.mark.unit
    def test_nested_container_tasks(self) -> None:
        """Tasks inside containers are scanned."""
        src = _make_source(columns=["Z"])
        dst = _make_dest(columns=["Z"])
        inner = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        container = ControlFlowTask(
            name="Seq",
            task_type=TaskType.SEQUENCE_CONTAINER,
            child_tasks=[inner],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[container])])
        assert len(graph.edges) == 1


# =====================================================================
# Cross-package tracing
# =====================================================================


class TestCrossPackageTracing:
    @pytest.mark.unit
    def test_execute_package_parameter_bindings(self) -> None:
        exec_pkg = ControlFlowTask(
            name="RunChild",
            task_type=TaskType.EXECUTE_PACKAGE,
            parameter_bindings={"ChildParam1": "@[User::ParentVar]"},
        )
        pkg = _simple_package(tasks=[exec_pkg])
        graph = ColumnLineageGraph()
        graph.build([pkg])

        assert len(graph.edges) == 1
        edge = graph.edges[0]
        assert edge.source_column == "@[User::ParentVar]"
        assert edge.destination_column == "ChildParam1"
        assert edge.transformation == TransformationType.PASSTHROUGH


# =====================================================================
# Impact analysis
# =====================================================================


class TestColumnImpact:
    @pytest.mark.unit
    def test_downstream_impact(self) -> None:
        src = _make_source(table="dbo.Sales", columns=["Amt"])
        dst = _make_dest(table="stg.Sales", columns=["Amt"])
        task = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])

        impact = graph.column_impact("dbo.Sales.Amt")
        assert len(impact["downstream"]) >= 1
        assert impact["downstream"][0]["destination_column"] == "Amt"

    @pytest.mark.unit
    def test_upstream_impact(self) -> None:
        src = _make_source(table="dbo.Sales", columns=["Amt"])
        dst = _make_dest(table="stg.Sales", columns=["Amt"])
        task = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])

        impact = graph.column_impact("stg.Sales.Amt")
        assert len(impact["upstream"]) >= 1
        assert impact["upstream"][0]["source_column"] == "Amt"

    @pytest.mark.unit
    def test_no_impact(self) -> None:
        graph = ColumnLineageGraph()
        impact = graph.column_impact("nonexistent.table.col")
        assert impact["downstream"] == []
        assert impact["upstream"] == []

    @pytest.mark.unit
    def test_transitive_downstream(self) -> None:
        """A→B→C: impact on A.X should transitively include C.X."""
        src1 = _make_source(name="Src1", table="A", columns=["X"])
        dst1 = _make_dest(name="Dst1", table="B", columns=["X"])
        t1 = _simple_df_task(
            task_name="DFT1",
            components=[src1, dst1],
            paths=[DataFlowPath(source_component="Src1", destination_component="Dst1")],
        )

        src2 = _make_source(name="Src2", table="B", columns=["X"])
        dst2 = _make_dest(name="Dst2", table="C", columns=["X"])
        t2 = _simple_df_task(
            task_name="DFT2",
            components=[src2, dst2],
            paths=[DataFlowPath(source_component="Src2", destination_component="Dst2")],
        )

        graph = ColumnLineageGraph()
        graph.build([_simple_package("P1", [t1]), _simple_package("P2", [t2])])

        impact = graph.column_impact("A.X")
        dest_cols = {e["destination_column"] for e in impact["downstream"]}
        assert "X" in dest_cols


# =====================================================================
# Export tests
# =====================================================================


class TestExport:
    def _build_simple_graph(self) -> ColumnLineageGraph:
        src = _make_source(columns=["A", "B"])
        dst = _make_dest(columns=["A", "B"])
        task = _simple_df_task(
            components=[src, dst],
            paths=[DataFlowPath(source_component="OLE_Source", destination_component="OLE_Dest")],
        )
        graph = ColumnLineageGraph()
        graph.build([_simple_package(tasks=[task])])
        return graph

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        graph = self._build_simple_graph()
        d = graph.to_dict()
        assert "column_edges" in d
        assert "columns" in d
        assert "summary" in d
        assert d["summary"]["total_edges"] == 2

    @pytest.mark.unit
    def test_sankey_data(self) -> None:
        graph = self._build_simple_graph()
        sankey = graph.to_sankey_data()
        assert "nodes" in sankey
        assert "links" in sankey
        assert len(sankey["nodes"]) == 4  # 2 source cols + 2 dest cols
        assert len(sankey["links"]) == 2

    @pytest.mark.unit
    def test_sankey_link_has_transform(self) -> None:
        graph = self._build_simple_graph()
        sankey = graph.to_sankey_data()
        for link in sankey["links"]:
            assert "transform" in link

    @pytest.mark.unit
    def test_write_json(self, tmp_path: Path) -> None:
        graph = self._build_simple_graph()
        path = graph.write_json(tmp_path)
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["summary"]["total_edges"] == 2

    @pytest.mark.unit
    def test_to_mermaid(self) -> None:
        graph = self._build_simple_graph()
        mermaid = graph.to_mermaid()
        assert "flowchart LR" in mermaid
        assert "PASSTHROUGH" in mermaid

    @pytest.mark.unit
    def test_all_columns(self) -> None:
        graph = self._build_simple_graph()
        cols = graph.all_columns()
        assert "dbo.Sales.A" in cols
        assert "stg.Sales.B" in cols

    @pytest.mark.unit
    def test_package_edges(self) -> None:
        graph = self._build_simple_graph()
        edges = graph.package_edges("TestPkg")
        assert len(edges) == 2
        assert graph.package_edges("NonExistent") == []


# =====================================================================
# Report generator integration
# =====================================================================


class TestReportSankeySection:
    @pytest.mark.unit
    def test_sankey_rendered_when_data_present(self) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        report: dict[str, Any] = {
            "project_name": "Test",
            "strategy": "hybrid",
            "items": [],
            "errors": [],
            "summary": {},
            "column_lineage": {
                "sankey": {
                    "nodes": [{"name": "a"}, {"name": "b"}],
                    "links": [{"source": 0, "target": 1, "value": 1, "transform": "PASSTHROUGH"}],
                },
                "summary": {"total_edges": 1, "total_columns": 2},
            },
        }
        section = gen._render_column_lineage(report)
        assert "d3-sankey" in section
        assert "Column-Level Lineage" in section

    @pytest.mark.unit
    def test_sankey_empty_when_no_data(self) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        report: dict[str, Any] = {
            "project_name": "Test",
            "items": [],
            "errors": [],
            "summary": {},
        }
        assert gen._render_column_lineage(report) == ""

    @pytest.mark.unit
    def test_full_report_includes_sankey(self) -> None:
        from ssis_to_fabric.engine.report_generator import ReportGenerator

        gen = ReportGenerator()
        report: dict[str, Any] = {
            "project_name": "Test",
            "strategy": "hybrid",
            "items": [],
            "errors": [],
            "summary": {},
            "column_lineage": {
                "sankey": {
                    "nodes": [{"name": "x"}, {"name": "y"}],
                    "links": [{"source": 0, "target": 1, "value": 1, "transform": "DERIVE"}],
                },
                "summary": {"total_edges": 1, "total_columns": 2},
            },
        }
        html = gen._render(report)
        assert "d3-sankey" in html
        assert "Column-Level Lineage" in html


# =====================================================================
# CLI --column option
# =====================================================================


class TestCLIColumnOption:
    @pytest.mark.unit
    def test_column_option_exists(self) -> None:
        from ssis_to_fabric.cli import lineage

        # Verify the --column option is registered
        param_names = [p.name for p in lineage.params]
        assert "column" in param_names

    @pytest.mark.unit
    def test_column_help_text(self) -> None:
        from ssis_to_fabric.cli import lineage

        col_param = next(p for p in lineage.params if p.name == "column")
        assert "column-level" in (col_param.help or "").lower()
