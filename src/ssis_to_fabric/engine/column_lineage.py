"""
Column-Level Lineage Tracking
================================
Extracts column-level data flow from parsed SSIS packages: which source column
feeds which destination column through which transformations.

Builds a directed acyclic graph of column edges with transformation semantics
(DERIVE, JOIN, AGGREGATE, FILTER, PASSTHROUGH) and supports column-level
impact analysis.

Usage::

    from ssis_to_fabric.engine.column_lineage import ColumnLineageGraph

    graph = ColumnLineageGraph()
    graph.build(packages)

    # Column-level impact
    affected = graph.column_impact("dbo.Sales.Amount")

    # Export
    data = graph.to_dict()
    graph.write_json(output_dir)
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.analyzer.models import DataFlowComponentType, TaskType
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import (
        Column,
        ControlFlowTask,
        DataFlowComponent,
        SSISPackage,
    )

logger = get_logger(__name__)


# =====================================================================
# Enums & models
# =====================================================================


class TransformationType(str, Enum):
    """Semantic type of transformation applied to a column."""

    PASSTHROUGH = "PASSTHROUGH"
    DERIVE = "DERIVE"
    JOIN = "JOIN"
    AGGREGATE = "AGGREGATE"
    FILTER = "FILTER"
    LOOKUP = "LOOKUP"
    SORT = "SORT"
    UNION = "UNION"
    PIVOT = "PIVOT"
    UNPIVOT = "UNPIVOT"
    CONVERT = "CONVERT"
    UNKNOWN = "UNKNOWN"


# Map DataFlowComponentType → TransformationType
_COMPONENT_TRANSFORM_MAP: dict[DataFlowComponentType, TransformationType] = {
    DataFlowComponentType.DERIVED_COLUMN: TransformationType.DERIVE,
    DataFlowComponentType.CONDITIONAL_SPLIT: TransformationType.FILTER,
    DataFlowComponentType.LOOKUP: TransformationType.LOOKUP,
    DataFlowComponentType.FUZZY_LOOKUP: TransformationType.LOOKUP,
    DataFlowComponentType.TERM_LOOKUP: TransformationType.LOOKUP,
    DataFlowComponentType.MERGE_JOIN: TransformationType.JOIN,
    DataFlowComponentType.MERGE: TransformationType.UNION,
    DataFlowComponentType.UNION_ALL: TransformationType.UNION,
    DataFlowComponentType.AGGREGATE: TransformationType.AGGREGATE,
    DataFlowComponentType.SORT: TransformationType.SORT,
    DataFlowComponentType.PIVOT: TransformationType.PIVOT,
    DataFlowComponentType.UNPIVOT: TransformationType.UNPIVOT,
    DataFlowComponentType.DATA_CONVERSION: TransformationType.CONVERT,
    DataFlowComponentType.CHARACTER_MAP: TransformationType.CONVERT,
    DataFlowComponentType.MULTICAST: TransformationType.PASSTHROUGH,
    DataFlowComponentType.ROW_COUNT: TransformationType.PASSTHROUGH,
    DataFlowComponentType.COPY_COLUMN: TransformationType.PASSTHROUGH,
}

# Source component types (produce data)
_SOURCE_TYPES = {
    DataFlowComponentType.OLE_DB_SOURCE,
    DataFlowComponentType.ADO_NET_SOURCE,
    DataFlowComponentType.FLAT_FILE_SOURCE,
    DataFlowComponentType.EXCEL_SOURCE,
    DataFlowComponentType.ODBC_SOURCE,
    DataFlowComponentType.XML_SOURCE,
    DataFlowComponentType.RAW_FILE_SOURCE,
    DataFlowComponentType.CDC_SOURCE,
    DataFlowComponentType.SCRIPT_COMPONENT_SOURCE,
}

# Destination component types (consume data)
_DEST_TYPES = {
    DataFlowComponentType.OLE_DB_DESTINATION,
    DataFlowComponentType.ADO_NET_DESTINATION,
    DataFlowComponentType.FLAT_FILE_DESTINATION,
    DataFlowComponentType.EXCEL_DESTINATION,
    DataFlowComponentType.ODBC_DESTINATION,
    DataFlowComponentType.RAW_FILE_DESTINATION,
    DataFlowComponentType.RECORDSET_DESTINATION,
    DataFlowComponentType.SQL_SERVER_DESTINATION,
    DataFlowComponentType.DATA_READER_DESTINATION,
}


@dataclass
class ColumnEdge:
    """A single column-level lineage edge: source_col → dest_col via a transform."""

    source_table: str
    source_column: str
    destination_table: str
    destination_column: str
    transformation: TransformationType
    expression: str = ""
    package_name: str = ""
    task_name: str = ""
    component_name: str = ""

    @property
    def source_fqn(self) -> str:
        return f"{self.source_table}.{self.source_column}"

    @property
    def destination_fqn(self) -> str:
        return f"{self.destination_table}.{self.destination_column}"

    def to_dict(self) -> dict[str, str]:
        return {
            "source_table": self.source_table,
            "source_column": self.source_column,
            "destination_table": self.destination_table,
            "destination_column": self.destination_column,
            "transformation": self.transformation.value,
            "expression": self.expression,
            "package": self.package_name,
            "task": self.task_name,
            "component": self.component_name,
        }


# =====================================================================
# Column Lineage Graph
# =====================================================================


class ColumnLineageGraph:
    """Column-level lineage graph built from parsed SSIS packages."""

    def __init__(self) -> None:
        self.edges: list[ColumnEdge] = []
        # Indexes for quick lookup
        self._by_source: dict[str, list[ColumnEdge]] = defaultdict(list)
        self._by_dest: dict[str, list[ColumnEdge]] = defaultdict(list)
        self._by_package: dict[str, list[ColumnEdge]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, packages: list[SSISPackage]) -> None:
        """Populate the graph from parsed SSIS packages."""
        for pkg in packages:
            self._scan_package(pkg)
        logger.info(
            "column_lineage_built",
            edges=len(self.edges),
            packages=len(packages),
        )

    def _add_edge(self, edge: ColumnEdge) -> None:
        self.edges.append(edge)
        self._by_source[edge.source_fqn].append(edge)
        self._by_dest[edge.destination_fqn].append(edge)
        self._by_package[edge.package_name].append(edge)

    def _scan_package(self, pkg: SSISPackage) -> None:
        """Scan all tasks in a package for column lineage."""
        self._scan_tasks(pkg.name, pkg.control_flow_tasks)
        # Cross-package tracing: map Execute Package parameter bindings
        self._trace_cross_package(pkg)

    def _scan_tasks(self, pkg_name: str, tasks: list[ControlFlowTask]) -> None:
        for task in tasks:
            if task.task_type == TaskType.DATA_FLOW and task.data_flow_components:
                self._extract_data_flow_lineage(pkg_name, task)
            # Recurse into containers
            self._scan_tasks(pkg_name, task.child_tasks)

    def _extract_data_flow_lineage(
        self, pkg_name: str, task: ControlFlowTask,
    ) -> None:
        """Extract column lineage from a data flow task.

        Strategy:
        1. Identify source components and their output columns.
        2. Follow data flow paths to find connected components.
        3. For each destination, map columns back through intermediate transforms.
        """
        components = task.data_flow_components
        paths = task.data_flow_paths

        # Build adjacency: source_component → [dest_component, ...]
        adj: dict[str, list[str]] = defaultdict(list)
        for path in paths:
            adj[path.source_component].append(path.destination_component)

        # Index components by name
        comp_by_name: dict[str, DataFlowComponent] = {c.name: c for c in components}

        # Find sources
        sources = [c for c in components if c.component_type in _SOURCE_TYPES]

        # For each source → trace paths to destinations
        for src in sources:
            src_table = self._component_table(src)
            src_columns = {col.name: col for col in src.columns}

            # BFS from source to all reachable destinations
            visited: set[str] = set()
            queue: list[tuple[str, dict[str, _ColumnState]]] = []

            # Initialize column states from source
            initial_state: dict[str, _ColumnState] = {}
            for col_name, _col in src_columns.items():
                initial_state[col_name] = _ColumnState(
                    current_name=col_name,
                    original_source_table=src_table,
                    original_source_column=col_name,
                    transform=TransformationType.PASSTHROUGH,
                    expression="",
                )
            queue.append((src.name, initial_state))

            while queue:
                comp_name, col_states = queue.pop(0)
                if comp_name in visited:
                    continue
                visited.add(comp_name)

                comp = comp_by_name.get(comp_name)
                if not comp:
                    continue

                # If destination — emit edges
                if comp.component_type in _DEST_TYPES:
                    dest_table = self._component_table(comp)
                    dest_columns = {col.name: col for col in comp.columns}
                    self._emit_dest_edges(
                        col_states, dest_table, dest_columns,
                        pkg_name, task.name, comp.name,
                    )
                    continue

                # If transform — apply transformation to column states
                new_states = self._apply_transform(comp, col_states)

                # Follow paths to next components
                for next_comp in adj.get(comp_name, []):
                    queue.append((next_comp, dict(new_states)))

    def _emit_dest_edges(
        self,
        col_states: dict[str, _ColumnState],
        dest_table: str,
        dest_columns: dict[str, Column],
        pkg_name: str,
        task_name: str,
        comp_name: str,
    ) -> None:
        """Emit edges from tracked column states to destination columns."""
        # Match by name: if dest has column X and we track column X
        for dest_col_name in dest_columns:
            state = col_states.get(dest_col_name)
            if state:
                self._add_edge(ColumnEdge(
                    source_table=state.original_source_table,
                    source_column=state.original_source_column,
                    destination_table=dest_table,
                    destination_column=dest_col_name,
                    transformation=state.transform,
                    expression=state.expression,
                    package_name=pkg_name,
                    task_name=task_name,
                    component_name=comp_name,
                ))
            else:
                # Column exists in destination but not tracked from source
                # Could be a derived column or new column
                for _, s in col_states.items():
                    if s.current_name == dest_col_name:
                        self._add_edge(ColumnEdge(
                            source_table=s.original_source_table,
                            source_column=s.original_source_column,
                            destination_table=dest_table,
                            destination_column=dest_col_name,
                            transformation=s.transform,
                            expression=s.expression,
                            package_name=pkg_name,
                            task_name=task_name,
                            component_name=comp_name,
                        ))
                        break

    def _apply_transform(
        self,
        comp: DataFlowComponent,
        col_states: dict[str, _ColumnState],
    ) -> dict[str, _ColumnState]:
        """Apply a transformation component's effect on column states."""
        transform_type = _COMPONENT_TRANSFORM_MAP.get(
            comp.component_type, TransformationType.UNKNOWN,
        )

        if comp.component_type == DataFlowComponentType.DERIVED_COLUMN:
            return self._apply_derived_column(comp, col_states, transform_type)

        # For most transforms, pass through all columns with updated transform type
        new_states: dict[str, _ColumnState] = {}
        for name, state in col_states.items():
            new_states[name] = _ColumnState(
                current_name=state.current_name,
                original_source_table=state.original_source_table,
                original_source_column=state.original_source_column,
                transform=transform_type if transform_type != TransformationType.UNKNOWN else state.transform,
                expression=state.expression,
            )

        # For Derived Column, add new columns
        if comp.component_type in (
            DataFlowComponentType.AGGREGATE,
            DataFlowComponentType.PIVOT,
            DataFlowComponentType.UNPIVOT,
        ):
            # Add output columns that may be new
            for col in comp.columns:
                if col.name not in new_states:
                    # Try to find source from expression or source_column
                    src_col = col.source_column or col.name
                    src_state = col_states.get(src_col)
                    new_states[col.name] = _ColumnState(
                        current_name=col.name,
                        original_source_table=src_state.original_source_table if src_state else "",
                        original_source_column=src_state.original_source_column if src_state else src_col,
                        transform=transform_type,
                        expression=col.expression,
                    )

        return new_states

    def _apply_derived_column(
        self,
        comp: DataFlowComponent,
        col_states: dict[str, _ColumnState],
        transform_type: TransformationType,
    ) -> dict[str, _ColumnState]:
        """Handle Derived Column: new columns or replaced columns."""
        new_states = dict(col_states)
        for col in comp.columns:
            # Extract referenced source columns from expression
            referenced = _extract_column_refs(col.expression) if col.expression else []
            src_state = None
            if referenced:
                src_state = col_states.get(referenced[0])
            elif col.source_column:
                src_state = col_states.get(col.source_column)
            elif col.name in col_states:
                # Replacing existing column
                src_state = col_states[col.name]

            new_states[col.name] = _ColumnState(
                current_name=col.name,
                original_source_table=src_state.original_source_table if src_state else "",
                original_source_column=src_state.original_source_column if src_state else col.name,
                transform=transform_type,
                expression=col.expression,
            )
        return new_states

    def _trace_cross_package(self, pkg: SSISPackage) -> None:
        """Trace column lineage across Execute Package tasks via parameter bindings."""
        for task in pkg.control_flow_tasks:
            if task.task_type == TaskType.EXECUTE_PACKAGE and task.parameter_bindings:
                for child_param, parent_expr in task.parameter_bindings.items():
                    # Create cross-package edges for parameter-bound columns
                    self._add_edge(ColumnEdge(
                        source_table=f"@{pkg.name}",
                        source_column=parent_expr,
                        destination_table=f"@{task.name}",
                        destination_column=child_param,
                        transformation=TransformationType.PASSTHROUGH,
                        package_name=pkg.name,
                        task_name=task.name,
                        component_name="ExecutePackage",
                    ))

    @staticmethod
    def _component_table(comp: DataFlowComponent) -> str:
        """Get the table name for a source or destination component."""
        raw = comp.table_name or comp.properties.get("tableName", "") or comp.properties.get("OpenRowset", "")
        if not raw:
            return comp.name
        return _normalise_table(str(raw))

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def column_impact(self, column_fqn: str) -> dict[str, list[dict[str, str]]]:
        """Impact analysis for a fully-qualified column (schema.table.column).

        Returns columns that are downstream (fed by this column) and
        upstream (feed into this column).
        """
        downstream: list[dict[str, str]] = []
        upstream: list[dict[str, str]] = []

        # Direct downstream: edges where this column is the source
        visited_down: set[str] = set()
        queue = [column_fqn]
        while queue:
            fqn = queue.pop(0)
            if fqn in visited_down:
                continue
            visited_down.add(fqn)
            for edge in self._by_source.get(fqn, []):
                downstream.append(edge.to_dict())
                queue.append(edge.destination_fqn)

        # Direct upstream: edges where this column is the destination
        visited_up: set[str] = set()
        queue = [column_fqn]
        while queue:
            fqn = queue.pop(0)
            if fqn in visited_up:
                continue
            visited_up.add(fqn)
            for edge in self._by_dest.get(fqn, []):
                upstream.append(edge.to_dict())
                queue.append(edge.source_fqn)

        return {"downstream": downstream, "upstream": upstream}

    def all_columns(self) -> set[str]:
        """Return all unique fully-qualified column names."""
        cols: set[str] = set()
        for edge in self.edges:
            cols.add(edge.source_fqn)
            cols.add(edge.destination_fqn)
        return cols

    def package_edges(self, package_name: str) -> list[ColumnEdge]:
        """Return all column edges for a given package."""
        return self._by_package.get(package_name, [])

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialisable representation of the column lineage graph."""
        return {
            "column_edges": [e.to_dict() for e in self.edges],
            "columns": sorted(self.all_columns()),
            "summary": {
                "total_edges": len(self.edges),
                "total_columns": len(self.all_columns()),
                "transformations": dict(self._transformation_counts()),
            },
        }

    def _transformation_counts(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for edge in self.edges:
            counts[edge.transformation.value] += 1
        return dict(sorted(counts.items()))

    def write_json(self, output_dir: Path) -> Path:
        """Write column lineage to ``output_dir/column_lineage.json``."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "column_lineage.json"
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        logger.info("column_lineage_written", path=str(path))
        return path

    def to_sankey_data(self) -> dict[str, Any]:
        """Generate Sankey diagram data (nodes + links) for D3.js visualization.

        Returns a dict with ``nodes`` (list of ``{name}``) and ``links``
        (list of ``{source, target, value, transform}``).
        """
        node_set: dict[str, int] = {}
        links: list[dict[str, Any]] = []

        def _node_idx(name: str) -> int:
            if name not in node_set:
                node_set[name] = len(node_set)
            return node_set[name]

        for edge in self.edges:
            src_idx = _node_idx(edge.source_fqn)
            dst_idx = _node_idx(edge.destination_fqn)
            links.append({
                "source": src_idx,
                "target": dst_idx,
                "value": 1,
                "transform": edge.transformation.value,
            })

        nodes = [{"name": name} for name in node_set]
        return {"nodes": nodes, "links": links}

    def to_mermaid(self) -> str:
        """Export column lineage as a Mermaid flowchart."""
        lines = ["flowchart LR"]
        node_ids: dict[str, str] = {}
        counter = [0]

        def _node(fqn: str) -> str:
            if fqn not in node_ids:
                counter[0] += 1
                node_ids[fqn] = f"c{counter[0]}"
            return node_ids[fqn]

        for edge in self.edges:
            src_id = _node(edge.source_fqn)
            dst_id = _node(edge.destination_fqn)
            safe_src = _mermaid_safe(edge.source_fqn)
            safe_dst = _mermaid_safe(edge.destination_fqn)
            lines.append(f'    {src_id}["{safe_src}"]')
            lines.append(f'    {dst_id}["{safe_dst}"]')
            lines.append(f"    {src_id} -->|{edge.transformation.value}| {dst_id}")
        return "\n".join(lines)


# =====================================================================
# Internal helpers
# =====================================================================


@dataclass
class _ColumnState:
    """Tracks a column as it flows through a data flow pipeline."""

    current_name: str
    original_source_table: str
    original_source_column: str
    transform: TransformationType
    expression: str = ""


def _normalise_table(raw: str) -> str:
    clean = raw.strip().strip('"').strip("'").strip("[").strip("]")
    clean = re.sub(r"\[([^\]]+)\]", r"\1", clean)
    return clean


def _extract_column_refs(expression: str) -> list[str]:
    """Extract column name references from an SSIS expression string."""
    # Match [ColumnName] or plain identifiers after column-related patterns
    refs: list[str] = []
    for m in re.finditer(r"\[([^\]]+)\]", expression):
        refs.append(m.group(1))
    return refs


def _mermaid_safe(s: str) -> str:
    return re.sub(r'["\[\]{}|]', "_", s)
