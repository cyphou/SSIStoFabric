"""
Data Lineage Graph & Impact Analysis
======================================
Build a lineage graph from parsed SSIS models, traverse upstream/downstream
for impact analysis, and export to Mermaid or D3.js JSON.
"""

from __future__ import annotations

import json
import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
    TaskType,
)

# =============================================================================
# Enumerations
# =============================================================================

# Source component types (read data from external systems)
_SOURCE_TYPES: frozenset[DataFlowComponentType] = frozenset(
    {
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
)

# Destination component types (write data to external systems)
_DESTINATION_TYPES: frozenset[DataFlowComponentType] = frozenset(
    {
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
)


class LineageNodeType(str, Enum):
    """Type of node in the lineage graph."""

    TABLE = "table"
    FILE = "file"
    TRANSFORMATION = "transformation"
    TASK = "task"
    PACKAGE = "package"


class LineageEdgeType(str, Enum):
    """Type of edge in the lineage graph."""

    DATA_FLOW = "data_flow"
    PRECEDENCE = "precedence"
    CONTAINS = "contains"


# =============================================================================
# Data Models
# =============================================================================


class LineageNode(BaseModel):
    """A node in the lineage graph (table, file, transformation, task, or package)."""

    id: str
    name: str
    node_type: LineageNodeType
    package: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    """A directed edge connecting two lineage nodes."""

    source_id: str
    target_id: str
    edge_type: LineageEdgeType
    label: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageGraph(BaseModel):
    """Complete lineage graph with nodes and edges."""

    nodes: list[LineageNode] = Field(default_factory=list)
    edges: list[LineageEdge] = Field(default_factory=list)

    # Internal lookup helpers (rebuilt lazily)
    _node_index: dict[str, LineageNode] = {}
    _forward: dict[str, list[str]] = {}  # node_id → downstream node_ids
    _backward: dict[str, list[str]] = {}  # node_id → upstream node_ids

    def model_post_init(self, _context: Any) -> None:  # noqa: ANN401
        self._rebuild_index()

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def _rebuild_index(self) -> None:
        self._node_index = {n.id: n for n in self.nodes}
        self._forward = {}
        self._backward = {}
        for e in self.edges:
            self._forward.setdefault(e.source_id, []).append(e.target_id)
            self._backward.setdefault(e.target_id, []).append(e.source_id)

    def add_node(self, node: LineageNode) -> None:
        """Add a node if it does not already exist."""
        if node.id not in self._node_index:
            self.nodes.append(node)
            self._node_index[node.id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge (deduplication by source+target+type)."""
        for existing in self.edges:
            if (
                existing.source_id == edge.source_id
                and existing.target_id == edge.target_id
                and existing.edge_type == edge.edge_type
            ):
                return
        self.edges.append(edge)
        self._forward.setdefault(edge.source_id, []).append(edge.target_id)
        self._backward.setdefault(edge.target_id, []).append(edge.source_id)

    # ------------------------------------------------------------------
    # Impact analysis
    # ------------------------------------------------------------------

    def get_downstream(self, node_id: str) -> list[LineageNode]:
        """Return all nodes reachable *downstream* from *node_id*."""
        return self._traverse(node_id, self._forward)

    def get_upstream(self, node_id: str) -> list[LineageNode]:
        """Return all nodes reachable *upstream* from *node_id*."""
        return self._traverse(node_id, self._backward)

    def _traverse(self, start: str, adjacency: dict[str, list[str]]) -> list[LineageNode]:
        visited: set[str] = set()
        stack = list(adjacency.get(start, []))
        result: list[LineageNode] = []
        while stack:
            nid = stack.pop()
            if nid in visited:
                continue
            visited.add(nid)
            node = self._node_index.get(nid)
            if node:
                result.append(node)
            stack.extend(adjacency.get(nid, []))
        return result

    def find_node(self, name: str) -> LineageNode | None:
        """Find a node by name (case-insensitive substring match)."""
        name_lower = name.lower()
        for node in self.nodes:
            if name_lower in node.name.lower():
                return node
        return None

    # ------------------------------------------------------------------
    # Export: Mermaid
    # ------------------------------------------------------------------

    def to_mermaid(self) -> str:
        """Export the lineage graph as a Mermaid flowchart string."""
        lines: list[str] = ["graph LR"]
        for node in self.nodes:
            safe_id = _mermaid_safe_id(node.id)
            label = _mermaid_escape(node.name)
            shape = _mermaid_shape(node.node_type, safe_id, label)
            lines.append(f"    {shape}")
        for edge in self.edges:
            src = _mermaid_safe_id(edge.source_id)
            tgt = _mermaid_safe_id(edge.target_id)
            if edge.label:
                lines.append(f"    {src} -->|{_mermaid_escape(edge.label)}| {tgt}")
            else:
                lines.append(f"    {src} --> {tgt}")
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Export: D3.js JSON
    # ------------------------------------------------------------------

    def to_d3_json(self) -> str:
        """Export the lineage graph as D3.js-compatible JSON (force-directed)."""
        d3_nodes = [
            {
                "id": n.id,
                "name": n.name,
                "type": n.node_type.value,
                "package": n.package,
                "metadata": n.metadata,
            }
            for n in self.nodes
        ]
        d3_links = [
            {
                "source": e.source_id,
                "target": e.target_id,
                "type": e.edge_type.value,
                "label": e.label,
            }
            for e in self.edges
        ]
        return json.dumps({"nodes": d3_nodes, "links": d3_links}, indent=2)


# =============================================================================
# Mermaid helpers
# =============================================================================

_MERMAID_ID_RE = re.compile(r"[^a-zA-Z0-9_]")


def _mermaid_safe_id(raw: str) -> str:
    return _MERMAID_ID_RE.sub("_", raw)


def _mermaid_escape(text: str) -> str:
    return text.replace('"', "&quot;")


def _mermaid_shape(node_type: LineageNodeType, safe_id: str, label: str) -> str:
    if node_type == LineageNodeType.TABLE:
        return f'{safe_id}[("{label}")]'
    if node_type == LineageNodeType.FILE:
        return f"{safe_id}>{label}]"
    if node_type == LineageNodeType.TRANSFORMATION:
        return f"{safe_id}{{{label}}}"
    if node_type == LineageNodeType.PACKAGE:
        return f"{safe_id}[[{label}]]"
    # TASK or default
    return f"{safe_id}[{label}]"


# =============================================================================
# Lineage Builder
# =============================================================================


class LineageBuilder:
    """
    Construct a :class:`LineageGraph` from one or more :class:`SSISPackage` models.

    The builder walks control-flow tasks and data-flow components,
    extracting source/destination table or file references, and connecting
    them through transformation nodes.
    """

    def __init__(self) -> None:
        self._graph = LineageGraph()

    def build(self, packages: list[SSISPackage]) -> LineageGraph:
        """Build the lineage graph from parsed SSIS packages."""
        for pkg in packages:
            self._process_package(pkg)
        return self._graph

    # ------------------------------------------------------------------
    # Package-level processing
    # ------------------------------------------------------------------

    def _process_package(self, pkg: SSISPackage) -> None:
        pkg_node_id = f"pkg:{pkg.name}"
        self._graph.add_node(
            LineageNode(
                id=pkg_node_id,
                name=pkg.name,
                node_type=LineageNodeType.PACKAGE,
                package=pkg.name,
            )
        )
        self._process_tasks(pkg.control_flow_tasks, pkg.name, pkg_node_id)
        # Precedence constraints → edges between task nodes
        for pc in pkg.precedence_constraints:
            src_id = f"task:{pkg.name}:{pc.source_task}"
            dst_id = f"task:{pkg.name}:{pc.destination_task}"
            self._graph.add_edge(
                LineageEdge(
                    source_id=src_id,
                    target_id=dst_id,
                    edge_type=LineageEdgeType.PRECEDENCE,
                    label=pc.constraint_type.value,
                )
            )

    # ------------------------------------------------------------------
    # Control-flow task processing
    # ------------------------------------------------------------------

    def _process_tasks(
        self,
        tasks: list[ControlFlowTask],
        package_name: str,
        parent_id: str,
    ) -> None:
        for task in tasks:
            task_id = f"task:{package_name}:{task.name}"
            self._graph.add_node(
                LineageNode(
                    id=task_id,
                    name=task.name,
                    node_type=LineageNodeType.TASK,
                    package=package_name,
                    metadata={"task_type": task.task_type.value},
                )
            )
            self._graph.add_edge(
                LineageEdge(
                    source_id=parent_id,
                    target_id=task_id,
                    edge_type=LineageEdgeType.CONTAINS,
                )
            )
            if task.task_type == TaskType.DATA_FLOW:
                self._process_data_flow(task, package_name, task_id)
            elif task.task_type == TaskType.EXECUTE_SQL:
                self._process_execute_sql(task, package_name, task_id)
            # Recurse into child tasks (containers)
            if task.child_tasks:
                self._process_tasks(task.child_tasks, package_name, task_id)
                for pc in task.child_precedence_constraints:
                    src = f"task:{package_name}:{pc.source_task}"
                    dst = f"task:{package_name}:{pc.destination_task}"
                    self._graph.add_edge(
                        LineageEdge(
                            source_id=src,
                            target_id=dst,
                            edge_type=LineageEdgeType.PRECEDENCE,
                            label=pc.constraint_type.value,
                        )
                    )

    # ------------------------------------------------------------------
    # Data-flow lineage
    # ------------------------------------------------------------------

    def _process_data_flow(
        self,
        task: ControlFlowTask,
        package_name: str,
        task_id: str,
    ) -> None:
        source_nodes: list[str] = []
        dest_nodes: list[str] = []
        transform_nodes: list[str] = []

        for comp in task.data_flow_components:
            if comp.component_type in _SOURCE_TYPES:
                ext_id = self._add_external_node(comp, package_name)
                source_nodes.append(ext_id)
                # External source → task
                self._graph.add_edge(
                    LineageEdge(
                        source_id=ext_id,
                        target_id=task_id,
                        edge_type=LineageEdgeType.DATA_FLOW,
                    )
                )
            elif comp.component_type in _DESTINATION_TYPES:
                ext_id = self._add_external_node(comp, package_name)
                dest_nodes.append(ext_id)
                # Task → external destination
                self._graph.add_edge(
                    LineageEdge(
                        source_id=task_id,
                        target_id=ext_id,
                        edge_type=LineageEdgeType.DATA_FLOW,
                    )
                )
            else:
                t_id = f"transform:{package_name}:{task.name}:{comp.name}"
                transform_nodes.append(t_id)
                self._graph.add_node(
                    LineageNode(
                        id=t_id,
                        name=comp.name,
                        node_type=LineageNodeType.TRANSFORMATION,
                        package=package_name,
                        metadata={"component_type": comp.component_type.value},
                    )
                )

        # Connect data-flow paths between components
        comp_name_to_node: dict[str, str] = {}
        for comp in task.data_flow_components:
            if comp.component_type in _SOURCE_TYPES or comp.component_type in _DESTINATION_TYPES:
                comp_name_to_node[comp.name] = self._add_external_node(comp, package_name)
            else:
                comp_name_to_node[comp.name] = f"transform:{package_name}:{task.name}:{comp.name}"

        for path in task.data_flow_paths:
            src = comp_name_to_node.get(path.source_component)
            dst = comp_name_to_node.get(path.destination_component)
            if src and dst:
                self._graph.add_edge(
                    LineageEdge(
                        source_id=src,
                        target_id=dst,
                        edge_type=LineageEdgeType.DATA_FLOW,
                        label=path.source_output,
                    )
                )

    # ------------------------------------------------------------------
    # Execute SQL lineage
    # ------------------------------------------------------------------

    def _process_execute_sql(
        self,
        task: ControlFlowTask,
        package_name: str,
        task_id: str,
    ) -> None:
        if not task.sql_statement:
            return
        tables = _extract_tables_from_sql(task.sql_statement)
        for table_name, role in tables:
            tbl_id = f"table:{table_name}"
            self._graph.add_node(
                LineageNode(
                    id=tbl_id,
                    name=table_name,
                    node_type=LineageNodeType.TABLE,
                    package=package_name,
                )
            )
            if role == "source":
                self._graph.add_edge(
                    LineageEdge(
                        source_id=tbl_id,
                        target_id=task_id,
                        edge_type=LineageEdgeType.DATA_FLOW,
                        label="SQL read",
                    )
                )
            else:
                self._graph.add_edge(
                    LineageEdge(
                        source_id=task_id,
                        target_id=tbl_id,
                        edge_type=LineageEdgeType.DATA_FLOW,
                        label="SQL write",
                    )
                )

    # ------------------------------------------------------------------
    # External node helpers
    # ------------------------------------------------------------------

    def _add_external_node(self, comp: DataFlowComponent, package_name: str) -> str:
        """Create or return a table/file node for a source/destination component."""
        ext_name = comp.table_name or comp.sql_command or comp.name
        if comp.component_type in (
            DataFlowComponentType.FLAT_FILE_SOURCE,
            DataFlowComponentType.FLAT_FILE_DESTINATION,
            DataFlowComponentType.RAW_FILE_SOURCE,
            DataFlowComponentType.RAW_FILE_DESTINATION,
            DataFlowComponentType.EXCEL_SOURCE,
            DataFlowComponentType.EXCEL_DESTINATION,
            DataFlowComponentType.XML_SOURCE,
        ):
            node_type = LineageNodeType.FILE
            node_id = f"file:{ext_name}"
        else:
            node_type = LineageNodeType.TABLE
            node_id = f"table:{ext_name}"

        self._graph.add_node(
            LineageNode(
                id=node_id,
                name=ext_name,
                node_type=node_type,
                package=package_name,
                metadata={"component_type": comp.component_type.value},
            )
        )
        return node_id


# =============================================================================
# SQL table extraction (lightweight, best-effort)
# =============================================================================

# Patterns for common SQL DML statements
# The table-name group captures bracketed multi-part names like [dbo].[Table]
_TABLE_NAME_PAT = r"((?:\[?\w+\]?\.)*\[?\w+\]?)"
_INSERT_RE = re.compile(r"\bINSERT\s+INTO\s+" + _TABLE_NAME_PAT, re.IGNORECASE)
_UPDATE_RE = re.compile(r"\bUPDATE\s+" + _TABLE_NAME_PAT, re.IGNORECASE)
_DELETE_RE = re.compile(r"\bDELETE\s+(?:FROM\s+)?" + _TABLE_NAME_PAT, re.IGNORECASE)
_TRUNCATE_RE = re.compile(r"\bTRUNCATE\s+TABLE\s+" + _TABLE_NAME_PAT, re.IGNORECASE)
_MERGE_RE = re.compile(r"\bMERGE\s+(?:INTO\s+)?" + _TABLE_NAME_PAT, re.IGNORECASE)
_FROM_RE = re.compile(r"\bFROM\s+" + _TABLE_NAME_PAT, re.IGNORECASE)
_JOIN_RE = re.compile(r"\bJOIN\s+" + _TABLE_NAME_PAT, re.IGNORECASE)
_EXEC_RE = re.compile(r"\bEXEC(?:UTE)?\s+" + _TABLE_NAME_PAT, re.IGNORECASE)


def _clean_table_name(raw: str) -> str:
    """Strip surrounding brackets from ``[dbo].[TableName]``-style names."""
    return raw.replace("[", "").replace("]", "")


def _extract_tables_from_sql(sql: str) -> list[tuple[str, str]]:
    """
    Extract ``(table_name, role)`` pairs from a SQL statement.

    *role* is ``"source"`` for tables read from (FROM / JOIN) and
    ``"target"`` for tables written to (INSERT / UPDATE / DELETE / TRUNCATE / MERGE).
    """
    results: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for pattern, role in [
        (_INSERT_RE, "target"),
        (_UPDATE_RE, "target"),
        (_DELETE_RE, "target"),
        (_TRUNCATE_RE, "target"),
        (_MERGE_RE, "target"),
        (_FROM_RE, "source"),
        (_JOIN_RE, "source"),
        (_EXEC_RE, "target"),
    ]:
        for match in pattern.finditer(sql):
            name = _clean_table_name(match.group(1))
            key = (name, role)
            if key not in seen:
                seen.add(key)
                results.append(key)

    return results
