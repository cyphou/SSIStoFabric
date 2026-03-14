"""
Data Lineage Graph & Impact Analysis
======================================
Extracts source/destination table references from parsed SSIS packages,
builds a directed lineage graph, and supports impact analysis queries.

Usage::

    from ssis_to_fabric.engine.lineage import LineageGraph
    from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

    packages = DTSXParser().parse_directory("path/to/ssis/")
    graph = LineageGraph()
    graph.build(packages)

    # Impact analysis
    affected = graph.impact("dbo.FactSales")
    print(affected)

    # Mermaid diagram
    print(graph.to_mermaid())

CLI::

    ssis2fabric lineage path/to/ssis/
    ssis2fabric lineage path/to/ssis/ --table dbo.FactSales
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from ssis_to_fabric.analyzer.models import DataFlowComponentType, TaskType
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import SSISPackage

logger = get_logger(__name__)

# Component types that READ data (sources)
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

# Component types that WRITE data (destinations)
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


class LineageGraph:
    """Directed dependency graph: table → packages that read/write it."""

    def __init__(self) -> None:
        # table_name → set of (package_name, "read"|"write")
        self._readers: dict[str, set[str]] = defaultdict(set)  # table → packages reading
        self._writers: dict[str, set[str]] = defaultdict(set)  # table → packages writing
        # package_name → (sources[], destinations[])
        self._pkg_tables: dict[str, tuple[list[str], list[str]]] = {}

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, packages: list[SSISPackage]) -> None:
        """Populate the graph from a list of parsed SSIS packages."""
        for pkg in packages:
            sources: list[str] = []
            destinations: list[str] = []
            self._scan_tasks(pkg.name, pkg.control_flow_tasks, sources, destinations)
            self._pkg_tables[pkg.name] = (sources, destinations)
            for tbl in sources:
                self._readers[tbl].add(pkg.name)
            for tbl in destinations:
                self._writers[tbl].add(pkg.name)
            # Execute SQL tasks can also be source/destination
            for task in pkg.control_flow_tasks:
                if task.task_type == TaskType.EXECUTE_SQL and task.sql_statement:
                    refs = _extract_sql_table_refs(task.sql_statement)
                    for tbl in refs:
                        self._readers[tbl].add(pkg.name)
        logger.info(
            "lineage_graph_built",
            tables=len(self.all_tables()),
            packages=len(packages),
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def all_tables(self) -> set[str]:
        """Return all known table names."""
        return set(self._readers) | set(self._writers)

    def impact(self, table: str) -> dict[str, list[str]]:
        """Return packages that read from or write to *table*.

        Returns::

            {
                "readers": ["pkg1", "pkg2"],
                "writers": ["pkg3"]
            }
        """
        return {
            "readers": sorted(self._readers.get(table, set())),
            "writers": sorted(self._writers.get(table, set())),
        }

    def package_tables(self, package_name: str) -> dict[str, list[str]]:
        """Return the source and destination tables for a given package."""
        srcs, dsts = self._pkg_tables.get(package_name, ([], []))
        return {"sources": srcs, "destinations": dsts}

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_mermaid(self) -> str:
        """Export the lineage graph as a Mermaid ``flowchart LR`` diagram."""
        lines = ["flowchart LR"]
        node_id: dict[str, str] = {}
        counter = [0]

        def _node(name: str) -> str:
            if name not in node_id:
                counter[0] += 1
                node_id[name] = f"n{counter[0]}"
            return node_id[name]

        for table, pkgs in sorted(self._readers.items()):
            tbl_node = _node(f"tbl:{table}")
            lines.append(f'    {tbl_node}["{_mermaid_safe(table)}"]')
            for pkg in sorted(pkgs):
                pkg_node = _node(f"pkg:{pkg}")
                lines.append(f'    {pkg_node}(("{_mermaid_safe(pkg)}"))')
                lines.append(f"    {tbl_node} -->|read| {pkg_node}")

        for table, pkgs in sorted(self._writers.items()):
            tbl_node = _node(f"tbl:{table}")
            if f"tbl:{table}" not in node_id:
                lines.append(f'    {tbl_node}["{_mermaid_safe(table)}"]')
            for pkg in sorted(pkgs):
                pkg_node = _node(f"pkg:{pkg}")
                lines.append(f'    {pkg_node}(("{_mermaid_safe(pkg)}"))')
                lines.append(f"    {pkg_node} -->|write| {tbl_node}")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Serialisable representation of the lineage graph."""
        return {
            "tables": sorted(self.all_tables()),
            "readers": {k: sorted(v) for k, v in sorted(self._readers.items())},
            "writers": {k: sorted(v) for k, v in sorted(self._writers.items())},
            "packages": {
                pkg: {"sources": srcs, "destinations": dsts} for pkg, (srcs, dsts) in sorted(self._pkg_tables.items())
            },
        }

    def write_json(self, output_dir: Path) -> Path:
        """Write the lineage graph to ``output_dir/lineage.json``."""
        import json

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "lineage.json"
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        logger.info("lineage_written", path=str(path))
        return path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan_tasks(
        self,
        pkg_name: str,
        tasks: list,
        sources: list[str],
        destinations: list[str],
    ) -> None:
        for task in tasks:
            # Data Flow tasks — inspect components
            for comp in task.data_flow_components:
                tbl = comp.table_name or comp.properties.get("tableName") or comp.properties.get("OpenRowset", "")
                if not tbl:
                    continue
                tbl = _normalise_table(str(tbl))
                if comp.component_type in _SOURCE_TYPES:
                    sources.append(tbl)
                elif comp.component_type in _DEST_TYPES:
                    destinations.append(tbl)
            # Recurse into containers
            self._scan_tasks(pkg_name, task.child_tasks, sources, destinations)


# ---------------------------------------------------------------------------
# Table-name helpers
# ---------------------------------------------------------------------------


def _normalise_table(raw: str) -> str:
    """Strip quotes/brackets and normalise to ``schema.table`` or ``table``."""
    clean = raw.strip().strip('"').strip("'").strip("[").strip("]")
    clean = re.sub(r"\[([^\]]+)\]", r"\1", clean)  # [dbo].[Table] → dbo.Table
    return clean


def _extract_sql_table_refs(sql: str) -> list[str]:
    """Very light extraction of table references from SQL text."""
    # Match: FROM <table>, JOIN <table>
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([\w\.\[\]\"]+)",
        re.IGNORECASE,
    )
    tables = []
    for m in pattern.finditer(sql):
        tbl = _normalise_table(m.group(1))
        if tbl and not tbl.upper().startswith("("):
            tables.append(tbl)
    return tables


def _mermaid_safe(s: str) -> str:
    return re.sub(r'["\[\]{}|]', "_", s)
