"""
Metadata Catalog & Discovery
==============================
Phase 29: Unified metadata store indexing all packages, tasks, connections,
and lineage edges with full-text search, tag-based classification,
dependency matrix, and catalog export.

Usage::

    from ssis_to_fabric.engine.metadata_catalog import MetadataCatalog

    catalog = MetadataCatalog()
    catalog.build(packages)

    # Full-text search
    results = catalog.search("customer")

    # Tag-based browse
    entries = catalog.browse(tags=["ETL", "SCD"])

    # Dependency matrix
    matrix = catalog.dependency_matrix()

    # Export
    catalog.write_catalog(Path("output/"))

CLI::

    ssis2fabric catalog path/to/ssis/ --query "customer"
    ssis2fabric catalog path/to/ssis/ --tags ETL,SCD
    ssis2fabric catalog path/to/ssis/ --export purview
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.analyzer.models import (
    DataFlowComponentType,
    MigrationComplexity,
    TaskType,
)
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import SSISPackage

logger = get_logger(__name__)


# =====================================================================
# Enumerations
# =====================================================================


class CatalogEntryType(str, Enum):
    """Type of entry in the metadata catalog."""

    PACKAGE = "package"
    TASK = "task"
    DATA_FLOW = "data_flow"
    CONNECTION = "connection"
    TABLE = "table"
    PARAMETER = "parameter"
    VARIABLE = "variable"
    EXPRESSION = "expression"


class CatalogTag(str, Enum):
    """Auto-classification tags for catalog entries."""

    ETL = "ETL"
    STAGING = "STAGING"
    DIMENSION = "DIMENSION"
    FACT = "FACT"
    LOOKUP = "LOOKUP"
    SCD = "SCD"
    CDC = "CDC"
    ORCHESTRATOR = "ORCHESTRATOR"
    FILE_PROCESSING = "FILE_PROCESSING"
    DATA_QUALITY = "DATA_QUALITY"
    INCREMENTAL = "INCREMENTAL"
    FULL_LOAD = "FULL_LOAD"
    MASTER_DATA = "MASTER_DATA"
    AGGREGATION = "AGGREGATION"
    ERROR_HANDLING = "ERROR_HANDLING"


class ExportFormat(str, Enum):
    """Supported catalog export formats."""

    JSON = "json"
    PURVIEW = "purview"
    DATA_CATALOG = "data_catalog"


# =====================================================================
# Data Classes
# =====================================================================


@dataclass
class CatalogEntry:
    """A single entry in the metadata catalog."""

    entry_id: str
    entry_type: CatalogEntryType
    name: str
    package_name: str
    description: str = ""
    tags: list[str] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    # Searchable text (concatenation of all text fields for full-text search)
    _search_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "entry_type": self.entry_type.value,
            "name": self.name,
            "package_name": self.package_name,
            "description": self.description,
            "tags": self.tags,
            "properties": self.properties,
        }


@dataclass
class SearchResult:
    """Result of a full-text search query."""

    query: str
    total_matches: int = 0
    entries: list[CatalogEntry] = field(default_factory=list)
    elapsed_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "total_matches": self.total_matches,
            "elapsed_ms": round(self.elapsed_ms, 2),
            "entries": [e.to_dict() for e in self.entries],
        }


@dataclass
class DependencyEdge:
    """Represents a shared dependency between two packages."""

    package_a: str
    package_b: str
    shared_resource: str
    resource_type: str  # "table", "connection", "parameter"

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_a": self.package_a,
            "package_b": self.package_b,
            "shared_resource": self.shared_resource,
            "resource_type": self.resource_type,
        }


@dataclass
class DependencyMatrix:
    """Dependency matrix showing shared resources across packages."""

    packages: list[str] = field(default_factory=list)
    edges: list[DependencyEdge] = field(default_factory=list)
    # package_name → {resource_type → [resource_names]}
    package_resources: dict[str, dict[str, list[str]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "packages": self.packages,
            "edges": [e.to_dict() for e in self.edges],
            "package_resources": self.package_resources,
            "summary": {
                "total_packages": len(self.packages),
                "total_edges": len(self.edges),
                "shared_tables": len({e.shared_resource for e in self.edges if e.resource_type == "table"}),
                "shared_connections": len(
                    {e.shared_resource for e in self.edges if e.resource_type == "connection"}
                ),
                "shared_parameters": len(
                    {e.shared_resource for e in self.edges if e.resource_type == "parameter"}
                ),
            },
        }


@dataclass
class CatalogSummary:
    """Top-level summary of the metadata catalog."""

    total_entries: int = 0
    total_packages: int = 0
    total_tasks: int = 0
    total_data_flows: int = 0
    total_connections: int = 0
    total_tables: int = 0
    total_parameters: int = 0
    total_variables: int = 0
    total_expressions: int = 0
    tag_counts: dict[str, int] = field(default_factory=dict)
    complexity_distribution: dict[str, int] = field(default_factory=dict)
    built_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_entries": self.total_entries,
            "total_packages": self.total_packages,
            "total_tasks": self.total_tasks,
            "total_data_flows": self.total_data_flows,
            "total_connections": self.total_connections,
            "total_tables": self.total_tables,
            "total_parameters": self.total_parameters,
            "total_variables": self.total_variables,
            "total_expressions": self.total_expressions,
            "tag_counts": self.tag_counts,
            "complexity_distribution": self.complexity_distribution,
            "built_at": self.built_at,
        }


# =====================================================================
# Auto-tagging rules
# =====================================================================

# keyword → tag mapping for auto-classification
_TAG_RULES: dict[str, CatalogTag] = {
    "stg": CatalogTag.STAGING,
    "staging": CatalogTag.STAGING,
    "dim": CatalogTag.DIMENSION,
    "dimension": CatalogTag.DIMENSION,
    "fact": CatalogTag.FACT,
    "lookup": CatalogTag.LOOKUP,
    "lkp": CatalogTag.LOOKUP,
    "scd": CatalogTag.SCD,
    "slowly": CatalogTag.SCD,
    "cdc": CatalogTag.CDC,
    "change_data": CatalogTag.CDC,
    "master": CatalogTag.MASTER_DATA,
    "mds": CatalogTag.MASTER_DATA,
    "aggregate": CatalogTag.AGGREGATION,
    "agg": CatalogTag.AGGREGATION,
    "error": CatalogTag.ERROR_HANDLING,
    "onerror": CatalogTag.ERROR_HANDLING,
    "watermark": CatalogTag.INCREMENTAL,
    "incremental": CatalogTag.INCREMENTAL,
    "delta": CatalogTag.INCREMENTAL,
}

# Component types that indicate source (reader)
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

# Component types that indicate destination (writer)
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


# =====================================================================
# MetadataCatalog
# =====================================================================


class MetadataCatalog:
    """Unified metadata store indexing all SSIS packages, tasks,
    connections, tables, and lineage edges.

    Supports full-text search, tag-based filtering, dependency matrix
    generation, and export to Purview / Azure Data Catalog formats.
    """

    def __init__(self) -> None:
        self._entries: list[CatalogEntry] = []
        self._by_type: dict[CatalogEntryType, list[CatalogEntry]] = {}
        self._by_package: dict[str, list[CatalogEntry]] = {}
        self._by_tag: dict[str, list[CatalogEntry]] = {}
        self._summary: CatalogSummary | None = None
        self._counter = 0

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, packages: list[SSISPackage]) -> CatalogSummary:
        """Populate the catalog from a list of parsed SSIS packages.

        Parameters
        ----------
        packages : list[SSISPackage]
            Parsed SSIS packages from the analyzer.

        Returns
        -------
        CatalogSummary
            Summary statistics of the built catalog.
        """
        self._entries.clear()
        self._by_type.clear()
        self._by_package.clear()
        self._by_tag.clear()
        self._counter = 0

        for pkg in packages:
            self._index_package(pkg)

        self._summary = self._compute_summary()

        logger.info(
            "catalog_built",
            entries=len(self._entries),
            packages=len(packages),
            tags=len(self._by_tag),
        )

        return self._summary

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, query: str, *, entry_type: CatalogEntryType | None = None) -> SearchResult:
        """Full-text search across all catalog entries.

        Parameters
        ----------
        query : str
            Search term (case-insensitive). Supports simple words and
            space-separated terms (AND semantics).
        entry_type : CatalogEntryType, optional
            Filter results to a specific entry type.

        Returns
        -------
        SearchResult
            Matching entries with hit count.
        """
        import time

        start = time.perf_counter()
        terms = query.lower().split()
        pool = self._by_type.get(entry_type, self._entries) if entry_type else self._entries

        matches: list[CatalogEntry] = []
        for entry in pool:
            text = entry._search_text
            if all(term in text for term in terms):
                matches.append(entry)

        elapsed = (time.perf_counter() - start) * 1000

        return SearchResult(
            query=query,
            total_matches=len(matches),
            entries=matches,
            elapsed_ms=elapsed,
        )

    # ------------------------------------------------------------------
    # Browse (tag-based)
    # ------------------------------------------------------------------

    def browse(
        self,
        *,
        tags: list[str] | None = None,
        entry_type: CatalogEntryType | None = None,
        package_name: str | None = None,
    ) -> list[CatalogEntry]:
        """Browse catalog entries by tag, type, or package.

        Parameters
        ----------
        tags : list[str], optional
            Filter to entries matching ALL of the given tags.
        entry_type : CatalogEntryType, optional
            Filter to a specific entry type.
        package_name : str, optional
            Filter to entries from a specific package.

        Returns
        -------
        list[CatalogEntry]
            Matching entries.
        """
        results = list(self._entries)

        if package_name:
            results = [e for e in results if e.package_name == package_name]

        if entry_type:
            results = [e for e in results if e.entry_type == entry_type]

        if tags:
            lower_tags = {t.lower() for t in tags}
            results = [e for e in results if lower_tags.issubset({t.lower() for t in e.tags})]

        return results

    # ------------------------------------------------------------------
    # Dependency Matrix
    # ------------------------------------------------------------------

    def dependency_matrix(self) -> DependencyMatrix:
        """Build a dependency matrix showing shared resources across packages.

        Identifies which packages share tables, connections, or parameters.

        Returns
        -------
        DependencyMatrix
            Matrix with edges and per-package resource lists.
        """
        pkg_names = sorted(self._by_package.keys())
        pkg_resources: dict[str, dict[str, list[str]]] = {}

        for pkg_name in pkg_names:
            resources: dict[str, list[str]] = {"table": [], "connection": [], "parameter": []}
            for entry in self._by_package.get(pkg_name, []):
                if entry.entry_type == CatalogEntryType.TABLE:
                    resources["table"].append(entry.name)
                elif entry.entry_type == CatalogEntryType.CONNECTION:
                    resources["connection"].append(entry.name)
                elif entry.entry_type == CatalogEntryType.PARAMETER:
                    resources["parameter"].append(entry.name)
            pkg_resources[pkg_name] = resources

        # Find shared resources between package pairs
        edges: list[DependencyEdge] = []
        for i, pkg_a in enumerate(pkg_names):
            for pkg_b in pkg_names[i + 1 :]:
                for res_type in ("table", "connection", "parameter"):
                    shared = set(pkg_resources[pkg_a][res_type]) & set(pkg_resources[pkg_b][res_type])
                    for resource in sorted(shared):
                        edges.append(
                            DependencyEdge(
                                package_a=pkg_a,
                                package_b=pkg_b,
                                shared_resource=resource,
                                resource_type=res_type,
                            )
                        )

        return DependencyMatrix(
            packages=pkg_names,
            edges=edges,
            package_resources=pkg_resources,
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    @property
    def summary(self) -> CatalogSummary:
        """Return the catalog summary (build must be called first)."""
        if self._summary is None:
            self._summary = self._compute_summary()
        return self._summary

    @property
    def entries(self) -> list[CatalogEntry]:
        """Return all catalog entries."""
        return list(self._entries)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialise the full catalog to a dictionary."""
        return {
            "summary": self.summary.to_dict(),
            "entries": [e.to_dict() for e in self._entries],
        }

    def write_catalog(self, output_dir: Path, *, fmt: ExportFormat = ExportFormat.JSON) -> Path:
        """Write the catalog to the output directory.

        Parameters
        ----------
        output_dir : Path
            Directory to write the catalog files to.
        fmt : ExportFormat
            Export format (json, purview, data_catalog).

        Returns
        -------
        Path
            Path to the written catalog file.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if fmt == ExportFormat.PURVIEW:
            return self._export_purview(output_dir)
        elif fmt == ExportFormat.DATA_CATALOG:
            return self._export_data_catalog(output_dir)
        else:
            return self._export_json(output_dir)

    # ------------------------------------------------------------------
    # Internal: Indexing
    # ------------------------------------------------------------------

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter:04d}"

    def _add_entry(self, entry: CatalogEntry) -> None:
        """Add an entry to all indexes."""
        # Build searchable text
        parts = [
            entry.name.lower(),
            entry.package_name.lower(),
            entry.description.lower(),
            entry.entry_type.value.lower(),
        ]
        parts.extend(t.lower() for t in entry.tags)
        for v in entry.properties.values():
            parts.append(str(v).lower())
        entry._search_text = " ".join(parts)

        self._entries.append(entry)

        # Index by type
        self._by_type.setdefault(entry.entry_type, []).append(entry)

        # Index by package
        self._by_package.setdefault(entry.package_name, []).append(entry)

        # Index by tag
        for tag in entry.tags:
            self._by_tag.setdefault(tag, []).append(entry)

    def _index_package(self, pkg: SSISPackage) -> None:
        """Index a single SSIS package and all its children."""
        pkg_name = pkg.name
        auto_tags = _auto_tag(pkg_name)

        # 1. Package entry
        self._add_entry(
            CatalogEntry(
                entry_id=self._next_id("pkg"),
                entry_type=CatalogEntryType.PACKAGE,
                name=pkg_name,
                package_name=pkg_name,
                description=pkg.description,
                tags=auto_tags + self._classify_package_tags(pkg),
                properties={
                    "file_path": pkg.file_path,
                    "total_tasks": pkg.total_tasks,
                    "total_data_flows": pkg.total_data_flows,
                    "complexity": pkg.overall_complexity.value,
                    "status": pkg.status,
                    "creation_date": pkg.creation_date,
                    "creator_name": pkg.creator_name,
                },
            )
        )

        # 2. Connection managers
        seen_connections: set[str] = set()
        for cm in pkg.connection_managers:
            if cm.name in seen_connections:
                continue
            seen_connections.add(cm.name)
            self._add_entry(
                CatalogEntry(
                    entry_id=self._next_id("conn"),
                    entry_type=CatalogEntryType.CONNECTION,
                    name=cm.name,
                    package_name=pkg_name,
                    description=f"{cm.connection_type.value} connection",
                    tags=auto_tags,
                    properties={
                        "connection_type": cm.connection_type.value,
                        "server": cm.server,
                        "database": cm.database,
                        "provider": cm.provider,
                        "connection_string": cm.connection_string,
                    },
                )
            )

        # 3. Parameters
        for param in pkg.parameters + pkg.project_parameters:
            self._add_entry(
                CatalogEntry(
                    entry_id=self._next_id("param"),
                    entry_type=CatalogEntryType.PARAMETER,
                    name=param.name,
                    package_name=pkg_name,
                    description=param.description,
                    tags=auto_tags,
                    properties={
                        "data_type": param.data_type,
                        "value": param.value,
                        "required": param.required,
                        "sensitive": param.sensitive,
                    },
                )
            )

        # 4. Variables
        for var in pkg.variables:
            if var.namespace == "System":
                continue
            self._add_entry(
                CatalogEntry(
                    entry_id=self._next_id("var"),
                    entry_type=CatalogEntryType.VARIABLE,
                    name=var.name,
                    package_name=pkg_name,
                    description="",
                    tags=auto_tags + _auto_tag(var.name),
                    properties={
                        "data_type": var.data_type,
                        "value": var.value,
                        "expression": var.expression,
                        "namespace": var.namespace,
                    },
                )
            )

        # 5. Tasks, data flows, tables, expressions
        self._index_tasks(pkg_name, pkg.control_flow_tasks, auto_tags)

    def _index_tasks(
        self, pkg_name: str, tasks: list[Any], parent_tags: list[str]
    ) -> None:
        """Recursively index control flow tasks."""
        for task in tasks:
            task_tags = parent_tags + _auto_tag(task.name)

            # Task entry
            self._add_entry(
                CatalogEntry(
                    entry_id=self._next_id("task"),
                    entry_type=CatalogEntryType.TASK,
                    name=task.name,
                    package_name=pkg_name,
                    description=task.description,
                    tags=task_tags,
                    properties={
                        "task_type": task.task_type.value if hasattr(task.task_type, "value") else str(task.task_type),
                        "complexity": task.migration_complexity.value
                        if hasattr(task.migration_complexity, "value")
                        else str(task.migration_complexity),
                        "disabled": task.disabled,
                        "connection_ref": task.connection_manager_ref,
                        "sql_statement": task.sql_statement[:200] if task.sql_statement else "",
                    },
                )
            )

            # Data flow components
            if task.task_type == TaskType.DATA_FLOW:
                self._add_entry(
                    CatalogEntry(
                        entry_id=self._next_id("dft"),
                        entry_type=CatalogEntryType.DATA_FLOW,
                        name=task.name,
                        package_name=pkg_name,
                        description=f"Data flow with {len(task.data_flow_components)} components",
                        tags=task_tags,
                        properties={
                            "component_count": len(task.data_flow_components),
                            "components": [c.name for c in task.data_flow_components],
                        },
                    )
                )

                # Index individual components for tables and expressions
                for comp in task.data_flow_components:
                    # Table references
                    table_name = comp.table_name or comp.properties.get("tableName") or comp.properties.get("OpenRowset", "")
                    if table_name:
                        table_name = _normalise_table(str(table_name))
                        role = "source" if comp.component_type in _SOURCE_TYPES else (
                            "destination" if comp.component_type in _DEST_TYPES else "reference"
                        )
                        self._add_entry(
                            CatalogEntry(
                                entry_id=self._next_id("tbl"),
                                entry_type=CatalogEntryType.TABLE,
                                name=table_name,
                                package_name=pkg_name,
                                description=f"{role} table in {task.name}",
                                tags=task_tags + [role],
                                properties={
                                    "component": comp.name,
                                    "component_type": comp.component_type.value,
                                    "role": role,
                                    "connection_ref": comp.connection_manager_ref,
                                },
                            )
                        )

                    # Expressions (Derived Column, Conditional Split)
                    for expr_name, expr_value in comp.expressions.items():
                        if expr_value:
                            self._add_entry(
                                CatalogEntry(
                                    entry_id=self._next_id("expr"),
                                    entry_type=CatalogEntryType.EXPRESSION,
                                    name=f"{comp.name}.{expr_name}",
                                    package_name=pkg_name,
                                    description=f"Expression in {comp.name}",
                                    tags=task_tags,
                                    properties={
                                        "expression": expr_value,
                                        "component": comp.name,
                                        "component_type": comp.component_type.value,
                                    },
                                )
                            )

            # SQL statements as expressions
            if task.sql_statement:
                self._add_entry(
                    CatalogEntry(
                        entry_id=self._next_id("expr"),
                        entry_type=CatalogEntryType.EXPRESSION,
                        name=f"{task.name}.sql",
                        package_name=pkg_name,
                        description=f"SQL in {task.name}",
                        tags=task_tags,
                        properties={
                            "expression": task.sql_statement,
                            "type": "sql",
                        },
                    )
                )

                # Extract table refs from SQL
                for tbl in _extract_sql_table_refs(task.sql_statement):
                    self._add_entry(
                        CatalogEntry(
                            entry_id=self._next_id("tbl"),
                            entry_type=CatalogEntryType.TABLE,
                            name=tbl,
                            package_name=pkg_name,
                            description=f"Referenced in SQL: {task.name}",
                            tags=task_tags + ["sql_ref"],
                            properties={
                                "component": task.name,
                                "role": "sql_reference",
                            },
                        )
                    )

            # Recurse into containers
            if task.child_tasks:
                self._index_tasks(pkg_name, task.child_tasks, task_tags)

    def _classify_package_tags(self, pkg: SSISPackage) -> list[str]:
        """Classify a package with structural tags based on its content."""
        tags: list[str] = []
        tasks = pkg.control_flow_tasks

        has_exec_pkg = any(t.task_type == TaskType.EXECUTE_PACKAGE for t in tasks)
        has_data_flow = any(t.task_type == TaskType.DATA_FLOW for t in tasks)
        has_loops = any(t.task_type in (TaskType.FOR_LOOP, TaskType.FOREACH_LOOP) for t in tasks)

        if has_exec_pkg and len(tasks) > 2:
            tags.append(CatalogTag.ORCHESTRATOR.value)
        if has_data_flow:
            tags.append(CatalogTag.ETL.value)
        if has_loops:
            tags.append(CatalogTag.FILE_PROCESSING.value)

        # Check data flow components for SCD/CDC
        for task in tasks:
            for comp in task.data_flow_components:
                if comp.component_type == DataFlowComponentType.SLOWLY_CHANGING_DIMENSION:
                    tags.append(CatalogTag.SCD.value)
                if comp.component_type == DataFlowComponentType.CDC_SOURCE:
                    tags.append(CatalogTag.CDC.value)
                if comp.component_type in (
                    DataFlowComponentType.AGGREGATE,
                ):
                    tags.append(CatalogTag.AGGREGATION.value)
                if comp.component_type == DataFlowComponentType.LOOKUP:
                    tags.append(CatalogTag.LOOKUP.value)

        return list(set(tags))  # deduplicate

    # ------------------------------------------------------------------
    # Internal: Summary
    # ------------------------------------------------------------------

    def _compute_summary(self) -> CatalogSummary:
        """Compute summary statistics from the current catalog."""
        tag_counts: dict[str, int] = {}
        for tag, entries in self._by_tag.items():
            tag_counts[tag] = len(entries)

        complexity_dist: dict[str, int] = {}
        for entry in self._by_type.get(CatalogEntryType.PACKAGE, []):
            c = entry.properties.get("complexity", "UNKNOWN")
            complexity_dist[c] = complexity_dist.get(c, 0) + 1

        return CatalogSummary(
            total_entries=len(self._entries),
            total_packages=len(self._by_type.get(CatalogEntryType.PACKAGE, [])),
            total_tasks=len(self._by_type.get(CatalogEntryType.TASK, [])),
            total_data_flows=len(self._by_type.get(CatalogEntryType.DATA_FLOW, [])),
            total_connections=len(self._by_type.get(CatalogEntryType.CONNECTION, [])),
            total_tables=len(self._by_type.get(CatalogEntryType.TABLE, [])),
            total_parameters=len(self._by_type.get(CatalogEntryType.PARAMETER, [])),
            total_variables=len(self._by_type.get(CatalogEntryType.VARIABLE, [])),
            total_expressions=len(self._by_type.get(CatalogEntryType.EXPRESSION, [])),
            tag_counts=dict(sorted(tag_counts.items())),
            complexity_distribution=dict(sorted(complexity_dist.items())),
            built_at=datetime.now(timezone.utc).isoformat(),
        )

    # ------------------------------------------------------------------
    # Internal: Export formats
    # ------------------------------------------------------------------

    def _export_json(self, output_dir: Path) -> Path:
        """Write catalog as plain JSON."""
        path = output_dir / "metadata_catalog.json"
        data = self.to_dict()
        data["dependency_matrix"] = self.dependency_matrix().to_dict()
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("catalog_written", path=str(path), format="json")
        return path

    def _export_purview(self, output_dir: Path) -> Path:
        """Export catalog in Microsoft Purview-compatible format.

        Generates entities following the Apache Atlas type system
        used by Microsoft Purview.
        """
        entities: list[dict[str, Any]] = []

        for entry in self._entries:
            entity: dict[str, Any] = {
                "typeName": _purview_type(entry.entry_type),
                "attributes": {
                    "qualifiedName": f"ssis://{entry.package_name}/{entry.entry_type.value}/{entry.name}",
                    "name": entry.name,
                    "description": entry.description,
                    "owner": entry.properties.get("creator_name", ""),
                },
                "classifications": [{"typeName": tag} for tag in entry.tags],
            }

            # Add type-specific attributes
            if entry.entry_type == CatalogEntryType.TABLE:
                entity["attributes"]["tableName"] = entry.name
                entity["attributes"]["role"] = entry.properties.get("role", "")
            elif entry.entry_type == CatalogEntryType.CONNECTION:
                entity["attributes"]["connectionType"] = entry.properties.get("connection_type", "")
                entity["attributes"]["server"] = entry.properties.get("server", "")
                entity["attributes"]["database"] = entry.properties.get("database", "")

            entities.append(entity)

        payload = {
            "entities": entities,
            "referredEntities": {},
        }

        path = output_dir / "purview_catalog.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("catalog_written", path=str(path), format="purview")
        return path

    def _export_data_catalog(self, output_dir: Path) -> Path:
        """Export catalog in Azure Data Catalog compatible format."""
        assets: list[dict[str, Any]] = []

        for entry in self._entries:
            if entry.entry_type not in (
                CatalogEntryType.TABLE,
                CatalogEntryType.CONNECTION,
                CatalogEntryType.PACKAGE,
            ):
                continue

            asset: dict[str, Any] = {
                "id": entry.entry_id,
                "name": entry.name,
                "type": entry.entry_type.value,
                "description": entry.description,
                "tags": entry.tags,
                "source": {
                    "sourceType": "SSIS",
                    "packageName": entry.package_name,
                },
                "properties": entry.properties,
            }
            assets.append(asset)

        catalog_data = {
            "assets": assets,
            "totalAssets": len(assets),
        }

        path = output_dir / "data_catalog_export.json"
        path.write_text(json.dumps(catalog_data, indent=2), encoding="utf-8")
        logger.info("catalog_written", path=str(path), format="data_catalog")
        return path


# =====================================================================
# Module-level helpers
# =====================================================================


def _auto_tag(name: str) -> list[str]:
    """Auto-classify a name into tags based on keyword matching."""
    tags: list[str] = []
    lower = name.lower()
    for keyword, tag in _TAG_RULES.items():
        if keyword in lower:
            if tag.value not in tags:
                tags.append(tag.value)
    return tags


def _normalise_table(raw: str) -> str:
    """Strip quotes/brackets and normalise to ``schema.table`` or ``table``."""
    clean = raw.strip()
    # Remove all bracket-quoted identifiers: [dbo].[Customers] → dbo.Customers
    clean = re.sub(r"\[([^\]]*)\]", r"\1", clean)
    # Remove all double-quoted identifiers: "dbo"."Customers" → dbo.Customers
    clean = re.sub(r'"([^"]*)"', r"\1", clean)
    # Remove single quotes
    clean = clean.strip("'")
    return clean


def _extract_sql_table_refs(sql: str) -> list[str]:
    """Extract table references from SQL text."""
    pattern = re.compile(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([\w\.\[\]\"]+)", re.IGNORECASE)
    tables: list[str] = []
    for m in pattern.finditer(sql):
        tbl = _normalise_table(m.group(1))
        if tbl and not tbl.upper().startswith("("):
            tables.append(tbl)
    return tables


def _purview_type(entry_type: CatalogEntryType) -> str:
    """Map catalog entry type to Purview (Atlas) type name."""
    mapping = {
        CatalogEntryType.PACKAGE: "ssis_package",
        CatalogEntryType.TASK: "ssis_task",
        CatalogEntryType.DATA_FLOW: "ssis_data_flow",
        CatalogEntryType.CONNECTION: "ssis_connection",
        CatalogEntryType.TABLE: "rdbms_table",
        CatalogEntryType.PARAMETER: "ssis_parameter",
        CatalogEntryType.VARIABLE: "ssis_variable",
        CatalogEntryType.EXPRESSION: "ssis_expression",
    }
    return mapping.get(entry_type, "ssis_entity")


# =====================================================================
# Public convenience functions
# =====================================================================


def build_catalog(packages: list[SSISPackage]) -> MetadataCatalog:
    """Build a metadata catalog from parsed SSIS packages.

    Parameters
    ----------
    packages : list[SSISPackage]
        Parsed SSIS packages.

    Returns
    -------
    MetadataCatalog
        Populated catalog with search, browse, and export capabilities.
    """
    catalog = MetadataCatalog()
    catalog.build(packages)
    return catalog


def write_catalog_report(
    catalog: MetadataCatalog,
    output_path: Path,
    *,
    fmt: ExportFormat = ExportFormat.JSON,
) -> Path:
    """Write the catalog to disk.

    Parameters
    ----------
    catalog : MetadataCatalog
        A built catalog.
    output_path : Path
        Output directory or file path.
    fmt : ExportFormat
        Export format.

    Returns
    -------
    Path
        Path to the written file.
    """
    output_dir = Path(output_path)
    if output_dir.suffix:
        output_dir = output_dir.parent
    return catalog.write_catalog(output_dir, fmt=fmt)
