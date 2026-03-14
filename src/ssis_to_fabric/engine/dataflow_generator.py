"""
Dataflow Gen2 Generator
========================
Generates Fabric Dataflow Gen2 definitions from SSIS Data Flow Tasks.
Produces Power Query M (mashup.pq) and query metadata (queryMetadata.json)
for each simple data flow that doesn't require Spark processing.

Dataflow Gen2 supports:
- SQL database sources (OLE DB, ADO.NET)
- File sources (CSV, Excel)
- Column transformations (Derived Column → Table.AddColumn)
- Lookups (→ Table.NestedJoin)
- Aggregations (→ Table.Group)
- Sorting (→ Table.Sort)
- Data type conversions (→ Table.TransformColumnTypes)
- Conditional splits (→ Table.SelectRows)
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.engine.utils import (
    filter_error_columns,
    is_destination,
    is_source,
    is_transform,
    sanitize_name,
)
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from ssis_to_fabric.config import MigrationConfig

logger = get_logger(__name__)


class DataflowGen2Generator:
    """
    Generates Fabric Dataflow Gen2 definitions from SSIS Data Flow Tasks.

    Each SSIS Data Flow Task (with simple transformations) is converted to a
    Dataflow Gen2 item containing:
    - A Power Query M mashup (section Section1; shared QueryName = let ... in ...)
    - Query metadata JSON (formatVersion, queriesMetadata, connections)

    The output is a combined JSON file that the deployer splits into the
    required definition parts for the Fabric REST API.
    """

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config

    def generate(self, package: SSISPackage, task: ControlFlowTask | None, output_dir: Path) -> Path:
        """
        Generate a Dataflow Gen2 definition for a Data Flow Task.

        Args:
            package: The source SSIS package
            task: The Data Flow Task to generate for
            output_dir: Output directory
        Returns:
            Path to the generated dataflow definition JSON file.
        """
        if task is None:
            raise ValueError("Dataflow Gen2 generation requires a specific task")

        if task.task_type != TaskType.DATA_FLOW:
            raise ValueError(f"Expected DATA_FLOW task, got {task.task_type.value}")

        safe_name = self._sanitize_name(f"{package.name}_{task.name}")
        query_name = self._sanitize_name(task.name)

        # Generate Power Query M mashup
        mashup = self._generate_mashup(task, package, query_name)

        # Generate query metadata
        query_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, safe_name))
        metadata = self._generate_metadata(safe_name, query_name, query_id)

        # Combined definition file
        definition = {
            "name": safe_name,
            "description": (f"Dataflow Gen2 migrated from SSIS Data Flow Task: {task.name} (package: {package.name})"),
            "queryMetadata": metadata,
            "mashup": mashup,
        }

        dataflows_dir = output_dir / "dataflows"
        dataflows_dir.mkdir(parents=True, exist_ok=True)
        output_path = dataflows_dir / f"{safe_name}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(definition, f, indent=2)

        # Write destination sidecar manifest
        self._write_destination_manifest(task, dataflows_dir, safe_name)

        logger.info(
            "dataflow_generated",
            task=task.name,
            path=str(output_path),
            query_name=query_name,
        )
        return output_path

    def _write_destination_manifest(
        self,
        task: ControlFlowTask,
        output_dir: Path,
        safe_name: str,
    ) -> None:
        """Write a sidecar JSON manifest describing destination tables/columns.

        This helps downstream steps (e.g. Lakehouse table creation) know what
        each dataflow intends to write without parsing the M code.
        """
        destinations = [c for c in task.data_flow_components if self._is_destination(c)]
        if not destinations:
            return

        entries: list[dict[str, Any]] = []
        for dest in destinations:
            cols = [
                {
                    "name": col.name,
                    "dataType": col.data_type,
                    "length": col.length,
                    "precision": col.precision,
                    "scale": col.scale,
                }
                for col in dest.columns
            ]
            entries.append(
                {
                    "table": dest.table_name or dest.name,
                    "connectionRef": dest.connection_manager_ref,
                    "enableStaging": False,
                    "columns": cols,
                }
            )

        manifest_path = output_dir / f"{safe_name}.destinations.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump({"destinations": entries}, f, indent=2)

    # =========================================================================
    # Mashup (Power Query M) Generation
    # =========================================================================

    def _generate_mashup(self, task: ControlFlowTask, package: SSISPackage, query_name: str) -> str:
        """Generate Power Query M code for the Data Flow Task."""
        # Classify components
        sources = [c for c in task.data_flow_components if self._is_source(c)]
        transforms = [c for c in task.data_flow_components if self._is_transform(c)]
        [c for c in task.data_flow_components if self._is_destination(c)]

        # Collect SSIS variable references from all transforms to declare as PQ parameters
        ssis_params = self._collect_ssis_variable_params(transforms, package)

        steps: list[str] = []
        prev_step = "Source"

        # Source step
        if sources:
            src = sources[0]
            source_code, prev_step = self._source_to_pq(src, package.connection_managers)
            steps.append(f"    Source = {source_code}")
        else:
            steps.append('    Source = #table({"Column1"}, {{"TODO: Configure source"}})')

        # Transformation steps
        for transform in transforms:
            step_name, step_code = self._transform_to_pq(transform, prev_step)
            steps.append(f'    #"{step_name}" = {step_code}')
            prev_step = f'#"{step_name}"'

        # Note: In Dataflow Gen2, the data destination (Lakehouse table,
        # Warehouse table, etc.) is configured via the Fabric UI using
        # "Add data destination" — it is NOT part of the M query code.

        # Build the final M expression
        let_block = ",\n".join(steps)
        result_step = prev_step

        # Build parameter declarations for SSIS variables used in the dataflow
        param_decls = ""
        for pname, pinfo in ssis_params.items():
            m_type = pinfo["m_type"]
            default = pinfo["default"]
            param_decls += (
                f"shared {pname} = {default} meta "
                f'[IsParameterQuery = true, Type = "{m_type}", '
                f"IsParameterQueryRequired = false];\n"
            )

        mashup = f"section Section1;\n{param_decls}shared {query_name} = let\n{let_block}\nin\n    {result_step};"

        return mashup

    def _source_to_pq(self, comp: DataFlowComponent, connections: list[ConnectionManager]) -> tuple[str, str]:
        """Generate Power Query M source expression.

        Returns
        -------
        (m_code, last_step_name)
            *m_code* is the M expression(s) starting from the ``Source`` step.
            *last_step_name* is the identifier of the final step produced
            (e.g. ``"Source"``, ``"Query"``, ``"Table"``, ``"Sheet"``).
        """
        # Find the connection manager
        conn = self._find_connection(comp, connections)
        server = conn.server if conn else "TODO_SERVER"
        database = conn.database if conn else "TODO_DATABASE"

        if comp.component_type in (
            DataFlowComponentType.OLE_DB_SOURCE,
            DataFlowComponentType.ADO_NET_SOURCE,
        ):
            if comp.sql_command:
                # Native query
                sql_escaped = comp.sql_command.replace('"', '""')
                return (
                    f'Sql.Database("{server}", "{database}"),\n'
                    f'    Query = Value.NativeQuery(Source, "{sql_escaped}", '
                    f"null, [EnableFolding = true])",
                    "Query",
                )
            elif comp.table_name:
                # Direct table reference
                parts = comp.table_name.rsplit(".", 1)
                schema = parts[0] if len(parts) > 1 else "dbo"
                table = parts[-1]
                return (
                    f'Sql.Database("{server}", "{database}"),\n'
                    f'    Table = Source{{[Schema="{schema}", '
                    f'Item="{table}"]}}[Data]',
                    "Table",
                )
            return (f'Sql.Database("{server}", "{database}")  // TODO: Configure source query or table', "Source")

        if comp.component_type == DataFlowComponentType.FLAT_FILE_SOURCE:
            file_path = conn.connection_string if conn else "TODO_FILE_PATH"
            return (
                f'Csv.Document(File.Contents("{file_path}"), '
                f'[Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None])',
                "Source",
            )

        if comp.component_type == DataFlowComponentType.EXCEL_SOURCE:
            file_path = conn.connection_string if conn else "TODO_FILE_PATH"
            sheet = comp.properties.get("OpenRowset", "Sheet1$")
            sheet_name = sheet.rstrip("$")
            return (
                f'Excel.Workbook(File.Contents("{file_path}"), null, true),\n'
                f'    Sheet = Source{{[Item="{sheet_name}", Kind="Sheet"]}}[Data]',
                "Sheet",
            )

        if comp.component_type == DataFlowComponentType.ODBC_SOURCE:
            # ODBC → Odbc.DataSource with native query or table
            dsn = conn.connection_string if conn else "TODO_DSN"
            if comp.sql_command:
                sql_escaped = comp.sql_command.replace('"', '""')
                return (
                    f'Odbc.DataSource("{dsn}", [HierarchicalNavigation = true]),\n'
                    f'    Query = Value.NativeQuery(Source, "{sql_escaped}", null)',
                    "Query",
                )
            elif comp.table_name:
                return (
                    f'Odbc.DataSource("{dsn}", [HierarchicalNavigation = true]),\n'
                    f'    Table = Source{{[Name="{comp.table_name}"]}}[Data]',
                    "Table",
                )
            return (
                f'Odbc.DataSource("{dsn}", [HierarchicalNavigation = true])  '
                f"// TODO: Configure ODBC source query or table",
                "Source",
            )

        if comp.component_type == DataFlowComponentType.XML_SOURCE:
            file_path = conn.connection_string if conn else "TODO_XML_PATH"
            return (f'Xml.Tables(File.Contents("{file_path}"))', "Source")

        if comp.component_type == DataFlowComponentType.RAW_FILE_SOURCE:
            file_path = comp.properties.get("FileName", "TODO_RAW_FILE_PATH")
            return (
                f"// TODO: SSIS Raw File has no direct M equivalent — reading as binary\n"
                f'    Binary.Buffer(File.Contents("{file_path}"))',
                "Source",
            )

        if comp.component_type == DataFlowComponentType.CDC_SOURCE:
            if comp.sql_command:
                sql_escaped = comp.sql_command.replace('"', '""')
                return (
                    f'Sql.Database("{server}", "{database}"),\n'
                    f'    Query = Value.NativeQuery(Source, "{sql_escaped}", null, [EnableFolding = true])\n'
                    f"    // TODO: CDC state tracking — configure __$start_lsn / __$operation filtering",
                    "Query",
                )
            elif comp.table_name:
                cdc_table = f"cdc.{comp.table_name}_CT" if not comp.table_name.startswith("cdc.") else comp.table_name
                return (
                    f'Sql.Database("{server}", "{database}"),\n'
                    f'    Table = Source{{[Schema="cdc", Item="{cdc_table.split(".")[-1]}"]}}[Data]\n'
                    f"    // TODO: Filter by __$operation for net changes",
                    "Table",
                )
            return (
                f'Sql.Database("{server}", "{database}")  // TODO: Configure CDC source — use cdc.<table>_CT',
                "Source",
            )

        # Fallback
        return ('// TODO: Configure data source\n    #table({"Column1"}, {{}})', "Source")

    def _transform_to_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Generate a Power Query M transformation step.

        Returns (step_name, m_expression).
        """
        if comp.component_type == DataFlowComponentType.DERIVED_COLUMN:
            return self._gen_derived_column_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.LOOKUP:
            return self._gen_lookup_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.AGGREGATE:
            return self._gen_aggregate_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.SORT:
            return self._gen_sort_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.DATA_CONVERSION:
            return self._gen_data_conversion_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.CONDITIONAL_SPLIT:
            return self._gen_conditional_split_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.UNION_ALL:
            return self._gen_union_all_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.MULTICAST:
            return self._gen_multicast_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.ROW_COUNT:
            return self._gen_row_count_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.PIVOT:
            return self._gen_pivot_pq(comp, prev_step)
        elif comp.component_type == DataFlowComponentType.UNPIVOT:
            return self._gen_unpivot_pq(comp, prev_step)
        else:
            name = self._sanitize_name(comp.name)
            return (
                f"Transform {name}",
                f'{prev_step}  /* TODO: Implement {comp.component_type.value} transform "{comp.name}" */',
            )

    # ---- Individual transform generators ----

    def _gen_derived_column_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Derived Column → Table.AddColumn."""
        if comp.columns:
            col = comp.columns[0]
            col_name = col.name
            expr = col.expression or "null"
            # Convert simple SSIS expressions to M
            m_expr = self._ssis_expr_to_m(expr)
            return (
                f"Added {col_name}",
                f'Table.AddColumn({prev_step}, "{col_name}", each {m_expr})',
            )
        return (
            f"Derived {self._sanitize_name(comp.name)}",
            f'{prev_step}  /* TODO: Add derived column "{comp.name}" */',
        )

    def _gen_lookup_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Lookup → Table.NestedJoin + Table.ExpandTableColumn."""
        table = comp.table_name or "LookupTable"
        sql = comp.sql_command
        if sql:
            lookup_source = (
                f'Value.NativeQuery(Sql.Database("TODO_SERVER", "TODO_DB"), '
                f'"{sql.replace(chr(34), chr(34) + chr(34))}")'
            )
        else:
            lookup_source = f'Sql.Database("TODO_SERVER", "TODO_DB"){{[Schema="dbo", Item="{table}"]}}[Data]'

        # Use extracted join keys when available
        join_keys = comp.properties.get("_join_keys", [])
        ref_keys = comp.properties.get("_ref_keys", [])
        if join_keys:
            left_cols = ", ".join(f'"{k}"' for k in join_keys)
            right_cols = ", ".join(f'"{k}"' for k in (ref_keys or join_keys))
        else:
            left_cols = '"JoinKey"  /* TODO: Set join key columns */'
            right_cols = '"JoinKey"  /* TODO: Set join key columns */'

        return (
            f"Lookup {self._sanitize_name(comp.name)}",
            (
                f"Table.NestedJoin({prev_step}, "
                f"{{{left_cols}}}, {lookup_source}, "
                f'{{{right_cols}}}, "Lookup", JoinKind.LeftOuter)'
            ),
        )

    def _gen_aggregate_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Aggregate → Table.Group."""
        group_cols = comp.properties.get("_group_by", [])
        agg_cols = comp.properties.get("_aggregations", [])

        if group_cols:
            group_str = ", ".join(f'"{c}"' for c in group_cols)
        else:
            group_str = '"GroupColumn"  /* TODO: Set group-by columns */'

        # Build aggregation list
        agg_parts: list[str] = []
        if agg_cols:
            func_map = {
                "count": ("Table.RowCount(_)", "Int64.Type"),
                "sum": ("List.Sum([{col}])", "type number"),
                "avg": ("List.Average([{col}])", "type number"),
                "min": ("List.Min([{col}])", "type number"),
                "max": ("List.Max([{col}])", "type number"),
                "count_distinct": ('Table.RowCount(Table.Distinct(Table.SelectColumns(_, {{"{col}"}})))', "Int64.Type"),
            }
            for a in agg_cols:
                col_name = a.get("column", "value")
                func = a.get("function", "count")
                tpl, m_type = func_map.get(func, ("Table.RowCount(_)", "Int64.Type"))
                m_expr = tpl.replace("{col}", col_name)
                agg_parts.append(f'{{"{col_name}_{func}", each {m_expr}, {m_type}}}')
        else:
            agg_parts.append('{"Count", each Table.RowCount(_), Int64.Type}  /* TODO: Set aggregations */')

        agg_str = ", ".join(agg_parts)
        return (
            f"Grouped {self._sanitize_name(comp.name)}",
            f"Table.Group({prev_step}, {{{group_str}}}, {{{agg_str}}})",
        )

    def _gen_sort_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Sort → Table.Sort."""
        sort_cols = comp.properties.get("_sort_columns", [])

        if sort_cols:
            parts = []
            for sc in sort_cols:
                col_name = sc.get("column", "col")
                direction = "Order.Ascending" if sc.get("direction") == "asc" else "Order.Descending"
                parts.append(f'{{"{col_name}", {direction}}}')
            sort_str = ", ".join(parts)
        else:
            sort_str = '{"SortColumn", Order.Ascending}  /* TODO: Set sort columns */'

        return (
            f"Sorted {self._sanitize_name(comp.name)}",
            f"Table.Sort({prev_step}, {{{sort_str}}})",
        )

    def _gen_data_conversion_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Data Conversion → Table.TransformColumnTypes."""
        type_mappings: list[str] = []
        for col in self._filter_error_columns(comp.columns):
            m_type = self._ssis_type_to_m_type(col.data_type)
            type_mappings.append(f'{{"{col.name}", {m_type}}}')

        if type_mappings:
            mappings_str = ", ".join(type_mappings)
            return (
                "Changed Type",
                f"Table.TransformColumnTypes({prev_step}, {{{mappings_str}}})",
            )
        return (
            "Changed Type",
            f'{prev_step}  /* TODO: Configure type conversions for "{comp.name}" */',
        )

    def _gen_conditional_split_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Conditional Split → Table.SelectRows (primary condition)."""
        conditions = comp.properties.get("_conditions", [])

        if conditions:
            # Use the first condition branch
            cond = conditions[0]
            raw_expr = cond.get("expression", "true")
            # Best-effort SSIS expression → M conversion
            m_expr = self._ssis_expr_to_m(raw_expr)
            return (
                f"Filtered {self._sanitize_name(comp.name)}",
                f"Table.SelectRows({prev_step}, each {m_expr})",
            )

        return (
            f"Filtered {self._sanitize_name(comp.name)}",
            (f'Table.SelectRows({prev_step}, each true)  /* TODO: Implement split condition for "{comp.name}" */'),
        )

    def _gen_union_all_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Union All → Table.Combine."""
        return (
            f"Combined {self._sanitize_name(comp.name)}",
            (f"Table.Combine({{{prev_step}, OtherSource}})  /* TODO: Configure second input source */"),
        )

    def _gen_multicast_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Multicast — in PQ, just pass through (reuse same table)."""
        return (f"Multicast {self._sanitize_name(comp.name)}", prev_step)

    def _gen_row_count_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Row Count — pass through (count is metadata, not a transform)."""
        return (f"RowCount {self._sanitize_name(comp.name)}", prev_step)

    def _gen_pivot_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Pivot → Table.Pivot."""
        return (
            f"Pivoted {self._sanitize_name(comp.name)}",
            (
                f"Table.Pivot({prev_step}, "
                f"Table.Distinct(Table.SelectColumns({prev_step}, "
                f'{{"PivotColumn"}})), "PivotColumn", "ValueColumn")  '
                f"/* TODO: Configure pivot columns */"
            ),
        )

    def _gen_unpivot_pq(self, comp: DataFlowComponent, prev_step: str) -> tuple[str, str]:
        """Unpivot → Table.UnpivotOtherColumns."""
        return (
            f"Unpivoted {self._sanitize_name(comp.name)}",
            (
                f"Table.UnpivotOtherColumns({prev_step}, "
                f'{{"KeyColumn"}}, "Attribute", "Value")  '
                f"/* TODO: Configure key columns to keep */"
            ),
        )

    # =========================================================================
    # Query Metadata Generation
    # =========================================================================

    def _generate_metadata(self, dataflow_name: str, query_name: str, query_id: str) -> dict[str, Any]:
        """Generate the queryMetadata.json content."""
        return {
            "formatVersion": "202502",
            "computeEngineSettings": {
                "allowFastCopy": True,
                "maxConcurrency": 1,
            },
            "name": dataflow_name,
            "queryGroups": [],
            "documentLocale": "en-US",
            "queriesMetadata": {
                query_name: {
                    "queryId": query_id,
                    "queryName": query_name,
                    "queryGroupId": None,
                    "isHidden": False,
                    "loadEnabled": True,
                    "destinationSettings": {
                        "enableStaging": False,
                    },
                }
            },
            "fastCombine": False,
            "allowNativeQueries": True,
            "skipAutomaticTypeAndHeaderDetection": False,
        }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _find_connection(
        self, comp: DataFlowComponent, connections: list[ConnectionManager]
    ) -> ConnectionManager | None:
        """Find the connection manager referenced by a component."""
        if comp.connection_manager_ref:
            for cm in connections:
                if cm.id == comp.connection_manager_ref or cm.name == comp.connection_manager_ref:
                    return cm

        # Type-aware fallback: match connection type to component type
        _comp_conn_affinity: dict[DataFlowComponentType, set[ConnectionType]] = {
            DataFlowComponentType.FLAT_FILE_SOURCE: {ConnectionType.FLAT_FILE, ConnectionType.FILE},
            DataFlowComponentType.FLAT_FILE_DESTINATION: {ConnectionType.FLAT_FILE, ConnectionType.FILE},
            DataFlowComponentType.EXCEL_SOURCE: {ConnectionType.EXCEL},
            DataFlowComponentType.EXCEL_DESTINATION: {ConnectionType.EXCEL},
            DataFlowComponentType.ODBC_SOURCE: {ConnectionType.ODBC},
            DataFlowComponentType.ODBC_DESTINATION: {ConnectionType.ODBC},
            DataFlowComponentType.XML_SOURCE: {ConnectionType.FILE, ConnectionType.HTTP},
        }
        preferred = _comp_conn_affinity.get(comp.component_type)
        if preferred:
            for cm in connections:
                if cm.connection_type in preferred:
                    return cm

        # Fallback: return first SQL-like connection
        for cm in connections:
            if cm.connection_type in (
                ConnectionType.OLEDB,
                ConnectionType.ADO_NET,
                ConnectionType.ODBC,
                ConnectionType.ORACLE,
            ):
                return cm
        return connections[0] if connections else None

    @staticmethod
    def _collect_ssis_variable_params(
        transforms: list[DataFlowComponent], package: SSISPackage
    ) -> dict[str, dict[str, str]]:
        """Scan transforms for SSIS variable references and build PQ parameter declarations.

        Returns a dict mapping parameter name → {"m_type": ..., "default": ...}.
        """
        import re as _re

        params: dict[str, dict[str, str]] = {}
        # Map SSIS data types to PQ parameter types + defaults
        _dtype_map = {
            "i1": ("Number", "0"),
            "i2": ("Number", "0"),
            "i4": ("Number", "0"),
            "i8": ("Number", "0"),
            "bool": ("Logical", "false"),
            "str": ("Text", '""'),
            "": ("Any", "null"),
        }

        for comp in transforms:
            for col in comp.columns:
                if not col.expression:
                    continue
                # Find all @[$Package::X], @[User::X], @[$Project::X] references
                for m in _re.finditer(
                    r"@\[\s*\$?(Package|User|Project)\s*::\s*(\w+)\s*\]",
                    col.expression,
                    _re.IGNORECASE,
                ):
                    var_name = m.group(2)
                    if var_name not in params:
                        dt = col.data_type.lower() if col.data_type else ""
                        m_type, default = _dtype_map.get(dt, ("Any", "null"))
                        params[var_name] = {"m_type": m_type, "default": default}

        return params

    @staticmethod
    def _ssis_expr_to_m(expr: str) -> str:  # noqa: C901 (intentionally complex)
        """Convert an SSIS expression to Power Query M (recursive, full parity).

        Supports date, string, math functions, type casts, nested expressions
        and SSIS variable references.  Complex patterns emit a ``/* TODO */``
        comment so reviewers can handle them manually.
        """
        import re as _re

        if not expr or expr == "null":
            return "null"

        expr = expr.strip()

        # ------------------------------------------------------------------
        # Internal helpers
        # ------------------------------------------------------------------

        def _find_close_paren(s: str, open_pos: int) -> int:
            """Return the index of the closing ')' that balances s[open_pos]='('."""
            depth = 0
            for i in range(open_pos, len(s)):
                if s[i] == "(":
                    depth += 1
                elif s[i] == ")":
                    depth -= 1
                    if depth == 0:
                        return i
            return -1

        def _split_args(s: str) -> list[str]:
            """Split a comma-separated arg string, respecting nested parens/brackets."""
            args: list[str] = []
            depth = 0
            current: list[str] = []
            in_str = False
            for ch in s:
                if ch == '"' and not in_str:
                    in_str = True
                    current.append(ch)
                elif ch == '"' and in_str:
                    in_str = False
                    current.append(ch)
                elif in_str:
                    current.append(ch)
                elif ch in ("(", "["):
                    depth += 1
                    current.append(ch)
                elif ch in (")", "]"):
                    depth -= 1
                    current.append(ch)
                elif ch == "," and depth == 0:
                    args.append("".join(current).strip())
                    current = []
                else:
                    current.append(ch)
            if current:
                args.append("".join(current).strip())
            return [a for a in args if a]

        def _conv(s: str) -> str:
            """Shorthand recursive call."""
            return DataflowGen2Generator._ssis_expr_to_m(s)

        # ------------------------------------------------------------------
        # 1. SSIS variable / parameter references
        # ------------------------------------------------------------------
        _system_var_map = {
            "StartTime": "DateTime.LocalNow()",
            "PackageName": '"PackageName"  /* TODO: replace with actual value */',
            "MachineName": '"MachineName"  /* TODO: replace with actual value */',
            "UserName": '"UserName"  /* TODO: replace with actual value */',
            "ExecutionInstanceGUID": '"00000000-0000-0000-0000-000000000000"  /* TODO */',
        }

        var_match = _re.match(
            r"^@\[\s*\$?(Package|User|Project|System)\s*::\s*(\w+)\s*\]$",
            expr,
            _re.IGNORECASE,
        )
        if var_match:
            ns, var_name = var_match.group(1), var_match.group(2)
            if ns.lower() == "system" and var_name in _system_var_map:
                return _system_var_map[var_name]
            return var_name

        def _replace_var(m: _re.Match) -> str:  # type: ignore[type-arg]
            ns, vn = m.group(1), m.group(2)
            if ns.lower() == "system" and vn in _system_var_map:
                return str(_system_var_map[vn])
            return str(vn)

        expr = _re.sub(
            r"@\[\s*\$?(Package|User|Project|System)\s*::\s*(\w+)\s*\]",
            _replace_var,
            expr,
            flags=_re.IGNORECASE,
        )

        # ------------------------------------------------------------------
        # 2. Type casts (DT_*) — only simple "cast operand" patterns
        # ------------------------------------------------------------------
        expr = _re.sub(r"\(DT_STR\s*,\s*\d+\s*,\s*\d+\s*\)\s*(\w+)", r"Text.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_WSTR\s*,\s*\d+\s*\)\s*(\w+)", r"Text.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_I4\)\s*(\w+)", r"Int64.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_I2\)\s*(\w+)", r"Int32.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_I8\)\s*(\w+)", r"Int64.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_R4\)\s*(\w+)", r"Number.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_R8\)\s*(\w+)", r"Number.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_BOOL\)\s*(\w+)", r"Logical.From([\1])", expr, flags=_re.IGNORECASE)
        _pat_dec = r"\(DT_DECIMAL\s*,\s*\d+\s*\)\s*(\w+)"
        expr = _re.sub(_pat_dec, r"Decimal.From([\1])", expr, flags=_re.IGNORECASE)
        _pat_num = r"\(DT_NUMERIC\s*,\s*\d+\s*,\s*\d+\s*\)\s*(\w+)"
        expr = _re.sub(_pat_num, r"Decimal.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_CY\)\s*(\w+)", r"Currency.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_GUID\)\s*(\w+)", r"Text.From([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\(DT_BYTES\s*,\s*\d+\s*\)\s*(\w+)", r"Binary.From([\1])", expr, flags=_re.IGNORECASE)

        # ------------------------------------------------------------------
        # 3. Top-level function call — dispatch & recurse into arguments
        # ------------------------------------------------------------------
        func_match = _re.match(r"^([A-Z_][A-Z0-9_]*)\s*\(", expr, _re.IGNORECASE)
        if func_match:
            func_name = func_match.group(1).upper()
            open_idx = expr.index("(", func_match.start())
            close_idx = _find_close_paren(expr, open_idx)
            if close_idx >= 0:
                args_raw = expr[open_idx + 1 : close_idx]
                args = _split_args(args_raw)
                tail = expr[close_idx + 1 :].strip()

                # ---- String functions ----
                if func_name == "UPPER" and len(args) == 1:
                    return f"Text.Upper({_conv(args[0])}){tail}"
                if func_name == "LOWER" and len(args) == 1:
                    return f"Text.Lower({_conv(args[0])}){tail}"
                if func_name == "TRIM" and len(args) == 1:
                    return f"Text.Trim({_conv(args[0])}){tail}"
                if func_name == "LTRIM" and len(args) == 1:
                    return f"Text.TrimStart({_conv(args[0])}){tail}"
                if func_name == "RTRIM" and len(args) == 1:
                    return f"Text.TrimEnd({_conv(args[0])}){tail}"
                if func_name == "LEN" and len(args) == 1:
                    return f"Text.Length({_conv(args[0])}){tail}"
                if func_name == "REVERSE" and len(args) == 1:
                    return f"Text.Reverse({_conv(args[0])}){tail}"
                if func_name == "REPLACE" and len(args) == 3:
                    return f"Text.Replace({_conv(args[0])}, {_conv(args[1])}, {_conv(args[2])}){tail}"
                if func_name == "SUBSTRING" and len(args) == 3:
                    start_arg = args[1].strip()
                    start_m = str(int(start_arg) - 1) if start_arg.isdigit() else f"({_conv(start_arg)} - 1)"
                    return f"Text.Middle({_conv(args[0])}, {start_m}, {_conv(args[2])}){tail}"
                if func_name == "LEFT" and len(args) == 2:
                    return f"Text.Start({_conv(args[0])}, {_conv(args[1])}){tail}"
                if func_name == "RIGHT" and len(args) == 2:
                    return f"Text.End({_conv(args[0])}, {_conv(args[1])}){tail}"
                if func_name == "FINDSTRING" and len(args) >= 2:
                    occ = f"  /* occurrence={_conv(args[2])} */" if len(args) >= 3 else ""
                    return f"Text.PositionOf({_conv(args[0])}, {_conv(args[1])}){occ}{tail}"
                if func_name == "TOKEN" and len(args) >= 2:
                    occ = _conv(args[2]) if len(args) >= 3 else "1"
                    return f"List.ItemAt(Text.Split({_conv(args[0])}, {_conv(args[1])}), {occ} - 1){tail}"
                if func_name == "TOKENCOUNT" and len(args) == 2:
                    return f"List.Count(Text.Split({_conv(args[0])}, {_conv(args[1])}))"
                if func_name == "CODEPOINT" and len(args) == 1:
                    return f"Character.ToNumber(Text.At({_conv(args[0])}, 0)){tail}"
                if func_name == "HEX" and len(args) == 1:
                    return f"Number.ToText({_conv(args[0])}, \"X\"){tail}"
                if func_name == "REPLACENULL" and len(args) == 2:
                    col = _conv(args[0])
                    val = _conv(args[1])
                    return f"(if {col} = null then {val} else {col}){tail}"
                if func_name == "ISNULL" and len(args) == 1:
                    return f"({_conv(args[0])} = null){tail}"
                if func_name == "GETDATE" and len(args) == 0:
                    return f"DateTime.LocalNow(){tail}"

                # ---- Date functions ----
                if func_name == "DATEADD" and len(args) == 3:
                    part = args[0].strip().strip('"').strip("'").upper()
                    _dateadd_map = {
                        "YEAR": "Date.AddYears",
                        "YY": "Date.AddYears",
                        "YYYY": "Date.AddYears",
                        "MONTH": "Date.AddMonths",
                        "MM": "Date.AddMonths",
                        "DAY": "Date.AddDays",
                        "DD": "Date.AddDays",
                        "D": "Date.AddDays",
                        "WEEK": "Date.AddWeeks",
                        "WK": "Date.AddWeeks",
                        "WW": "Date.AddWeeks",
                    }
                    m_func = _dateadd_map.get(part)
                    if m_func:
                        return f"{m_func}({_conv(args[1])}, {_conv(args[2])}){tail}"
                    return f"/* TODO: DATEADD({part}, ...) */ {_conv(args[1])}{tail}"

                if func_name == "DATEDIFF" and len(args) == 3:
                    part = args[0].strip().strip('"').strip("'").upper()
                    a, b = _conv(args[1]), _conv(args[2])
                    _datediff_map = {
                        "DAY": "Duration.Days",
                        "DD": "Duration.Days",
                        "D": "Duration.Days",
                        "HOUR": "Duration.TotalHours",
                        "HH": "Duration.TotalHours",
                        "MINUTE": "Duration.TotalMinutes",
                        "MI": "Duration.TotalMinutes",
                        "N": "Duration.TotalMinutes",
                        "SECOND": "Duration.TotalSeconds",
                        "SS": "Duration.TotalSeconds",
                        "S": "Duration.TotalSeconds",
                    }
                    diff_func = _datediff_map.get(part, "Duration.Days")
                    prefix = "" if part in _datediff_map else f"/* TODO: DATEDIFF({part}, ...) */ "
                    return f"{prefix}{diff_func}({b} - {a}){tail}"

                if func_name == "DATEPART" and len(args) == 2:
                    part = args[0].strip().strip('"').strip("'").upper()
                    _datepart_map = {
                        "YEAR": "Date.Year",
                        "YY": "Date.Year",
                        "YYYY": "Date.Year",
                        "MONTH": "Date.Month",
                        "MM": "Date.Month",
                        "DAY": "Date.Day",
                        "DD": "Date.Day",
                        "D": "Date.Day",
                        "HOUR": "Time.Hour",
                        "HH": "Time.Hour",
                        "MINUTE": "Time.Minute",
                        "MI": "Time.Minute",
                        "N": "Time.Minute",
                        "SECOND": "Time.Second",
                        "SS": "Time.Second",
                        "S": "Time.Second",
                        "DAYOFWEEK": "Date.DayOfWeek",
                        "DW": "Date.DayOfWeek",
                        "WEEK": "Date.WeekOfYear",
                        "WK": "Date.WeekOfYear",
                        "WW": "Date.WeekOfYear",
                    }
                    dp_func = _datepart_map.get(part)
                    if dp_func:
                        return f"{dp_func}({_conv(args[1])}){tail}"
                    return f"/* TODO: DATEPART({part}, ...) */ Date.Year({_conv(args[1])}){tail}"

                if func_name == "YEAR" and len(args) == 1:
                    return f"Date.Year({_conv(args[0])}){tail}"
                if func_name == "MONTH" and len(args) == 1:
                    return f"Date.Month({_conv(args[0])}){tail}"
                if func_name == "DAY" and len(args) == 1:
                    return f"Date.Day({_conv(args[0])}){tail}"

                # ---- Math functions ----
                if func_name == "ABS" and len(args) == 1:
                    return f"Number.Abs({_conv(args[0])}){tail}"
                if func_name == "CEILING" and len(args) == 1:
                    return f"Number.RoundUp({_conv(args[0])}){tail}"
                if func_name == "FLOOR" and len(args) == 1:
                    return f"Number.RoundDown({_conv(args[0])}){tail}"
                if func_name == "ROUND":
                    if len(args) == 2:
                        return f"Number.Round({_conv(args[0])}, {_conv(args[1])}){tail}"
                    if len(args) == 1:
                        return f"Number.Round({_conv(args[0])}){tail}"
                if func_name == "POWER" and len(args) == 2:
                    return f"Number.Power({_conv(args[0])}, {_conv(args[1])}){tail}"
                if func_name == "SQRT" and len(args) == 1:
                    return f"Number.Sqrt({_conv(args[0])}){tail}"
                if func_name == "SIGN" and len(args) == 1:
                    return f"Number.Sign({_conv(args[0])}){tail}"
                if func_name == "EXP" and len(args) == 1:
                    return f"Number.Power(Number.E, {_conv(args[0])}){tail}"
                if func_name == "LOG" and len(args) == 1:
                    return f"Number.Log({_conv(args[0])}){tail}"
                if func_name == "LOG10" and len(args) == 1:
                    return f"Number.Log10({_conv(args[0])}){tail}"
                if func_name == "LOG2" and len(args) == 1:
                    return f"Number.Log(Number.Log({_conv(args[0])}) / Number.Log(2)){tail}"

                # ---- Trig functions ----
                if func_name in ("SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN") and len(args) == 1:
                    return f"Number.{func_name.capitalize()}({_conv(args[0])}){tail}"
                if func_name == "ATAN2" and len(args) == 2:
                    return f"Number.Atan2({_conv(args[0])}, {_conv(args[1])}){tail}"

                # ---- Constants ----
                if func_name == "PI" and len(args) == 0:
                    return f"Number.PI{tail}"
                if func_name == "RAND" and len(args) <= 1:
                    return f"Number.Random(){tail}"

                # ---- Bitwise operators ----
                if func_name == "BITAND" and len(args) == 2:
                    return f"Number.BitwiseAnd({_conv(args[0])}, {_conv(args[1])}){tail}"
                if func_name == "BITOR" and len(args) == 2:
                    return f"Number.BitwiseOr({_conv(args[0])}, {_conv(args[1])}){tail}"
                if func_name == "BITXOR" and len(args) == 2:
                    return f"Number.BitwiseXor({_conv(args[0])}, {_conv(args[1])}){tail}"

                if func_name == "GETUTCDATE" and len(args) == 0:
                    return f"DateTimeZone.UtcNow(){tail}"

        # ------------------------------------------------------------------
        # 4. Miscellaneous non-function patterns
        # ------------------------------------------------------------------
        # GETDATE() anywhere in expression
        expr = _re.sub(r"\bGETDATE\s*\(\s*\)", "DateTime.LocalNow()", expr, flags=_re.IGNORECASE)
        # GETUTCDATE()
        expr = _re.sub(r"\bGETUTCDATE\s*\(\s*\)", "DateTimeZone.UtcNow()", expr, flags=_re.IGNORECASE)
        # ISNULL(simple_col)
        expr = _re.sub(r"\bISNULL\s*\(\s*(\w+)\s*\)", r"[\1] = null", expr, flags=_re.IGNORECASE)
        # YEAR/MONTH/DAY(simple_col)
        expr = _re.sub(r"\bYEAR\s*\(\s*(\w+)\s*\)", r"Date.Year([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\bMONTH\s*\(\s*(\w+)\s*\)", r"Date.Month([\1])", expr, flags=_re.IGNORECASE)
        expr = _re.sub(r"\bDAY\s*\(\s*(\w+)\s*\)", r"Date.Day([\1])", expr, flags=_re.IGNORECASE)
        # Concatenation: + → &
        expr = expr.replace(" + ", " & ")
        # Ternary: cond ? a : b → if cond then a else b
        ternary = _re.match(r"^(.+?)\s*\?\s*(.+?)\s*:\s*(.+)$", expr)
        if ternary:
            return f"if {ternary.group(1)} then {ternary.group(2)} else {ternary.group(3)}"

        return expr

    @staticmethod
    def _ssis_type_to_m_type(ssis_type: str) -> str:
        """Map SSIS data type to Power Query M type."""
        type_map = {
            "str": "type text",
            "i4": "Int64.Type",
            "i2": "Int32.Type",
            "i1": "Int16.Type",
            "bool": "type logical",
            "numeric": "type number",
            "decimal": "type number",
            "float": "type number",
            "double": "type number",
            "dbTimeStamp": "type datetime",
            "date": "type date",
            "time": "type time",
            "guid": "type text",
            "binary": "type binary",
        }
        return type_map.get(ssis_type.lower(), "type text")

    @staticmethod
    def _is_source(comp: DataFlowComponent) -> bool:
        return is_source(comp)

    @staticmethod
    def _is_destination(comp: DataFlowComponent) -> bool:
        return is_destination(comp)

    @staticmethod
    def _is_transform(comp: DataFlowComponent) -> bool:
        return is_transform(comp) and comp.component_type != DataFlowComponentType.SCRIPT_COMPONENT

    @staticmethod
    def _sanitize_name(name: str) -> str:
        return sanitize_name(name)

    @staticmethod
    def _filter_error_columns(columns: list[Any]) -> list[Any]:
        return filter_error_columns(columns)
