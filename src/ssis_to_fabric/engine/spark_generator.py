"""
Spark Notebook Generator
=========================
Generates PySpark notebooks for complex SSIS data flow transformations
that cannot be directly expressed as Data Factory pipelines.
Outputs Fabric-compatible .py notebook files.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from textwrap import dedent
from typing import TYPE_CHECKING

from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from ssis_to_fabric.config import MigrationConfig

logger = get_logger(__name__)


class SparkNotebookGenerator:
    """
    Generates PySpark notebooks for SSIS tasks that require Spark processing.

    Handles:
    - Complex data flows with transformations (Derived Column, Lookup, etc.)
    - Script components (generates skeleton with TODO markers)
    - Aggregations, pivots, conditional splits
    - Multi-source joins (Merge Join)
    """

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config

    def generate(self, package: SSISPackage, task: ControlFlowTask | None, output_dir: Path) -> Path:
        """
        Generate a PySpark notebook for a specific task.

        Args:
            package: The source SSIS package
            task: The task to generate code for
            output_dir: Output directory
        Returns:
            Path to the generated notebook file.
        """
        if task is None:
            # Generate a master notebook that calls sub-notebooks
            return self._generate_orchestrator_notebook(package, output_dir)

        notebook_code = self._generate_notebook_code(package, task)

        notebooks_dir = output_dir / "notebooks"
        notebooks_dir.mkdir(parents=True, exist_ok=True)

        safe_name = self._sanitize_name(f"{package.name}_{task.name}")
        output_path = notebooks_dir / f"{safe_name}.py"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(notebook_code)

        # Write destination sidecar manifest for Data Flow tasks
        if task.task_type == TaskType.DATA_FLOW:
            self._write_destination_manifest(task, notebooks_dir, safe_name)

        logger.info("notebook_generated", task=task.name, path=str(output_path))
        return output_path

    def _generate_notebook_code(self, package: SSISPackage, task: ControlFlowTask) -> str:
        """Generate the full PySpark notebook code."""
        sections = [
            self._generate_header(package, task),
            self._generate_imports(),
            self._generate_config_section(package),
        ]

        if task.task_type == TaskType.DATA_FLOW:
            sections.extend(self._generate_data_flow_code(task, package))
        elif task.task_type == TaskType.EXECUTE_SQL:
            sections.append(self._generate_sql_execution(task))
        elif task.task_type == TaskType.SCRIPT:
            sections.append(self._generate_script_placeholder(task))
        elif task.task_type == TaskType.EXECUTE_PROCESS:
            sections.append(self._generate_process_placeholder(task))
        else:
            sections.append(self._generate_generic_placeholder(task))

        sections.append(self._generate_footer(task))

        return "\n\n".join(sections)

    # =========================================================================
    # Code Sections
    # =========================================================================

    def _generate_header(self, package: SSISPackage, task: ControlFlowTask) -> str:
        """Generate notebook header with metadata."""
        return dedent(f"""\
            # Fabric Notebook
            # Migrated from SSIS Package: {package.name}
            # Task: {task.name}
            # Task Type: {task.task_type.value}
            # Generated: {datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC
            # Migration Complexity: {task.migration_complexity.value}
            #
            # NOTE: Review all TODO comments before running in production.
            # This notebook was auto-generated and may require adjustments.
        """)

    def _generate_imports(self) -> str:
        """Generate standard imports."""
        return dedent("""\
            # --- Imports ---
            from pyspark.sql import SparkSession, DataFrame
            from pyspark.sql import functions as F
            from pyspark.sql.types import *
            from pyspark.sql.window import Window
            from datetime import datetime
            import logging

            logger = logging.getLogger(__name__)

            # Fabric-specific utilities (available in Fabric notebook runtime)
            try:
                import notebookutils
            except ImportError:
                notebookutils = None  # type: ignore[assignment]
        """)

    def _resolve_notebook_connection_id(self, conn_ref: str, connections: list) -> str:
        """Resolve an SSIS connection-manager reference to a Fabric connection ID.

        Uses ``config.connection_mappings`` when available, otherwise emits a
        TODO placeholder so the user can fill in the ID later.
        """
        if not conn_ref:
            return ""
        conn_name = conn_ref
        for cm in connections:
            if cm.id == conn_ref or cm.name == conn_ref:
                conn_name = cm.name
                break
        mapped_id = self.config.connection_mappings.mappings.get(conn_name, "")
        if mapped_id:
            return mapped_id
        return f"{conn_name}  -- TODO: replace with Fabric connection id"

    def _generate_config_section(self, package: SSISPackage) -> str:
        """Generate configuration section that reads parameters at runtime.

        Parameters flow into the notebook via two mechanisms:

        1. **Pipeline parameters** – When the notebook is invoked from a
           Fabric Data Factory pipeline (TridentNotebook activity) the
           pipeline can forward its parameters.  The notebook reads them
           with ``notebookutils.notebook.getContext()``.
        2. **Variable Library fallback** – When the notebook is executed
           standalone (e.g. during development) it falls back to the
           workspace Variable Library for shared configuration.

        Connections are referenced by their Fabric connection ID, exactly
        like in Data Factory pipelines.  At runtime the notebook fetches
        credentials/JDBC-URL through ``notebookutils.credentials``.
        """
        lines = [
            "# --- Parameters (from pipeline or Variable Library) ---",
            "# When run from a pipeline, parameters are injected by the",
            "# TridentNotebook activity.  When run standalone, defaults are used.",
            "",
            "try:",
            "    _ctx = notebookutils.notebook.getContext()",
            "    _params = _ctx.get('currentRunConfig', {}).get('parameters', {})",
            "except Exception:",
            "    _params = {}",
            "",
            "",
            "def _get_param(name: str, default: str = '') -> str:",
            '    """Read a parameter from the pipeline context, falling back to the default."""',
            "    return _params.get(name, default)",
            "",
        ]

        # Emit each project/package parameter as a Python variable
        emitted: set[str] = set()
        for param in package.project_parameters:
            if param.name in emitted:
                continue
            emitted.add(param.name)
            safe = self._sanitize_name(param.name).upper()
            default = (param.value or "").replace('"', '\\"')
            lines.append(f'{safe} = _get_param("{param.name}", "{default}")')

        for param in package.parameters:
            if param.name in emitted:
                continue
            emitted.add(param.name)
            safe = self._sanitize_name(param.name).upper()
            default = (param.value or "").replace('"', '\\"')
            lines.append(f'{safe} = _get_param("{param.name}", "{default}")')

        # Also expose User-scoped variables
        for var in package.variables:
            if var.namespace == "System" or var.name in emitted:
                continue
            emitted.add(var.name)
            safe = self._sanitize_name(var.name).upper()
            default = (var.value or "").replace('"', '\\"')
            lines.append(f'{safe} = _get_param("{var.name}", "{default}")')

        lines.append("")

        # --- Fabric Connections ---
        # Map each SSIS connection manager to a Fabric connection ID.
        # This mirrors Data Factory pipelines' externalReferences.connection.
        lines.append("# --- Fabric Connections ---")
        lines.append("# Map SSIS connection managers to Fabric connection IDs.")
        lines.append("# Update the IDs below to match your Fabric workspace connections.")
        lines.append("# (same IDs used by Data Factory pipeline externalReferences)")
        lines.append("_FABRIC_CONNECTIONS = {")
        for cm in package.connection_managers:
            conn_id = self._resolve_notebook_connection_id(cm.name, package.connection_managers)
            comment_parts = [cm.connection_type.value]
            if cm.server:
                comment_parts.append(cm.server)
            if cm.database:
                comment_parts.append(cm.database)
            comment = " / ".join(comment_parts)
            lines.append(f'    "{cm.name}": "{conn_id}",  # {comment}')
        lines.append("}")
        lines.append("")

        # Helper functions
        lines.append("")
        lines.append("def _get_connection_id(conn_name: str) -> str:")
        lines.append('    """Get a Fabric connection ID for an SSIS connection manager."""')
        lines.append('    conn_id = _FABRIC_CONNECTIONS.get(conn_name, "")')
        lines.append('    if not conn_id or "TODO" in conn_id:')
        lines.append('        raise ValueError(')
        lines.append('            f"Fabric connection not configured for: {conn_name}. "')
        lines.append('            f"Update _FABRIC_CONNECTIONS dict with the Fabric connection ID."')
        lines.append('        )')
        lines.append('    return conn_id')
        lines.append("")
        lines.append("")
        lines.append("def _jdbc_url_for(conn_name: str) -> str:")
        lines.append('    """Get JDBC connection string from a Fabric connection."""')
        lines.append('    conn_id = _get_connection_id(conn_name)')
        lines.append('    return notebookutils.credentials.getConnectionStringOrCreds(conn_id)')
        lines.append("")

        return "\n".join(lines)

    def _generate_data_flow_code(self, task: ControlFlowTask, package: SSISPackage) -> list[str]:
        """Generate PySpark code for a Data Flow task."""
        code_sections = []

        # Group components by type
        sources = [c for c in task.data_flow_components if self._is_source(c)]
        transforms = [c for c in task.data_flow_components if self._is_transform(c)]
        destinations = [c for c in task.data_flow_components if self._is_destination(c)]

        # Generate source reads
        if sources:
            code_sections.append("# === Source Data Reads ===")
            for i, src in enumerate(sources):
                code_sections.append(self._generate_source_read(src, i))

        # Generate transformations
        if transforms:
            code_sections.append("# === Transformations ===")
            for transform in transforms:
                code_sections.append(self._generate_transformation(transform))

        # Generate destination writes
        if destinations:
            code_sections.append("# === Destination Writes ===")
            for dest in destinations:
                code_sections.append(self._generate_destination_write(dest, package))

        return code_sections

    def _generate_source_read(self, comp: DataFlowComponent, index: int) -> str:
        """Generate PySpark code to read from a source.

        For database sources (OLE DB, ADO.NET, ODBC) the generated code
        uses the Fabric connection pattern: the ``_jdbc_url_for()`` helper
        fetches the JDBC URL from the Fabric connection at runtime via
        ``notebookutils.credentials``, exactly like pipelines reference
        ``externalReferences.connection``.
        """
        var_name = f"df_source_{self._sanitize_name(comp.name).lower()}"
        conn_name = comp.connection_manager_ref or ""

        # --- Flat File → spark.read.csv ---
        if comp.component_type == DataFlowComponentType.FLAT_FILE_SOURCE:
            file_path = comp.properties.get("_file_path", "TODO_FILE_PATH")
            return dedent(f'''\
                # Source: {comp.name} (Flat File)
                {var_name} = spark.read.csv(
                    "{file_path}",
                    header=True,
                    inferSchema=True,
                )
                logger.info(f"Read {{count}} rows from flat file", count={var_name}.count())
            ''')

        # --- Excel → spark.read.format("com.crealytics.spark.excel") ---
        if comp.component_type == DataFlowComponentType.EXCEL_SOURCE:
            file_path = comp.properties.get("_file_path", "TODO_FILE_PATH")
            sheet = comp.properties.get("OpenRowset", "Sheet1$").rstrip("$")
            return dedent(f'''\
                # Source: {comp.name} (Excel)
                {var_name} = spark.read.format("com.crealytics.spark.excel") \\
                    .option("header", "true") \\
                    .option("dataAddress", "'{sheet}'!A1") \\
                    .option("inferSchema", "true") \\
                    .load("{file_path}")
                logger.info(f"Read {{count}} rows from Excel sheet {sheet}", count={var_name}.count())
            ''')

        # --- XML Source ---
        if comp.component_type == DataFlowComponentType.XML_SOURCE:
            file_path = comp.properties.get("_file_path", "TODO_XML_PATH")
            return dedent(f'''\
                # Source: {comp.name} (XML)
                {var_name} = spark.read.format("xml") \\
                    .option("rowTag", "row") \\
                    .load("{file_path}")
                logger.info(f"Read {{count}} rows from XML", count={var_name}.count())
            ''')

        # --- Raw File Source ---
        if comp.component_type == DataFlowComponentType.RAW_FILE_SOURCE:
            file_path = comp.properties.get("FileName", "TODO_RAW_FILE_PATH")
            return dedent(f'''\
                # Source: {comp.name} (Raw File)
                # SSIS Raw File format has no direct Spark equivalent
                # TODO: Convert the raw file to Parquet/CSV first, or use a custom reader
                {var_name} = spark.read.parquet("{file_path}")
                logger.info(f"Read {{count}} rows from raw file", count={var_name}.count())
            ''')

        # --- CDC Source ---
        if comp.component_type == DataFlowComponentType.CDC_SOURCE:
            if comp.sql_command:
                return self._gen_jdbc_source_read(comp, var_name, conn_name, query=comp.sql_command, label="CDC")
            table = comp.table_name or "TODO_TABLE"
            cdc_table = f"cdc.{table}_CT" if not table.startswith("cdc.") else table
            return self._gen_jdbc_source_read(comp, var_name, conn_name, table=cdc_table, label="CDC")

        # --- ODBC Source → Fabric Connection ---
        if comp.component_type == DataFlowComponentType.ODBC_SOURCE:
            if comp.sql_command:
                return self._gen_jdbc_source_read(comp, var_name, conn_name, query=comp.sql_command, label="ODBC")
            table = comp.table_name or "TODO_TABLE"
            return self._gen_jdbc_source_read(comp, var_name, conn_name, table=table, label="ODBC")

        # --- OLE DB / ADO.NET → Fabric Connection ---
        if comp.sql_command:
            return self._gen_jdbc_source_read(comp, var_name, conn_name, query=comp.sql_command)

        if comp.table_name:
            return self._gen_jdbc_source_read(comp, var_name, conn_name, table=comp.table_name)

        return dedent(f"""\
            # Source: {comp.name}
            # TODO: Configure source data read
            {var_name} = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")
        """)

    def _gen_jdbc_source_read(
        self,
        comp: DataFlowComponent,
        var_name: str,
        conn_name: str,
        *,
        query: str = "",
        table: str = "",
        label: str = "",
    ) -> str:
        """Generate a JDBC read using the Fabric connection helper."""
        type_label = f" ({label})" if label else ""
        conn_comment = f"  # Fabric Connection: {conn_name}" if conn_name else ""
        conn_arg = f'"{conn_name}"' if conn_name else '"TODO_CONNECTION_NAME"  # TODO: set connection name'
        lines = [
            f"# Source: {comp.name}{type_label}",
            f"# Uses Fabric connection (same as pipeline externalReferences)",
            f"{var_name} = spark.read.format(\"jdbc\") \\",
            f"    .option(\"url\", _jdbc_url_for({conn_arg})){conn_comment} \\",
        ]
        if query:
            # Use triple-quoted string for SQL
            lines.append(f'    .option("query", """{query}""") \\')
        elif table:
            lines.append(f'    .option("dbtable", "{table}") \\')
        lines.append("    .load()")
        lines.append(f'logger.info(f"Read {{{{count}}}} rows from {comp.name}", count={var_name}.count())')
        return "\n".join(lines)

    def _generate_transformation(self, comp: DataFlowComponent) -> str:
        """Generate PySpark code for a transformation component."""
        if comp.component_type == DataFlowComponentType.DERIVED_COLUMN:
            return self._gen_derived_column(comp)
        elif comp.component_type == DataFlowComponentType.CONDITIONAL_SPLIT:
            return self._gen_conditional_split(comp)
        elif comp.component_type == DataFlowComponentType.LOOKUP:
            return self._gen_lookup(comp)
        elif comp.component_type == DataFlowComponentType.MERGE_JOIN:
            return self._gen_merge_join(comp)
        elif comp.component_type == DataFlowComponentType.AGGREGATE:
            return self._gen_aggregate(comp)
        elif comp.component_type == DataFlowComponentType.SORT:
            return self._gen_sort(comp)
        elif comp.component_type == DataFlowComponentType.UNION_ALL:
            return self._gen_union_all(comp)
        elif comp.component_type == DataFlowComponentType.DATA_CONVERSION:
            return self._gen_data_conversion(comp)
        elif comp.component_type == DataFlowComponentType.PIVOT:
            return self._gen_pivot(comp)
        elif comp.component_type == DataFlowComponentType.UNPIVOT:
            return self._gen_unpivot(comp)
        elif comp.component_type == DataFlowComponentType.SCRIPT_COMPONENT:
            return self._gen_script_component(comp)
        elif comp.component_type == DataFlowComponentType.MULTICAST:
            return self._gen_multicast(comp)
        elif comp.component_type == DataFlowComponentType.ROW_COUNT:
            return self._gen_row_count(comp)
        elif comp.component_type == DataFlowComponentType.OLE_DB_COMMAND:
            return self._gen_oledb_command(comp)
        elif comp.component_type == DataFlowComponentType.SLOWLY_CHANGING_DIMENSION:
            return self._gen_scd(comp)
        elif comp.component_type == DataFlowComponentType.FUZZY_LOOKUP:
            return self._gen_fuzzy_lookup(comp)
        elif comp.component_type == DataFlowComponentType.FUZZY_GROUPING:
            return self._gen_fuzzy_grouping(comp)
        elif comp.component_type == DataFlowComponentType.TERM_LOOKUP:
            return self._gen_term_lookup(comp)
        elif comp.component_type == DataFlowComponentType.COPY_COLUMN:
            return self._gen_copy_column(comp)
        elif comp.component_type == DataFlowComponentType.CHARACTER_MAP:
            return self._gen_character_map(comp)
        elif comp.component_type == DataFlowComponentType.AUDIT:
            return self._gen_audit(comp)
        elif comp.component_type == DataFlowComponentType.MERGE:
            return self._gen_merge(comp)
        elif comp.component_type == DataFlowComponentType.CDC_SPLITTER:
            return self._gen_cdc_splitter(comp)
        elif comp.component_type == DataFlowComponentType.PERCENTAGE_SAMPLING:
            return self._gen_percentage_sampling(comp)
        elif comp.component_type == DataFlowComponentType.ROW_SAMPLING:
            return self._gen_row_sampling(comp)
        elif comp.component_type == DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR:
            return self._gen_balanced_data_distributor(comp)
        elif comp.component_type == DataFlowComponentType.CACHE_TRANSFORM:
            return self._gen_cache_transform(comp)
        else:
            return self._gen_unknown_transform(comp)

    def _gen_derived_column(self, comp: DataFlowComponent) -> str:
        """Generate Derived Column transformation code."""
        lines = [f"# Derived Column: {comp.name}"]

        if comp.columns:
            for col in comp.columns:
                ssis_expr = col.expression or ""
                if ssis_expr:
                    pyspark_expr = self._ssis_expr_to_pyspark(ssis_expr)
                    lines.append(f'df = df.withColumn("{col.name}", {pyspark_expr})  # SSIS: {ssis_expr}')
                else:
                    lines.append(f'df = df.withColumn("{col.name}", F.lit(None))  # TODO: Set derived expression')
        else:
            lines.append('df = df.withColumn("new_column", F.lit(None))  # TODO: Add derived column logic')
        return "\n".join(lines)

    def _gen_conditional_split(self, comp: DataFlowComponent) -> str:
        """Generate Conditional Split transformation code."""
        conditions = comp.properties.get("_conditions", [])

        if conditions:
            lines = [f"# Conditional Split: {comp.name}"]
            for i, cond in enumerate(conditions):
                out_name = cond.get("output", f"branch_{i}")
                expr = cond.get("expression", "True")
                safe_name = self._sanitize_name(out_name).lower()
                lines.append(f'df_{safe_name} = df.filter(F.expr("{expr}"))  # Branch: {out_name}')
            lines.append("df_default = df  # Default branch (rows not matching conditions above)")
            return "\n".join(lines)

        return dedent(f"""\
            # Conditional Split: {comp.name}
            # TODO: Define split conditions based on SSIS expressions
            df_condition_met = df.filter(F.lit(True))  # TODO: Replace with actual condition
            df_default = df.filter(F.lit(False))  # TODO: Replace with default condition
        """)

    def _gen_lookup(self, comp: DataFlowComponent) -> str:
        """Generate Lookup transformation code."""
        lookup_table = comp.table_name or "lookup_table"
        join_keys = comp.properties.get("_join_keys", [])
        ref_keys = comp.properties.get("_ref_keys", [])

        if join_keys and ref_keys and len(join_keys) == len(ref_keys):
            # Build explicit join condition
            conditions = " & ".join(
                f'(df["{jk}"] == df_lookup["{rk}"])' for jk, rk in zip(join_keys, ref_keys, strict=False)
            )
            join_expr = conditions
        elif join_keys:
            # Same column names on both sides
            join_expr = f'"{join_keys[0]}"' if len(join_keys) == 1 else repr(join_keys)
        else:
            join_expr = '"join_key"  # TODO: Set correct join key(s)'

        return dedent(f"""\
            # Lookup: {comp.name}
            df_lookup = spark.sql("SELECT * FROM {lookup_table}")  # TODO: verify lookup query
            df = df.join(
                df_lookup,
                on={join_expr},
                how="left"       # Match type: left = partial match, inner = full match
            )
        """)

    def _gen_merge_join(self, comp: DataFlowComponent) -> str:
        """Generate Merge Join transformation code."""
        join_type = comp.properties.get("_join_type", "inner")
        left_keys = comp.properties.get("_left_keys", [])
        right_keys = comp.properties.get("_right_keys", [])

        if left_keys and right_keys and len(left_keys) == len(right_keys):
            conditions = " & ".join(
                f'(df_left["{lk}"] == df_right["{rk}"])' for lk, rk in zip(left_keys, right_keys, strict=False)
            )
            join_expr = conditions
        elif left_keys:
            join_expr = f'"{left_keys[0]}"' if len(left_keys) == 1 else repr(left_keys)
        else:
            join_expr = '"join_key"  # TODO: Set correct join key(s)'

        return dedent(f'''\
            # Merge Join: {comp.name}
            # df_left and df_right should reference the appropriate source DataFrames
            df = df_left.join(
                df_right,
                on={join_expr},
                how="{join_type}"
            )
        ''')

    def _gen_aggregate(self, comp: DataFlowComponent) -> str:
        """Generate Aggregate transformation code."""
        group_cols = comp.properties.get("_group_by", [])
        agg_cols = comp.properties.get("_aggregations", [])

        if group_cols:
            group_str = f'"{group_cols[0]}"' if len(group_cols) == 1 else ", ".join(f'"{c}"' for c in group_cols)
        else:
            group_str = '"group_column"  # TODO: Set group-by column(s)'

        agg_lines: list[str] = []
        if agg_cols:
            func_map = {
                "count": "F.count",
                "sum": "F.sum",
                "avg": "F.avg",
                "min": "F.min",
                "max": "F.max",
                "count_distinct": "F.countDistinct",
            }
            for a in agg_cols:
                col_name = a.get("column", "*")
                func = a.get("function", "count")
                spark_func = func_map.get(func, "F.count")
                alias = f"{col_name}_{func}"
                agg_lines.append(f'    {spark_func}("{col_name}").alias("{alias}"),')
        else:
            agg_lines.append('    F.count("*").alias("row_count"),  # TODO: Set aggregate functions')

        agg_block = "\n".join(agg_lines)
        return dedent(f"""\
            # Aggregate: {comp.name}
            df = df.groupBy(
                {group_str}
            ).agg(
            {agg_block}
            )
        """)

    def _gen_sort(self, comp: DataFlowComponent) -> str:
        """Generate Sort transformation code."""
        sort_cols = comp.properties.get("_sort_columns", [])

        if sort_cols:
            parts = []
            for sc in sort_cols:
                col_name = sc.get("column", "col")
                direction = "asc" if sc.get("direction") == "asc" else "desc"
                parts.append(f'F.col("{col_name}").{direction}()')
            sort_str = ", ".join(parts)
        else:
            sort_str = 'F.col("sort_column").asc()  # TODO: Set sort column(s) and direction'

        return dedent(f"""\
            # Sort: {comp.name}
            df = df.orderBy(
                {sort_str}
            )
        """)

    def _gen_union_all(self, comp: DataFlowComponent) -> str:
        """Generate Union All transformation code."""
        return dedent(f"""\
            # Union All: {comp.name}
            # TODO: Union all input DataFrames
            df = df_input_1.unionByName(df_input_2, allowMissingColumns=True)
        """)

    def _gen_data_conversion(self, comp: DataFlowComponent) -> str:
        """Generate Data Conversion transformation code."""
        lines = [f"# Data Conversion: {comp.name}"]
        lines.append("# TODO: Cast columns to target data types")
        for col in comp.columns:
            lines.append(f'# df = df.withColumn("{col.name}", df["{col.name}"].cast("{col.data_type}"))')
        if not comp.columns:
            lines.append('# df = df.withColumn("column_name", df["column_name"].cast("string"))')
        return "\n".join(lines)

    def _gen_pivot(self, comp: DataFlowComponent) -> str:
        return dedent(f"""\
            # Pivot: {comp.name}
            # TODO: Configure pivot operation
            df = df.groupBy("group_col").pivot("pivot_col").agg(F.first("value_col"))
        """)

    def _gen_unpivot(self, comp: DataFlowComponent) -> str:
        return dedent(f"""\
            # Unpivot: {comp.name}
            # TODO: Configure unpivot (use stack expression)
            df = df.selectExpr(
                "id_col",
                "stack(2, 'col1', col1, 'col2', col2) as (attribute, value)"
                # TODO: Adjust stack parameters
            )
        """)

    def _gen_script_component(self, comp: DataFlowComponent) -> str:
        return dedent(f'''\
            # Script Component: {comp.name}
            # IMPORTANT: This was a C#/VB.NET script in SSIS.
            # TODO: Manually translate the script logic to PySpark.
            #
            # Original script properties:
            #   {comp.properties}
            #
            # Implement the transformation logic below:
            def transform_{self._sanitize_name(comp.name).lower()}(df: DataFrame) -> DataFrame:
                """
                TODO: Implement the custom transformation logic
                that was in the SSIS Script Component.
                """
                # Example: Apply row-level transformation
                # df = df.withColumn("result", F.udf(custom_function)(F.col("input")))
                return df

            df = transform_{self._sanitize_name(comp.name).lower()}(df)
        ''')

    def _gen_multicast(self, comp: DataFlowComponent) -> str:
        return dedent(f"""\
            # Multicast: {comp.name}
            # In Spark, simply reuse the same DataFrame for multiple outputs
            df_output_1 = df  # Branch 1
            df_output_2 = df  # Branch 2
        """)

    def _gen_row_count(self, comp: DataFlowComponent) -> str:
        return dedent(f"""\
            # Row Count: {comp.name}
            row_count_{self._sanitize_name(comp.name).lower()} = df.count()
            logger.info(f"Row count for {comp.name}: {{row_count_{self._sanitize_name(comp.name).lower()}}}")
        """)

    def _gen_oledb_command(self, comp: DataFlowComponent) -> str:
        sql = comp.sql_command or comp.properties.get("SqlCommand", "-- TODO: Add SQL")
        return dedent(f'''\
            # OLE DB Command: {comp.name}
            # TODO: Translate row-by-row command execution to batch Spark operation
            # Original SQL: {sql}
            # Consider using DataFrame operations instead of row-by-row processing
            df.createOrReplaceTempView("temp_{self._sanitize_name(comp.name).lower()}")
            spark.sql("""
                {sql}
            """)
        ''')

    # -----------------------------------------------------------------
    # Slowly Changing Dimension (SCD)
    # -----------------------------------------------------------------
    def _gen_scd(self, comp: DataFlowComponent) -> str:
        table = comp.table_name or comp.properties.get("OpenRowset", "dim_table")
        safe_table = self._sanitize_name(table)
        bk_cols = comp.properties.get("_business_keys", [])
        scd1_cols = comp.properties.get("_scd1_columns", [])
        scd2_cols = comp.properties.get("_scd2_columns", [])

        bk_str = ", ".join(f'"{c}"' for c in bk_cols) if bk_cols else '"business_key"  # TODO: set BK column(s)'
        lines = [
            f"# Slowly Changing Dimension: {comp.name}",
            f"# Target table: {table}",
            f'df_existing = spark.read.format("delta").table("{safe_table}")',
            "",
            "# Join incoming rows to existing dimension on business key(s)",
            f"df_merged = df.alias(\"src\").join(",
            f'    df_existing.alias("tgt"),',
            f"    on=[{bk_str}],",
            '    how="full"',
            ")",
        ]

        if scd1_cols:
            cols_str = ", ".join(f'"{c}"' for c in scd1_cols)
            lines += [
                "",
                f"# --- Type 1 (overwrite) columns: {cols_str}",
                "# When matched and values differ → update in place",
            ]
            for c in scd1_cols:
                lines.append(f'df_merged = df_merged.withColumn("{c}", F.coalesce(F.col("src.{c}"), F.col("tgt.{c}")))')

        if scd2_cols:
            cols_str = ", ".join(f'"{c}"' for c in scd2_cols)
            lines += [
                "",
                f"# --- Type 2 (history) columns: {cols_str}",
                "# When matched and values differ → expire old row, insert new",
                '# TODO: Implement SCD2 logic: set EndDate on old row, insert new row with StartDate',
                'df_merged = df_merged.withColumn("_scd2_changed", F.lit(False))  # TODO: detect changes',
            ]
            for c in scd2_cols:
                lines.append(f'df_merged = df_merged.withColumn("_scd2_changed", df_merged["_scd2_changed"] | (F.col("src.{c}") != F.col("tgt.{c}")))')
            lines += [
                'df_merged = df_merged.withColumn("EffectiveDate", F.when(F.col("_scd2_changed"), F.current_timestamp()).otherwise(F.col("tgt.EffectiveDate")))',
                'df_merged = df_merged.withColumn("ExpirationDate", F.when(F.col("_scd2_changed"), F.lit(None)).otherwise(F.col("tgt.ExpirationDate")))',
                'df_merged = df_merged.withColumn("IsCurrent", F.when(F.col("_scd2_changed"), F.lit(True)).otherwise(F.col("tgt.IsCurrent")))',
                'df_merged = df_merged.drop("_scd2_changed")',
            ]

        lines += [
            "",
            "# Write back to dimension table",
            "df_merged.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\") \\",
            f'    .saveAsTable("{safe_table}")',
            f'logger.info("SCD merge completed for {safe_table}")',
        ]
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # Fuzzy Lookup
    # -----------------------------------------------------------------
    def _gen_fuzzy_lookup(self, comp: DataFlowComponent) -> str:
        ref_table = comp.table_name or comp.properties.get("OpenRowset", "reference_table")
        similarity = comp.properties.get("MinSimilarity", "0.5")
        return dedent(f'''\
            # Fuzzy Lookup: {comp.name}
            # Reference table: {ref_table}
            # IMPORTANT: PySpark has no built-in fuzzy join.
            # Options:
            #   1. Use Soundex / Levenshtein for approximate matching
            #   2. Use a dedicated library (e.g. spark-fuzzy-matching)
            df_ref = spark.read.format("delta").table("{ref_table}")

            # Example using Levenshtein distance (edit distance ≤ threshold)
            _match_col = "match_column"  # TODO: Set the column to fuzzy-match on
            _threshold = 3  # TODO: Adjust threshold (original similarity: {similarity})
            df = df.alias("src").crossJoin(df_ref.alias("ref")).filter(
                F.levenshtein(F.col("src." + _match_col), F.col("ref." + _match_col)) <= _threshold
            )
            logger.info("Fuzzy lookup completed for {comp.name}")
        ''')

    # -----------------------------------------------------------------
    # Fuzzy Grouping
    # -----------------------------------------------------------------
    def _gen_fuzzy_grouping(self, comp: DataFlowComponent) -> str:
        similarity = comp.properties.get("MinSimilarity", "0.5")
        return dedent(f'''\
            # Fuzzy Grouping: {comp.name}
            # Groups similar rows together (de-duplication).
            # PySpark has no built-in fuzzy grouping.  Approach:
            #   1. Use Soundex to create a grouping key
            #   2. Use Levenshtein within each group to pick canonical value
            _group_col = "group_column"  # TODO: Set the column to group on
            df = df.withColumn("_fuzzy_key", F.soundex(F.col(_group_col)))
            # Within each soundex group, keep the first (canonical) row
            from pyspark.sql.window import Window as _W
            _w = _W.partitionBy("_fuzzy_key").orderBy(F.col(_group_col))
            df = df.withColumn("_group_id", F.dense_rank().over(_w))
            df = df.withColumn("_canonical", F.first(F.col(_group_col)).over(_w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
            logger.info("Fuzzy grouping completed for {comp.name} (similarity >= {similarity})")
        ''')

    # -----------------------------------------------------------------
    # Term Lookup
    # -----------------------------------------------------------------
    def _gen_term_lookup(self, comp: DataFlowComponent) -> str:
        ref_table = comp.table_name or "term_reference_table"
        return dedent(f'''\
            # Term Lookup: {comp.name}
            # Looks up terms/phrases from a reference table and scores matches.
            df_terms = spark.read.format("delta").table("{ref_table}")  # TODO: verify reference table
            _input_col = "text_column"  # TODO: Set the text column to scan
            _term_col = "Term"  # Column in reference table containing terms

            # Cross-join and check if term appears in the text
            df = df.alias("src").crossJoin(df_terms.alias("terms")).filter(
                F.col("src." + _input_col).contains(F.col("terms." + _term_col))
            )
            logger.info("Term lookup completed for {comp.name}")
        ''')

    # -----------------------------------------------------------------
    # Copy Column
    # -----------------------------------------------------------------
    def _gen_copy_column(self, comp: DataFlowComponent) -> str:
        lines = [f"# Copy Column: {comp.name}"]
        if comp.columns:
            for col in comp.columns:
                src = col.source_column or col.name
                dst = col.name if col.source_column else f"{col.name}_copy"
                lines.append(f'df = df.withColumn("{dst}", F.col("{src}"))')
        else:
            lines.append('# TODO: Specify columns to copy')
            lines.append('# df = df.withColumn("col_copy", F.col("col"))')
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # Character Map
    # -----------------------------------------------------------------
    def _gen_character_map(self, comp: DataFlowComponent) -> str:
        lines = [f"# Character Map: {comp.name}"]
        if comp.columns:
            for col in comp.columns:
                op = (col.expression or "uppercase").lower()
                if "upper" in op:
                    lines.append(f'df = df.withColumn("{col.name}", F.upper(F.col("{col.name}")))')
                elif "lower" in op:
                    lines.append(f'df = df.withColumn("{col.name}", F.lower(F.col("{col.name}")))')
                elif "fullwidth" in op or "halfwidth" in op:
                    lines.append(f'# df = df.withColumn("{col.name}", ...)  # TODO: fullwidth/halfwidth conversion')
                elif "linguistic" in op or "kana" in op or "katakana" in op or "hiragana" in op:
                    lines.append(f'# df = df.withColumn("{col.name}", ...)  # TODO: linguistic casing / kana conversion')
                else:
                    lines.append(f'df = df.withColumn("{col.name}", F.upper(F.col("{col.name}")))  # TODO: verify mapping type ({op})')
        else:
            lines.append('# TODO: Specify character mapping operations')
            lines.append('# df = df.withColumn("col", F.upper(F.col("col")))')
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # Audit
    # -----------------------------------------------------------------
    def _gen_audit(self, comp: DataFlowComponent) -> str:
        lines = [
            f"# Audit: {comp.name}",
            "# Adds audit columns to the data flow (execution metadata)",
        ]
        audit_cols = {
            "ExecutionInstanceGUID": 'F.lit(spark.conf.get("spark.app.id", "unknown"))',
            "PackageID": 'F.lit("pkg_id")  # TODO: Set actual package ID',
            "PackageName": f'F.lit("{comp.properties.get("_package_name", "unknown")}")',
            "VersionID": 'F.lit("1.0")',
            "ExecutionStartTime": "F.current_timestamp()",
            "MachineName": 'F.lit(spark.conf.get("spark.driver.host", "unknown"))',
            "UserName": 'F.lit("fabric_user")',
            "TaskName": f'F.lit("{comp.name}")',
            "TaskID": f'F.lit("{comp.id}")',
        }
        if comp.columns:
            for col in comp.columns:
                expr = audit_cols.get(col.name, f'F.lit("{col.name}")  # TODO: map audit column')
                lines.append(f'df = df.withColumn("{col.name}", {expr})')
        else:
            # Add all standard audit columns
            lines.append('df = df.withColumn("AuditExecutionId", F.lit(spark.conf.get("spark.app.id", "unknown")))')
            lines.append('df = df.withColumn("AuditTimestamp", F.current_timestamp())')
            lines.append('df = df.withColumn("AuditTaskName", F.lit("' + comp.name + '"))')
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # Merge (sorted merge of two inputs)
    # -----------------------------------------------------------------
    def _gen_merge(self, comp: DataFlowComponent) -> str:
        sort_col = comp.properties.get("_sort_key", "sort_key")
        return dedent(f'''\
            # Merge: {comp.name}
            # Merges two pre-sorted inputs into a single sorted output.
            # In Spark, use unionByName followed by orderBy.
            df = df_input_1.unionByName(df_input_2, allowMissingColumns=True) \\
                .orderBy(F.col("{sort_col}"))  # TODO: verify sort column(s)
            logger.info("Merge completed for {comp.name}")
        ''')

    # -----------------------------------------------------------------
    # CDC Splitter
    # -----------------------------------------------------------------
    def _gen_cdc_splitter(self, comp: DataFlowComponent) -> str:
        return dedent(f'''\
            # CDC Splitter: {comp.name}
            # Splits CDC rows by operation type (__$operation column).
            #   1 = Delete, 2 = Insert, 3 = Update (before), 4 = Update (after)
            df_inserts = df.filter(F.col("__$operation") == 2)
            df_updates = df.filter(F.col("__$operation") == 4)
            df_deletes = df.filter(F.col("__$operation") == 1)
            logger.info(f"CDC split: {{df_inserts.count()}} inserts, {{df_updates.count()}} updates, {{df_deletes.count()}} deletes")
        ''')

    # -----------------------------------------------------------------
    # Percentage Sampling
    # -----------------------------------------------------------------
    def _gen_percentage_sampling(self, comp: DataFlowComponent) -> str:
        pct = comp.properties.get("SamplingValue", "10")
        seed = comp.properties.get("SamplingSeed", "42")
        return dedent(f'''\
            # Percentage Sampling: {comp.name}
            # Sample {pct}% of rows
            df_sampled = df.sample(fraction={int(pct) / 100}, seed={seed})
            df_unselected = df.subtract(df_sampled)  # Rows not in sample
            df = df_sampled
            logger.info(f"Sampled {{df.count()}} rows ({pct}%)")
        ''')

    # -----------------------------------------------------------------
    # Row Sampling
    # -----------------------------------------------------------------
    def _gen_row_sampling(self, comp: DataFlowComponent) -> str:
        count = comp.properties.get("SamplingValue", "1000")
        seed = comp.properties.get("SamplingSeed", "42")
        return dedent(f'''\
            # Row Sampling: {comp.name}
            # Sample exactly {count} rows
            _total = df.count()
            _fraction = min(1.0, {count} / max(_total, 1))
            df_sampled = df.sample(fraction=_fraction, seed={seed}).limit({count})
            df_unselected = df.subtract(df_sampled)
            df = df_sampled
            logger.info(f"Sampled {{df.count()}} rows (target: {count})")
        ''')

    # -----------------------------------------------------------------
    # Balanced Data Distributor
    # -----------------------------------------------------------------
    def _gen_balanced_data_distributor(self, comp: DataFlowComponent) -> str:
        return dedent(f'''\
            # Balanced Data Distributor: {comp.name}
            # In SSIS this distributes rows across multiple threads.
            # Spark handles parallelism natively via partitions.
            df = df.repartition({comp.properties.get("_partitions", 8)})  # Redistribute across partitions
            logger.info("Data repartitioned for parallel processing")
        ''')

    # -----------------------------------------------------------------
    # Cache Transform
    # -----------------------------------------------------------------
    def _gen_cache_transform(self, comp: DataFlowComponent) -> str:
        return dedent(f'''\
            # Cache Transform: {comp.name}
            # In SSIS, this writes data to a Cache connection manager for use by Lookup.
            # In Spark, persist the DataFrame in memory for reuse.
            df.cache()
            df.count()  # Force materialization
            logger.info("DataFrame cached for {comp.name}")
        ''')

    def _gen_unknown_transform(self, comp: DataFlowComponent) -> str:
        return dedent(f"""\
            # Unknown Transform: {comp.name} (Type: {comp.component_type.value})
            # TODO: Manually implement this transformation
            # Properties: {comp.properties}
        """)

    def _generate_destination_write(
        self, comp: DataFlowComponent, package: SSISPackage | None = None,
    ) -> str:
        """Generate PySpark code to write to a destination.

        The write strategy mirrors the SSIS connection type and uses
        Fabric connections (same as pipeline externalReferences):
        - OLE DB / ADO.NET / SQL Server / ODBC → JDBC via ``_jdbc_url_for()``
        - Flat File → CSV write
        - Others → generic delta/saveAsTable
        """
        table = comp.table_name or comp.properties.get("OpenRowset", "target_table")
        safe_table = self._sanitize_name(table) if table else "target_table"
        conn_ref = comp.connection_manager_ref

        # Resolve connection manager from package
        conn = None
        if package and conn_ref:
            for cm in package.connection_managers:
                if cm.name == conn_ref or cm.id == conn_ref:
                    conn = cm
                    break

        # --- Flat File Destination → CSV ---
        if comp.component_type == DataFlowComponentType.FLAT_FILE_DESTINATION:
            file_path = comp.properties.get("_file_path", "TODO_FILE_PATH")
            if conn and not file_path.startswith("TODO"):
                pass  # keep file_path from properties
            elif conn:
                file_path = conn.connection_string or file_path
            return dedent(f'''\
                # Destination: {comp.name} -> File: {file_path}
                df.write \\
                    .mode("overwrite") \\
                    .option("header", "true") \\
                    .csv("{file_path}")
                logger.info("Data written to {file_path}")
            ''')

        # --- OLE DB / ADO.NET → Fabric Connection (JDBC) ---
        if conn and conn.connection_type.value in ("OLEDB", "ADO.NET"):
            conn_name = conn.name
            lines = [
                f"# Destination: {comp.name} -> Table: {table}",
                f"# Connection: {conn_name} ({conn.connection_type.value} → Fabric Connection)",
                "df.write \\",
                '    .mode("append") \\',
                '    .format("jdbc") \\',
                f'    .option("url", _jdbc_url_for("{conn_name}")) \\',
                f'    .option("dbtable", "{table}") \\',
                "    .save()",
                f'logger.info("Data written to {table} via Fabric connection")',
            ]
            return "\n".join(lines)

        # --- SQL Server Destination → Fabric Connection (JDBC fast load) ---
        if comp.component_type == DataFlowComponentType.SQL_SERVER_DESTINATION:
            conn_name = conn.name if conn else "TODO_CONNECTION_NAME"
            lines = [
                f"# Destination: {comp.name} -> Table: {table} (SQL Server fast load)",
                f"# Connection: {conn_name} → Fabric Connection",
                "df.write \\",
                '    .mode("append") \\',
                '    .format("jdbc") \\',
                f'    .option("url", _jdbc_url_for("{conn_name}")) \\',
                f'    .option("dbtable", "{table}") \\',
                '    .option("batchsize", "10000") \\',
                "    .save()",
                f'logger.info("Data written to {table} via Fabric connection (fast load)")',
            ]
            return "\n".join(lines)

        # --- ODBC Destination → Fabric Connection (JDBC) ---
        if comp.component_type == DataFlowComponentType.ODBC_DESTINATION:
            conn_name = conn.name if conn else "TODO_CONNECTION_NAME"
            lines = [
                f"# Destination: {comp.name} -> Table: {table} (ODBC → Fabric Connection)",
                f"# Connection: {conn_name}",
                "df.write \\",
                '    .mode("append") \\',
                '    .format("jdbc") \\',
                f'    .option("url", _jdbc_url_for("{conn_name}")) \\',
                f'    .option("dbtable", "{table}") \\',
                "    .save()",
                f'logger.info("Data written to {table} via Fabric connection")',
            ]
            return "\n".join(lines)

        # --- Default: delta / saveAsTable (Fabric Lakehouse) ---
        return dedent(f'''\
            # Destination: {comp.name} -> Table: {table}
            df.write \\
                .mode("append") \\
                .format("delta") \\
                .saveAsTable("{safe_table}")
            logger.info("Data written to {safe_table}")
        ''')

    def _generate_sql_execution(self, task: ControlFlowTask) -> str:
        """Generate code for an Execute SQL task."""
        sql = task.sql_statement or "-- TODO: Add SQL statement"
        return dedent(f'''\
            # === Execute SQL Task: {task.name} ===
            sql_statement = """
            {sql}
            """
            spark.sql(sql_statement)
            logger.info("SQL executed: {task.name}")
        ''')

    def _generate_script_placeholder(self, task: ControlFlowTask) -> str:
        """Generate placeholder for Script tasks."""
        return dedent(f'''\
            # === Script Task: {task.name} ===
            # IMPORTANT: This was a C#/VB.NET script task in SSIS.
            # TODO: Manually translate the script logic.
            #
            # Description: {task.description}
            # Properties: {task.properties}
            #
            def execute_script_task():
                """
                TODO: Implement the script task logic in Python.
                Original SSIS Script Task needed manual translation.
                """
                raise NotImplementedError("Script task requires manual migration")

            execute_script_task()
        ''')

    def _generate_process_placeholder(self, task: ControlFlowTask) -> str:
        """Generate placeholder for Execute Process tasks."""
        return dedent(f"""\
            # === Execute Process Task: {task.name} ===
            # TODO: Translate external process execution to Fabric equivalent.
            # Description: {task.description}
            # Consider using:
            #   - Fabric Data Pipeline activities
            #   - Python subprocess (if appropriate)
            #   - Azure Functions
            import subprocess
            # result = subprocess.run(["command", "args"], capture_output=True, text=True)
            raise NotImplementedError("Execute Process task requires manual migration")
        """)

    def _generate_generic_placeholder(self, task: ControlFlowTask) -> str:
        """Generate placeholder for unsupported task types."""
        return dedent(f'''\
            # === {task.task_type.value} Task: {task.name} ===
            # TODO: This task type ({task.task_type.value}) requires manual migration.
            # Description: {task.description}
            raise NotImplementedError("{task.task_type.value} task requires manual migration")
        ''')

    def _generate_footer(self, task: ControlFlowTask) -> str:
        """Generate notebook footer with completion logging."""
        return dedent(f"""\
            # === Completion ===
            logger.info("Notebook completed: {task.name}")
            print("Migration notebook execution complete: {task.name}")
        """)

    def _generate_orchestrator_notebook(self, package: SSISPackage, output_dir: Path) -> Path:
        """Generate an orchestrator notebook that calls sub-notebooks."""
        notebooks_dir = output_dir / "notebooks"
        notebooks_dir.mkdir(parents=True, exist_ok=True)

        lines = [
            f"# Orchestrator Notebook for Package: {package.name}",
            f"# Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "",
            "# This notebook orchestrates the execution of migrated SSIS tasks.",
            "# Each task is implemented as a separate notebook.",
            "",
        ]

        for task in package.control_flow_tasks:
            safe_name = self._sanitize_name(f"{package.name}_{task.name}")
            lines.append(f"# Task: {task.name} ({task.task_type.value})")
            lines.append(f'# mssparkutils.notebook.run("{safe_name}")')
            lines.append("")

        output_path = notebooks_dir / f"{self._sanitize_name(package.name)}_orchestrator.py"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return output_path

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _is_source(comp: DataFlowComponent) -> bool:
        return comp.component_type in {
            DataFlowComponentType.OLE_DB_SOURCE,
            DataFlowComponentType.ADO_NET_SOURCE,
            DataFlowComponentType.FLAT_FILE_SOURCE,
            DataFlowComponentType.EXCEL_SOURCE,
            DataFlowComponentType.ODBC_SOURCE,
            DataFlowComponentType.XML_SOURCE,
            DataFlowComponentType.RAW_FILE_SOURCE,
            DataFlowComponentType.CDC_SOURCE,
        }

    @staticmethod
    def _is_destination(comp: DataFlowComponent) -> bool:
        return comp.component_type in {
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

    @staticmethod
    def _is_transform(comp: DataFlowComponent) -> bool:
        return not (SparkNotebookGenerator._is_source(comp) or SparkNotebookGenerator._is_destination(comp))

    @staticmethod
    def _ssis_expr_to_pyspark(expr: str) -> str:
        """Best-effort SSIS expression → PySpark expression.

        Handles common patterns. Complex expressions need manual review.
        """
        import re as _re

        if not expr or expr.lower() == "null":
            return "F.lit(None)"

        result = expr.strip()

        # @[$Package::VarName] → Python variable reference
        m = _re.match(r'^@\[\$Package::([\w]+)\]$', result)
        if m:
            return f'F.lit({SparkNotebookGenerator._sanitize_name(m.group(1)).upper()})'
        # @[User::VarName] → Python variable reference
        m = _re.match(r'^@\[User::([\w]+)\]$', result)
        if m:
            return f'F.lit({SparkNotebookGenerator._sanitize_name(m.group(1)).upper()})'
        # @[Namespace::VarName] — any namespace (e.g. SystemLog::intLoadID)
        m = _re.match(r'^@\[[\w\$]+::([\w]+)\]$', result)
        if m:
            return f'F.lit({SparkNotebookGenerator._sanitize_name(m.group(1)).upper()})'
        # @[VarName] — simple variable ref
        m = _re.match(r'^@\[([\w]+)\]$', result)
        if m:
            return f'F.lit({SparkNotebookGenerator._sanitize_name(m.group(1)).upper()})'

        # Inline references within larger expressions
        result = _re.sub(
            r'@\[\$Package::([\w]+)\]',
            lambda mm: SparkNotebookGenerator._sanitize_name(mm.group(1)).upper(),
            result,
        )
        result = _re.sub(
            r'@\[User::([\w]+)\]',
            lambda mm: SparkNotebookGenerator._sanitize_name(mm.group(1)).upper(),
            result,
        )
        # Generic @[Namespace::Var] inline
        result = _re.sub(
            r'@\[[\w\$]+::([\w]+)\]',
            lambda mm: SparkNotebookGenerator._sanitize_name(mm.group(1)).upper(),
            result,
        )

        # GETDATE() → F.current_timestamp()
        result = _re.sub(r"\bGETDATE\s*\(\s*\)", "F.current_timestamp()", result, flags=_re.IGNORECASE)

        # DATEADD("part", num, col) → F.date_add / F.add_months
        m = _re.match(
            r'^DATEADD\s*\(\s*"?(\w+)"?\s*,\s*(-?\d+)\s*,\s*(\w+)\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            part, n, col = m.group(1).lower(), m.group(2), m.group(3)
            if part in ("dd", "day", "d"):
                return f'F.date_add(F.col("{col}"), {n})'
            if part in ("mm", "month", "m"):
                return f'F.add_months(F.col("{col}"), {n})'
            if part in ("yyyy", "year", "yy"):
                return f'F.add_months(F.col("{col}"), {n} * 12)'
            if part in ("hh", "hour"):
                return f'(F.col("{col}") + F.expr("INTERVAL {n} HOURS"))'
            if part in ("mi", "minute", "n"):
                return f'(F.col("{col}") + F.expr("INTERVAL {n} MINUTES"))'
            if part in ("ss", "second", "s"):
                return f'(F.col("{col}") + F.expr("INTERVAL {n} SECONDS"))'
            return f'F.expr("date_add({col}, {n})")  # TODO: verify datepart={part}'

        # DATEDIFF("part", start, end) → F.datediff
        m = _re.match(
            r'^DATEDIFF\s*\(\s*"?(\w+)"?\s*,\s*(\w+)\s*,\s*(\w+)\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            part, start, end = m.group(1).lower(), m.group(2), m.group(3)
            if part in ("dd", "day", "d"):
                return f'F.datediff(F.col("{end}"), F.col("{start}"))'
            if part in ("mm", "month", "m"):
                return f'F.months_between(F.col("{end}"), F.col("{start}")).cast("int")'
            return f'F.datediff(F.col("{end}"), F.col("{start}"))  # TODO: datepart={part}'

        # DATEPART("part", col) → F.year / F.month / ...
        m = _re.match(
            r'^DATEPART\s*\(\s*"?(\w+)"?\s*,\s*(\w+)\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            part, col = m.group(1).lower(), m.group(2)
            func_map = {
                "yyyy": "year", "year": "year", "yy": "year",
                "mm": "month", "month": "month", "m": "month",
                "dd": "dayofmonth", "day": "dayofmonth", "d": "dayofmonth",
                "hh": "hour", "hour": "hour",
                "mi": "minute", "minute": "minute", "n": "minute",
                "ss": "second", "second": "second", "s": "second",
                "dw": "dayofweek", "weekday": "dayofweek",
                "dy": "dayofyear",
                "wk": "weekofyear", "week": "weekofyear",
                "qq": "quarter", "quarter": "quarter",
            }
            spark_func = func_map.get(part, "dayofmonth")
            return f'F.{spark_func}(F.col("{col}"))'

        # YEAR(col) / MONTH(col) / DAY(col)
        m = _re.match(r"^(YEAR|MONTH|DAY)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func_map = {"YEAR": "year", "MONTH": "month", "DAY": "dayofmonth"}
            return f'F.{func_map[m.group(1).upper()]}(F.col("{m.group(2)}"))'

        # ISNULL(col) → F.col("col").isNull()
        m = _re.match(r"^ISNULL\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").isNull()'

        # REPLACENULL(col, replacement) → F.coalesce
        m = _re.match(
            r'^REPLACENULL\s*\(\s*(\w+)\s*,\s*(.+?)\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            col, replacement = m.group(1), m.group(2).strip()
            if replacement.startswith('"') and replacement.endswith('"'):
                return f'F.coalesce(F.col("{col}"), F.lit({replacement}))'
            return f'F.coalesce(F.col("{col}"), F.lit({replacement}))'

        # NULL(DT_WSTR, len) → F.lit(None).cast("string")
        m = _re.match(r"^NULL\s*\(\s*DT_WSTR\s*,\s*\d+\s*\)$", result, _re.IGNORECASE)
        if m:
            return 'F.lit(None).cast("string")'
        m = _re.match(r"^NULL\s*\(\s*DT_I4\s*\)$", result, _re.IGNORECASE)
        if m:
            return 'F.lit(None).cast("int")'
        m = _re.match(r"^NULL\s*\(\s*DT_(?:R8|FLOAT)\s*\)$", result, _re.IGNORECASE)
        if m:
            return 'F.lit(None).cast("double")'
        m = _re.match(r"^NULL\s*\(\s*DT_(?:DATE|DBTIMESTAMP)\s*\)$", result, _re.IGNORECASE)
        if m:
            return 'F.lit(None).cast("timestamp")'

        # UPPER(col) / LOWER(col)
        m = _re.match(r"^(UPPER|LOWER)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func = "upper" if m.group(1).upper() == "UPPER" else "lower"
            return f'F.{func}(F.col("{m.group(2)}"))'

        # LTRIM / RTRIM / TRIM
        m = _re.match(r"^(LTRIM|RTRIM|TRIM)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func_map = {"LTRIM": "ltrim", "RTRIM": "rtrim", "TRIM": "trim"}
            return f'F.{func_map[m.group(1).upper()]}(F.col("{m.group(2)}"))'

        # LEN(col)
        m = _re.match(r"^LEN\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.length(F.col("{m.group(1)}"))'

        # RIGHT(col, n) / LEFT(col, n)
        m = _re.match(r"^(RIGHT|LEFT)\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func_name = m.group(1).upper()
            col, n = m.group(2), m.group(3)
            if func_name == "LEFT":
                return f'F.substring(F.col("{col}"), 1, {n})'
            return f'F.substring(F.col("{col}"), F.length(F.col("{col}")) - {n} + 1, {n})'

        # FINDSTRING(col, search, occurrence) → F.locate
        m = _re.match(
            r'^FINDSTRING\s*\(\s*(\w+)\s*,\s*"([^"]*)"\s*(?:,\s*(\d+))?\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            col, search = m.group(1), m.group(2)
            return f'F.locate("{search}", F.col("{col}"))'

        # TOKEN(col, delimiters, n) → F.split + getItem
        m = _re.match(
            r'^TOKEN\s*\(\s*(\w+)\s*,\s*"([^"]*)"\s*,\s*(\d+)\s*\)$',
            result, _re.IGNORECASE,
        )
        if m:
            col, delim, n = m.group(1), m.group(2), int(m.group(3))
            escaped_delim = _re.escape(delim)
            return f'F.split(F.col("{col}"), "[{escaped_delim}]").getItem({n - 1})'

        # REVERSE(col)
        m = _re.match(r"^REVERSE\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.reverse(F.col("{m.group(1)}"))'

        # REPLACE(col, old, new)
        m = _re.match(r'^REPLACE\s*\(\s*(\w+)\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)$', result, _re.IGNORECASE)
        if m:
            return f'F.regexp_replace(F.col("{m.group(1)}"), "{m.group(2)}", "{m.group(3)}")'

        # SUBSTRING(col, start, len)
        m = _re.match(r"^SUBSTRING\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.substring(F.col("{m.group(1)}"), {m.group(2)}, {m.group(3)})'

        # CONCATENATE / string concatenation with +
        # "literal" + col + "literal" → F.concat
        if _re.search(r'"[^"]*"\s*\+\s*\w+|\w+\s*\+\s*"[^"]*"', result):
            parts = [p.strip() for p in result.split("+")]
            spark_parts = []
            for p in parts:
                if p.startswith('"') and p.endswith('"'):
                    spark_parts.append(f'F.lit({p})')
                else:
                    spark_parts.append(f'F.col("{p}")')
            return f'F.concat({", ".join(spark_parts)})'

        # ABS(col)
        m = _re.match(r"^ABS\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.abs(F.col("{m.group(1)}"))'

        # CEILING(col) / FLOOR(col)
        m = _re.match(r"^(CEILING|FLOOR)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func = "ceil" if m.group(1).upper() == "CEILING" else "floor"
            return f'F.{func}(F.col("{m.group(2)}"))'

        # ROUND(col, precision)
        m = _re.match(r"^ROUND\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.round(F.col("{m.group(1)}"), {m.group(2)})'

        # POWER(col, exp)
        m = _re.match(r"^POWER\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.pow(F.col("{m.group(1)}"), {m.group(2)})'

        # SIGN(col)
        m = _re.match(r"^SIGN\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.signum(F.col("{m.group(1)}"))'

        # SQUARE(col)
        m = _re.match(r"^SQUARE\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.pow(F.col("{m.group(1)}"), 2)'

        # SQRT(col)
        m = _re.match(r"^SQRT\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.sqrt(F.col("{m.group(1)}"))'

        # EXP(col) / LOG(col) / LOG10(col)
        m = _re.match(r"^(EXP|LOG|LOG10)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func_map = {"EXP": "exp", "LOG": "log", "LOG10": "log10"}
            return f'F.{func_map[m.group(1).upper()]}(F.col("{m.group(2)}"))'

        # HEX(col)
        m = _re.match(r"^HEX\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.hex(F.col("{m.group(1)}"))'

        # --- Data type casts ---
        # (DT_STR,len,codepage) col → col.cast("string")
        m = _re.match(r"^\(DT_STR\s*,\s*\d+\s*,\s*\d+\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("string")'
        # (DT_WSTR,len) col → col.cast("string")
        m = _re.match(r"^\(DT_WSTR\s*,\s*\d+\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("string")'
        # (DT_I1) / (DT_UI1) → tinyint
        m = _re.match(r"^\(DT_(?:I1|UI1)\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("tinyint")'
        # (DT_I2) / (DT_UI2) → smallint
        m = _re.match(r"^\(DT_(?:I2|UI2)\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("smallint")'
        # (DT_I4) → int
        m = _re.match(r"^\(DT_I4\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("int")'
        # (DT_UI4) → int (unsigned not native in Spark)
        m = _re.match(r"^\(DT_UI4\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("int")'
        # (DT_I8) / (DT_UI8) → bigint
        m = _re.match(r"^\(DT_(?:I8|UI8)\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("bigint")'
        # (DT_R4) → float
        m = _re.match(r"^\(DT_R4\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("float")'
        # (DT_R8) → double
        m = _re.match(r"^\(DT_R8\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("double")'
        # (DT_DECIMAL,scale) / (DT_NUMERIC,precision,scale) → decimal
        m = _re.match(r"^\(DT_DECIMAL\s*,\s*(\d+)\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(2)}").cast("decimal(38,{m.group(1)})")'
        m = _re.match(r"^\(DT_NUMERIC\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(3)}").cast("decimal({m.group(1)},{m.group(2)})")'
        # (DT_CY) → decimal(19,4) (currency)
        m = _re.match(r"^\(DT_CY\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("decimal(19,4)")'
        # (DT_BOOL) → boolean
        m = _re.match(r"^\(DT_BOOL\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("boolean")'
        # (DT_DATE) / (DT_DBDATE) → date
        m = _re.match(r"^\(DT_(?:DATE|DBDATE)\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("date")'
        # (DT_DBTIMESTAMP) / (DT_DBTIMESTAMP2) → timestamp
        m = _re.match(r"^\(DT_DBTIMESTAMP2?\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("timestamp")'
        # (DT_GUID) → string
        m = _re.match(r"^\(DT_GUID\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("string")  # GUID → string'
        # (DT_BYTES,len) → binary
        m = _re.match(r"^\(DT_BYTES\s*,\s*\d+\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("binary")'

        # --- Ternary: cond ? a : b → F.when(cond, a).otherwise(b) ---
        m = _re.match(r'^(.+?)\s*\?\s*(.+?)\s*:\s*(.+)$', result)
        if m:
            cond, then, else_ = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            return f'F.when(F.expr("{cond}"), F.expr("{then}")).otherwise(F.expr("{else_}"))'

        # --- Boolean operators ---
        # col == val / col != val / col > val etc. → pass through as expr
        # || → .or. and && → .and.
        result = result.replace("||", " | ").replace("&&", " & ")

        # Fallback
        return f'F.expr("{result}")  # TODO: Review SSIS expression'

    def _write_destination_manifest(
        self,
        task: ControlFlowTask,
        output_dir: Path,
        safe_name: str,
    ) -> None:
        """Write sidecar JSON describing destination tables/columns."""
        destinations = [c for c in task.data_flow_components if self._is_destination(c)]
        if not destinations:
            return

        entries: list[dict] = []
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
                    "columns": cols,
                }
            )

        manifest_path = output_dir / f"{safe_name}.destinations.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump({"destinations": entries}, f, indent=2)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        sanitized = re.sub(r"_+", "_", sanitized)
        return sanitized.strip("_")[:200]
