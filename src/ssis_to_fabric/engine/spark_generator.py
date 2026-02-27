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

        Connection-manager details are still emitted as constants so that
        the developer can override them easily during local testing.
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

        # Connection details (still static for now)
        for cm in package.connection_managers:
            safe_name = self._sanitize_name(cm.name).upper()
            lines.append(f"# Connection: {cm.name} ({cm.connection_type.value})")
            if cm.server:
                lines.append(f'{safe_name}_SERVER = "{cm.server}"')
            if cm.database:
                lines.append(f'{safe_name}_DATABASE = "{cm.database}"')
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
        """Generate PySpark code to read from a source."""
        var_name = f"df_source_{self._sanitize_name(comp.name).lower()}"

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
                return dedent(f'''\
                    # Source: {comp.name} (CDC)
                    # TODO: Configure CDC state tracking (__$start_lsn / __$operation)
                    {var_name} = spark.sql("""
                    {comp.sql_command}
                    """)
                    logger.info(f"Read {{count}} CDC rows", count={var_name}.count())
                ''')
            table = comp.table_name or "TODO_TABLE"
            cdc_table = f"cdc.{table}_CT" if not table.startswith("cdc.") else table
            return dedent(f"""\
                # Source: {comp.name} (CDC)
                # TODO: Filter by __$operation for net changes (1=delete, 2=insert, 4=update)
                {var_name} = spark.sql("SELECT * FROM {cdc_table}")
                logger.info(f"Read {{count}} CDC rows", count={var_name}.count())
            """)

        # --- ODBC Source → JDBC read ---
        if comp.component_type == DataFlowComponentType.ODBC_SOURCE:
            if comp.sql_command:
                return dedent(f'''\
                    # Source: {comp.name} (ODBC → JDBC)
                    # TODO: Update the JDBC URL for your environment
                    {var_name} = spark.read.format("jdbc") \\
                        .option("url", "jdbc:TODO_DRIVER://TODO_HOST:TODO_PORT/TODO_DB") \\
                        .option("query", """{comp.sql_command}""") \\
                        .option("driver", "TODO_JDBC_DRIVER") \\
                        .load()
                    logger.info(f"Read {{count}} rows via JDBC", count={var_name}.count())
                ''')
            table = comp.table_name or "TODO_TABLE"
            return dedent(f'''\
                # Source: {comp.name} (ODBC → JDBC)
                # TODO: Update the JDBC URL for your environment
                {var_name} = spark.read.format("jdbc") \\
                    .option("url", "jdbc:TODO_DRIVER://TODO_HOST:TODO_PORT/TODO_DB") \\
                    .option("dbtable", "{table}") \\
                    .option("driver", "TODO_JDBC_DRIVER") \\
                    .load()
                logger.info(f"Read {{count}} rows via JDBC", count={var_name}.count())
            ''')

        # --- OLE DB / ADO.NET (SQL) ---
        if comp.sql_command:
            return dedent(f'''\
                # Source: {comp.name}
                # TODO: Update the JDBC connection string for your Fabric environment
                {var_name} = spark.sql("""
                {comp.sql_command}
                """)
                logger.info(f"Read {{count}} rows from {comp.name}", count={var_name}.count())
            ''')

        if comp.table_name:
            return dedent(f"""\
                # Source: {comp.name} -> Table: {comp.table_name}
                {var_name} = spark.sql("SELECT * FROM {comp.table_name}")
                logger.info(f"Read from {comp.table_name}")
            """)

        return dedent(f"""\
            # Source: {comp.name}
            # TODO: Configure source data read
            {var_name} = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")
        """)

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

    @staticmethod
    def _resolve_conn_var(name: str) -> str:
        """Turn an SSIS connection manager name into a Python variable prefix."""
        import re
        return re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_').upper()

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

        The write strategy mirrors the SSIS connection type:
        - OLE DB / ADO.NET → JDBC write using the connection's server/database
        - Flat File → CSV write
        - ODBC → JDBC write
        - SQL Server (fast load) → JDBC write
        - Others → generic delta/saveAsTable with TODO
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

        # --- OLE DB / ADO.NET / SQL Server Destination → JDBC ---
        if conn and conn.connection_type.value in ("OLEDB", "ADO.NET"):
            prefix = self._resolve_conn_var(conn.name)
            srv_var = f"{prefix}_SERVER"
            db_var = f"{prefix}_DATABASE"
            lines = [
                f"# Destination: {comp.name} -> Table: {table}",
                f"# Connection: {conn.name} ({conn.connection_type.value})",
                f'_jdbc_url = f"jdbc:sqlserver://{{{srv_var}}};databaseName={{{db_var}}}"',
                "df.write \\",
                '    .mode("append") \\',
                '    .format("jdbc") \\',
                '    .option("url", _jdbc_url) \\',
                f'    .option("dbtable", "{table}") \\',
                '    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\',
                "    .save()",
                f'logger.info("Data written to {table} via JDBC")',
            ]
            return "\n".join(lines)

        if comp.component_type == DataFlowComponentType.SQL_SERVER_DESTINATION:
            prefix = self._resolve_conn_var(conn.name) if conn else "DEST"
            srv_var = f"{prefix}_SERVER"
            db_var = f"{prefix}_DATABASE"
            lines = [
                f"# Destination: {comp.name} -> Table: {table} (SQL Server fast load)",
                f'_jdbc_url = f"jdbc:sqlserver://{{{srv_var}}};databaseName={{{db_var}}}"',
                "df.write \\",
                '    .mode("append") \\',
                '    .format("jdbc") \\',
                '    .option("url", _jdbc_url) \\',
                f'    .option("dbtable", "{table}") \\',
                '    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\',
                '    .option("batchsize", "10000") \\',
                "    .save()",
                f'logger.info("Data written to {table} via JDBC (fast load)")',
            ]
            return "\n".join(lines)

        # --- ODBC Destination → JDBC ---
        if comp.component_type == DataFlowComponentType.ODBC_DESTINATION:
            return dedent(f'''\
                # Destination: {comp.name} -> Table: {table} (ODBC → JDBC)
                # TODO: Update the JDBC URL for your environment
                df.write \\
                    .mode("append") \\
                    .format("jdbc") \\
                    .option("url", "jdbc:TODO_DRIVER://TODO_HOST:TODO_PORT/TODO_DB") \\
                    .option("dbtable", "{table}") \\
                    .option("driver", "TODO_JDBC_DRIVER") \\
                    .save()
                logger.info("Data written to {table} via JDBC")
            ''')

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
        # ISNULL(col) → F.col("col").isNull()
        m = _re.match(r"^ISNULL\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").isNull()'
        # UPPER(col) / LOWER(col)
        m = _re.match(r"^(UPPER|LOWER)\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            func = "upper" if m.group(1).upper() == "UPPER" else "lower"
            return f'F.{func}(F.col("{m.group(2)}"))'
        # TRIM(col)
        m = _re.match(r"^TRIM\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.trim(F.col("{m.group(1)}"))'
        # LEN(col)
        m = _re.match(r"^LEN\s*\(\s*(\w+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.length(F.col("{m.group(1)}"))'
        # REPLACE(col, old, new)
        m = _re.match(r'^REPLACE\s*\(\s*(\w+)\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)$', result, _re.IGNORECASE)
        if m:
            return f'F.regexp_replace(F.col("{m.group(1)}"), "{m.group(2)}", "{m.group(3)}")'
        # SUBSTRING(col, start, len)
        m = _re.match(r"^SUBSTRING\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$", result, _re.IGNORECASE)
        if m:
            return f'F.substring(F.col("{m.group(1)}"), {m.group(2)}, {m.group(3)})'
        # (DT_STR,len,codepage) col → col.cast("string")
        m = _re.match(r"^\(DT_STR\s*,\s*\d+\s*,\s*\d+\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("string")'
        # (DT_I4) col → col.cast("int")
        m = _re.match(r"^\(DT_I4\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("int")'
        # (DT_WSTR,len) col → col.cast("string")
        m = _re.match(r"^\(DT_WSTR\s*,\s*\d+\s*\)\s*(\w+)$", result, _re.IGNORECASE)
        if m:
            return f'F.col("{m.group(1)}").cast("string")'
        # Ternary: cond ? a : b → F.when(cond, a).otherwise(b) – passthrough, too complex to fully parse
        if "?" in result and ":" in result:
            return f'F.expr("{result}")  # TODO: Review SSIS ternary expression'
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
