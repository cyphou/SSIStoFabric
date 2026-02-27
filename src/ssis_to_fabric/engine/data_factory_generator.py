"""
Data Factory Pipeline Generator
=================================
Generates Microsoft Fabric Data Factory pipeline JSON definitions
from SSIS package analysis results.

Supports two modes:
  1. Legacy per-task pipelines (generate method with task parameter)
  2. Consolidated per-package pipelines (generate_package_pipeline method)

The consolidated mode produces one pipeline per SSIS package with:
  - Inline Script activities for Execute SQL tasks
  - Dataflow references for simple Data Flow Tasks → Dataflow Gen2
  - TridentNotebook references for complex Data Flow Tasks → Spark notebooks
  - Flattened sequence containers with proper dependsOn chains
  - InvokePipeline for cross-package references (Execute Package tasks)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    SqlParameterBinding,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from ssis_to_fabric.config import MigrationConfig
    from ssis_to_fabric.engine.migration_engine import TargetArtifact

logger = get_logger(__name__)


class DataFactoryGenerator:
    """
    Generates Fabric Data Factory pipeline definitions from SSIS tasks.

    Supports:
    - Copy Activity (from SSIS Data Flow Source->Destination)
    - Script Activity / Stored Procedure (from Execute SQL tasks)
    - ForEach / If Condition (from SSIS loop containers)
    - Pipeline chaining (from Execute Package tasks)
    """

    def __init__(self, config: MigrationConfig) -> None:
        self.config = config
        # Set during generate_package_pipeline so helpers can access the package
        self._current_package: SSISPackage | None = None

    def generate(self, package: SSISPackage, task: ControlFlowTask | None, output_dir: Path) -> Path:
        """
        Generate a Data Factory pipeline JSON for a task or full package.

        Args:
            package: The source SSIS package model
            task: Specific task to generate for (or None for full package)
            output_dir: Directory to write pipeline JSON
        Returns:
            Path to the generated pipeline JSON file.
        """
        if task is None:
            return self._generate_full_pipeline(package, output_dir)
        return self._generate_task_pipeline(package, task, output_dir)

    def _generate_full_pipeline(self, package: SSISPackage, output_dir: Path) -> Path:
        """Generate a pipeline for the entire package control flow."""
        activities = []

        prev_activity_name: str | None = None

        for task in package.control_flow_tasks:
            activity = self._task_to_activity(task, package.connection_managers)
            if activity:
                if prev_activity_name:
                    activity["dependsOn"] = [{"activity": prev_activity_name, "dependencyConditions": ["Succeeded"]}]
                activities.append(activity)
                prev_activity_name = activity["name"]

        pipeline = self._build_pipeline_definition(
            name=self._sanitize_name(package.name),
            activities=activities,
            parameters=self._package_params_to_df_params(package),
            variables=self._variables_to_df_variables(package),
        )

        pipelines_dir = output_dir / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        output_path = pipelines_dir / f"{self._sanitize_name(package.name)}.json"

        with open(output_path, "w") as f:
            json.dump(pipeline, f, indent=2)

        logger.info("pipeline_generated", name=package.name, path=str(output_path))
        return output_path

    def _generate_task_pipeline(self, package: SSISPackage, task: ControlFlowTask, output_dir: Path) -> Path:
        """Generate pipeline for a specific task."""
        activity = self._task_to_activity(task, package.connection_managers)
        activities = [activity] if activity else []

        pipeline = self._build_pipeline_definition(
            name=self._sanitize_name(f"{package.name}_{task.name}"),
            activities=activities,
        )

        pipelines_dir = output_dir / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        output_path = pipelines_dir / f"{self._sanitize_name(f'{package.name}_{task.name}')}.json"

        with open(output_path, "w") as f:
            json.dump(pipeline, f, indent=2)

        logger.info("task_pipeline_generated", task=task.name, path=str(output_path))
        return output_path

    # =========================================================================
    # Consolidated Package Pipeline (one pipeline per SSIS package)
    # =========================================================================

    def generate_package_pipeline(
        self,
        package: SSISPackage,
        task_routing: dict[str, TargetArtifact],
        output_dir: Path,
    ) -> Path:
        """
        Generate a single consolidated pipeline for an entire SSIS package.

        All tasks become inline activities within one pipeline.
        Sequence containers are flattened. Data Flow Tasks become
        Dataflow or TridentNotebook references based on routing.

        Args:
            package: The source SSIS package
            task_routing: Map of task_name → TargetArtifact (from plan)
            output_dir: Output directory
        Returns:
            Path to the generated pipeline JSON file.
        """
        self._current_package = package  # expose to helpers

        activities = self._flatten_tasks_to_activities(
            package.control_flow_tasks,
            package.connection_managers,
            task_routing,
            preceding=None,
            precedence_constraints=package.precedence_constraints,
        )

        pipeline = self._build_pipeline_definition(
            name=self._sanitize_name(package.name),
            activities=activities,
            parameters=self._package_params_to_df_params(package),
            variables=self._variables_to_df_variables(package),
        )

        pipelines_dir = output_dir / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        output_path = pipelines_dir / f"{self._sanitize_name(package.name)}.json"

        with open(output_path, "w") as f:
            json.dump(pipeline, f, indent=2)

        logger.info(
            "package_pipeline_generated",
            name=package.name,
            activities=len(activities),
            path=str(output_path),
        )
        return output_path

    def _flatten_tasks_to_activities(
        self,
        tasks: list[ControlFlowTask],
        connections: list[ConnectionManager],
        task_routing: dict[str, TargetArtifact],
        preceding: list[str] | None = None,
        precedence_constraints: list | None = None,
    ) -> list[dict]:
        """
        Flatten tasks into pipeline activities with proper dependency chains.

        Sequence containers are dissolved — their children become top-level
        activities. When the scope has precedence constraints, those are used
        to build the correct dependency graph (preserving parallelism).
        Otherwise children are chained sequentially.

        Args:
            tasks: Control flow tasks to convert
            connections: Package connection managers
            task_routing: Task name → target artifact mapping
            preceding: Names of activities that must complete before these tasks
            precedence_constraints: Precedence constraints within the current scope
        Returns:
            Flat list of pipeline activity dicts with dependsOn chains.
        """
        activities: list[dict] = []
        prev_names: list[str] = list(preceding) if preceding else []

        # Build dependency graph from precedence constraints if available.
        # Constraints reference tasks via DTS:From / DTS:To which may be
        # either refId paths (real VS packages) or DTSIDs (hand-crafted XML).
        constraint_graph: dict[str, list[str]] = {}  # key → [depends_on_keys]
        if precedence_constraints:
            for pc in precedence_constraints:
                dest = pc.destination_task
                src = pc.source_task
                constraint_graph.setdefault(dest, []).append(src)

        # Build task lookup supporting both ref_id and DTSID keys
        id_to_task: dict[str, ControlFlowTask] = {}
        for t in tasks:
            if t.ref_id:
                id_to_task[t.ref_id] = t
            if t.id:
                id_to_task[t.id] = t

        # If we have a constraint graph, use it for dependency resolution
        has_graph = bool(constraint_graph)
        # precedence_constraints == [] means container had NO constraints → parallel
        # precedence_constraints is None means no constraint info → sequential fallback
        is_parallel = precedence_constraints is not None and not has_graph

        # When containers are dissolved, map their ref_id/id to their leaf
        # activity names so downstream tasks depend on the right activities.
        resolved_names: dict[str, list[str]] = {}

        def _resolve_deps(src_ids: list[str]) -> list[str]:
            """Resolve source IDs to activity names, following container dissolutions."""
            names: list[str] = []
            for src_id in src_ids:
                if src_id in resolved_names:
                    names.extend(resolved_names[src_id])
                else:
                    src_task = id_to_task.get(src_id)
                    if src_task:
                        names.append(self._sanitize_name(src_task.name))
            return names

        def _task_constraint_key(task: ControlFlowTask) -> str:
            """Find which key (ref_id or id) this task appears under in the constraint graph."""
            if task.ref_id in constraint_graph:
                return task.ref_id
            if task.id in constraint_graph:
                return task.id
            return ""

        for task in tasks:
            if task.disabled:
                continue

            if task.task_type == TaskType.SEQUENCE_CONTAINER:
                # Resolve what this container depends on
                container_preceding: list[str] = []
                ck = _task_constraint_key(task)
                if has_graph and ck:
                    container_preceding = _resolve_deps(constraint_graph[ck])
                elif has_graph and not ck:
                    container_preceding = list(prev_names)
                else:
                    container_preceding = list(prev_names)

                # Flatten: children become top-level activities
                child_activities = self._flatten_tasks_to_activities(
                    task.child_tasks,
                    connections,
                    task_routing,
                    preceding=container_preceding,
                    precedence_constraints=task.child_precedence_constraints,
                )
                activities.extend(child_activities)

                # Register the dissolved container's leaf activities so
                # downstream tasks that reference this container resolve correctly.
                if child_activities:
                    leaf_names = [a["name"] for a in child_activities]
                    if task.ref_id:
                        resolved_names[task.ref_id] = leaf_names
                    if task.id:
                        resolved_names[task.id] = leaf_names
                    if not has_graph:
                        prev_names = leaf_names
                continue

            activity = self._convert_task_for_package(task, connections, task_routing)
            if activity is None:
                continue

            # Determine dependencies
            tk = _task_constraint_key(task)
            if has_graph and tk:
                dep_names = _resolve_deps(constraint_graph[tk])
                if dep_names:
                    activity["dependsOn"] = [{"activity": n, "dependencyConditions": ["Succeeded"]} for n in dep_names]
                elif prev_names:
                    activity["dependsOn"] = [
                        {"activity": name, "dependencyConditions": ["Succeeded"]} for name in prev_names
                    ]
            elif has_graph and not tk:
                # Task has no incoming constraints in this scope
                if prev_names:
                    activity["dependsOn"] = [
                        {"activity": name, "dependencyConditions": ["Succeeded"]} for name in prev_names
                    ]
            elif is_parallel:
                # Parallel container: all children depend on preceding context only
                if prev_names:
                    activity["dependsOn"] = [
                        {"activity": name, "dependencyConditions": ["Succeeded"]} for name in prev_names
                    ]
                # Don't update prev_names — keep original preceding for all siblings
            else:
                # No constraint info: sequential chaining
                if prev_names:
                    activity["dependsOn"] = [
                        {"activity": name, "dependencyConditions": ["Succeeded"]} for name in prev_names
                    ]

            activities.append(activity)

            # Register this task's activity name for downstream resolution
            if has_graph:
                act_name = [activity["name"]]
                if task.ref_id:
                    resolved_names[task.ref_id] = act_name
                if task.id:
                    resolved_names[task.id] = act_name

            if not has_graph and not is_parallel:
                prev_names = [activity["name"]]

        return activities

    def _convert_task_for_package(
        self,
        task: ControlFlowTask,
        connections: list[ConnectionManager],
        task_routing: dict[str, TargetArtifact],
    ) -> dict | None:
        """
        Convert a single task to a pipeline activity using routing info.

        Data Flow Tasks are converted based on their routing target:
        - DATAFLOW_GEN2 → Dataflow reference
        - SPARK_NOTEBOOK → TridentNotebook reference
        - DATA_FACTORY_PIPELINE → Copy activity (legacy inline)

        Script and Execute Process tasks routed to Spark become
        TridentNotebook references.
        """
        from ssis_to_fabric.engine.migration_engine import TargetArtifact

        if task.disabled:
            return None

        route = task_routing.get(task.name)

        # Data Flow Tasks
        if task.task_type == TaskType.DATA_FLOW:
            if route == TargetArtifact.DATAFLOW_GEN2:
                return self._to_dataflow_ref(task)
            elif route == TargetArtifact.SPARK_NOTEBOOK:
                return self._to_notebook_ref(task, self._current_package)
            else:
                return self._data_flow_to_copy_activity(task, connections)

        # Script tasks → Spark notebook
        if task.task_type == TaskType.SCRIPT and route == TargetArtifact.SPARK_NOTEBOOK:
            return self._to_notebook_ref(task, self._current_package)

        # Execute Process → ExecutePipeline placeholder
        if task.task_type == TaskType.EXECUTE_PROCESS:
            return self._execute_process_to_activity(task)

        # Send Mail → Outlook activity
        if task.task_type == TaskType.SEND_MAIL:
            return self._send_mail_to_activity(task)

        # ForEach Loop — children go inline, using routing-aware conversion
        if task.task_type == TaskType.FOREACH_LOOP:
            return self._foreach_to_activity_routed(task, connections, task_routing)

        # For Loop
        if task.task_type == TaskType.FOR_LOOP:
            return self._for_loop_to_activity_routed(task, connections, task_routing)

        # File System → Notebook with mssparkutils.fs
        if task.task_type == TaskType.FILE_SYSTEM:
            return self._file_system_to_activity(task)

        # FTP → Copy / Notebook
        if task.task_type == TaskType.FTP:
            return self._ftp_to_activity(task)

        # Standard conversion for all other types (Execute SQL, Execute Package, etc.)
        return self._task_to_activity(task, connections)

    def _to_dataflow_ref(self, task: ControlFlowTask) -> dict:
        """Generate a Dataflow activity referencing a Dataflow Gen2 item."""
        return {
            "name": self._sanitize_name(task.name),
            "type": "Dataflow",
            "typeProperties": {
                "dataflowId": "",  # Resolved at deploy time
                "workspaceId": "",
            },
        }

    def _to_notebook_ref(
        self,
        task: ControlFlowTask,
        package: SSISPackage | None = None,
    ) -> dict:
        """Generate a TridentNotebook activity referencing a Spark notebook.

        When *package* is provided the activity forwards all pipeline
        parameters as notebook parameters so that the notebook can
        consume them at runtime via ``notebookutils``.
        """
        activity: dict[str, Any] = {
            "name": self._sanitize_name(task.name),
            "type": "TridentNotebook",
            "typeProperties": {
                "notebookId": "",  # Resolved at deploy time
                "workspaceId": "",
            },
        }

        # Forward all pipeline parameters to the notebook
        if package is not None:
            nb_params: dict[str, Any] = {}
            for p in package.project_parameters:
                nb_params[p.name] = {
                    "value": f"@pipeline().parameters.{p.name}",
                    "type": "Expression",
                }
            for p in package.parameters:
                nb_params[p.name] = {
                    "value": f"@pipeline().parameters.{p.name}",
                    "type": "Expression",
                }
            # Also pass key variables that notebooks may need
            for v in package.variables:
                if v.namespace != "System":
                    nb_params[v.name] = {
                        "value": f"@variables('{v.name}')",
                        "type": "Expression",
                    }
            if nb_params:
                activity["typeProperties"]["parameters"] = nb_params

        return activity

    def _foreach_to_activity_routed(
        self,
        task: ControlFlowTask,
        connections: list[ConnectionManager],
        task_routing: dict[str, TargetArtifact],
    ) -> dict:
        """ForEach activity with routing-aware child conversion."""
        child_activities = [self._convert_task_for_package(c, connections, task_routing) for c in task.child_tasks]
        child_activities = [a for a in child_activities if a is not None]

        items_expr = self._foreach_items_expression(task)

        return {
            "name": self._sanitize_name(task.name),
            "type": "ForEach",
            "typeProperties": {
                "isSequential": True,
                "items": {
                    "value": items_expr,
                    "type": "Expression",
                },
                "activities": child_activities,
            },
        }

    def _for_loop_to_activity_routed(
        self,
        task: ControlFlowTask,
        connections: list[ConnectionManager],
        task_routing: dict[str, TargetArtifact],
    ) -> dict:
        """For Loop (Until) activity with routing-aware child conversion."""
        child_activities = [self._convert_task_for_package(c, connections, task_routing) for c in task.child_tasks]
        child_activities = [a for a in child_activities if a is not None]

        return {
            "name": self._sanitize_name(task.name),
            "type": "Until",
            "typeProperties": {
                "expression": {
                    "value": task.expression or "@bool(true)",
                    "type": "Expression",
                },
                "activities": child_activities,
                "timeout": "0.01:00:00",
            },
        }

    # =========================================================================
    # Pipeline Definition Builder
    # =========================================================================

    def _build_pipeline_definition(
        self,
        name: str,
        activities: list[dict],
        parameters: dict | None = None,
        variables: dict | None = None,
    ) -> dict:
        """Build a Fabric Data Factory pipeline JSON structure."""
        pipeline: dict[str, Any] = {
            "name": name,
            "properties": {
                "activities": activities,
                "annotations": [
                    "migrated-from-ssis",
                    f"generated-{__import__('datetime').datetime.now(__import__('datetime').timezone.utc).strftime('%Y%m%d')}",
                ],
            },
        }
        if parameters:
            pipeline["properties"]["parameters"] = parameters
        if variables:
            pipeline["properties"]["variables"] = variables
        return pipeline

    # =========================================================================
    # Task-to-Activity Conversion
    # =========================================================================

    def _task_to_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict | None:
        """Convert an SSIS task to a Data Factory activity."""
        if task.disabled:
            return None

        if task.task_type == TaskType.DATA_FLOW:
            return self._data_flow_to_copy_activity(task, connections)
        elif task.task_type == TaskType.EXECUTE_SQL:
            return self._execute_sql_to_activity(task, connections)
        elif task.task_type == TaskType.EXECUTE_PROCESS:
            return self._execute_process_to_activity(task)
        elif task.task_type == TaskType.SEND_MAIL:
            return self._send_mail_to_activity(task)
        elif task.task_type == TaskType.SEQUENCE_CONTAINER:
            return self._container_to_activity(task, connections)
        elif task.task_type == TaskType.FOR_LOOP:
            return self._for_loop_to_activity(task, connections)
        elif task.task_type == TaskType.FOREACH_LOOP:
            return self._foreach_to_activity(task, connections)
        elif task.task_type == TaskType.EXECUTE_PACKAGE:
            return self._execute_package_to_activity(task)
        elif task.task_type == TaskType.EXPRESSION:
            return self._expression_to_set_variable(task)
        elif task.task_type == TaskType.FILE_SYSTEM:
            return self._file_system_to_activity(task)
        elif task.task_type == TaskType.FTP:
            return self._ftp_to_activity(task)
        else:
            return self._unknown_task_placeholder(task)

    def _data_flow_to_copy_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict:
        """
        Convert SSIS Data Flow Task to Copy Activity.
        Maps source and destination components to Copy Activity source/sink.
        """
        source_comp = None
        dest_comp = None

        source_types = {
            DataFlowComponentType.OLE_DB_SOURCE,
            DataFlowComponentType.ADO_NET_SOURCE,
            DataFlowComponentType.FLAT_FILE_SOURCE,
            DataFlowComponentType.EXCEL_SOURCE,
            DataFlowComponentType.ODBC_SOURCE,
            DataFlowComponentType.XML_SOURCE,
            DataFlowComponentType.RAW_FILE_SOURCE,
            DataFlowComponentType.CDC_SOURCE,
        }
        dest_types = {
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

        for comp in task.data_flow_components:
            if comp.component_type in source_types:
                source_comp = comp
            elif comp.component_type in dest_types:
                dest_comp = comp

        activity: dict[str, Any] = {
            "name": self._sanitize_name(task.name),
            "type": "Copy",
            "typeProperties": {
                "source": self._build_copy_source(source_comp, connections),
                "sink": self._build_copy_sink(dest_comp, connections),
                "enableStaging": False,
            },
            "inputs": [],
            "outputs": [],
        }

        # Add column mapping if we have both source and dest columns
        if source_comp and dest_comp and source_comp.columns and dest_comp.columns:
            mappings = self._build_column_mappings(source_comp, dest_comp)
            if mappings:
                activity["typeProperties"]["translator"] = {
                    "type": "TabularTranslator",
                    "mappings": mappings,
                }

        return activity

    def _build_copy_source(self, comp: DataFlowComponent | None, connections: list[ConnectionManager]) -> dict:
        """Build Copy Activity source configuration (type-aware)."""
        if comp is None:
            return {"type": "SqlSource", "sqlReaderQuery": "-- TODO: Configure source query"}

        # Flat File → DelimitedTextSource
        if comp.component_type == DataFlowComponentType.FLAT_FILE_SOURCE:
            return {
                "type": "DelimitedTextSource",
                "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True},
                "formatSettings": {
                    "type": "DelimitedTextReadSettings",
                    "columnDelimiter": ",",
                    "quoteChar": '"',
                },
            }

        # Excel → ExcelSource
        if comp.component_type == DataFlowComponentType.EXCEL_SOURCE:
            sheet = comp.properties.get("OpenRowset", "Sheet1$").rstrip("$")
            return {
                "type": "ExcelSource",
                "storeSettings": {"type": "AzureBlobFSReadSettings"},
                "sheetName": sheet,
            }

        # ODBC → OdbcSource
        if comp.component_type == DataFlowComponentType.ODBC_SOURCE:
            src: dict[str, Any] = {"type": "OdbcSource"}
            if comp.sql_command:
                src["query"] = comp.sql_command
            elif comp.table_name:
                src["query"] = f"SELECT * FROM {comp.table_name}"
            return src

        # XML → XmlSource
        if comp.component_type == DataFlowComponentType.XML_SOURCE:
            return {
                "type": "XmlSource",
                "storeSettings": {"type": "AzureBlobFSReadSettings"},
                "formatSettings": {"type": "XmlReadSettings"},
            }

        # CDC → SqlSource with CDC query
        if comp.component_type == DataFlowComponentType.CDC_SOURCE:
            query = comp.sql_command or f"SELECT * FROM cdc.{comp.table_name or 'TODO_TABLE'}_CT"
            return {
                "type": "SqlSource",
                "sqlReaderQuery": query,
                "queryTimeout": "02:00:00",
                "isolationLevel": "ReadCommitted",
            }

        # Default SQL sources (OLE DB, ADO.NET, SQL Server Destination read-back)
        if comp.sql_command:
            return {
                "type": "SqlSource",
                "sqlReaderQuery": comp.sql_command,
            }
        elif comp.table_name:
            return {
                "type": "SqlSource",
                "sqlReaderQuery": f"SELECT * FROM {comp.table_name}",
            }
        return {"type": "SqlSource", "sqlReaderQuery": "-- TODO: Configure source"}

    def _build_copy_sink(self, comp: DataFlowComponent | None, connections: list[ConnectionManager]) -> dict:
        """Build Copy Activity sink configuration (type-aware)."""
        if comp is None:
            return {"type": "SqlSink", "writeBehavior": "insert"}

        table = comp.table_name or comp.properties.get("OpenRowset", "")

        # Flat File → DelimitedTextSink
        if comp.component_type == DataFlowComponentType.FLAT_FILE_DESTINATION:
            return {
                "type": "DelimitedTextSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {
                    "type": "DelimitedTextWriteSettings",
                    "columnDelimiter": ",",
                    "quoteChar": '"',
                    "fileExtension": ".csv",
                },
            }

        # Excel → not natively supported as sink, use CSV
        if comp.component_type == DataFlowComponentType.EXCEL_DESTINATION:
            return {
                "type": "DelimitedTextSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {
                    "type": "DelimitedTextWriteSettings",
                    "columnDelimiter": ",",
                    "fileExtension": ".csv",
                },
                # TODO: Fabric Copy does not support Excel sink natively
            }

        # ODBC → OdbcSink
        if comp.component_type == DataFlowComponentType.ODBC_DESTINATION:
            return {
                "type": "OdbcSink",
                "writeBatchSize": 10000,
                **({"tableName": table} if table else {}),
            }

        # Default SQL sink (OLE DB, ADO.NET, SQL Server Dest, Recordset, DataReader)
        return {
            "type": "SqlSink",
            "writeBehavior": "insert",
            "tableOption": "autoCreate",
            **({"tableName": table} if table else {}),
        }

    def _build_column_mappings(self, source: DataFlowComponent, dest: DataFlowComponent) -> list[dict]:
        """Build column mappings between source and destination."""
        mappings = []
        dest_cols = {c.name.lower(): c.name for c in dest.columns}

        for src_col in source.columns:
            dest_name = dest_cols.get(src_col.name.lower(), src_col.name)
            mappings.append(
                {
                    "source": {"name": src_col.name},
                    "sink": {"name": dest_name},
                }
            )
        return mappings

    def _execute_sql_to_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict:
        """Convert Execute SQL task to a Fabric Script activity.

        In Fabric Data Factory, all SQL execution uses the **Script** activity
        type (not ``SqlServerStoredProcedure`` which is ADF-only).  The method:

        * Classifies the SQL as ``Query`` (SELECT) or ``NonQuery``
          (INSERT / UPDATE / DELETE / EXEC / DDL).
        * Replaces positional ``?`` placeholders with named ``@p0``, ``@p1``
          parameters and emits a ``parameters`` array referencing pipeline
          parameters derived from the SSIS variable bindings.
        * Attaches a ``connection`` reference derived from the SSIS
          connection manager so the user only needs to fill in the
          Fabric connection id.
        """
        sql = task.sql_statement or "-- TODO: Add SQL statement"
        script_type = self._classify_sql_type(sql)

        # Build script block with optional parameter bindings
        script_block: dict[str, Any] = {"type": script_type}

        # Replace positional ? placeholders with named @p0, @p1, ... and
        # generate the Fabric scriptParameters array.
        input_params = [p for p in task.sql_parameters if p.direction == "Input"]
        if input_params:
            replaced_sql = sql
            for i, _p in enumerate(input_params):
                replaced_sql = replaced_sql.replace("?", f"@p{i}", 1)
            script_block["text"] = replaced_sql
            script_block["parameters"] = [self._sql_param_to_fabric(i, p) for i, p in enumerate(input_params)]
        else:
            script_block["text"] = sql

        activity: dict[str, Any] = {
            "name": self._sanitize_name(task.name),
            "type": "Script",
            "typeProperties": {
                "scripts": [script_block],
                "scriptBlockExecutionTimeout": "02:00:00",
            },
        }

        # Attach connection reference from the SSIS connection manager.
        # Fabric Data Factory uses ``externalReferences.connection`` at the
        # activity level (not ``typeProperties.connection``) so that the
        # runtime validates and links the connection in the UI.
        conn_id = self._resolve_connection_id(task.connection_manager_ref, connections)
        if conn_id:
            activity["externalReferences"] = {"connection": conn_id}
        else:
            # Emit a TODO marker in externalReferences so the deployer or
            # user can fill it in later.
            activity["externalReferences"] = {
                "connection": "TODO: configure Fabric Warehouse connection",
            }

        return activity

    def _sql_param_to_fabric(self, index: int, param: SqlParameterBinding) -> dict:
        """Convert a single SSIS SQL parameter binding to a Fabric scriptParameter.

        Returns a dict like::

            {
                "name": "p0",
                "type": "Int32",
                "value": {"value": "@pipeline().parameters.LoadExecutionId", "type": "Expression"},
                "direction": "Input"
            }
        """

        fabric_type = self._oledb_type_to_fabric(param.data_type)
        pipeline_param = self._ssis_variable_to_pipeline_param(param.variable_name)

        return {
            "name": f"p{index}",
            "type": fabric_type,
            "value": {
                "value": f"@pipeline().parameters.{pipeline_param}",
                "type": "Expression",
            },
            "direction": param.direction,
        }

    @staticmethod
    def _oledb_type_to_fabric(oledb_type: int) -> str:
        """Map an OLE DB data-type code to a Fabric Script parameter type.

        Common OLE DB type codes from SSIS ParameterBinding:
        2=Int16, 3=Int32, 4=Single, 5=Double, 6=Decimal, 7=Date,
        8=String(BSTR), 11=Boolean, 20=Int64, 72=GUID, 129=String(CHAR),
        130=WChar, 131=Numeric, 135=DBTimeStamp.
        """
        type_map: dict[int, str] = {
            2: "Int16",
            3: "Int32",
            4: "Single",
            5: "Double",
            6: "Decimal",
            7: "DateTime",
            8: "String",
            11: "Boolean",
            20: "Int64",
            72: "Guid",
            129: "String",
            130: "String",
            131: "Decimal",
            135: "DateTime",
        }
        return type_map.get(oledb_type, "String")

    @staticmethod
    def _ssis_variable_to_pipeline_param(variable_name: str) -> str:
        """Convert an SSIS variable reference to a Fabric pipeline parameter name.

        SSIS variables have forms like:
        - ``$Package::LoadExecutionId``
        - ``$Project::LoadId``
        - ``User::MyVar``
        - ``System::ServerExecutionID``
        - ``SystemLog::LoadExecutionId``  (custom namespace)

        We strip the namespace prefix and ``$`` to produce the bare name
        that matches pipeline parameters emitted by ``_package_params_to_df_params``.
        """
        # Strip "$" prefix
        name = variable_name.lstrip("$")
        # Take the part after "::"
        if "::" in name:
            name = name.split("::", 1)[1]
        return name

    # =========================================================================
    # SQL helpers
    # =========================================================================

    @staticmethod
    def _classify_sql_type(sql: str) -> str:
        """Return ``'Query'`` for SELECT statements, ``'NonQuery'`` otherwise.

        NonQuery covers INSERT, UPDATE, DELETE, EXEC, MERGE, TRUNCATE,
        CREATE, ALTER, DROP and any other DDL / DML.
        """
        stripped = sql.strip().lstrip("-").strip()  # skip leading -- comments
        # Walk past block comments
        while stripped.startswith("/*"):
            end = stripped.find("*/")
            if end == -1:
                break
            stripped = stripped[end + 2 :].strip()
        first_word = stripped.split()[0].upper() if stripped.split() else ""
        if first_word == "SELECT":
            return "Query"
        return "NonQuery"

    def _resolve_connection_ref(self, conn_ref: str, connections: list[ConnectionManager]) -> dict | None:
        """Map an SSIS connection-manager reference to a Fabric connection.

        .. deprecated:: Use :meth:`_resolve_connection_id` instead.

        Resolution order:
        1. If ``config.connection_mappings`` has an entry, use the Fabric id.
        2. Otherwise emit a TODO placeholder with the SSIS name.
        """
        if not conn_ref:
            return None

        # Try to find the SSIS connection manager by id
        conn_name = conn_ref
        for cm in connections:
            if cm.id == conn_ref or cm.name == conn_ref:
                conn_name = cm.name
                break

        # Check config-driven mapping
        mapped_id = self.config.connection_mappings.mappings.get(conn_name, "")
        if mapped_id:
            return {
                "referenceName": mapped_id,
            }

        return {
            "referenceName": f"{conn_name}  -- TODO: replace with Fabric connection id",
        }

    def _resolve_connection_id(self, conn_ref: str, connections: list[ConnectionManager]) -> str | None:
        """Resolve an SSIS connection-manager reference to a Fabric connection ID.

        Returns a plain string (the connection GUID) when a mapping exists in
        ``config.connection_mappings``, or a ``TODO`` placeholder string when
        no mapping is configured.  Returns ``None`` when *conn_ref* is empty.
        """
        if not conn_ref:
            return None

        conn_name = conn_ref
        for cm in connections:
            if cm.id == conn_ref or cm.name == conn_ref:
                conn_name = cm.name
                break

        mapped_id = self.config.connection_mappings.mappings.get(conn_name, "")
        if mapped_id:
            return mapped_id

        return f"{conn_name}  -- TODO: replace with Fabric connection id"

    def _container_to_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict:
        """Convert Sequence Container to a group of activities."""
        child_activities = []
        for child in task.child_tasks:
            act = self._task_to_activity(child, connections)
            if act:
                child_activities.append(act)

        # Wrap in a pipeline-level grouping (annotation-based) since ADF
        # doesn't have a Sequence Container equivalent - we flatten them
        if len(child_activities) == 1:
            return child_activities[0]

        # Use a ForEach with items [1] as a workaround for grouping,
        # or just return first activity with a note
        return {
            "name": self._sanitize_name(task.name),
            "type": "ExecutePipeline",
            "typeProperties": {
                "pipeline": {
                    "referenceName": self._sanitize_name(f"sub_{task.name}"),
                    "type": "PipelineReference",
                },
                "waitOnCompletion": True,
            },
        }

    def _for_loop_to_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict:
        """Convert For Loop Container to Until activity."""
        child_activities = [self._task_to_activity(c, connections) for c in task.child_tasks]
        child_activities = [a for a in child_activities if a is not None]

        return {
            "name": self._sanitize_name(task.name),
            "type": "Until",
            "typeProperties": {
                "expression": {
                    "value": task.expression or "@bool(true)",
                    "type": "Expression",
                },
                "activities": child_activities,
                "timeout": "0.01:00:00",
            },
        }

    def _foreach_to_activity(self, task: ControlFlowTask, connections: list[ConnectionManager]) -> dict:
        """Convert ForEach Loop to ForEach activity."""
        child_activities = [self._task_to_activity(c, connections) for c in task.child_tasks]
        child_activities = [a for a in child_activities if a is not None]

        items_expr = self._foreach_items_expression(task)

        return {
            "name": self._sanitize_name(task.name),
            "type": "ForEach",
            "typeProperties": {
                "isSequential": True,
                "items": {
                    "value": items_expr,
                    "type": "Expression",
                },
                "activities": child_activities,
            },
        }

    def _execute_package_to_activity(self, task: ControlFlowTask) -> dict:
        """Convert Execute Package task to Invoke Pipeline activity.

        Includes parameter passing when the SSIS task has parameter bindings.
        """
        ref_name = task.properties.get("PackageName", task.name)
        # Strip .dtsx extension if present
        if ref_name.lower().endswith(".dtsx"):
            ref_name = ref_name[:-5]
        # Strip leading numeric prefix (e.g. '10_' from '10_PostProcess_Aggregations')
        import re as _re

        ref_name = _re.sub(r"^\d+_", "", ref_name)

        activity: dict[str, Any] = {
            "name": self._sanitize_name(task.name),
            "type": "ExecutePipeline",
            "typeProperties": {
                "pipeline": {
                    "referenceName": self._sanitize_name(ref_name),
                    "type": "PipelineReference",
                },
                "waitOnCompletion": True,
            },
        }

        # Pass parameter bindings from parent to child pipeline.
        # If explicit SSIS bindings exist, use them; otherwise forward
        # ALL parent parameters so child pipelines always receive the
        # project-wide configuration (Environment, ServerName, …).
        params: dict[str, Any] = {}
        if task.parameter_bindings:
            for child_param, parent_var in task.parameter_bindings.items():
                # Convert SSIS variable reference to ADF expression
                if parent_var.startswith("$"):
                    # $Package::VarName or $Project::VarName
                    parts = parent_var.split("::")
                    var_name = parts[-1] if len(parts) > 1 else parent_var.lstrip("$")
                    params[child_param] = {
                        "value": f"@pipeline().parameters.{var_name}",
                        "type": "Expression",
                    }
                else:
                    # Strip SSIS namespace prefix (e.g. SystemLog::LoadExecutionId → LoadExecutionId)
                    var_name = parent_var.split("::")[-1] if "::" in parent_var else parent_var
                    params[child_param] = {
                        "value": f"@pipeline().parameters.{var_name}",
                        "type": "Expression",
                    }
        elif self._current_package is not None:
            # No explicit bindings → forward all pipeline parameters
            for p in self._current_package.project_parameters:
                params[p.name] = {
                    "value": f"@pipeline().parameters.{p.name}",
                    "type": "Expression",
                }
            for p in self._current_package.parameters:
                params[p.name] = {
                    "value": f"@pipeline().parameters.{p.name}",
                    "type": "Expression",
                }

        if params:
            activity["typeProperties"]["parameters"] = params

        return activity

    def _expression_to_set_variable(self, task: ControlFlowTask) -> dict:
        """Convert Expression task to Set Variable activity."""
        return {
            "name": self._sanitize_name(task.name),
            "type": "SetVariable",
            "typeProperties": {
                "variableName": task.name,
                "value": {
                    "value": task.expression or "",
                    "type": "Expression",
                },
            },
        }

    def _execute_process_to_activity(self, task: ControlFlowTask) -> dict:
        """Convert Execute Process task to an ExecutePipeline activity.

        SSIS Execute Process runs an external executable.  In Fabric there is
        no direct equivalent, so we create an ExecutePipeline activity that
        references a sub-pipeline the user should implement manually.
        The original command line is preserved in the description.
        """
        exe_path = task.properties.get("Executable", task.description or "")
        return {
            "name": self._sanitize_name(task.name),
            "type": "ExecutePipeline",
            "typeProperties": {
                "pipeline": {
                    "referenceName": self._sanitize_name(f"proc_{task.name}"),
                    "type": "PipelineReference",
                },
                "waitOnCompletion": True,
            },
            "description": (f"TODO: Replace with Fabric-native logic. Original SSIS Execute Process: {exe_path}"),
        }

    def _send_mail_to_activity(self, task: ControlFlowTask) -> dict:
        """Convert Send Mail task to an Office 365 Outlook activity.

        Fabric Data Factory supports the **Office365Outlook** activity type
        for sending emails.  The SSIS ToLine, Subject and MessageSource
        properties are mapped to the Outlook activity fields.
        """
        to_line = task.properties.get("ToLine", "TODO@example.com")
        subject = task.properties.get("Subject", "ETL Notification")
        body = task.properties.get("MessageSource", task.description or "")
        cc_line = task.properties.get("CCLine", "")

        activity: dict[str, Any] = {
            "name": self._sanitize_name(task.name),
            "type": "Office365Outlook",
            "typeProperties": {
                "method": "sendmail",
                "to": to_line,
                "subject": subject,
                "body": body,
            },
        }
        if cc_line:
            activity["typeProperties"]["cc"] = cc_line
        return activity

    # -----------------------------------------------------------------
    # ForEach enumerator → items expression
    # -----------------------------------------------------------------
    @staticmethod
    def _foreach_items_expression(task: ControlFlowTask) -> str:
        """Derive ForEach items expression from parsed enumerator metadata."""
        enum_type = task.properties.get("_enumerator_type", "")
        if enum_type == "File":
            folder = task.properties.get("_enum_Folder", task.properties.get("_enum_Directory", ""))
            spec = task.properties.get("_enum_FileSpec", "*.*")
            if folder:
                return f"@activity('ListFiles').output.childItems  {{/* source: {folder}/{spec} */}}"
            return "@activity('ListFiles').output.childItems  /* TODO: configure GetMetadata source */"
        if enum_type == "Item":
            return "@pipeline().parameters.items  /* SSIS ForEach Item enumerator */"
        if enum_type == "ADO":
            return "@activity('LookupQuery').output.value  /* SSIS ForEach ADO enumerator */"
        if enum_type == "FromVariable":
            return "@variables('iterationArray')  /* TODO: populate from SSIS variable */"
        if enum_type == "NodeList":
            return "@json(variables('xmlNodes'))  /* TODO: SSIS ForEach NodeList */"
        # Default fallback
        return "@pipeline().parameters.items  /* TODO: configure enumerator */"

    # -----------------------------------------------------------------
    # FILE_SYSTEM task → Notebook / Script activity
    # -----------------------------------------------------------------
    def _file_system_to_activity(self, task: ControlFlowTask) -> dict:
        """Convert SSIS File System task to a Fabric TridentNotebook activity.

        The notebook should contain mssparkutils.fs calls matching the
        SSIS operation (CopyFile, MoveFile, DeleteFile, etc.).
        """
        op = task.properties.get("_operation_name", task.properties.get("Operation", "Unknown"))
        src = task.properties.get("Source", "")
        dst = task.properties.get("Destination", "")

        # Map SSIS File System operations to mssparkutils.fs calls
        fs_call_map = {
            "CopyFile": f'mssparkutils.fs.cp("{src}", "{dst}", True)',
            "MoveFile": f'mssparkutils.fs.mv("{src}", "{dst}", True)',
            "DeleteFile": f'mssparkutils.fs.rm("{src}", True)',
            "RenameFile": f'mssparkutils.fs.mv("{src}", "{dst}", True)',
            "CreateDirectory": f'mssparkutils.fs.mkdirs("{dst or src}")',
            "CopyDirectory": f'mssparkutils.fs.cp("{src}", "{dst}", True)',
            "MoveDirectory": f'mssparkutils.fs.mv("{src}", "{dst}", True)',
            "DeleteDirectory": f'mssparkutils.fs.rm("{src}", True)',
            "DeleteDirectoryContent": f'mssparkutils.fs.rm("{src}", True)',
        }
        fs_call = fs_call_map.get(op, f'mssparkutils.fs.ls("{src or dst}")  # TODO: unknown op {op}')

        return {
            "name": self._sanitize_name(task.name),
            "type": "TridentNotebook",
            "typeProperties": {
                "notebookId": "",
            },
            "description": (
                f"SSIS File System Task: {op}. "
                f"Source='{src}' Destination='{dst}'. "
                f"Suggested mssparkutils call: {fs_call}"
            ),
        }

    # -----------------------------------------------------------------
    # FTP task → Web / Copy activity
    # -----------------------------------------------------------------
    def _ftp_to_activity(self, task: ControlFlowTask) -> dict:
        """Convert SSIS FTP task to a Fabric Copy or Web activity.

        FTP Send/Receive map to Copy activities; other operations
        become placeholder Web activities.
        """
        op = task.properties.get("_ftp_operation_name", task.properties.get("Operation", "Unknown"))
        local_path = task.properties.get("LocalPath", "")
        remote_path = task.properties.get("RemotePath", "")

        if op in ("Send", "Receive"):
            return {
                "name": self._sanitize_name(task.name),
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "BinarySource",
                        "storeSettings": {
                            "type": "FtpReadSettings" if op == "Receive" else "FileSystemReadSettings",
                        },
                    },
                    "sink": {
                        "type": "BinarySink",
                        "storeSettings": {
                            "type": "LakehouseWriteSettings" if op == "Receive" else "FtpWriteSettings",
                        },
                    },
                },
                "description": (
                    f"SSIS FTP {op}: local='{local_path}' remote='{remote_path}'. "
                    f"TODO: Configure linked service and dataset references."
                ),
            }

        # Other FTP ops → placeholder
        return {
            "name": self._sanitize_name(task.name),
            "type": "TridentNotebook",
            "typeProperties": {
                "notebookId": "",
            },
            "description": (
                f"SSIS FTP Task: {op}. "
                f"Local='{local_path}' Remote='{remote_path}'. "
                f"TODO: Implement FTP {op} operation in notebook."
            ),
        }

    def _unknown_task_placeholder(self, task: ControlFlowTask) -> dict:
        """Create a placeholder for unsupported task types."""
        return {
            "name": self._sanitize_name(task.name),
            "type": "Wait",
            "typeProperties": {
                "waitTimeInSeconds": 1,
            },
            "description": (
                f"TODO: Manual migration required. "
                f"Original SSIS task type: {task.task_type.value}. "
                f"Description: {task.description}"
            ),
        }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _package_params_to_df_params(self, package: SSISPackage) -> dict:
        """Convert SSIS parameters (package + project) to ADF pipeline parameters."""
        params = {}
        # Project-level parameters
        for param in package.project_parameters:
            params[param.name] = {
                "type": self._map_data_type(param.data_type),
                "defaultValue": param.value,
            }
        # Package-level parameters (may override project params)
        for param in package.parameters:
            params[param.name] = {
                "type": self._map_data_type(param.data_type),
                "defaultValue": param.value,
            }
        return params

    def _variables_to_df_variables(self, package: SSISPackage) -> dict:
        """Convert SSIS variables to ADF pipeline variables."""
        variables: dict[str, Any] = {}
        for var in package.variables:
            # Skip system variables
            if var.namespace == "System":
                continue
            variables[var.name] = {
                "type": self._map_data_type(var.data_type),
                "defaultValue": var.value,
            }
        return variables

    def _map_data_type(self, ssis_type: str) -> str:
        """Map SSIS data type to ADF parameter type."""
        type_map = {
            "string": "String",
            "int16": "Int",
            "int32": "Int",
            "int64": "Int",
            "boolean": "Bool",
            "datetime": "String",
            "double": "Float",
            "single": "Float",
            "decimal": "Float",
        }
        return type_map.get(ssis_type.lower(), "String")

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Sanitize a name for use in Data Factory."""
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        sanitized = re.sub(r"_+", "_", sanitized)
        return sanitized.strip("_")[:260]
