"""
SSIS .dtsx Package Parser
==========================
Parses SSIS .dtsx (XML) files and produces SSISPackage model instances.
Handles SSIS 2012-2022 package formats.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from lxml import etree

from ssis_to_fabric.analyzer.models import (
    Column,
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    DataFlowPath,
    EventHandler,
    MigrationComplexity,
    PrecedenceConstraint,
    PrecedenceConstraintType,
    SqlParameterBinding,
    SSISPackage,
    TaskType,
    Variable,
    VariableScope,
)
from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# SSIS XML namespace mappings
NAMESPACES = {
    "DTS": "www.microsoft.com/SqlServer/Dts",
    "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
    "Pipeline": "www.microsoft.com/SqlServer/Dts",
}

# Map SSIS class IDs to TaskType
TASK_CLASS_MAP: dict[str, TaskType] = {
    "Microsoft.Pipeline": TaskType.DATA_FLOW,
    "Microsoft.ExecuteSQLTask": TaskType.EXECUTE_SQL,
    "Microsoft.ExecuteProcess": TaskType.EXECUTE_PROCESS,
    "Microsoft.FileSystemTask": TaskType.FILE_SYSTEM,
    "Microsoft.FtpTask": TaskType.FTP,
    "Microsoft.SendMailTask": TaskType.SEND_MAIL,
    "Microsoft.ScriptTask": TaskType.SCRIPT,
    "STOCK:SEQUENCE": TaskType.SEQUENCE_CONTAINER,
    "STOCK:FORLOOP": TaskType.FOR_LOOP,
    "STOCK:FOREACHLOOP": TaskType.FOREACH_LOOP,
    "Microsoft.ExpressionTask": TaskType.EXPRESSION,
    "Microsoft.ExecutePackageTask": TaskType.EXECUTE_PACKAGE,
}

# Map component class IDs to DataFlowComponentType
COMPONENT_CLASS_MAP: dict[str, DataFlowComponentType] = {
    # Sources
    "Microsoft.OLEDBSource": DataFlowComponentType.OLE_DB_SOURCE,
    "Microsoft.ADONETSource": DataFlowComponentType.ADO_NET_SOURCE,
    "Microsoft.FlatFileSource": DataFlowComponentType.FLAT_FILE_SOURCE,
    "Microsoft.ExcelSource": DataFlowComponentType.EXCEL_SOURCE,
    "Microsoft.ODBCSource": DataFlowComponentType.ODBC_SOURCE,
    "Microsoft.XMLSource": DataFlowComponentType.XML_SOURCE,
    "Microsoft.RawFileSource": DataFlowComponentType.RAW_FILE_SOURCE,
    "Microsoft.CDCSource": DataFlowComponentType.CDC_SOURCE,
    # Destinations
    "Microsoft.OLEDBDestination": DataFlowComponentType.OLE_DB_DESTINATION,
    "Microsoft.ADONETDestination": DataFlowComponentType.ADO_NET_DESTINATION,
    "Microsoft.FlatFileDestination": DataFlowComponentType.FLAT_FILE_DESTINATION,
    "Microsoft.ExcelDestination": DataFlowComponentType.EXCEL_DESTINATION,
    "Microsoft.ODBCDestination": DataFlowComponentType.ODBC_DESTINATION,
    "Microsoft.RawFileDestination": DataFlowComponentType.RAW_FILE_DESTINATION,
    "Microsoft.RecordsetDestination": DataFlowComponentType.RECORDSET_DESTINATION,
    "Microsoft.SQLServerDestination": DataFlowComponentType.SQL_SERVER_DESTINATION,
    "Microsoft.DataReaderDestination": DataFlowComponentType.DATA_READER_DESTINATION,
    # Transforms
    "Microsoft.DerivedColumn": DataFlowComponentType.DERIVED_COLUMN,
    "Microsoft.ConditionalSplit": DataFlowComponentType.CONDITIONAL_SPLIT,
    "Microsoft.Lookup": DataFlowComponentType.LOOKUP,
    "Microsoft.MergeJoin": DataFlowComponentType.MERGE_JOIN,
    "Microsoft.UnionAll": DataFlowComponentType.UNION_ALL,
    "Microsoft.Aggregate": DataFlowComponentType.AGGREGATE,
    "Microsoft.Sort": DataFlowComponentType.SORT,
    "Microsoft.Multicast": DataFlowComponentType.MULTICAST,
    "Microsoft.RowCount": DataFlowComponentType.ROW_COUNT,
    "Microsoft.DataConvert": DataFlowComponentType.DATA_CONVERSION,
    "Microsoft.ScriptComponent": DataFlowComponentType.SCRIPT_COMPONENT,
    "Microsoft.OLEDBCommand": DataFlowComponentType.OLE_DB_COMMAND,
    "Microsoft.SlowlyChangingDimension": DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
    "Microsoft.Pivot": DataFlowComponentType.PIVOT,
    "Microsoft.UnPivot": DataFlowComponentType.UNPIVOT,
    "Microsoft.FuzzyLookup": DataFlowComponentType.FUZZY_LOOKUP,
    "Microsoft.FuzzyGrouping": DataFlowComponentType.FUZZY_GROUPING,
    "Microsoft.TermLookup": DataFlowComponentType.TERM_LOOKUP,
    "Microsoft.CopyColumn": DataFlowComponentType.COPY_COLUMN,
    "Microsoft.CharacterMap": DataFlowComponentType.CHARACTER_MAP,
    "Microsoft.Audit": DataFlowComponentType.AUDIT,
    "Microsoft.Merge": DataFlowComponentType.MERGE,
    "Microsoft.CDCSplitter": DataFlowComponentType.CDC_SPLITTER,
    "Microsoft.PercentageSampling": DataFlowComponentType.PERCENTAGE_SAMPLING,
    "Microsoft.RowSampling": DataFlowComponentType.ROW_SAMPLING,
    "Microsoft.BalancedDataDistributor": DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR,
    "Microsoft.CacheTransform": DataFlowComponentType.CACHE_TRANSFORM,
}

# Complexity mapping for data flow components
COMPONENT_COMPLEXITY: dict[DataFlowComponentType, MigrationComplexity] = {
    # Sources
    DataFlowComponentType.OLE_DB_SOURCE: MigrationComplexity.LOW,
    DataFlowComponentType.ADO_NET_SOURCE: MigrationComplexity.LOW,
    DataFlowComponentType.FLAT_FILE_SOURCE: MigrationComplexity.LOW,
    DataFlowComponentType.EXCEL_SOURCE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.ODBC_SOURCE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.XML_SOURCE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.RAW_FILE_SOURCE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.CDC_SOURCE: MigrationComplexity.HIGH,
    # Destinations
    DataFlowComponentType.OLE_DB_DESTINATION: MigrationComplexity.LOW,
    DataFlowComponentType.ADO_NET_DESTINATION: MigrationComplexity.LOW,
    DataFlowComponentType.FLAT_FILE_DESTINATION: MigrationComplexity.MEDIUM,
    DataFlowComponentType.EXCEL_DESTINATION: MigrationComplexity.MEDIUM,
    DataFlowComponentType.ODBC_DESTINATION: MigrationComplexity.MEDIUM,
    DataFlowComponentType.RAW_FILE_DESTINATION: MigrationComplexity.MEDIUM,
    DataFlowComponentType.RECORDSET_DESTINATION: MigrationComplexity.HIGH,
    DataFlowComponentType.SQL_SERVER_DESTINATION: MigrationComplexity.LOW,
    DataFlowComponentType.DATA_READER_DESTINATION: MigrationComplexity.HIGH,
    # Transforms
    DataFlowComponentType.DERIVED_COLUMN: MigrationComplexity.MEDIUM,
    DataFlowComponentType.CONDITIONAL_SPLIT: MigrationComplexity.MEDIUM,
    DataFlowComponentType.LOOKUP: MigrationComplexity.MEDIUM,
    DataFlowComponentType.MERGE_JOIN: MigrationComplexity.MEDIUM,
    DataFlowComponentType.UNION_ALL: MigrationComplexity.LOW,
    DataFlowComponentType.AGGREGATE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.SORT: MigrationComplexity.LOW,
    DataFlowComponentType.MULTICAST: MigrationComplexity.LOW,
    DataFlowComponentType.ROW_COUNT: MigrationComplexity.LOW,
    DataFlowComponentType.DATA_CONVERSION: MigrationComplexity.MEDIUM,
    DataFlowComponentType.SCRIPT_COMPONENT: MigrationComplexity.HIGH,
    DataFlowComponentType.OLE_DB_COMMAND: MigrationComplexity.MEDIUM,
    DataFlowComponentType.SLOWLY_CHANGING_DIMENSION: MigrationComplexity.HIGH,
    DataFlowComponentType.PIVOT: MigrationComplexity.MEDIUM,
    DataFlowComponentType.UNPIVOT: MigrationComplexity.MEDIUM,
    DataFlowComponentType.FUZZY_LOOKUP: MigrationComplexity.HIGH,
    DataFlowComponentType.FUZZY_GROUPING: MigrationComplexity.HIGH,
    DataFlowComponentType.TERM_LOOKUP: MigrationComplexity.HIGH,
    DataFlowComponentType.COPY_COLUMN: MigrationComplexity.LOW,
    DataFlowComponentType.CHARACTER_MAP: MigrationComplexity.MEDIUM,
    DataFlowComponentType.AUDIT: MigrationComplexity.LOW,
    DataFlowComponentType.MERGE: MigrationComplexity.MEDIUM,
    DataFlowComponentType.CDC_SPLITTER: MigrationComplexity.HIGH,
    DataFlowComponentType.PERCENTAGE_SAMPLING: MigrationComplexity.MEDIUM,
    DataFlowComponentType.ROW_SAMPLING: MigrationComplexity.MEDIUM,
    DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR: MigrationComplexity.LOW,
    DataFlowComponentType.CACHE_TRANSFORM: MigrationComplexity.MEDIUM,
}


class DTSXParser:
    """
    Parses SSIS .dtsx packages into SSISPackage models.

    Usage:
        parser = DTSXParser()
        package = parser.parse("path/to/package.dtsx")
    """

    def parse(self, file_path: str | Path) -> SSISPackage:
        """Parse a .dtsx file and return an SSISPackage model."""
        file_path = Path(file_path)
        logger.info("parsing_package", file=str(file_path))

        tree = etree.parse(str(file_path))
        root = tree.getroot()

        # Handle namespace
        nsmap = self._build_nsmap(root)

        package = SSISPackage(
            name=self._get_dts_attr(root, "ObjectName", nsmap) or file_path.stem,
            file_path=str(file_path),
            description=self._get_dts_attr(root, "Description", nsmap) or "",
            creation_date=self._get_dts_attr(root, "CreationDate", nsmap) or "",
            creator_name=self._get_dts_attr(root, "CreatorName", nsmap) or "",
            format_version=self._get_dts_attr(root, "FormatVersion", nsmap) or "",
        )

        # Parse sections
        package.connection_managers = self._parse_connection_managers(root, nsmap)
        package.variables = self._parse_variables(root, nsmap)
        package.parameters = self._parse_parameters(root, nsmap)
        package.control_flow_tasks = self._parse_executables(root, nsmap)
        package.precedence_constraints = self._parse_precedence_constraints(root, nsmap)
        package.event_handlers = self._parse_event_handlers(root, nsmap)

        package.compute_stats()

        logger.info(
            "package_parsed",
            name=package.name,
            tasks=package.total_tasks,
            data_flows=package.total_data_flows,
            complexity=package.overall_complexity.value,
        )
        return package

    def parse_directory(self, dir_path: str | Path) -> list[SSISPackage]:
        """Parse all .dtsx files in a directory (recursively).

        Also parses ``Project.params`` if present and merges project-level
        parameters onto each package.  Parses ``.conmgr`` files to provide
        project-level connection managers.
        """
        dir_path = Path(dir_path)
        packages = []

        # Parse project-level parameters
        project_params = self._parse_project_params(dir_path)

        # Parse project-level connection managers (.conmgr files)
        project_connections = self._parse_project_connections(dir_path)

        for dtsx_file in sorted(dir_path.rglob("*.dtsx")):
            try:
                pkg = self.parse(dtsx_file)
                if project_params:
                    pkg.project_parameters = project_params
                # Merge project-level connections into each package
                if project_connections:
                    existing_ids = {cm.id for cm in pkg.connection_managers}
                    existing_names = {cm.name for cm in pkg.connection_managers}
                    for pc in project_connections:
                        if pc.id not in existing_ids and pc.name not in existing_names:
                            pkg.connection_managers.append(pc)
                packages.append(pkg)
            except Exception as e:
                logger.error("parse_failed", file=str(dtsx_file), error=str(e))
        return packages

    def _parse_project_params(self, dir_path: Path) -> list[Variable]:
        """Parse a ``Project.params`` file if present in the directory.

        The Project.params file uses SSIS XML format::

            <SSIS:Parameters>
              <SSIS:Parameter SSIS:Name="ParamName">
                <SSIS:Properties>
                  <SSIS:Property SSIS:Name="DataType">String</SSIS:Property>
                  <SSIS:Property SSIS:Name="DesignDefaultValue">val</SSIS:Property>
                  <SSIS:Property SSIS:Name="Required">True</SSIS:Property>
                  <SSIS:Property SSIS:Name="Sensitive">False</SSIS:Property>
                  <SSIS:Property SSIS:Name="Description">desc</SSIS:Property>
                </SSIS:Properties>
              </SSIS:Parameter>
            </SSIS:Parameters>
        """
        params_file = dir_path / "Project.params"
        if not params_file.exists():
            return []

        try:
            tree = etree.parse(str(params_file))
            root = tree.getroot()
        except Exception as e:
            logger.warning("project_params_parse_failed", file=str(params_file), error=str(e))
            return []

        # Build SSIS namespace map
        ssis_ns = ""
        for _prefix, uri in (root.nsmap or {}).items():
            if uri and "SqlServer/SSIS" in uri:
                ssis_ns = uri
                break
        if not ssis_ns:
            ssis_ns = "www.microsoft.com/SqlServer/SSIS"

        parameters: list[Variable] = []
        for param_elem in root.findall(f"{{{ssis_ns}}}Parameter"):
            name = param_elem.get(f"{{{ssis_ns}}}Name", "")
            props: dict[str, str] = {}
            for props_container in param_elem.findall(f"{{{ssis_ns}}}Properties"):
                for prop_elem in props_container.findall(f"{{{ssis_ns}}}Property"):
                    prop_name = prop_elem.get(f"{{{ssis_ns}}}Name", "")
                    prop_val = prop_elem.text or ""
                    props[prop_name] = prop_val

            parameters.append(
                Variable(
                    name=name,
                    data_type=props.get("DataType", "String"),
                    value=props.get("DesignDefaultValue", ""),
                    description=props.get("Description", ""),
                    required=props.get("Required", "").lower() == "true",
                    sensitive=props.get("Sensitive", "").lower() == "true",
                    scope=VariableScope.PACKAGE,
                    is_parameter=True,
                )
            )

        logger.info("project_params_parsed", count=len(parameters), file=str(params_file))
        return parameters

    def _parse_project_connections(self, dir_path: Path) -> list[ConnectionManager]:
        """Parse ``.conmgr`` files in the project directory.

        Each ``.conmgr`` file defines a project-level connection manager::

            <DTS:ConnectionManager ...
              DTS:ObjectName="cmgr_Source" DTS:DTSID="{GUID}" DTS:CreationName="OLEDB">
              <DTS:ObjectData>
                <DTS:ConnectionManager DTS:ConnectionString="..." />
              </DTS:ObjectData>
            </DTS:ConnectionManager>
        """
        connections: list[ConnectionManager] = []
        for conmgr_file in sorted(dir_path.rglob("*.conmgr")):
            try:
                tree = etree.parse(str(conmgr_file))
                root = tree.getroot()
                nsmap = self._build_nsmap(root)
                cm = self._parse_single_connection_manager(root, nsmap)
                if cm:
                    connections.append(cm)
            except Exception as e:
                logger.warning("conmgr_parse_failed", file=str(conmgr_file), error=str(e))
        if connections:
            logger.info("project_connections_parsed", count=len(connections))
        return connections

    # =========================================================================
    # Namespace Helpers
    # =========================================================================

    def _build_nsmap(self, root: etree._Element) -> dict[str, str]:
        """Build namespace map from root element."""
        nsmap = {}
        for _prefix, uri in root.nsmap.items():
            if _prefix:
                nsmap[_prefix] = uri
        # Add DTS namespace if not present
        if "DTS" not in nsmap:
            for _prefix, uri in root.nsmap.items():
                if uri and "SqlServer/Dts" in uri:
                    nsmap["DTS"] = uri
                    break
        return nsmap

    def _get_dts_attr(self, elem: etree._Element, attr_name: str, nsmap: dict) -> str | None:
        """Get a DTS-namespaced attribute value."""
        dts_ns = nsmap.get("DTS", "www.microsoft.com/SqlServer/Dts")
        return elem.get(f"{{{dts_ns}}}{attr_name}")

    def _find_dts(self, parent: etree._Element, tag: str, nsmap: dict) -> list[etree._Element]:
        """Find child elements with DTS namespace."""
        dts_ns = nsmap.get("DTS", "www.microsoft.com/SqlServer/Dts")
        return parent.findall(f"{{{dts_ns}}}{tag}")

    # =========================================================================
    # Connection Managers
    # =========================================================================

    def _parse_connection_managers(self, root: etree._Element, nsmap: dict) -> list[ConnectionManager]:
        """Parse all connection managers from the package.

        DTSX files nest ``<DTS:ConnectionManager>`` elements inside a
        ``<DTS:ConnectionManagers>`` container.  We look there first, then
        fall back to direct children of *root* for backwards compatibility
        (e.g. synthetic test files).
        """
        managers: list[ConnectionManager] = []
        dts_ns = nsmap.get("DTS", "www.microsoft.com/SqlServer/Dts")

        # Primary: look inside <DTS:ConnectionManagers> container
        container_path = f"{{{dts_ns}}}ConnectionManagers/{{{dts_ns}}}ConnectionManager"
        cm_elems = root.findall(container_path)

        # Fallback: direct children (synthetic / test files)
        if not cm_elems:
            cm_elems = self._find_dts(root, "ConnectionManager", nsmap)

        for cm_elem in cm_elems:
            cm = self._parse_single_connection_manager(cm_elem, nsmap)
            if cm:
                managers.append(cm)
        return managers

    def _parse_single_connection_manager(self, elem: etree._Element, nsmap: dict) -> ConnectionManager | None:
        """Parse a single connection manager element."""
        name = self._get_dts_attr(elem, "ObjectName", nsmap) or "Unknown"
        creation_name = self._get_dts_attr(elem, "CreationName", nsmap) or ""
        dtsid = self._get_dts_attr(elem, "DTSID", nsmap) or ""

        conn_type = self._classify_connection_type(creation_name)

        # Get connection string from ObjectData
        conn_string = ""
        server = ""
        database = ""
        dts_ns = nsmap.get("DTS", "www.microsoft.com/SqlServer/Dts")
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                # Try non-namespaced first (inline ConnectionManager in .dtsx)
                conn_string = child.get("ConnectionString", "") or child.get("connectionString", "")
                # Try DTS-namespaced attribute (used in .conmgr files)
                if not conn_string:
                    conn_string = child.get(f"{{{dts_ns}}}ConnectionString", "")
                if conn_string:
                    server = self._extract_from_conn_string(conn_string, "Data Source", "Server")
                    database = self._extract_from_conn_string(conn_string, "Initial Catalog", "Database")

        return ConnectionManager(
            id=dtsid,
            name=name,
            connection_type=conn_type,
            connection_string=conn_string,
            server=server,
            database=database,
            provider=creation_name,
        )

    def _classify_connection_type(self, creation_name: str) -> ConnectionType:
        """Determine connection type from SSIS creation name."""
        cn = creation_name.upper()
        if "OLEDB" in cn:
            return ConnectionType.OLEDB
        if "ADO.NET" in cn or "ADONET" in cn:
            return ConnectionType.ADO_NET
        if "FLATFILE" in cn or "FLAT FILE" in cn:
            return ConnectionType.FLAT_FILE
        if "EXCEL" in cn:
            return ConnectionType.EXCEL
        if "ODBC" in cn:
            return ConnectionType.ODBC
        if "SMTP" in cn:
            return ConnectionType.SMTP
        if "FTP" in cn:
            return ConnectionType.FTP
        if "HTTP" in cn:
            return ConnectionType.HTTP
        if "ORACLE" in cn:
            return ConnectionType.ORACLE
        if "SHAREPOINT" in cn:
            return ConnectionType.SHAREPOINT
        if "MSOLAP" in cn or "ANALYSISSERVICES" in cn or "SSAS" in cn:
            return ConnectionType.ANALYSIS_SERVICES
        if "FILE" in cn:
            return ConnectionType.FILE
        return ConnectionType.UNKNOWN

    def _extract_from_conn_string(self, conn_string: str, *keys: str) -> str:
        """Extract a value from a connection string by key."""
        for part in conn_string.split(";"):
            for key in keys:
                if part.strip().lower().startswith(key.lower() + "="):
                    return part.split("=", 1)[1].strip()
        return ""

    # =========================================================================
    # Variables & Parameters
    # =========================================================================

    def _parse_variables(self, root: etree._Element, nsmap: dict) -> list[Variable]:
        """Parse package-level variables."""
        variables = []
        for var_elem in self._find_dts(root, "Variable", nsmap):
            name = self._get_dts_attr(var_elem, "ObjectName", nsmap) or ""
            namespace = self._get_dts_attr(var_elem, "Namespace", nsmap) or "User"
            expression = self._get_dts_attr(var_elem, "Expression", nsmap) or ""

            # Get value from VariableValue element
            value = ""
            data_type = "String"
            for val_elem in self._find_dts(var_elem, "VariableValue", nsmap):
                value = val_elem.text or ""
                type_attr = self._get_dts_attr(val_elem, "DataType", nsmap) or ""
                if type_attr:
                    data_type = type_attr

            variables.append(
                Variable(
                    name=name,
                    namespace=namespace,
                    data_type=data_type,
                    value=value,
                    expression=expression,
                    scope=VariableScope.PACKAGE,
                )
            )
        return variables

    def _parse_parameters(self, root: etree._Element, nsmap: dict) -> list[Variable]:
        """Parse package parameters (SSIS 2012+).

        Handles both:
        - Direct ``<DTS:PackageParameter>`` children of root (loose format)
        - ``<DTS:PackageParameters>`` container wrapping the elements (standard format)
        """
        parameters: list[Variable] = []
        param_elems: list[etree._Element] = []

        # Standard format: <DTS:PackageParameters> container
        for container in self._find_dts(root, "PackageParameters", nsmap):
            param_elems.extend(self._find_dts(container, "PackageParameter", nsmap))

        # Loose format: direct children
        param_elems.extend(self._find_dts(root, "PackageParameter", nsmap))

        for param_elem in param_elems:
            name = self._get_dts_attr(param_elem, "ObjectName", nsmap) or ""
            data_type = self._get_dts_attr(param_elem, "DataType", nsmap) or "String"
            value = ""

            # Try DTS:Property[@Name='ParameterValue'] first
            for val_elem in self._find_dts(param_elem, "Property", nsmap):
                if self._get_dts_attr(val_elem, "Name", nsmap) == "ParameterValue":
                    value = val_elem.text or ""

            # Fallback: DTS:VariableValue (used in standard format)
            if not value:
                for val_elem in self._find_dts(param_elem, "VariableValue", nsmap):
                    value = val_elem.text or ""

            # Avoid duplicates (if both container + loose yield same param)
            if any(p.name == name for p in parameters):
                continue

            parameters.append(
                Variable(
                    name=name,
                    data_type=data_type,
                    value=value,
                    scope=VariableScope.PACKAGE,
                    is_parameter=True,
                )
            )
        return parameters

    # =========================================================================
    # Control Flow Tasks (Executables)
    # =========================================================================

    def _parse_executables(self, parent: etree._Element, nsmap: dict) -> list[ControlFlowTask]:
        """Parse executable elements (tasks and containers)."""
        tasks = []
        # Find Executables container
        for executables in self._find_dts(parent, "Executables", nsmap):
            for exec_elem in self._find_dts(executables, "Executable", nsmap):
                task = self._parse_single_executable(exec_elem, nsmap)
                if task:
                    tasks.append(task)
        return tasks

    def _parse_single_executable(self, elem: etree._Element, nsmap: dict) -> ControlFlowTask | None:
        """Parse a single executable element into a ControlFlowTask."""
        name = self._get_dts_attr(elem, "ObjectName", nsmap) or "Unknown"
        creation_name = self._get_dts_attr(elem, "CreationName", nsmap) or ""
        dtsid = self._get_dts_attr(elem, "DTSID", nsmap) or ""
        ref_id = self._get_dts_attr(elem, "refId", nsmap) or ""
        description = self._get_dts_attr(elem, "Description", nsmap) or ""
        disabled = self._get_dts_attr(elem, "Disabled", nsmap) == "True"

        task_type = self._classify_task_type(creation_name)

        task = ControlFlowTask(
            id=dtsid,
            ref_id=ref_id,
            name=name,
            task_type=task_type,
            description=description,
            disabled=disabled,
        )

        # Parse task-specific content
        if task_type == TaskType.EXECUTE_SQL:
            self._parse_execute_sql(elem, task, nsmap)
        elif task_type == TaskType.DATA_FLOW:
            self._parse_data_flow(elem, task, nsmap)
        elif task_type == TaskType.EXECUTE_PACKAGE:
            self._parse_execute_package(elem, task, nsmap)
        elif task_type == TaskType.SEND_MAIL:
            self._parse_send_mail(elem, task, nsmap)
        elif task_type == TaskType.FILE_SYSTEM:
            self._parse_file_system(elem, task, nsmap)
        elif task_type == TaskType.FTP:
            self._parse_ftp(elem, task, nsmap)
        elif task_type == TaskType.FOREACH_LOOP:
            self._parse_foreach_enumerator(elem, task, nsmap)

        # Parse child tasks for containers
        if task_type in (TaskType.SEQUENCE_CONTAINER, TaskType.FOR_LOOP, TaskType.FOREACH_LOOP):
            task.child_tasks = self._parse_executables(elem, nsmap)
            task.child_precedence_constraints = self._parse_precedence_constraints(elem, nsmap)

        # Assign complexity
        task.migration_complexity = self._assess_task_complexity(task)

        return task

    def _classify_task_type(self, creation_name: str) -> TaskType:
        """Classify task type from SSIS creation name."""
        for key, task_type in TASK_CLASS_MAP.items():
            if key.lower() in creation_name.lower():
                return task_type
        return TaskType.UNKNOWN

    def _parse_execute_sql(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract SQL statement and parameter bindings from Execute SQL task."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                # Look for SqlStatementSource - may be plain or namespaced attribute
                sql = child.get("SqlStatementSource", "")
                conn_ref = child.get("Connection", "")

                # Also check namespaced attributes (SQLTask:SqlStatementSource)
                if not sql:
                    for attr_key, attr_val in child.attrib.items():
                        local_name = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                        if local_name == "SqlStatementSource":
                            sql = attr_val
                        elif local_name == "Connection" and not conn_ref:
                            conn_ref = attr_val

                if sql:
                    task.sql_statement = sql
                if conn_ref:
                    task.connection_manager_ref = conn_ref

                # Parse ParameterBinding elements
                for sub in child:
                    tag = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if tag != "ParameterBinding":
                        continue
                    param_name = ""
                    var_name = ""
                    direction = "Input"
                    data_type = 0
                    for attr_key, attr_val in sub.attrib.items():
                        local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                        if local == "ParameterName":
                            param_name = attr_val
                        elif local == "DtsVariableName":
                            var_name = attr_val
                        elif local == "ParameterDirection":
                            direction = attr_val
                        elif local == "DataType":
                            with contextlib.suppress(ValueError):
                                data_type = int(attr_val)
                    if var_name:
                        task.sql_parameters.append(
                            SqlParameterBinding(
                                parameter_name=param_name,
                                variable_name=var_name,
                                direction=direction,
                                data_type=data_type,
                            )
                        )

    def _parse_execute_package(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract package reference from Execute Package task."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                # Look for PackageName and Connection in DTS:ExecutePackageTask
                for sub in child:
                    local_name = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if local_name == "PackageName" and sub.text:
                        task.properties["PackageName"] = sub.text.strip()
                    elif local_name == "Connection" and sub.text:
                        task.connection_manager_ref = sub.text.strip()
                    elif local_name == "ParameterAssignment":
                        # Parse parameter binding: child param ← parent variable
                        param_name = ""
                        variable_name = ""
                        for binding in sub:
                            btag = binding.tag.split("}")[-1] if "}" in binding.tag else binding.tag
                            if btag == "ParameterName" and binding.text:
                                param_name = binding.text.strip()
                            elif btag == "BindedVariableOrParameterName" and binding.text:
                                variable_name = binding.text.strip()
                        if param_name and variable_name:
                            task.parameter_bindings[param_name] = variable_name

    def _parse_data_flow(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Parse a Data Flow task's components and paths."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for pipeline in obj_data:
                # Parse components
                for components in pipeline.findall(".//{*}components") or [pipeline]:
                    for comp_elem in components.findall("{*}component") if components != pipeline else []:
                        comp = self._parse_data_flow_component(comp_elem, nsmap)
                        if comp:
                            task.data_flow_components.append(comp)

                    # Fallback: direct children named component
                    if not task.data_flow_components:
                        for comp_elem in pipeline.iter():
                            if comp_elem.tag.endswith("component") and comp_elem != pipeline:
                                comp = self._parse_data_flow_component(comp_elem, nsmap)
                                if comp:
                                    task.data_flow_components.append(comp)

                # Parse paths
                for paths_elem in pipeline.findall(".//{*}paths") or []:
                    for path_elem in paths_elem.findall("{*}path"):
                        path = self._parse_data_flow_path(path_elem)
                        if path:
                            task.data_flow_paths.append(path)

    def _parse_data_flow_component(self, elem: etree._Element, nsmap: dict) -> DataFlowComponent | None:
        """Parse a data flow component element."""
        name = elem.get("name", "") or elem.get("Name", "") or "Unknown"
        class_id = elem.get("componentClassID", "") or elem.get("ComponentClassID", "")

        comp_type = self._classify_component_type(class_id)

        component = DataFlowComponent(
            name=name,
            component_type=comp_type,
            migration_complexity=COMPONENT_COMPLEXITY.get(comp_type, MigrationComplexity.MEDIUM),
        )

        # Extract properties
        for prop_elem in elem.findall(".//{*}property"):
            prop_name = prop_elem.get("name", "")
            prop_value = prop_elem.text or ""
            if prop_name:
                component.properties[prop_name] = prop_value
                if prop_name.lower() in ("sqlcommand", "openrowset", "tableorviewname"):
                    if "select" in prop_value.lower() or "exec" in prop_value.lower():
                        component.sql_command = prop_value
                    else:
                        component.table_name = prop_value

        # Extract columns from input/output, skipping error outputs
        for output_elem in elem.findall(".//{*}output"):
            # Skip error outputs — they contain ErrorCode/ErrorColumn
            if output_elem.get("isErrorOut", "").lower() == "true":
                continue
            for col_elem in output_elem.findall(".//{*}outputColumn"):
                col = Column(
                    name=col_elem.get("name", ""),
                    data_type=col_elem.get("dataType", ""),
                    length=int(col_elem.get("length", "0") or "0"),
                    precision=int(col_elem.get("precision", "0") or "0"),
                    scale=int(col_elem.get("scale", "0") or "0"),
                )

                # Capture Derived Column / Conditional Split expressions
                if comp_type in (
                    DataFlowComponentType.DERIVED_COLUMN,
                    DataFlowComponentType.CONDITIONAL_SPLIT,
                ):
                    for p in col_elem.findall(".//{*}property"):
                        pn = p.get("name", "")
                        if pn in ("Expression", "FriendlyExpression") and p.text:
                            col.expression = p.text

                component.columns.append(col)

        # Also collect inputColumns (not inside <output> elements)
        for col_elem in elem.findall(".//{*}inputColumn"):
            col = Column(
                name=col_elem.get("name", ""),
                data_type=col_elem.get("dataType", ""),
                length=int(col_elem.get("length", "0") or "0"),
                precision=int(col_elem.get("precision", "0") or "0"),
                scale=int(col_elem.get("scale", "0") or "0"),
            )
            component.columns.append(col)

        # Extract connection manager reference from <connections> element
        for conn_elem in elem.findall(".//{*}connection"):
            cm_id = conn_elem.get("connectionManagerID", "")
            cm_ref = conn_elem.get("connectionManagerRefId", "")
            if cm_ref:
                # e.g. "Project.ConnectionManagers[cmgr_Source]" → extract "cmgr_Source"
                import re as _re

                m = _re.search(r"\[([^\]]+)\]", cm_ref)
                component.connection_manager_ref = m.group(1) if m else cm_ref
            elif cm_id:
                # Strip ":external" suffix from GUID references
                component.connection_manager_ref = cm_id.replace(":external", "")
            break  # Use first connection

        # ---- component-type–specific metadata extraction ----
        self._extract_component_metadata(elem, component, comp_type)

        return component

    # ------------------------------------------------------------------
    # Component-specific metadata helpers
    # ------------------------------------------------------------------

    def _extract_component_metadata(
        self,
        elem: etree._Element,
        component: DataFlowComponent,
        comp_type: DataFlowComponentType,
    ) -> None:
        """Extract rich metadata from specific component types into *component.properties*."""

        if comp_type == DataFlowComponentType.LOOKUP:
            self._extract_lookup_metadata(elem, component)
        elif comp_type == DataFlowComponentType.MERGE_JOIN:
            self._extract_merge_join_metadata(elem, component)
        elif comp_type == DataFlowComponentType.AGGREGATE:
            self._extract_aggregate_metadata(elem, component)
        elif comp_type == DataFlowComponentType.SORT:
            self._extract_sort_metadata(elem, component)
        elif comp_type == DataFlowComponentType.CONDITIONAL_SPLIT:
            self._extract_conditional_split_metadata(elem, component)

    def _extract_lookup_metadata(self, elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract join-key column names from a Lookup component.

        SSIS stores join keys as ``inputColumn`` elements inside the
        **Lookup Input** whose ``cachedName`` identifies the reference column.
        We extract both the upstream column name (``inputColumn/@name``) and
        the reference column (the ``cachedName`` or the first ``property`` with
        name == ``JoinToReferenceColumn``).
        """
        join_keys: list[str] = []
        ref_keys: list[str] = []

        for inp in elem.findall(".//{*}input"):
            # The Lookup component has inputs named "Lookup Input"
            inp_name = inp.get("name", "")
            if "lookup" not in inp_name.lower() and "input" not in inp_name.lower():
                continue
            for ic in inp.findall(".//{*}inputColumn"):
                col_name = ic.get("name", "") or ic.get("cachedName", "")
                if col_name:
                    join_keys.append(col_name)
                # Also look for JoinToReferenceColumn in sub-properties
                for p in ic.findall(".//{*}property"):
                    pn = p.get("name", "")
                    if pn == "JoinToReferenceColumn" and p.text:
                        ref_keys.append(p.text)

        if join_keys:
            component.properties["_join_keys"] = join_keys
        if ref_keys:
            component.properties["_ref_keys"] = ref_keys

    def _extract_merge_join_metadata(self, elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract join type and key columns from a Merge Join component."""
        # JoinType property: 0 = inner, 1 = left, 2 = full
        jt = component.properties.get("JoinType", "0")
        type_map = {"0": "inner", "1": "left", "2": "full"}
        component.properties["_join_type"] = type_map.get(str(jt), "inner")

        # Join key columns come from sorted input columns
        left_keys: list[str] = []
        right_keys: list[str] = []
        for inp in elem.findall(".//{*}input"):
            inp_name = inp.get("name", "")
            for ic in inp.findall(".//{*}inputColumn"):
                col_name = ic.get("name", "") or ic.get("cachedName", "")
                if not col_name:
                    continue
                # SSIS marks join keys with SortKeyPosition > 0
                skp = ic.get("sortKeyPosition", "") or ""
                if skp and skp != "0":
                    if "left" in inp_name.lower() or "1" in inp_name:
                        left_keys.append(col_name)
                    else:
                        right_keys.append(col_name)

        if left_keys:
            component.properties["_left_keys"] = left_keys
        if right_keys:
            component.properties["_right_keys"] = right_keys

    def _extract_aggregate_metadata(self, elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract group-by and aggregation columns from an Aggregate component."""
        group_cols: list[str] = []
        agg_cols: list[dict[str, str]] = []

        for out in elem.findall(".//{*}output"):
            for oc in out.findall(".//{*}outputColumn"):
                col_name = oc.get("name", "")
                for p in oc.findall(".//{*}property"):
                    pn = p.get("name", "")
                    if pn == "AggregationType":
                        agg_type = p.text or "0"
                        if agg_type == "0":
                            # GroupBy
                            group_cols.append(col_name)
                        else:
                            agg_map = {
                                "1": "count",
                                "2": "sum",
                                "3": "avg",
                                "4": "min",
                                "5": "max",
                                "7": "count_distinct",
                            }
                            agg_cols.append(
                                {
                                    "column": col_name,
                                    "function": agg_map.get(agg_type, f"agg_{agg_type}"),
                                }
                            )

        if group_cols:
            component.properties["_group_by"] = group_cols
        if agg_cols:
            component.properties["_aggregations"] = agg_cols

    def _extract_sort_metadata(self, elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract sort column names and directions from a Sort component."""
        sort_cols: list[dict[str, str]] = []

        for out in elem.findall(".//{*}output"):
            for oc in out.findall(".//{*}outputColumn"):
                col_name = oc.get("name", "")
                # sortKeyPosition > 0 means it's a sort key; negative = desc
                skp = oc.get("sortKeyPosition", "") or ""
                if not skp or skp == "0":
                    for p in oc.findall(".//{*}property"):
                        if p.get("name", "") == "SortKeyPosition":
                            skp = p.text or ""
                if skp and skp != "0":
                    direction = "desc" if skp.startswith("-") else "asc"
                    sort_cols.append({"column": col_name, "direction": direction})

        if sort_cols:
            component.properties["_sort_columns"] = sort_cols

    def _extract_conditional_split_metadata(self, elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract split conditions from a Conditional Split component.

        SSIS stores conditions in output properties named ``Expression``
        (one per output branch, excluding the Default Output).
        """
        conditions: list[dict[str, str]] = []

        for out in elem.findall(".//{*}output"):
            out_name = out.get("name", "")
            if out_name.lower().startswith("default"):
                continue
            for p in out.findall(".//{*}property"):
                if p.get("name", "") in ("Expression", "FriendlyExpression") and p.text:
                    conditions.append(
                        {
                            "output": out_name,
                            "expression": p.text,
                        }
                    )
                    break

        if conditions:
            component.properties["_conditions"] = conditions

    def _classify_component_type(self, class_id: str) -> DataFlowComponentType:
        """Classify data flow component type."""
        for key, comp_type in COMPONENT_CLASS_MAP.items():
            if key.lower() in class_id.lower():
                return comp_type
        return DataFlowComponentType.UNKNOWN

    def _parse_data_flow_path(self, elem: etree._Element) -> DataFlowPath | None:
        """Parse a data flow path element."""
        start_id = elem.get("startId", "")
        end_id = elem.get("endId", "")
        if start_id and end_id:
            return DataFlowPath(
                source_component=start_id,
                destination_component=end_id,
            )
        return None

    # =========================================================================
    # Precedence Constraints
    # =========================================================================

    def _parse_precedence_constraints(self, root: etree._Element, nsmap: dict) -> list[PrecedenceConstraint]:
        """Parse precedence constraints from the package."""
        constraints = []
        for pc_container in self._find_dts(root, "PrecedenceConstraints", nsmap):
            for pc_elem in self._find_dts(pc_container, "PrecedenceConstraint", nsmap):
                src = self._get_dts_attr(pc_elem, "From", nsmap) or ""
                dst = self._get_dts_attr(pc_elem, "To", nsmap) or ""
                value = self._get_dts_attr(pc_elem, "Value", nsmap) or ""
                expression = self._get_dts_attr(pc_elem, "Expression", nsmap) or ""

                constraint_type = PrecedenceConstraintType.SUCCESS
                if value == "1":
                    constraint_type = PrecedenceConstraintType.FAILURE
                elif value == "2":
                    constraint_type = PrecedenceConstraintType.COMPLETION
                elif expression:
                    constraint_type = PrecedenceConstraintType.EXPRESSION

                constraints.append(
                    PrecedenceConstraint(
                        source_task=src,
                        destination_task=dst,
                        constraint_type=constraint_type,
                        expression=expression,
                    )
                )
        return constraints

    # =========================================================================
    # Event Handlers
    # =========================================================================

    def _parse_event_handlers(self, root: etree._Element, nsmap: dict) -> list[EventHandler]:
        """Parse event handlers from the package."""
        handlers = []
        for eh_container in self._find_dts(root, "EventHandlers", nsmap):
            for eh_elem in self._find_dts(eh_container, "EventHandler", nsmap):
                event_type = self._get_dts_attr(eh_elem, "EventName", nsmap) or "Unknown"
                tasks = self._parse_executables(eh_elem, nsmap)
                handlers.append(EventHandler(event_type=event_type, tasks=tasks))
        return handlers

    # =========================================================================
    # Complexity Assessment
    # =========================================================================

    def _parse_send_mail(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract email properties from Send Mail task."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                # Attributes may be plain or namespaced
                to_line = child.get("ToLine", "")
                subject = child.get("Subject", "")
                message = child.get("MessageSource", "")
                cc_line = child.get("CCLine", "")

                if not to_line:
                    for attr_key, attr_val in child.attrib.items():
                        local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                        if local == "ToLine":
                            to_line = attr_val
                        elif local == "Subject":
                            subject = attr_val
                        elif local == "MessageSource":
                            message = attr_val
                        elif local == "CCLine":
                            cc_line = attr_val

                if to_line:
                    task.properties["ToLine"] = to_line
                if subject:
                    task.properties["Subject"] = subject
                if message:
                    task.properties["MessageSource"] = message
                if cc_line:
                    task.properties["CCLine"] = cc_line

    # -----------------------------------------------------------------
    # ForEach Loop enumerator parsing
    # -----------------------------------------------------------------
    def _parse_foreach_enumerator(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract ForEach enumerator type and configuration."""
        # The ForEachEnumerator sits inside <DTS:ForEachEnumerator> or
        # as an <DTS:ObjectData> inside a <ForEachEnumerator> child.
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "ForEachEnumerator":
                creation_name = ""
                for attr_key, attr_val in child.attrib.items():
                    local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                    if local == "CreationName":
                        creation_name = attr_val
                task.properties["_enumerator_creation_name"] = creation_name
                task.properties["_enumerator_type"] = self._classify_enumerator(creation_name)

                # Dig into ObjectData for enumerator-specific properties
                for obj_data in child:
                    obj_tag = obj_data.tag.split("}")[-1] if "}" in obj_data.tag else obj_data.tag
                    if obj_tag == "ObjectData":
                        for inner in obj_data:
                            for attr_key, attr_val in inner.attrib.items():
                                local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                                task.properties[f"_enum_{local}"] = attr_val
                break

        # Variable mappings for ForEach
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "ForEachVariableMappings" or tag.endswith("ForEachVariableMappings"):
                for mapping in child:
                    var_name = ""
                    idx = ""
                    for attr_key, attr_val in mapping.attrib.items():
                        local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                        if local == "VariableName":
                            var_name = attr_val
                        elif local == "ValueIndex":
                            idx = attr_val
                    if var_name:
                        task.properties.setdefault("_enum_variable_mappings", []).append(
                            {"variable": var_name, "index": idx}
                        )

    @staticmethod
    def _classify_enumerator(creation_name: str) -> str:
        """Map SSIS ForEachEnumerator CreationName to a friendly type."""
        cn = creation_name.lower()
        if "foreachfileenumerator" in cn:
            return "File"
        if "foreachitemenumerator" in cn:
            return "Item"
        if "foreachadoenumerator" in cn:
            return "ADO"
        if "foreachnodelistenumerator" in cn or "nodelist" in cn:
            return "NodeList"
        if "foreachsmoenumerator" in cn or "smo" in cn:
            return "SMO"
        if "foreachfromenumerator" in cn or "fromvar" in cn:
            return "FromVariable"
        return "Unknown"

    # -----------------------------------------------------------------
    # File System task parsing
    # -----------------------------------------------------------------
    def _parse_file_system(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract File System task properties."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                for attr_key, attr_val in child.attrib.items():
                    local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                    if local in (
                        "Operation",
                        "OperationName",
                        "IsSourcePathVariable",
                        "IsDestinationPathVariable",
                        "OverwriteDestination",
                        "Source",
                        "Destination",
                    ):
                        task.properties[local] = attr_val

        # Map numeric Operation to name
        op_map = {
            "0": "CopyFile",
            "1": "MoveFile",
            "2": "DeleteFile",
            "3": "RenameFile",
            "4": "SetAttributes",
            "5": "CreateDirectory",
            "6": "CopyDirectory",
            "7": "MoveDirectory",
            "8": "DeleteDirectory",
            "9": "DeleteDirectoryContent",
        }
        op_val = task.properties.get("Operation", "")
        if op_val in op_map:
            task.properties["_operation_name"] = op_map[op_val]

    # -----------------------------------------------------------------
    # FTP task parsing
    # -----------------------------------------------------------------
    def _parse_ftp(self, elem: etree._Element, task: ControlFlowTask, nsmap: dict) -> None:
        """Extract FTP task properties."""
        for obj_data in self._find_dts(elem, "ObjectData", nsmap):
            for child in obj_data:
                for attr_key, attr_val in child.attrib.items():
                    local = attr_key.split("}")[-1] if "}" in attr_key else attr_key
                    if local in (
                        "Operation",
                        "IsLocalPathVariable",
                        "IsRemotePathVariable",
                        "LocalPath",
                        "RemotePath",
                        "IsTransferTypeASCII",
                        "OverwriteFileAtDest",
                    ):
                        task.properties[local] = attr_val

        # Map numeric FTP Operation to name
        ftp_op_map = {
            "0": "Send",
            "1": "Receive",
            "2": "DeleteLocal",
            "3": "DeleteRemote",
            "4": "MakeLocalDir",
            "5": "MakeRemoteDir",
            "6": "RemoveLocalDir",
            "7": "RemoveRemoteDir",
        }
        op_val = task.properties.get("Operation", "")
        if op_val in ftp_op_map:
            task.properties["_ftp_operation_name"] = ftp_op_map[op_val]

    def _assess_task_complexity(self, task: ControlFlowTask) -> MigrationComplexity:
        """Assess migration complexity for a task."""
        if task.task_type == TaskType.SCRIPT:
            return MigrationComplexity.HIGH
        if task.task_type == TaskType.EXECUTE_PROCESS:
            return MigrationComplexity.HIGH
        if task.task_type == TaskType.WEB_SERVICE:
            return MigrationComplexity.HIGH
        if task.task_type == TaskType.UNKNOWN:
            return MigrationComplexity.MANUAL

        if task.task_type == TaskType.DATA_FLOW:
            complexities = [c.migration_complexity for c in task.data_flow_components]
            if MigrationComplexity.MANUAL in complexities:
                return MigrationComplexity.MANUAL
            if MigrationComplexity.HIGH in complexities:
                return MigrationComplexity.HIGH
            if MigrationComplexity.MEDIUM in complexities:
                return MigrationComplexity.MEDIUM

        return MigrationComplexity.LOW
