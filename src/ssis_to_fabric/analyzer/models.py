"""
Data models representing SSIS package structure.
These models provide a normalized, technology-agnostic representation
of SSIS packages that the migration engine consumes.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# =============================================================================
# Enumerations
# =============================================================================


class ConnectionType(str, Enum):
    OLEDB = "OLEDB"
    ADO_NET = "ADO.NET"
    FLAT_FILE = "FLAT_FILE"
    EXCEL = "EXCEL"
    ODBC = "ODBC"
    SMTP = "SMTP"
    FTP = "FTP"
    HTTP = "HTTP"
    FILE = "FILE"
    ORACLE = "ORACLE"
    SHAREPOINT = "SHAREPOINT"
    ANALYSIS_SERVICES = "ANALYSIS_SERVICES"
    UNKNOWN = "UNKNOWN"


class TaskType(str, Enum):
    DATA_FLOW = "DATA_FLOW"
    EXECUTE_SQL = "EXECUTE_SQL"
    EXECUTE_PROCESS = "EXECUTE_PROCESS"
    FILE_SYSTEM = "FILE_SYSTEM"
    FTP = "FTP"
    SEND_MAIL = "SEND_MAIL"
    SCRIPT = "SCRIPT"
    SEQUENCE_CONTAINER = "SEQUENCE_CONTAINER"
    FOR_LOOP = "FOR_LOOP"
    FOREACH_LOOP = "FOREACH_LOOP"
    EXPRESSION = "EXPRESSION"
    EXECUTE_PACKAGE = "EXECUTE_PACKAGE"
    WEB_SERVICE = "WEB_SERVICE"
    XML = "XML"
    UNKNOWN = "UNKNOWN"


class DataFlowComponentType(str, Enum):
    OLE_DB_SOURCE = "OLE_DB_SOURCE"
    ADO_NET_SOURCE = "ADO_NET_SOURCE"
    FLAT_FILE_SOURCE = "FLAT_FILE_SOURCE"
    EXCEL_SOURCE = "EXCEL_SOURCE"
    ODBC_SOURCE = "ODBC_SOURCE"
    XML_SOURCE = "XML_SOURCE"
    RAW_FILE_SOURCE = "RAW_FILE_SOURCE"
    CDC_SOURCE = "CDC_SOURCE"
    SCRIPT_COMPONENT_SOURCE = "SCRIPT_COMPONENT_SOURCE"
    OLE_DB_DESTINATION = "OLE_DB_DESTINATION"
    ADO_NET_DESTINATION = "ADO_NET_DESTINATION"
    FLAT_FILE_DESTINATION = "FLAT_FILE_DESTINATION"
    EXCEL_DESTINATION = "EXCEL_DESTINATION"
    ODBC_DESTINATION = "ODBC_DESTINATION"
    RAW_FILE_DESTINATION = "RAW_FILE_DESTINATION"
    RECORDSET_DESTINATION = "RECORDSET_DESTINATION"
    SQL_SERVER_DESTINATION = "SQL_SERVER_DESTINATION"
    DATA_READER_DESTINATION = "DATA_READER_DESTINATION"
    DERIVED_COLUMN = "DERIVED_COLUMN"
    CONDITIONAL_SPLIT = "CONDITIONAL_SPLIT"
    LOOKUP = "LOOKUP"
    MERGE_JOIN = "MERGE_JOIN"
    UNION_ALL = "UNION_ALL"
    AGGREGATE = "AGGREGATE"
    SORT = "SORT"
    MULTICAST = "MULTICAST"
    ROW_COUNT = "ROW_COUNT"
    DATA_CONVERSION = "DATA_CONVERSION"
    SCRIPT_COMPONENT = "SCRIPT_COMPONENT"
    OLE_DB_COMMAND = "OLE_DB_COMMAND"
    SLOWLY_CHANGING_DIMENSION = "SCD"
    PIVOT = "PIVOT"
    UNPIVOT = "UNPIVOT"
    FUZZY_LOOKUP = "FUZZY_LOOKUP"
    FUZZY_GROUPING = "FUZZY_GROUPING"
    TERM_LOOKUP = "TERM_LOOKUP"
    COPY_COLUMN = "COPY_COLUMN"
    CHARACTER_MAP = "CHARACTER_MAP"
    AUDIT = "AUDIT"
    MERGE = "MERGE"
    CDC_SPLITTER = "CDC_SPLITTER"
    PERCENTAGE_SAMPLING = "PERCENTAGE_SAMPLING"
    ROW_SAMPLING = "ROW_SAMPLING"
    BALANCED_DATA_DISTRIBUTOR = "BALANCED_DATA_DISTRIBUTOR"
    CACHE_TRANSFORM = "CACHE_TRANSFORM"
    UNKNOWN = "UNKNOWN"


class PrecedenceConstraintType(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    COMPLETION = "COMPLETION"
    EXPRESSION = "EXPRESSION"


class VariableScope(str, Enum):
    PACKAGE = "PACKAGE"
    TASK = "TASK"


class MigrationComplexity(str, Enum):
    """Estimated migration complexity for a component."""

    LOW = "LOW"  # Direct 1:1 mapping to Fabric
    MEDIUM = "MEDIUM"  # Requires transformation logic
    HIGH = "HIGH"  # Requires Spark/custom code
    MANUAL = "MANUAL"  # Cannot be auto-migrated


# =============================================================================
# Component Models
# =============================================================================


class ConnectionManager(BaseModel):
    """Represents an SSIS connection manager."""

    id: str = ""
    name: str
    connection_type: ConnectionType
    connection_string: str = ""
    server: str = ""
    database: str = ""
    provider: str = ""
    properties: dict[str, Any] = Field(default_factory=dict)


class Variable(BaseModel):
    """Represents an SSIS variable or parameter."""

    name: str
    namespace: str = "User"
    data_type: str = "String"
    value: str = ""
    expression: str = ""
    scope: VariableScope = VariableScope.PACKAGE
    is_parameter: bool = False
    sensitive: bool = False
    required: bool = False
    description: str = ""


class Column(BaseModel):
    """Represents a column in a data flow."""

    name: str
    data_type: str = ""
    length: int = 0
    precision: int = 0
    scale: int = 0
    expression: str = ""
    source_column: str = ""


class DataFlowComponent(BaseModel):
    """Represents a component within a Data Flow Task."""

    id: str = ""
    name: str
    component_type: DataFlowComponentType
    connection_manager_ref: str = ""
    sql_command: str = ""
    table_name: str = ""
    columns: list[Column] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)
    expressions: dict[str, str] = Field(default_factory=dict)
    migration_complexity: MigrationComplexity = MigrationComplexity.LOW


class DataFlowPath(BaseModel):
    """Represents a path (connection) between data flow components."""

    source_component: str
    destination_component: str
    source_output: str = "Output"


class PrecedenceConstraint(BaseModel):
    """Represents a precedence constraint between control flow tasks."""

    source_task: str
    destination_task: str
    constraint_type: PrecedenceConstraintType = PrecedenceConstraintType.SUCCESS
    expression: str = ""


class SqlParameterBinding(BaseModel):
    """Represents a parameter binding for an SSIS Execute SQL task.

    Maps a positional ``?`` placeholder in the SQL to an SSIS variable.
    """

    parameter_name: str  # Positional index as string, e.g. "0", "1"
    variable_name: str  # SSIS variable, e.g. "$Package::LoadExecutionId"
    direction: str = "Input"  # "Input" or "Output"
    data_type: int = 0  # OLE DB data type code (3=Int32, 8=String, …)


class ControlFlowTask(BaseModel):
    """Represents a task in the SSIS control flow."""

    id: str = ""
    ref_id: str = ""  # DTS:refId – hierarchical path used by precedence constraints
    name: str
    task_type: TaskType
    description: str = ""
    connection_manager_ref: str = ""
    sql_statement: str = ""
    expression: str = ""
    disabled: bool = False
    properties: dict[str, Any] = Field(default_factory=dict)

    # Execute SQL parameter bindings (? placeholders → SSIS variables)
    sql_parameters: list[SqlParameterBinding] = Field(default_factory=list)

    # Execute Package parameter bindings: child_param_name → parent_variable_expr
    parameter_bindings: dict[str, str] = Field(default_factory=dict)

    # Data flow specific
    data_flow_components: list[DataFlowComponent] = Field(default_factory=list)
    data_flow_paths: list[DataFlowPath] = Field(default_factory=list)

    # Container children (for Sequence, ForLoop, ForEachLoop)
    child_tasks: list[ControlFlowTask] = Field(default_factory=list)

    # Container-level precedence constraints (for Sequence, ForLoop, ForEachLoop)
    child_precedence_constraints: list[PrecedenceConstraint] = Field(default_factory=list)

    migration_complexity: MigrationComplexity = MigrationComplexity.LOW


class EventHandler(BaseModel):
    """Represents an SSIS event handler."""

    event_type: str  # e.g., "OnError", "OnPreExecute"
    tasks: list[ControlFlowTask] = Field(default_factory=list)


class SSISPackage(BaseModel):
    """
    Complete parsed representation of an SSIS package.
    This is the primary output of the analyzer and input to the migration engine.
    """

    name: str
    file_path: str = ""
    description: str = ""
    creation_date: str = ""
    creator_name: str = ""
    format_version: str = ""

    connection_managers: list[ConnectionManager] = Field(default_factory=list)
    variables: list[Variable] = Field(default_factory=list)
    parameters: list[Variable] = Field(default_factory=list)
    project_parameters: list[Variable] = Field(default_factory=list)
    control_flow_tasks: list[ControlFlowTask] = Field(default_factory=list)
    precedence_constraints: list[PrecedenceConstraint] = Field(default_factory=list)
    event_handlers: list[EventHandler] = Field(default_factory=list)

    # Analysis metadata
    total_tasks: int = 0
    total_data_flows: int = 0
    overall_complexity: MigrationComplexity = MigrationComplexity.LOW
    warnings: list[str] = Field(default_factory=list)

    def compute_stats(self) -> None:
        """Recompute package statistics after parsing."""
        self.total_tasks = self._count_tasks(self.control_flow_tasks)
        self.total_data_flows = self._count_data_flows(self.control_flow_tasks)
        self.overall_complexity = self._compute_complexity()

    def _count_tasks(self, tasks: list[ControlFlowTask]) -> int:
        count = len(tasks)
        for task in tasks:
            count += self._count_tasks(task.child_tasks)
        return count

    def _count_data_flows(self, tasks: list[ControlFlowTask]) -> int:
        count = sum(1 for t in tasks if t.task_type == TaskType.DATA_FLOW)
        for task in tasks:
            count += self._count_data_flows(task.child_tasks)
        return count

    def _compute_complexity(self) -> MigrationComplexity:
        complexities = self._gather_complexities(self.control_flow_tasks)
        if MigrationComplexity.MANUAL in complexities:
            return MigrationComplexity.MANUAL
        if MigrationComplexity.HIGH in complexities:
            return MigrationComplexity.HIGH
        if MigrationComplexity.MEDIUM in complexities:
            return MigrationComplexity.MEDIUM
        return MigrationComplexity.LOW

    def _gather_complexities(self, tasks: list[ControlFlowTask]) -> set[MigrationComplexity]:
        result: set[MigrationComplexity] = set()
        for task in tasks:
            result.add(task.migration_complexity)
            for comp in task.data_flow_components:
                result.add(comp.migration_complexity)
            result |= self._gather_complexities(task.child_tasks)
        return result
