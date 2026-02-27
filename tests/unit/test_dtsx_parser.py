"""
Unit tests for the DTSX Parser.
Validates parsing of SSIS packages into structured models.
"""

from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import (
    ConnectionType,
    DataFlowComponentType,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


class TestDTSXParserSimple:
    """Tests against the simple ETL package."""

    @pytest.fixture
    def package(self) -> SSISPackage:
        parser = DTSXParser()
        return parser.parse(FIXTURES_DIR / "simple_etl.dtsx")

    @pytest.mark.unit
    def test_package_name(self, package: SSISPackage) -> None:
        assert package.name == "SampleETLPackage"

    @pytest.mark.unit
    def test_package_description(self, package: SSISPackage) -> None:
        assert "sample SSIS package" in package.description

    @pytest.mark.unit
    def test_connection_managers_count(self, package: SSISPackage) -> None:
        assert len(package.connection_managers) == 2

    @pytest.mark.unit
    def test_connection_manager_types(self, package: SSISPackage) -> None:
        cm_types = {cm.name: cm.connection_type for cm in package.connection_managers}
        assert cm_types["SourceDB_OLEDB"] == ConnectionType.OLEDB
        assert cm_types["DestDB_OLEDB"] == ConnectionType.OLEDB

    @pytest.mark.unit
    def test_connection_manager_server_extraction(self, package: SSISPackage) -> None:
        source_cm = next(cm for cm in package.connection_managers if cm.name == "SourceDB_OLEDB")
        assert source_cm.server == "source-server"
        assert source_cm.database == "SourceDB"

    @pytest.mark.unit
    def test_variables_parsed(self, package: SSISPackage) -> None:
        assert len(package.variables) == 2
        var_names = {v.name for v in package.variables}
        assert "BatchDate" in var_names
        assert "RowCount" in var_names

    @pytest.mark.unit
    def test_control_flow_tasks_count(self, package: SSISPackage) -> None:
        assert len(package.control_flow_tasks) == 3

    @pytest.mark.unit
    def test_execute_sql_task(self, package: SSISPackage) -> None:
        truncate_task = package.control_flow_tasks[0]
        assert truncate_task.name == "Truncate Staging Table"
        assert truncate_task.task_type == TaskType.EXECUTE_SQL
        assert "TRUNCATE TABLE" in truncate_task.sql_statement

    @pytest.mark.unit
    def test_data_flow_task(self, package: SSISPackage) -> None:
        df_task = package.control_flow_tasks[1]
        assert df_task.name == "Load Customers"
        assert df_task.task_type == TaskType.DATA_FLOW
        assert len(df_task.data_flow_components) >= 2  # At least source + destination

    @pytest.mark.unit
    def test_data_flow_components(self, package: SSISPackage) -> None:
        df_task = package.control_flow_tasks[1]
        comp_types = {c.component_type for c in df_task.data_flow_components}
        assert DataFlowComponentType.OLE_DB_SOURCE in comp_types
        assert DataFlowComponentType.OLE_DB_DESTINATION in comp_types

    @pytest.mark.unit
    def test_stored_procedure_detection(self, package: SSISPackage) -> None:
        audit_task = package.control_flow_tasks[2]
        assert audit_task.task_type == TaskType.EXECUTE_SQL
        assert "EXEC" in audit_task.sql_statement

    @pytest.mark.unit
    def test_precedence_constraints(self, package: SSISPackage) -> None:
        assert len(package.precedence_constraints) == 2

    @pytest.mark.unit
    def test_package_stats(self, package: SSISPackage) -> None:
        assert package.total_tasks == 3
        assert package.total_data_flows == 1


class TestDTSXParserComplex:
    """Tests against the complex ETL package."""

    @pytest.fixture
    def package(self) -> SSISPackage:
        parser = DTSXParser()
        return parser.parse(FIXTURES_DIR / "complex_etl.dtsx")

    @pytest.mark.unit
    def test_package_name(self, package: SSISPackage) -> None:
        assert package.name == "ComplexETLPackage"

    @pytest.mark.unit
    def test_three_connection_managers(self, package: SSISPackage) -> None:
        assert len(package.connection_managers) == 3

    @pytest.mark.unit
    def test_flat_file_connection(self, package: SSISPackage) -> None:
        ff = next(cm for cm in package.connection_managers if cm.name == "FlatFileExport")
        assert ff.connection_type == ConnectionType.FLAT_FILE

    @pytest.mark.unit
    def test_sequence_container(self, package: SSISPackage) -> None:
        init_task = package.control_flow_tasks[0]
        assert init_task.name == "Initialize"
        assert init_task.task_type == TaskType.SEQUENCE_CONTAINER
        assert len(init_task.child_tasks) == 1

    @pytest.mark.unit
    def test_foreach_loop(self, package: SSISPackage) -> None:
        foreach = next(t for t in package.control_flow_tasks if t.task_type == TaskType.FOREACH_LOOP)
        assert foreach.name == "Process Each Region"
        assert len(foreach.child_tasks) == 1

    @pytest.mark.unit
    def test_script_task(self, package: SSISPackage) -> None:
        script = next(t for t in package.control_flow_tasks if t.task_type == TaskType.SCRIPT)
        assert script.name == "Send Notification"
        assert script.migration_complexity == MigrationComplexity.HIGH

    @pytest.mark.unit
    def test_complex_data_flow_components(self, package: SSISPackage) -> None:
        df_task = next(t for t in package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        comp_types = {c.component_type for c in df_task.data_flow_components}
        assert DataFlowComponentType.LOOKUP in comp_types
        assert DataFlowComponentType.CONDITIONAL_SPLIT in comp_types
        assert DataFlowComponentType.SCRIPT_COMPONENT in comp_types

    @pytest.mark.unit
    def test_event_handlers(self, package: SSISPackage) -> None:
        assert len(package.event_handlers) == 1
        assert package.event_handlers[0].event_type == "OnError"

    @pytest.mark.unit
    def test_overall_complexity_high(self, package: SSISPackage) -> None:
        # Has a Script Component which is HIGH complexity
        assert package.overall_complexity in (MigrationComplexity.HIGH, MigrationComplexity.MANUAL)

    @pytest.mark.unit
    def test_total_tasks_with_children(self, package: SSISPackage) -> None:
        # 4 top-level + 1 child in sequence + 1 child in foreach = 6
        assert package.total_tasks == 6


class TestDTSXParserDirectory:
    """Tests for directory-level parsing."""

    @pytest.mark.unit
    def test_parse_directory(self) -> None:
        parser = DTSXParser()
        packages = parser.parse_directory(FIXTURES_DIR)
        assert len(packages) >= 2

    @pytest.mark.unit
    def test_parse_empty_directory(self, tmp_path: Path) -> None:
        parser = DTSXParser()
        packages = parser.parse_directory(tmp_path)
        assert len(packages) == 0


class TestDTSXParserSqlParameters:
    """Tests for SQL parameter binding extraction."""

    SSISCOOKBOOK_DIR = Path(__file__).parent.parent / ".." / "examples" / "full_ssis_project" / "SSISCookbook"

    @pytest.mark.unit
    def test_single_parameter_binding(self) -> None:
        """StgProduct has DELETE ... WHERE LoadExecutionId = ? with one binding."""
        parser = DTSXParser()
        pkg = parser.parse(self.SSISCOOKBOOK_DIR / "StgProduct.dtsx")
        sql_tasks = [t for t in pkg.control_flow_tasks if t.task_type == TaskType.EXECUTE_SQL]
        assert len(sql_tasks) >= 1
        delete_task = sql_tasks[0]
        assert "?" in delete_task.sql_statement
        assert len(delete_task.sql_parameters) == 1
        param = delete_task.sql_parameters[0]
        assert param.parameter_name == "0"
        assert "LoadExecutionId" in param.variable_name
        assert param.direction == "Input"
        assert param.data_type == 3  # Int32
