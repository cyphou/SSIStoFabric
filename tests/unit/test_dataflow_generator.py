"""
Unit tests for the Dataflow Gen2 Generator.
Validates Power Query M generation and metadata structure.
"""

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import (
    Column,
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


@pytest.fixture
def generator(default_config: MigrationConfig) -> DataflowGen2Generator:
    return DataflowGen2Generator(default_config)


@pytest.fixture
def simple_package() -> SSISPackage:
    parser = DTSXParser()
    return parser.parse(FIXTURES_DIR / "simple_etl.dtsx")


@pytest.fixture
def simple_data_flow(simple_package: SSISPackage) -> ControlFlowTask:
    """Get the Data Flow task from the simple package."""
    return next(t for t in simple_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)


class TestDataflowGen2Generator:
    """Tests for Dataflow Gen2 definition generation."""

    @pytest.mark.unit
    def test_generate_produces_json_file(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        assert output.exists()
        assert output.suffix == ".json"

    @pytest.mark.unit
    def test_output_in_dataflows_directory(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        assert "dataflows" in str(output.parent)

    @pytest.mark.unit
    def test_definition_has_required_fields(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)
        assert "name" in data
        assert "queryMetadata" in data
        assert "mashup" in data

    @pytest.mark.unit
    def test_query_metadata_structure(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)

        metadata = data["queryMetadata"]
        assert metadata["formatVersion"] == "202502"
        assert "queriesMetadata" in metadata
        assert "name" in metadata

        # Should have one query entry
        assert len(metadata["queriesMetadata"]) == 1
        query = list(metadata["queriesMetadata"].values())[0]
        assert "queryId" in query
        assert query["loadEnabled"] is True
        # Staging must be disabled on destinations
        assert query["destinationSettings"]["enableStaging"] is False

    @pytest.mark.unit
    def test_mashup_is_valid_power_query(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)

        mashup = data["mashup"]
        # Must have section, shared, let/in structure
        assert "section Section1;" in mashup
        assert "shared " in mashup
        assert " = let" in mashup
        assert "\nin\n" in mashup

    @pytest.mark.unit
    def test_mashup_contains_source_query(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)

        mashup = data["mashup"]
        # Should contain the SQL query from the source component
        assert "Sql.Database" in mashup
        assert "CustomerID" in mashup

    @pytest.mark.unit
    def test_mashup_contains_derived_column(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)

        mashup = data["mashup"]
        # Simple package has a Derived Column transform for FullName
        assert "FullName" in mashup or "Table.AddColumn" in mashup or "Derived" in mashup

    @pytest.mark.unit
    def test_raises_for_none_task(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
    ) -> None:
        with pytest.raises(ValueError, match="requires a specific task"):
            generator.generate(simple_package, None, Path("/tmp"))

    @pytest.mark.unit
    def test_raises_for_non_data_flow_task(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        sql_task = next(t for t in simple_package.control_flow_tasks if t.task_type == TaskType.EXECUTE_SQL)
        with pytest.raises(ValueError, match="Expected DATA_FLOW"):
            generator.generate(simple_package, sql_task, tmp_path)

    @pytest.mark.unit
    def test_no_destination_in_mashup(
        self,
        generator: DataflowGen2Generator,
        simple_package: SSISPackage,
        simple_data_flow: ControlFlowTask,
        tmp_path: Path,
    ) -> None:
        """Dataflow Gen2 destinations are configured in Fabric UI, not in M code."""
        output = generator.generate(simple_package, simple_data_flow, tmp_path)
        with open(output) as f:
            data = json.load(f)

        mashup = data["mashup"]
        # M code should NOT contain destination references — data destination
        # is configured via the Fabric UI "Add data destination" feature.
        assert "Destination:" not in mashup


class TestMashupFormats:
    """Test Power Query M generation for different source types."""

    @pytest.mark.unit
    def test_sql_source_with_query(self, generator: DataflowGen2Generator) -> None:
        """SQL source with a query should use Value.NativeQuery."""
        comp = DataFlowComponent(
            name="Source",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            sql_command="SELECT * FROM dbo.Orders",
        )
        conn = ConnectionManager(
            name="SourceDB",
            connection_type=ConnectionType.OLEDB,
            server="myserver",
            database="mydb",
        )
        result, step_name = generator._source_to_pq(comp, [conn])
        assert "Sql.Database" in result
        assert "Value.NativeQuery" in result
        assert "myserver" in result
        assert "mydb" in result
        assert step_name == "Query"

    @pytest.mark.unit
    def test_sql_source_with_table(self, generator: DataflowGen2Generator) -> None:
        """SQL source with table name should use schema navigation."""
        comp = DataFlowComponent(
            name="Source",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            table_name="dbo.Customers",
        )
        conn = ConnectionManager(
            name="SourceDB",
            connection_type=ConnectionType.OLEDB,
            server="srv",
            database="db",
        )
        result, step_name = generator._source_to_pq(comp, [conn])
        assert "Sql.Database" in result
        assert 'Schema="dbo"' in result
        assert 'Item="Customers"' in result
        assert step_name == "Table"

    @pytest.mark.unit
    def test_flat_file_source(self, generator: DataflowGen2Generator) -> None:
        """Flat file source should use Csv.Document."""
        comp = DataFlowComponent(
            name="FlatFileSource",
            component_type=DataFlowComponentType.FLAT_FILE_SOURCE,
        )
        conn = ConnectionManager(
            name="FlatFile",
            connection_type=ConnectionType.FLAT_FILE,
            connection_string="C:\\data\\input.csv",
        )
        result, step_name = generator._source_to_pq(comp, [conn])
        assert "Csv.Document" in result
        assert step_name == "Source"

    @pytest.mark.unit
    def test_excel_source(self, generator: DataflowGen2Generator) -> None:
        """Excel source should use Excel.Workbook."""
        comp = DataFlowComponent(
            name="ExcelSource",
            component_type=DataFlowComponentType.EXCEL_SOURCE,
            properties={"OpenRowset": "Sheet1$"},
        )
        conn = ConnectionManager(
            name="Excel",
            connection_type=ConnectionType.EXCEL,
            connection_string="C:\\data\\data.xlsx",
        )
        result, step_name = generator._source_to_pq(comp, [conn])
        assert "Excel.Workbook" in result
        assert step_name == "Sheet"


class TestTransformToPQ:
    """Test Power Query M conversion for transform components."""

    @pytest.mark.unit
    def test_derived_column(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="AddTotal",
            component_type=DataFlowComponentType.DERIVED_COLUMN,
            columns=[Column(name="TotalAmount", expression="Price * Qty")],
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.AddColumn" in code
        assert "TotalAmount" in code

    @pytest.mark.unit
    def test_aggregate(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="SumByCategory",
            component_type=DataFlowComponentType.AGGREGATE,
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.Group" in code

    @pytest.mark.unit
    def test_sort(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="SortByDate",
            component_type=DataFlowComponentType.SORT,
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.Sort" in code

    @pytest.mark.unit
    def test_data_conversion(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="ConvertTypes",
            component_type=DataFlowComponentType.DATA_CONVERSION,
            columns=[
                Column(name="Amount", data_type="numeric"),
                Column(name="Name", data_type="str"),
            ],
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.TransformColumnTypes" in code
        assert "Amount" in code

    @pytest.mark.unit
    def test_lookup(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="ProductLookup",
            component_type=DataFlowComponentType.LOOKUP,
            table_name="Products",
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.NestedJoin" in code

    @pytest.mark.unit
    def test_conditional_split(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="SplitByRegion",
            component_type=DataFlowComponentType.CONDITIONAL_SPLIT,
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "Table.SelectRows" in code

    @pytest.mark.unit
    def test_unknown_transform_has_todo(self, generator: DataflowGen2Generator) -> None:
        comp = DataFlowComponent(
            name="CustomOp",
            component_type=DataFlowComponentType.OLE_DB_COMMAND,
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "TODO" in code


class TestSsisExprToM:
    """Test SSIS expression → Power Query M conversion."""

    @pytest.mark.unit
    def test_package_variable_reference(self, generator: DataflowGen2Generator) -> None:
        """@[$Package::LoadExecutionId] → LoadExecutionId (PQ parameter)."""
        result = generator._ssis_expr_to_m("@[$Package::LoadExecutionId]")
        assert result == "LoadExecutionId"

    @pytest.mark.unit
    def test_user_variable_reference(self, generator: DataflowGen2Generator) -> None:
        """@[User::BatchId] → BatchId."""
        result = generator._ssis_expr_to_m("@[User::BatchId]")
        assert result == "BatchId"

    @pytest.mark.unit
    def test_project_variable_reference(self, generator: DataflowGen2Generator) -> None:
        """@[$Project::Environment] → Environment."""
        result = generator._ssis_expr_to_m("@[$Project::Environment]")
        assert result == "Environment"

    @pytest.mark.unit
    def test_system_starttime_variable(self, generator: DataflowGen2Generator) -> None:
        """@[System::StartTime] → DateTime.LocalNow()."""
        result = generator._ssis_expr_to_m("@[System::StartTime]")
        assert result == "DateTime.LocalNow()"

    @pytest.mark.unit
    def test_inline_variable_in_expression(self, generator: DataflowGen2Generator) -> None:
        """Variable inside a larger expression."""
        result = generator._ssis_expr_to_m('@[$Package::Prefix] + "_suffix"')
        assert "Prefix" in result
        assert "@[$Package" not in result

    @pytest.mark.unit
    def test_null_expression(self, generator: DataflowGen2Generator) -> None:
        assert generator._ssis_expr_to_m("null") == "null"
        assert generator._ssis_expr_to_m("") == "null"

    @pytest.mark.unit
    def test_getdate(self, generator: DataflowGen2Generator) -> None:
        result = generator._ssis_expr_to_m("GETDATE()")
        assert "DateTime.LocalNow()" in result


class TestErrorColumnFiltering:
    """Test that SSIS error output columns are filtered out."""

    @pytest.mark.unit
    def test_filter_error_columns(self, generator: DataflowGen2Generator) -> None:
        """ErrorCode, ErrorColumn, and empty-name columns are removed."""
        columns = [
            Column(name="Name", data_type="str"),
            Column(name="ErrorCode", data_type="i4"),
            Column(name="ErrorColumn", data_type="i4"),
            Column(name="", data_type="str"),
        ]
        filtered = generator._filter_error_columns(columns)
        assert len(filtered) == 1
        assert filtered[0].name == "Name"

    @pytest.mark.unit
    def test_data_conversion_excludes_error_columns(self, generator: DataflowGen2Generator) -> None:
        """Data Conversion output should not include ErrorCode/ErrorColumn."""
        comp = DataFlowComponent(
            name="ConvertTypes",
            component_type=DataFlowComponentType.DATA_CONVERSION,
            columns=[
                Column(name="Name", data_type="str"),
                Column(name="ErrorCode", data_type="i4"),
                Column(name="ErrorColumn", data_type="i4"),
            ],
        )
        name, code = generator._transform_to_pq(comp, "Source")
        assert "ErrorCode" not in code
        assert "ErrorColumn" not in code
        assert '"Name"' in code


class TestPQParameterDeclarations:
    """Test PQ parameter declarations for SSIS variables in dataflows."""

    @pytest.mark.unit
    def test_collect_ssis_variable_params(self, generator: DataflowGen2Generator) -> None:
        """Variables in derived columns generate PQ parameter entries."""
        transforms = [
            DataFlowComponent(
                name="der_LoadExecutionId",
                component_type=DataFlowComponentType.DERIVED_COLUMN,
                columns=[
                    Column(
                        name="LoadExecutionId",
                        data_type="i8",
                        expression="@[$Package::LoadExecutionId]",
                    ),
                ],
            ),
        ]
        pkg = SSISPackage(name="test")
        params = generator._collect_ssis_variable_params(transforms, pkg)
        assert "LoadExecutionId" in params
        assert params["LoadExecutionId"]["m_type"] == "Number"

    @pytest.mark.unit
    def test_mashup_includes_parameter_declaration(self, generator: DataflowGen2Generator) -> None:
        """Mashup should include 'shared VarName = ... meta [IsParameterQuery = true]'."""
        task = ControlFlowTask(
            name="dft_Test",
            task_type=TaskType.DATA_FLOW,
            data_flow_components=[
                DataFlowComponent(
                    name="src",
                    component_type=DataFlowComponentType.OLE_DB_SOURCE,
                    sql_command="SELECT 1 AS Col1",
                ),
                DataFlowComponent(
                    name="der_LoadId",
                    component_type=DataFlowComponentType.DERIVED_COLUMN,
                    columns=[
                        Column(
                            name="LoadId",
                            data_type="i4",
                            expression="@[$Package::LoadId]",
                        ),
                    ],
                ),
                DataFlowComponent(
                    name="dest",
                    component_type=DataFlowComponentType.OLE_DB_DESTINATION,
                    table_name="DestTable",
                ),
            ],
        )
        pkg = SSISPackage(name="test")
        mashup = generator._generate_mashup(task, pkg, "dft_Test")
        assert "shared LoadId" in mashup
        assert "IsParameterQuery = true" in mashup
        # Variable reference should be converted, not raw SSIS syntax
        assert "@[$Package::" not in mashup
        assert "each LoadId" in mashup


class TestProjectConnectionParsing:
    """Test that .conmgr files are parsed into package connections."""

    @pytest.mark.unit
    def test_conmgr_parsed_in_directory(self) -> None:
        """Project .conmgr files should be parsed and merged into packages."""
        ssis_cookbook_dir = Path(__file__).parent.parent.parent / "examples" / "full_ssis_project" / "SSISCookbook"
        if not ssis_cookbook_dir.exists():
            pytest.skip("SSISCookbook directory not available")

        parser = DTSXParser()
        packages = parser.parse_directory(ssis_cookbook_dir)
        assert len(packages) > 0

        # Each package should have project connections merged in
        pkg = packages[0]
        conn_names = [cm.name for cm in pkg.connection_managers]
        # cmgr_Source and cmgr_DW should be present from .conmgr files
        assert "cmgr_Source" in conn_names or "cmgr_DW" in conn_names

    @pytest.mark.unit
    def test_conmgr_has_server_database(self) -> None:
        """Parsed .conmgr connections should have server and database."""
        ssis_cookbook_dir = Path(__file__).parent.parent.parent / "examples" / "full_ssis_project" / "SSISCookbook"
        if not ssis_cookbook_dir.exists():
            pytest.skip("SSISCookbook directory not available")

        parser = DTSXParser()
        packages = parser.parse_directory(ssis_cookbook_dir)
        pkg = packages[0]
        source_conn = next((cm for cm in pkg.connection_managers if cm.name == "cmgr_Source"), None)
        assert source_conn is not None
        assert source_conn.server != ""
        assert source_conn.database != ""


class TestErrorColumnsNotParsed:
    """Test that error output columns are excluded during parsing."""

    @pytest.mark.unit
    def test_error_columns_excluded_from_components(self) -> None:
        """Components parsed from DTSX should not include error output columns."""
        ssis_cookbook_dir = Path(__file__).parent.parent.parent / "examples" / "full_ssis_project" / "SSISCookbook"
        stg_file = ssis_cookbook_dir / "StgProductCategory.dtsx"
        if not stg_file.exists():
            pytest.skip("StgProductCategory.dtsx not available")

        parser = DTSXParser()
        pkg = parser.parse(stg_file)

        # Find the data conversion component
        for task in pkg.control_flow_tasks:
            for comp in task.data_flow_components:
                col_names = [c.name for c in comp.columns]
                assert "ErrorCode" not in col_names, f"ErrorCode should not be in {comp.name} columns"
                assert "ErrorColumn" not in col_names, f"ErrorColumn should not be in {comp.name} columns"


class TestDataFlowComponentConnectionRef:
    """Test that data flow components extract connection_manager_ref."""

    @pytest.mark.unit
    def test_component_has_connection_ref(self) -> None:
        """Source/dest components should have connection_manager_ref from <connection> elements."""
        ssis_cookbook_dir = Path(__file__).parent.parent.parent / "examples" / "full_ssis_project" / "SSISCookbook"
        stg_file = ssis_cookbook_dir / "StgProductCategory.dtsx"
        if not stg_file.exists():
            pytest.skip("StgProductCategory.dtsx not available")

        parser = DTSXParser()
        pkg = parser.parse(stg_file)

        # Find the OLE_DB source component
        for task in pkg.control_flow_tasks:
            for comp in task.data_flow_components:
                if comp.component_type == DataFlowComponentType.OLE_DB_SOURCE:
                    assert comp.connection_manager_ref != "", f"Source {comp.name} should have connection_manager_ref"
