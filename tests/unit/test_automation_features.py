"""
Tests for new automation features added in the automation sprint:
- Connection mapping config
- Lookup / Aggregate / Sort / Conditional Split metadata wiring
- SSIS expression transpiler (M and PySpark)
- ForEach enumerator conversion
- FILE_SYSTEM task mapping
- FTP task mapping
- Destination sidecar manifests
- SSISDB extractor (import only, no live DB)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from ssis_to_fabric.analyzer.models import (
    Column,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    DataFlowPath,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator
from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

if TYPE_CHECKING:
    from pathlib import Path

# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture
def config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(
        project_name="test-auto",
        strategy=MigrationStrategy.HYBRID,
        output_dir=tmp_path / "output",
    )


@pytest.fixture
def df_gen(config: MigrationConfig) -> DataFactoryGenerator:
    return DataFactoryGenerator(config)


@pytest.fixture
def dataflow_gen(config: MigrationConfig) -> DataflowGen2Generator:
    return DataflowGen2Generator(config)


@pytest.fixture
def spark_gen(config: MigrationConfig) -> SparkNotebookGenerator:
    return SparkNotebookGenerator(config)


def _make_package(**kwargs) -> SSISPackage:
    defaults = dict(name="TestPackage", connection_managers=[], variables=[])
    defaults.update(kwargs)
    return SSISPackage(**defaults)


# ===================================================================
# 1. Connection Mapping Config
# ===================================================================


class TestConnectionMappingConfig:
    def test_default_empty_mappings(self, config):
        assert config.connection_mappings.mappings == {}

    def test_custom_mappings(self, tmp_path):
        cfg = MigrationConfig(
            project_name="t",
            strategy=MigrationStrategy.HYBRID,
            output_dir=tmp_path,
            connection_mappings={"mappings": {"MyConn": "fabric-conn-id"}},
        )
        assert cfg.connection_mappings.mappings["MyConn"] == "fabric-conn-id"


# ===================================================================
# 2. Lookup metadata wiring
# ===================================================================


class TestLookupMetadata:
    def _lookup_component(self) -> DataFlowComponent:
        return DataFlowComponent(
            name="Lookup_Country",
            component_type=DataFlowComponentType.LOOKUP,
            table_name="dim_country",
            properties={
                "_join_keys": ["CountryCode"],
                "_ref_keys": ["Code"],
            },
        )

    def test_dataflow_lookup_uses_join_keys(self, dataflow_gen):
        comp = self._lookup_component()
        step_name, step_expr = dataflow_gen._gen_lookup_pq(comp, "prev_step")
        assert "CountryCode" in step_expr
        assert "Code" in step_expr

    def test_spark_lookup_uses_join_keys(self, spark_gen):
        comp = self._lookup_component()
        result = spark_gen._gen_lookup(comp)
        assert "CountryCode" in result
        assert "Code" in result

    def test_dataflow_lookup_fallback_no_metadata(self, dataflow_gen):
        comp = DataFlowComponent(
            name="Lookup_NoMeta",
            component_type=DataFlowComponentType.LOOKUP,
            table_name="some_table",
        )
        step_name, step_expr = dataflow_gen._gen_lookup_pq(comp, "prev_step")
        assert "TODO" in step_expr


# ===================================================================
# 3. Aggregate metadata wiring
# ===================================================================


class TestAggregateMetadata:
    def _agg_component(self) -> DataFlowComponent:
        return DataFlowComponent(
            name="Agg_Sales",
            component_type=DataFlowComponentType.AGGREGATE,
            properties={
                "_group_by": ["Region", "Year"],
                "_aggregations": [
                    {"column": "Amount", "function": "sum"},
                    {"column": "OrderId", "function": "count"},
                ],
            },
        )

    def test_dataflow_aggregate_uses_metadata(self, dataflow_gen):
        comp = self._agg_component()
        step_name, step_expr = dataflow_gen._gen_aggregate_pq(comp, "prev_step")
        assert "Region" in step_expr
        assert "Year" in step_expr
        assert "List.Sum" in step_expr

    def test_spark_aggregate_uses_metadata(self, spark_gen):
        comp = self._agg_component()
        result = spark_gen._gen_aggregate(comp)
        assert "Region" in result
        assert "F.sum" in result


# ===================================================================
# 4. Sort metadata wiring
# ===================================================================


class TestSortMetadata:
    def _sort_component(self) -> DataFlowComponent:
        return DataFlowComponent(
            name="Sort_Date",
            component_type=DataFlowComponentType.SORT,
            properties={
                "_sort_columns": [
                    {"column": "OrderDate", "direction": "desc"},
                    {"column": "CustomerId", "direction": "asc"},
                ],
            },
        )

    def test_dataflow_sort_uses_metadata(self, dataflow_gen):
        comp = self._sort_component()
        step_name, step_expr = dataflow_gen._gen_sort_pq(comp, "prev_step")
        assert "OrderDate" in step_expr
        assert "Descending" in step_expr

    def test_spark_sort_uses_metadata(self, spark_gen):
        comp = self._sort_component()
        result = spark_gen._gen_sort(comp)
        assert "OrderDate" in result
        assert ".desc()" in result


# ===================================================================
# 5. Conditional Split metadata wiring
# ===================================================================


class TestConditionalSplitMetadata:
    def _split_component(self) -> DataFlowComponent:
        return DataFlowComponent(
            name="Split_Region",
            component_type=DataFlowComponentType.CONDITIONAL_SPLIT,
            properties={
                "_conditions": [
                    {"output": "US_Sales", "expression": '[Region] == "US"'},
                    {"output": "EU_Sales", "expression": '[Region] == "EU"'},
                ],
            },
        )

    def test_dataflow_split_uses_first_condition(self, dataflow_gen):
        comp = self._split_component()
        step_name, step_expr = dataflow_gen._gen_conditional_split_pq(comp, "prev_step")
        assert "Region" in step_expr

    def test_spark_split_generates_filters(self, spark_gen):
        comp = self._split_component()
        result = spark_gen._gen_conditional_split(comp)
        assert "US_Sales" in result or "us_sales" in result.lower()
        assert "EU_Sales" in result or "eu_sales" in result.lower()


# ===================================================================
# 6. SSIS Expression Transpiler
# ===================================================================


class TestSSISExpressionTranspiler:
    """Tests for _ssis_expr_to_m and _ssis_expr_to_pyspark."""

    def test_m_getdate(self):
        result = DataflowGen2Generator._ssis_expr_to_m("GETDATE()")
        assert "DateTime.LocalNow()" in result

    def test_m_upper(self):
        result = DataflowGen2Generator._ssis_expr_to_m("UPPER(Name)")
        assert "Text.Upper" in result

    def test_m_lower(self):
        result = DataflowGen2Generator._ssis_expr_to_m("LOWER(Name)")
        assert "Text.Lower" in result

    def test_m_trim(self):
        result = DataflowGen2Generator._ssis_expr_to_m("TRIM(Code)")
        assert "Text.Trim" in result

    def test_m_len(self):
        result = DataflowGen2Generator._ssis_expr_to_m("LEN(Name)")
        assert "Text.Length" in result

    def test_m_replace(self):
        result = DataflowGen2Generator._ssis_expr_to_m('REPLACE(Name, "a", "b")')
        assert "Text.Replace" in result

    def test_m_isnull(self):
        result = DataflowGen2Generator._ssis_expr_to_m("ISNULL(Code)")
        assert "= null" in result

    def test_m_substring(self):
        result = DataflowGen2Generator._ssis_expr_to_m("SUBSTRING(Name, 1, 3)")
        assert "Text.Middle" in result
        assert "0" in result  # 1-based → 0-based

    def test_m_dt_str_cast(self):
        result = DataflowGen2Generator._ssis_expr_to_m("(DT_STR, 50, 1252) Code")
        assert "Text.From" in result

    def test_m_concatenation(self):
        result = DataflowGen2Generator._ssis_expr_to_m('FirstName + " " + LastName')
        assert "&" in result

    def test_m_ternary(self):
        result = DataflowGen2Generator._ssis_expr_to_m("x > 0 ? x : 0")
        assert "if" in result and "then" in result and "else" in result

    def test_m_null_input(self):
        assert DataflowGen2Generator._ssis_expr_to_m("null") == "null"
        assert DataflowGen2Generator._ssis_expr_to_m("") == "null"

    # PySpark transpiler
    def test_pyspark_getdate(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("GETDATE()")
        assert "current_timestamp" in result

    def test_pyspark_upper(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("UPPER(Name)")
        assert "F.upper" in result

    def test_pyspark_lower(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("LOWER(Name)")
        assert "F.lower" in result

    def test_pyspark_trim(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("TRIM(Code)")
        assert "F.trim" in result

    def test_pyspark_len(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("LEN(Name)")
        assert "F.length" in result

    def test_pyspark_replace(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark('REPLACE(Name, "a", "b")')
        assert "regexp_replace" in result

    def test_pyspark_substring(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("SUBSTRING(Name, 1, 3)")
        assert "F.substring" in result

    def test_pyspark_isnull(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("ISNULL(Code)")
        assert "isNull" in result

    def test_pyspark_dt_str_cast(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("(DT_STR, 50, 1252) Code")
        assert 'cast("string")' in result

    def test_pyspark_dt_i4_cast(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("(DT_I4) OrderId")
        assert 'cast("int")' in result

    def test_pyspark_dt_wstr_cast(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("(DT_WSTR, 100) Name")
        assert 'cast("string")' in result

    def test_pyspark_ternary(self):
        result = SparkNotebookGenerator._ssis_expr_to_pyspark("x > 0 ? x : 0")
        assert "F.expr" in result

    def test_pyspark_null(self):
        assert SparkNotebookGenerator._ssis_expr_to_pyspark("") == "F.lit(None)"
        assert SparkNotebookGenerator._ssis_expr_to_pyspark("null") == "F.lit(None)"


# ===================================================================
# 7. ForEach Enumerator
# ===================================================================


class TestForEachEnumerator:
    def test_file_enumerator_items_expression(self, df_gen):
        task = ControlFlowTask(
            name="ForEach_Files",
            task_type=TaskType.FOREACH_LOOP,
            properties={
                "_enumerator_type": "File",
                "_enum_Folder": "/data/incoming",
                "_enum_FileSpec": "*.csv",
            },
        )
        result = df_gen._foreach_items_expression(task)
        assert "ListFiles" in result
        assert "/data/incoming" in result

    def test_item_enumerator_items_expression(self, df_gen):
        task = ControlFlowTask(
            name="ForEach_Items",
            task_type=TaskType.FOREACH_LOOP,
            properties={"_enumerator_type": "Item"},
        )
        result = df_gen._foreach_items_expression(task)
        assert "pipeline().parameters.items" in result

    def test_ado_enumerator_items_expression(self, df_gen):
        task = ControlFlowTask(
            name="ForEach_ADO",
            task_type=TaskType.FOREACH_LOOP,
            properties={"_enumerator_type": "ADO"},
        )
        result = df_gen._foreach_items_expression(task)
        assert "LookupQuery" in result

    def test_default_enumerator_fallback(self, df_gen):
        task = ControlFlowTask(
            name="ForEach_Unknown",
            task_type=TaskType.FOREACH_LOOP,
        )
        result = df_gen._foreach_items_expression(task)
        assert "pipeline().parameters.items" in result

    def test_foreach_activity_uses_enumerator(self, df_gen):
        task = ControlFlowTask(
            name="ForEach_Files",
            task_type=TaskType.FOREACH_LOOP,
            properties={"_enumerator_type": "File", "_enum_Folder": "/data"},
        )
        activity = df_gen._foreach_to_activity(task, [])
        items_value = activity["typeProperties"]["items"]["value"]
        assert "ListFiles" in items_value


# ===================================================================
# 8. FILE_SYSTEM Task Mapping
# ===================================================================


class TestFileSystemTask:
    def test_copy_file(self, df_gen):
        task = ControlFlowTask(
            name="Copy_Report",
            task_type=TaskType.FILE_SYSTEM,
            properties={
                "_operation_name": "CopyFile",
                "Source": "/data/report.csv",
                "Destination": "/archive/report.csv",
            },
        )
        activity = df_gen._file_system_to_activity(task)
        assert activity["type"] == "TridentNotebook"
        assert "CopyFile" in activity["description"]
        assert "mssparkutils.fs.cp" in activity["description"]

    def test_delete_file(self, df_gen):
        task = ControlFlowTask(
            name="Delete_Temp",
            task_type=TaskType.FILE_SYSTEM,
            properties={"_operation_name": "DeleteFile", "Source": "/tmp/temp.csv"},
        )
        activity = df_gen._file_system_to_activity(task)
        assert "mssparkutils.fs.rm" in activity["description"]

    def test_create_directory(self, df_gen):
        task = ControlFlowTask(
            name="MkDir",
            task_type=TaskType.FILE_SYSTEM,
            properties={"_operation_name": "CreateDirectory", "Destination": "/data/new_dir"},
        )
        activity = df_gen._file_system_to_activity(task)
        assert "mssparkutils.fs.mkdirs" in activity["description"]

    def test_dispatch_routes_file_system(self, df_gen):
        task = ControlFlowTask(
            name="FS_Task",
            task_type=TaskType.FILE_SYSTEM,
            properties={"_operation_name": "MoveFile"},
        )
        activity = df_gen._task_to_activity(task, [])
        assert activity["type"] == "TridentNotebook"


# ===================================================================
# 9. FTP Task Mapping
# ===================================================================


class TestFTPTask:
    def test_ftp_receive(self, df_gen):
        task = ControlFlowTask(
            name="FTP_Download",
            task_type=TaskType.FTP,
            properties={
                "_ftp_operation_name": "Receive",
                "LocalPath": "/data/local",
                "RemotePath": "/remote/data",
            },
        )
        activity = df_gen._ftp_to_activity(task)
        assert activity["type"] == "Copy"
        assert "Receive" in activity["description"]

    def test_ftp_send(self, df_gen):
        task = ControlFlowTask(
            name="FTP_Upload",
            task_type=TaskType.FTP,
            properties={"_ftp_operation_name": "Send"},
        )
        activity = df_gen._ftp_to_activity(task)
        assert activity["type"] == "Copy"

    def test_ftp_other_operation(self, df_gen):
        task = ControlFlowTask(
            name="FTP_Delete",
            task_type=TaskType.FTP,
            properties={"_ftp_operation_name": "DeleteRemote"},
        )
        activity = df_gen._ftp_to_activity(task)
        assert activity["type"] == "TridentNotebook"

    def test_dispatch_routes_ftp(self, df_gen):
        task = ControlFlowTask(
            name="FTP_Task",
            task_type=TaskType.FTP,
            properties={"_ftp_operation_name": "Receive"},
        )
        activity = df_gen._task_to_activity(task, [])
        assert activity["type"] == "Copy"


# ===================================================================
# 10. Destination Sidecar Manifest
# ===================================================================


class TestDestinationSidecar:
    def _data_flow_task(self) -> ControlFlowTask:
        return ControlFlowTask(
            name="Load_DimCustomer",
            task_type=TaskType.DATA_FLOW,
            data_flow_components=[
                DataFlowComponent(
                    name="OLE_DB_Source",
                    component_type=DataFlowComponentType.OLE_DB_SOURCE,
                    table_name="staging.Customer",
                ),
                DataFlowComponent(
                    name="OLE_DB_Dest",
                    component_type=DataFlowComponentType.OLE_DB_DESTINATION,
                    table_name="dw.DimCustomer",
                    connection_manager_ref="DW_Conn",
                    columns=[
                        Column(name="CustomerKey", data_type="i4"),
                        Column(name="CustomerName", data_type="str", length=100),
                    ],
                ),
            ],
            data_flow_paths=[
                DataFlowPath(source_component="OLE_DB_Source", destination_component="OLE_DB_Dest"),
            ],
        )

    def test_dataflow_sidecar_written(self, dataflow_gen, config, tmp_path):
        task = self._data_flow_task()
        pkg = _make_package()
        output_dir = tmp_path / "output"
        dataflow_gen.generate(pkg, task, output_dir)

        # Check sidecar exists
        dataflows_dir = output_dir / "dataflows"
        sidecars = list(dataflows_dir.glob("*.destinations.json"))
        assert len(sidecars) == 1

        manifest = json.loads(sidecars[0].read_text())
        assert "destinations" in manifest
        assert len(manifest["destinations"]) == 1
        dest = manifest["destinations"][0]
        assert dest["table"] == "dw.DimCustomer"
        assert dest["connectionRef"] == "DW_Conn"
        assert dest["enableStaging"] is False
        assert len(dest["columns"]) == 2

    def test_spark_sidecar_written(self, spark_gen, config, tmp_path):
        task = self._data_flow_task()
        pkg = _make_package()
        output_dir = tmp_path / "output"
        spark_gen.generate(pkg, task, output_dir)

        notebooks_dir = output_dir / "notebooks"
        sidecars = list(notebooks_dir.glob("*.destinations.json"))
        assert len(sidecars) == 1

    def test_no_sidecar_when_no_destinations(self, dataflow_gen, config, tmp_path):
        task = ControlFlowTask(
            name="Transform_Only",
            task_type=TaskType.DATA_FLOW,
            data_flow_components=[
                DataFlowComponent(
                    name="OLE_DB_Source",
                    component_type=DataFlowComponentType.OLE_DB_SOURCE,
                    table_name="staging.Data",
                ),
            ],
        )
        pkg = _make_package()
        output_dir = tmp_path / "output"
        dataflow_gen.generate(pkg, task, output_dir)

        dataflows_dir = output_dir / "dataflows"
        sidecars = list(dataflows_dir.glob("*.destinations.json"))
        assert len(sidecars) == 0


# ===================================================================
# 11. SSISDB Extractor (import test only)
# ===================================================================


class TestSSISDBExtractorImport:
    def test_can_import_module(self):
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        assert SSISDBExtractor is not None

    def test_constructor(self):
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        ext = SSISDBExtractor("Driver={ODBC Driver 17};Server=localhost;")
        assert ext.connection_string.startswith("Driver=")

    def test_not_connected_raises(self):
        from ssis_to_fabric.engine.ssisdb_extractor import SSISDBExtractor

        ext = SSISDBExtractor("fake")
        with pytest.raises(RuntimeError, match="Not connected"):
            ext.list_packages()


# ===================================================================
# 12. Spark Derived Column uses expression transpiler
# ===================================================================


class TestDerivedColumnExpression:
    def test_spark_derived_column_with_expression(self, spark_gen):
        comp = DataFlowComponent(
            name="Derive_FullName",
            component_type=DataFlowComponentType.DERIVED_COLUMN,
            columns=[
                Column(name="FullName", expression="UPPER(FirstName)"),
            ],
        )
        result = spark_gen._gen_derived_column(comp)
        assert "F.upper" in result
        assert "FullName" in result

    def test_spark_derived_column_no_expression(self, spark_gen):
        comp = DataFlowComponent(
            name="Derive_Empty",
            component_type=DataFlowComponentType.DERIVED_COLUMN,
            columns=[
                Column(name="NewCol"),
            ],
        )
        result = spark_gen._gen_derived_column(comp)
        assert "F.lit(None)" in result


# ===================================================================
# 13. Merge Join metadata wiring
# ===================================================================


class TestMergeJoinMetadata:
    def _merge_component(self) -> DataFlowComponent:
        return DataFlowComponent(
            name="MergeJoin_Orders",
            component_type=DataFlowComponentType.MERGE_JOIN,
            properties={
                "_join_type": "left",
                "_left_keys": ["OrderId"],
                "_right_keys": ["OrderId"],
            },
        )

    def test_spark_merge_join_uses_metadata(self, spark_gen):
        comp = self._merge_component()
        result = spark_gen._gen_merge_join(comp)
        assert "OrderId" in result
        assert '"left"' in result

    def test_spark_merge_join_fallback(self, spark_gen):
        comp = DataFlowComponent(
            name="MergeJoin_NoMeta",
            component_type=DataFlowComponentType.MERGE_JOIN,
        )
        result = spark_gen._gen_merge_join(comp)
        assert "TODO" in result


# ===================================================================
# 14. Parser enumerator classifier
# ===================================================================


class TestEnumeratorClassifier:
    def test_file_enumerator(self):
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        assert DTSXParser._classify_enumerator("Microsoft.ForEachFileEnumerator") == "File"

    def test_item_enumerator(self):
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        assert DTSXParser._classify_enumerator("Microsoft.ForEachItemEnumerator") == "Item"

    def test_ado_enumerator(self):
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        assert DTSXParser._classify_enumerator("Microsoft.ForEachADOEnumerator") == "ADO"

    def test_unknown_enumerator(self):
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        assert DTSXParser._classify_enumerator("SomeCustomEnumerator") == "Unknown"
