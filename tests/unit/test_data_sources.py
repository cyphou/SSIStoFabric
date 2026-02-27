"""
Tests for expanded data source support.

Covers:
- New enum values (ODBC, XML, Raw File, CDC, Recordset, SQL Server Dest, etc.)
- COMPONENT_CLASS_MAP recognition
- Parser connection type detection (Oracle, SharePoint, Analysis Services)
- Dataflow generator: M code for ODBC, XML, Raw File, CDC sources
- Spark generator: type-aware reads (Flat File, Excel, XML, Raw, CDC, ODBC)
- Data Factory generator: type-aware Copy source/sink
- Updated _is_source / _is_destination / _is_transform helpers
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import (
    COMPONENT_CLASS_MAP,
    COMPONENT_COMPLEXITY,
    DTSXParser,
)
from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
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
        project_name="test-datasources",
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
    defaults = dict(name="TestPkg", connection_managers=[], variables=[])
    defaults.update(kwargs)
    return SSISPackage(**defaults)


def _odbc_conn() -> ConnectionManager:
    return ConnectionManager(
        id="odbc-1",
        name="ODBC_Conn",
        connection_type=ConnectionType.ODBC,
        connection_string="DSN=MyDSN",
    )


def _flat_file_conn() -> ConnectionManager:
    return ConnectionManager(
        id="ff-1",
        name="FlatFile_Conn",
        connection_type=ConnectionType.FLAT_FILE,
        connection_string="/data/input.csv",
    )


def _excel_conn() -> ConnectionManager:
    return ConnectionManager(
        id="xl-1",
        name="Excel_Conn",
        connection_type=ConnectionType.EXCEL,
        connection_string="/data/workbook.xlsx",
    )


# ===================================================================
# 1. Enum values exist
# ===================================================================


class TestEnumValues:
    """Ensure all newly added enum members are accessible."""

    @pytest.mark.parametrize(
        "member",
        [
            "ODBC_SOURCE",
            "XML_SOURCE",
            "RAW_FILE_SOURCE",
            "CDC_SOURCE",
            "SCRIPT_COMPONENT_SOURCE",
            "ODBC_DESTINATION",
            "RAW_FILE_DESTINATION",
            "RECORDSET_DESTINATION",
            "SQL_SERVER_DESTINATION",
            "DATA_READER_DESTINATION",
            "FUZZY_LOOKUP",
            "FUZZY_GROUPING",
            "TERM_LOOKUP",
            "COPY_COLUMN",
            "CHARACTER_MAP",
            "AUDIT",
            "MERGE",
            "CDC_SPLITTER",
            "PERCENTAGE_SAMPLING",
            "ROW_SAMPLING",
            "BALANCED_DATA_DISTRIBUTOR",
            "CACHE_TRANSFORM",
        ],
    )
    def test_component_type_member(self, member):
        assert hasattr(DataFlowComponentType, member)

    @pytest.mark.parametrize(
        "member",
        [
            "ORACLE",
            "SHAREPOINT",
            "ANALYSIS_SERVICES",
        ],
    )
    def test_connection_type_member(self, member):
        assert hasattr(ConnectionType, member)


# ===================================================================
# 2. COMPONENT_CLASS_MAP
# ===================================================================


class TestComponentClassMap:
    """Each new SSIS class ID is mapped to the correct enum."""

    @pytest.mark.parametrize(
        "class_id, expected",
        [
            ("Microsoft.ODBCSource", DataFlowComponentType.ODBC_SOURCE),
            ("Microsoft.ODBCDestination", DataFlowComponentType.ODBC_DESTINATION),
            ("Microsoft.XMLSource", DataFlowComponentType.XML_SOURCE),
            ("Microsoft.RawFileSource", DataFlowComponentType.RAW_FILE_SOURCE),
            ("Microsoft.RawFileDestination", DataFlowComponentType.RAW_FILE_DESTINATION),
            ("Microsoft.CDCSource", DataFlowComponentType.CDC_SOURCE),
            ("Microsoft.RecordsetDestination", DataFlowComponentType.RECORDSET_DESTINATION),
            ("Microsoft.SQLServerDestination", DataFlowComponentType.SQL_SERVER_DESTINATION),
            ("Microsoft.DataReaderDestination", DataFlowComponentType.DATA_READER_DESTINATION),
            ("Microsoft.FuzzyLookup", DataFlowComponentType.FUZZY_LOOKUP),
            ("Microsoft.FuzzyGrouping", DataFlowComponentType.FUZZY_GROUPING),
            ("Microsoft.TermLookup", DataFlowComponentType.TERM_LOOKUP),
            ("Microsoft.CopyColumn", DataFlowComponentType.COPY_COLUMN),
            ("Microsoft.CharacterMap", DataFlowComponentType.CHARACTER_MAP),
            ("Microsoft.Audit", DataFlowComponentType.AUDIT),
            ("Microsoft.Merge", DataFlowComponentType.MERGE),
            ("Microsoft.CDCSplitter", DataFlowComponentType.CDC_SPLITTER),
            ("Microsoft.PercentageSampling", DataFlowComponentType.PERCENTAGE_SAMPLING),
            ("Microsoft.RowSampling", DataFlowComponentType.ROW_SAMPLING),
            ("Microsoft.BalancedDataDistributor", DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR),
            ("Microsoft.CacheTransform", DataFlowComponentType.CACHE_TRANSFORM),
        ],
    )
    def test_class_id_mapping(self, class_id, expected):
        assert COMPONENT_CLASS_MAP[class_id] == expected


# ===================================================================
# 3. Complexity mapping
# ===================================================================


class TestComplexityMapping:
    """All new types must have a complexity entry."""

    @pytest.mark.parametrize(
        "comp_type",
        [
            DataFlowComponentType.ODBC_SOURCE,
            DataFlowComponentType.XML_SOURCE,
            DataFlowComponentType.RAW_FILE_SOURCE,
            DataFlowComponentType.CDC_SOURCE,
            DataFlowComponentType.ODBC_DESTINATION,
            DataFlowComponentType.RAW_FILE_DESTINATION,
            DataFlowComponentType.RECORDSET_DESTINATION,
            DataFlowComponentType.SQL_SERVER_DESTINATION,
            DataFlowComponentType.DATA_READER_DESTINATION,
            DataFlowComponentType.FUZZY_LOOKUP,
            DataFlowComponentType.FUZZY_GROUPING,
            DataFlowComponentType.TERM_LOOKUP,
            DataFlowComponentType.COPY_COLUMN,
            DataFlowComponentType.CHARACTER_MAP,
            DataFlowComponentType.AUDIT,
            DataFlowComponentType.MERGE,
            DataFlowComponentType.CDC_SPLITTER,
            DataFlowComponentType.PERCENTAGE_SAMPLING,
            DataFlowComponentType.ROW_SAMPLING,
            DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR,
            DataFlowComponentType.CACHE_TRANSFORM,
        ],
    )
    def test_has_complexity(self, comp_type):
        assert comp_type in COMPONENT_COMPLEXITY


# ===================================================================
# 4. Parser connection type detection
# ===================================================================


class TestConnectionTypeDetection:
    @pytest.fixture(autouse=True)
    def _parser(self):
        self.parser = DTSXParser()

    @pytest.mark.parametrize(
        "creation_name, expected",
        [
            ("ORACLE", ConnectionType.ORACLE),
            ("OracleClient", ConnectionType.ORACLE),
            ("SHAREPOINT", ConnectionType.SHAREPOINT),
            ("SharePointList", ConnectionType.SHAREPOINT),
            ("MSOLAP", ConnectionType.ANALYSIS_SERVICES),
            ("AnalysisServicesConnection", ConnectionType.ANALYSIS_SERVICES),
            ("ODBC", ConnectionType.ODBC),
            ("OLEDB", ConnectionType.OLEDB),
        ],
    )
    def test_connection_type(self, creation_name, expected):
        assert self.parser._classify_connection_type(creation_name) == expected


# ===================================================================
# 5. _is_source / _is_destination / _is_transform helpers
# ===================================================================


class TestHelperClassification:
    """New types must be classified correctly in all three generators."""

    _new_sources = [
        DataFlowComponentType.ODBC_SOURCE,
        DataFlowComponentType.XML_SOURCE,
        DataFlowComponentType.RAW_FILE_SOURCE,
        DataFlowComponentType.CDC_SOURCE,
    ]
    _new_destinations = [
        DataFlowComponentType.ODBC_DESTINATION,
        DataFlowComponentType.RAW_FILE_DESTINATION,
        DataFlowComponentType.RECORDSET_DESTINATION,
        DataFlowComponentType.SQL_SERVER_DESTINATION,
        DataFlowComponentType.DATA_READER_DESTINATION,
    ]

    @pytest.mark.parametrize("comp_type", _new_sources)
    def test_dataflow_is_source(self, comp_type):
        comp = DataFlowComponent(name="x", component_type=comp_type)
        assert DataflowGen2Generator._is_source(comp) is True
        assert DataflowGen2Generator._is_destination(comp) is False

    @pytest.mark.parametrize("comp_type", _new_destinations)
    def test_dataflow_is_destination(self, comp_type):
        comp = DataFlowComponent(name="x", component_type=comp_type)
        assert DataflowGen2Generator._is_destination(comp) is True
        assert DataflowGen2Generator._is_source(comp) is False

    @pytest.mark.parametrize("comp_type", _new_sources)
    def test_spark_is_source(self, comp_type):
        comp = DataFlowComponent(name="x", component_type=comp_type)
        assert SparkNotebookGenerator._is_source(comp) is True
        assert SparkNotebookGenerator._is_destination(comp) is False

    @pytest.mark.parametrize("comp_type", _new_destinations)
    def test_spark_is_destination(self, comp_type):
        comp = DataFlowComponent(name="x", component_type=comp_type)
        assert SparkNotebookGenerator._is_destination(comp) is True
        assert SparkNotebookGenerator._is_source(comp) is False


# ===================================================================
# 6. Dataflow generator (Power Query M) — new sources
# ===================================================================


class TestDataflowNewSources:
    """M code generation for ODBC, XML, Raw File, CDC sources."""

    def test_odbc_source_with_query(self, dataflow_gen):
        comp = DataFlowComponent(
            name="ODBC_Src",
            component_type=DataFlowComponentType.ODBC_SOURCE,
            sql_command="SELECT * FROM orders",
            connection_manager_ref="odbc-1",
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [_odbc_conn()])
        assert "Odbc.DataSource" in m_code
        assert "SELECT * FROM orders" in m_code

    def test_odbc_source_with_table(self, dataflow_gen):
        comp = DataFlowComponent(
            name="ODBC_Src",
            component_type=DataFlowComponentType.ODBC_SOURCE,
            table_name="products",
            connection_manager_ref="odbc-1",
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [_odbc_conn()])
        assert "Odbc.DataSource" in m_code
        assert "products" in m_code

    def test_xml_source(self, dataflow_gen):
        conn = ConnectionManager(
            id="xml-1",
            name="XML",
            connection_type=ConnectionType.FILE,
            connection_string="/data/feed.xml",
        )
        comp = DataFlowComponent(
            name="XML_Src",
            component_type=DataFlowComponentType.XML_SOURCE,
            connection_manager_ref="xml-1",
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [conn])
        assert "Xml.Tables" in m_code

    def test_raw_file_source(self, dataflow_gen):
        comp = DataFlowComponent(
            name="Raw_Src",
            component_type=DataFlowComponentType.RAW_FILE_SOURCE,
            properties={"FileName": "/data/raw_output.raw"},
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [])
        assert "Binary" in m_code or "raw" in m_code.lower()

    def test_cdc_source_with_table(self, dataflow_gen):
        conn = ConnectionManager(
            id="sql-1",
            name="SQL",
            connection_type=ConnectionType.OLEDB,
            server="myserver",
            database="mydb",
        )
        comp = DataFlowComponent(
            name="CDC_Src",
            component_type=DataFlowComponentType.CDC_SOURCE,
            table_name="dbo.orders",
            connection_manager_ref="sql-1",
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [conn])
        assert "cdc" in m_code.lower()

    def test_cdc_source_with_query(self, dataflow_gen):
        conn = ConnectionManager(
            id="sql-1",
            name="SQL",
            connection_type=ConnectionType.OLEDB,
            server="myserver",
            database="mydb",
        )
        comp = DataFlowComponent(
            name="CDC_Src",
            component_type=DataFlowComponentType.CDC_SOURCE,
            sql_command="SELECT * FROM cdc.dbo_orders_CT",
            connection_manager_ref="sql-1",
        )
        m_code, step = dataflow_gen._source_to_pq(comp, [conn])
        assert "cdc.dbo_orders_CT" in m_code


# ===================================================================
# 7. Spark generator — type-aware reads
# ===================================================================


class TestSparkTypeAwareReads:
    """Spark generator produces correct reader calls per source type."""

    def test_flat_file_uses_read_csv(self, spark_gen):
        comp = DataFlowComponent(
            name="FF_Src",
            component_type=DataFlowComponentType.FLAT_FILE_SOURCE,
            properties={"_file_path": "/data/input.csv"},
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "spark.read.csv" in code

    def test_excel_uses_excel_format(self, spark_gen):
        comp = DataFlowComponent(
            name="Excel_Src",
            component_type=DataFlowComponentType.EXCEL_SOURCE,
            properties={"OpenRowset": "Sales$", "_file_path": "/data/wb.xlsx"},
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "excel" in code.lower()
        assert "Sales" in code

    def test_xml_uses_xml_format(self, spark_gen):
        comp = DataFlowComponent(
            name="XML_Src",
            component_type=DataFlowComponentType.XML_SOURCE,
            properties={"_file_path": "/data/feed.xml"},
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "xml" in code.lower()

    def test_raw_file_uses_parquet(self, spark_gen):
        comp = DataFlowComponent(
            name="Raw_Src",
            component_type=DataFlowComponentType.RAW_FILE_SOURCE,
            properties={"FileName": "/data/raw.raw"},
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "parquet" in code.lower() or "raw" in code.lower()

    def test_cdc_source_with_table(self, spark_gen):
        comp = DataFlowComponent(
            name="CDC_Src",
            component_type=DataFlowComponentType.CDC_SOURCE,
            table_name="dbo.orders",
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "cdc" in code.lower()

    def test_cdc_source_with_query(self, spark_gen):
        comp = DataFlowComponent(
            name="CDC_Src",
            component_type=DataFlowComponentType.CDC_SOURCE,
            sql_command="SELECT * FROM cdc.dbo_orders_CT",
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "cdc.dbo_orders_CT" in code

    def test_odbc_source_with_table(self, spark_gen):
        comp = DataFlowComponent(
            name="ODBC_Src",
            component_type=DataFlowComponentType.ODBC_SOURCE,
            table_name="products",
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "jdbc" in code.lower()
        assert "products" in code

    def test_odbc_source_with_query(self, spark_gen):
        comp = DataFlowComponent(
            name="ODBC_Src",
            component_type=DataFlowComponentType.ODBC_SOURCE,
            sql_command="SELECT id, name FROM products",
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "jdbc" in code.lower()
        assert "SELECT id, name FROM products" in code

    def test_oledb_still_works(self, spark_gen):
        comp = DataFlowComponent(
            name="OLE_Src",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            sql_command="SELECT * FROM dbo.customers",
        )
        code = spark_gen._generate_source_read(comp, 0)
        assert "SELECT * FROM dbo.customers" in code


# ===================================================================
# 8. Data Factory generator — type-aware Copy source/sink
# ===================================================================


class TestAdfTypeAwareCopy:
    """Copy activity source/sink types match the SSIS component type."""

    def test_flat_file_source(self, df_gen):
        comp = DataFlowComponent(
            name="FF_Src",
            component_type=DataFlowComponentType.FLAT_FILE_SOURCE,
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "DelimitedTextSource"

    def test_excel_source(self, df_gen):
        comp = DataFlowComponent(
            name="XL_Src",
            component_type=DataFlowComponentType.EXCEL_SOURCE,
            properties={"OpenRowset": "Sheet1$"},
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "ExcelSource"

    def test_odbc_source(self, df_gen):
        comp = DataFlowComponent(
            name="ODBC_Src",
            component_type=DataFlowComponentType.ODBC_SOURCE,
            sql_command="SELECT 1",
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "OdbcSource"
        assert src["query"] == "SELECT 1"

    def test_xml_source(self, df_gen):
        comp = DataFlowComponent(
            name="XML_Src",
            component_type=DataFlowComponentType.XML_SOURCE,
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "XmlSource"

    def test_cdc_source_uses_sql(self, df_gen):
        comp = DataFlowComponent(
            name="CDC_Src",
            component_type=DataFlowComponentType.CDC_SOURCE,
            table_name="orders",
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "SqlSource"
        assert "cdc" in src["sqlReaderQuery"]

    def test_oledb_source_table(self, df_gen):
        comp = DataFlowComponent(
            name="OLE_Src",
            component_type=DataFlowComponentType.OLE_DB_SOURCE,
            table_name="dbo.orders",
        )
        src = df_gen._build_copy_source(comp, [])
        assert src["type"] == "SqlSource"
        assert "dbo.orders" in src["sqlReaderQuery"]

    def test_flat_file_sink(self, df_gen):
        comp = DataFlowComponent(
            name="FF_Dest",
            component_type=DataFlowComponentType.FLAT_FILE_DESTINATION,
        )
        sink = df_gen._build_copy_sink(comp, [])
        assert sink["type"] == "DelimitedTextSink"

    def test_excel_sink(self, df_gen):
        comp = DataFlowComponent(
            name="XL_Dest",
            component_type=DataFlowComponentType.EXCEL_DESTINATION,
        )
        sink = df_gen._build_copy_sink(comp, [])
        assert sink["type"] == "DelimitedTextSink"  # Excel not natively supported as sink

    def test_odbc_sink(self, df_gen):
        comp = DataFlowComponent(
            name="ODBC_Dest",
            component_type=DataFlowComponentType.ODBC_DESTINATION,
            table_name="target_table",
        )
        sink = df_gen._build_copy_sink(comp, [])
        assert sink["type"] == "OdbcSink"
        assert sink["tableName"] == "target_table"

    def test_oledb_sink_still_sql(self, df_gen):
        comp = DataFlowComponent(
            name="OLE_Dest",
            component_type=DataFlowComponentType.OLE_DB_DESTINATION,
            table_name="dbo.facts",
        )
        sink = df_gen._build_copy_sink(comp, [])
        assert sink["type"] == "SqlSink"

    def test_sql_server_dest_sink(self, df_gen):
        comp = DataFlowComponent(
            name="SQLSrv_Dest",
            component_type=DataFlowComponentType.SQL_SERVER_DESTINATION,
            table_name="dbo.staging",
        )
        sink = df_gen._build_copy_sink(comp, [])
        assert sink["type"] == "SqlSink"
        assert sink["tableName"] == "dbo.staging"


# ===================================================================
# 9. Dataflow _find_connection type-aware fallback
# ===================================================================


class TestFindConnectionFallback:
    """_find_connection returns the right connection type for each source."""

    def test_flat_file_prefers_flat_file_conn(self, dataflow_gen):
        comp = DataFlowComponent(
            name="FF",
            component_type=DataFlowComponentType.FLAT_FILE_SOURCE,
        )
        conns = [
            ConnectionManager(id="sql-1", name="SQL", connection_type=ConnectionType.OLEDB),
            _flat_file_conn(),
        ]
        result = dataflow_gen._find_connection(comp, conns)
        assert result.connection_type == ConnectionType.FLAT_FILE

    def test_excel_prefers_excel_conn(self, dataflow_gen):
        comp = DataFlowComponent(
            name="XL",
            component_type=DataFlowComponentType.EXCEL_SOURCE,
        )
        conns = [
            ConnectionManager(id="sql-1", name="SQL", connection_type=ConnectionType.OLEDB),
            _excel_conn(),
        ]
        result = dataflow_gen._find_connection(comp, conns)
        assert result.connection_type == ConnectionType.EXCEL

    def test_odbc_prefers_odbc_conn(self, dataflow_gen):
        comp = DataFlowComponent(
            name="ODBC",
            component_type=DataFlowComponentType.ODBC_SOURCE,
        )
        conns = [
            ConnectionManager(id="sql-1", name="SQL", connection_type=ConnectionType.OLEDB),
            _odbc_conn(),
        ]
        result = dataflow_gen._find_connection(comp, conns)
        assert result.connection_type == ConnectionType.ODBC

    def test_explicit_ref_overrides_affinity(self, dataflow_gen):
        comp = DataFlowComponent(
            name="FF",
            component_type=DataFlowComponentType.FLAT_FILE_SOURCE,
            connection_manager_ref="sql-1",
        )
        sql_conn = ConnectionManager(
            id="sql-1",
            name="SQL",
            connection_type=ConnectionType.OLEDB,
        )
        conns = [sql_conn, _flat_file_conn()]
        result = dataflow_gen._find_connection(comp, conns)
        assert result.id == "sql-1"


# ===================================================================
# 10. Parser _classify_component_type for new IDs
# ===================================================================


class TestParserClassifyComponent:
    @pytest.fixture(autouse=True)
    def _parser(self):
        self.parser = DTSXParser()

    @pytest.mark.parametrize(
        "class_id, expected",
        [
            ("Microsoft.ODBCSource", DataFlowComponentType.ODBC_SOURCE),
            ("Microsoft.XMLSource", DataFlowComponentType.XML_SOURCE),
            ("Microsoft.CDCSource", DataFlowComponentType.CDC_SOURCE),
            ("Microsoft.RecordsetDestination", DataFlowComponentType.RECORDSET_DESTINATION),
            ("Microsoft.FuzzyLookup", DataFlowComponentType.FUZZY_LOOKUP),
            ("Microsoft.Merge", DataFlowComponentType.MERGE),
            ("Microsoft.CacheTransform", DataFlowComponentType.CACHE_TRANSFORM),
        ],
    )
    def test_classify_component(self, class_id, expected):
        assert self.parser._classify_component_type(class_id) == expected
