"""Phase 10 — Advanced SSIS Features tests.

Covers:
- TransactionOption model & enum values
- LogProvider model
- Package annotations field
- WMI_EVENT_WATCHER / WMI_DATA_READER task types
- Parser: transaction option extraction
- Parser: log provider parsing
- Parser: annotation parsing
- Data Factory: WebService, XML, WMI task activities
- Data Factory: transaction scope application
- Data Factory: annotation / log provider in pipeline definition
- Spark: WebService, XML, WMI placeholders
- Spark: header with annotations and transaction info
- Migration Engine: logging config output
- Migration Engine: task-level checkpoints
- Disabled task skipping in generators
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    LogProvider,
    SSISPackage,
    TaskType,
    TransactionOption,
)
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.migration_engine import MigrationEngine
from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(
        project_name="test-phase10",
        strategy=MigrationStrategy.HYBRID,
        output_dir=tmp_path / "output",
    )


@pytest.fixture
def df_generator(config: MigrationConfig) -> DataFactoryGenerator:
    return DataFactoryGenerator(config)


@pytest.fixture
def spark_generator(config: MigrationConfig) -> SparkNotebookGenerator:
    return SparkNotebookGenerator(config)


def _make_task(
    name: str = "T1",
    task_type: TaskType = TaskType.EXECUTE_SQL,
    *,
    disabled: bool = False,
    transaction_option: TransactionOption = TransactionOption.SUPPORTED,
    properties: dict[str, Any] | None = None,
    description: str = "",
) -> ControlFlowTask:
    return ControlFlowTask(
        name=name,
        task_type=task_type,
        disabled=disabled,
        transaction_option=transaction_option,
        description=description,
        properties=properties or {},
    )


def _make_package(
    name: str = "TestPkg",
    tasks: list[ControlFlowTask] | None = None,
    annotations: list[str] | None = None,
    logging_providers: list[LogProvider] | None = None,
    description: str = "",
) -> SSISPackage:
    return SSISPackage(
        name=name,
        description=description,
        control_flow_tasks=tasks or [],
        annotations=annotations or [],
        logging_providers=logging_providers or [],
    )


# =====================================================================
# Model tests
# =====================================================================


class TestTransactionOptionModel:
    @pytest.mark.unit
    def test_enum_values(self) -> None:
        assert TransactionOption.NOT_SUPPORTED.value == "NotSupported"
        assert TransactionOption.SUPPORTED.value == "Supported"
        assert TransactionOption.REQUIRED.value == "Required"

    @pytest.mark.unit
    def test_default_on_task(self) -> None:
        task = ControlFlowTask(name="t", task_type=TaskType.EXECUTE_SQL)
        assert task.transaction_option == TransactionOption.SUPPORTED

    @pytest.mark.unit
    def test_required_roundtrip(self) -> None:
        task = _make_task(transaction_option=TransactionOption.REQUIRED)
        assert task.transaction_option == TransactionOption.REQUIRED

    @pytest.mark.unit
    def test_isolation_level_default(self) -> None:
        task = ControlFlowTask(name="t", task_type=TaskType.EXECUTE_SQL)
        assert task.isolation_level == ""


class TestLogProviderModel:
    @pytest.mark.unit
    def test_basic_provider(self) -> None:
        lp = LogProvider(
            name="SQL Logger",
            provider_type="DTS.LogProviderSQLServer",
            connection_manager_ref="LogDB",
        )
        assert lp.name == "SQL Logger"
        assert lp.provider_type == "DTS.LogProviderSQLServer"
        assert lp.connection_manager_ref == "LogDB"
        assert lp.description == ""
        assert lp.properties == {}

    @pytest.mark.unit
    def test_provider_with_properties(self) -> None:
        lp = LogProvider(
            name="File Logger",
            provider_type="DTS.LogProviderTextFile",
            properties={"LogFilePath": "C:\\logs\\etl.log"},
        )
        assert lp.properties["LogFilePath"] == "C:\\logs\\etl.log"


class TestPackageAnnotationsAndLogProviders:
    @pytest.mark.unit
    def test_default_empty_annotations(self) -> None:
        pkg = SSISPackage(name="p")
        assert pkg.annotations == []

    @pytest.mark.unit
    def test_annotations_populated(self) -> None:
        pkg = _make_package(annotations=["Author: John", "Version: 2.0"])
        assert len(pkg.annotations) == 2
        assert "Author: John" in pkg.annotations

    @pytest.mark.unit
    def test_default_empty_logging_providers(self) -> None:
        pkg = SSISPackage(name="p")
        assert pkg.logging_providers == []

    @pytest.mark.unit
    def test_logging_providers_populated(self) -> None:
        pkg = _make_package(
            logging_providers=[
                LogProvider(name="SQL Log", provider_type="DTS.LogProviderSQLServer"),
                LogProvider(name="Text Log", provider_type="DTS.LogProviderTextFile"),
            ]
        )
        assert len(pkg.logging_providers) == 2


class TestWmiTaskTypes:
    @pytest.mark.unit
    def test_wmi_event_watcher_value(self) -> None:
        assert TaskType.WMI_EVENT_WATCHER.value == "WMI_EVENT_WATCHER"

    @pytest.mark.unit
    def test_wmi_data_reader_value(self) -> None:
        assert TaskType.WMI_DATA_READER.value == "WMI_DATA_READER"


# =====================================================================
# Parser tests (using mock XML)
# =====================================================================


def _write_dtsx(tmp_path: Path, xml: bytes, name: str = "test.dtsx") -> Path:
    """Write XML to a temp .dtsx file and return its path."""
    p = tmp_path / name
    p.write_bytes(xml)
    return p


class TestParserTransactionOption:
    @pytest.mark.unit
    def test_parse_transaction_option_required(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="SQL Task"
      DTS:ExecutableType="Microsoft.ExecuteSQLTask"
      DTS:TransactionOption="2">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert len(pkg.control_flow_tasks) >= 1
        assert pkg.control_flow_tasks[0].transaction_option == TransactionOption.REQUIRED

    @pytest.mark.unit
    def test_parse_transaction_option_not_supported(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="SQL Task"
      DTS:ExecutableType="Microsoft.ExecuteSQLTask"
      DTS:TransactionOption="0">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].transaction_option == TransactionOption.NOT_SUPPORTED

    @pytest.mark.unit
    def test_parse_transaction_option_default(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="SQL Task"
      DTS:ExecutableType="Microsoft.ExecuteSQLTask">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].transaction_option == TransactionOption.SUPPORTED


class TestParserLogProviders:
    @pytest.mark.unit
    def test_parse_log_providers(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:LogProviders>
    <DTS:LogProvider
      DTS:ObjectName="SQL Server Log"
      DTS:CreationName="DTS.LogProviderSQLServer"
      DTS:Description="Logs to SQL Server">
      <DTS:PropertyExpression DTS:Name="ConfigString">LogDB</DTS:PropertyExpression>
    </DTS:LogProvider>
    <DTS:LogProvider
      DTS:ObjectName="Text File Log"
      DTS:CreationName="DTS.LogProviderTextFile">
    </DTS:LogProvider>
  </DTS:LogProviders>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert len(pkg.logging_providers) == 2
        sql_log = next(lp for lp in pkg.logging_providers if "SQL" in lp.name)
        assert sql_log.provider_type == "DTS.LogProviderSQLServer"
        assert sql_log.description == "Logs to SQL Server"

    @pytest.mark.unit
    def test_empty_log_providers(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.logging_providers == []


class TestParserAnnotations:
    @pytest.mark.unit
    def test_parse_annotations(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:PackageAnnotations>
    <DTS:Annotation>Author: Jane Doe</DTS:Annotation>
    <DTS:Annotation>Review: 2024-01-15</DTS:Annotation>
  </DTS:PackageAnnotations>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert len(pkg.annotations) == 2
        assert "Author: Jane Doe" in pkg.annotations

    @pytest.mark.unit
    def test_no_annotations(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.annotations == []


class TestParserTaskClassMap:
    @pytest.mark.unit
    def test_web_service_task_type(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="Call API"
      DTS:CreationName="Microsoft.WebServiceTask">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].task_type == TaskType.WEB_SERVICE

    @pytest.mark.unit
    def test_xml_task_type(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="Parse XML"
      DTS:CreationName="Microsoft.XMLTask">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].task_type == TaskType.XML

    @pytest.mark.unit
    def test_wmi_event_watcher_task_type(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="Watch Events"
      DTS:CreationName="Microsoft.WmiEventWatcherTask">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].task_type == TaskType.WMI_EVENT_WATCHER

    @pytest.mark.unit
    def test_wmi_data_reader_task_type(self, tmp_path: Path) -> None:
        from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser

        xml = b"""\
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPkg"
  DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
      DTS:ObjectName="Read WMI"
      DTS:CreationName="Microsoft.WmiDataReaderTask">
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>"""
        pkg = DTSXParser().parse(_write_dtsx(tmp_path, xml))
        assert pkg.control_flow_tasks[0].task_type == TaskType.WMI_DATA_READER


# =====================================================================
# Data Factory Generator tests
# =====================================================================


class TestDFWebServiceActivity:
    @pytest.mark.unit
    def test_web_service_type(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Call API", TaskType.WEB_SERVICE, properties={"Url": "https://api.example.com"})
        act = df_generator._web_service_to_activity(task)
        assert act["type"] == "WebActivity"
        assert act["typeProperties"]["url"] == "https://api.example.com"

    @pytest.mark.unit
    def test_web_service_default_method(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Call API", TaskType.WEB_SERVICE)
        act = df_generator._web_service_to_activity(task)
        assert act["typeProperties"]["method"] == "GET"

    @pytest.mark.unit
    def test_web_service_custom_method(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Post Data", TaskType.WEB_SERVICE, properties={"HttpMethod": "POST"})
        act = df_generator._web_service_to_activity(task)
        assert act["typeProperties"]["method"] == "POST"


class TestDFXmlActivity:
    @pytest.mark.unit
    def test_xml_type(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Parse XML", TaskType.XML, properties={"OperationType": "XSLT"})
        act = df_generator._xml_task_to_activity(task)
        assert act["type"] == "Script"
        assert "XSLT" in act["typeProperties"]["scriptBlock"]

    @pytest.mark.unit
    def test_xml_description(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Parse XML", TaskType.XML)
        act = df_generator._xml_task_to_activity(task)
        assert "XML Task" in act["description"]


class TestDFWmiActivity:
    @pytest.mark.unit
    def test_wmi_type(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task(
            "Watch", TaskType.WMI_EVENT_WATCHER,
            properties={"WqlQuerySource": "SELECT * FROM Win32_Process"},
        )
        act = df_generator._wmi_to_activity(task)
        assert act["type"] == "Wait"
        assert "Win32_Process" in act["description"]

    @pytest.mark.unit
    def test_wmi_data_reader(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("Read WMI", TaskType.WMI_DATA_READER)
        act = df_generator._wmi_to_activity(task)
        assert act["type"] == "Wait"


class TestDFTransactionScope:
    @pytest.mark.unit
    def test_required_transaction_annotated(self, df_generator: DataFactoryGenerator) -> None:
        tasks = [_make_task("T1", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.REQUIRED)]
        activities = [{"name": df_generator._sanitize_name("T1"), "type": "Script"}]
        result = df_generator._apply_transaction_scope(activities, tasks)
        assert "[Transaction: REQUIRED]" in result[0]["description"]
        assert result[0]["policy"]["retry"] == 0

    @pytest.mark.unit
    def test_supported_transaction_not_annotated(self, df_generator: DataFactoryGenerator) -> None:
        tasks = [_make_task("T1", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.SUPPORTED)]
        activities = [{"name": df_generator._sanitize_name("T1"), "type": "Script"}]
        result = df_generator._apply_transaction_scope(activities, tasks)
        assert result[0].get("description", "") == ""

    @pytest.mark.unit
    def test_mixed_transactions(self, df_generator: DataFactoryGenerator) -> None:
        tasks = [
            _make_task("TCritical", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.REQUIRED),
            _make_task("TOptional", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.SUPPORTED),
        ]
        activities = [
            {"name": df_generator._sanitize_name("TCritical"), "type": "Script"},
            {"name": df_generator._sanitize_name("TOptional"), "type": "Script"},
        ]
        result = df_generator._apply_transaction_scope(activities, tasks)
        assert "[Transaction: REQUIRED]" in result[0].get("description", "")
        assert "[Transaction: REQUIRED]" not in result[1].get("description", "")


class TestDFPipelineAnnotationsAndLogProviders:
    @pytest.mark.unit
    def test_annotations_in_pipeline(self, df_generator: DataFactoryGenerator) -> None:
        pkg = _make_package(annotations=["v2.0", "Reviewed"])
        pipeline = df_generator._build_pipeline_definition("test", [], package=pkg)
        ann = pipeline["properties"]["annotations"]
        assert "v2.0" in ann
        assert "Reviewed" in ann

    @pytest.mark.unit
    def test_log_providers_in_description(self, df_generator: DataFactoryGenerator) -> None:
        pkg = _make_package(
            logging_providers=[
                LogProvider(name="SQL Log", provider_type="DTS.LogProviderSQLServer"),
            ]
        )
        pipeline = df_generator._build_pipeline_definition("test", [], package=pkg)
        desc = pipeline["properties"].get("description", "")
        assert "SQL Log" in desc

    @pytest.mark.unit
    def test_no_description_without_package_meta(self, df_generator: DataFactoryGenerator) -> None:
        pkg = _make_package()
        pipeline = df_generator._build_pipeline_definition("test", [], package=pkg)
        assert "description" not in pipeline["properties"]


class TestDFDisabledTaskSkipped:
    @pytest.mark.unit
    def test_disabled_task_returns_none(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("DisabledSQL", TaskType.EXECUTE_SQL, disabled=True)
        result = df_generator._task_to_activity(task, [])
        assert result is None


class TestDFTaskDispatch:
    @pytest.mark.unit
    def test_web_service_dispatched(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("WS", TaskType.WEB_SERVICE)
        act = df_generator._task_to_activity(task, [])
        assert act is not None
        assert act["type"] == "WebActivity"

    @pytest.mark.unit
    def test_xml_dispatched(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("XML", TaskType.XML)
        act = df_generator._task_to_activity(task, [])
        assert act is not None
        assert act["type"] == "Script"

    @pytest.mark.unit
    def test_wmi_dispatched(self, df_generator: DataFactoryGenerator) -> None:
        task = _make_task("WMI", TaskType.WMI_EVENT_WATCHER)
        act = df_generator._task_to_activity(task, [])
        assert act is not None
        assert act["type"] == "Wait"


# =====================================================================
# Spark Generator tests
# =====================================================================


class TestSparkWebServicePlaceholder:
    @pytest.mark.unit
    def test_web_service_code(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("Call API", TaskType.WEB_SERVICE, properties={"Url": "https://api.example.com"})
        code = spark_generator._generate_web_service_placeholder(task)
        assert "requests" in code
        assert "https://api.example.com" in code

    @pytest.mark.unit
    def test_web_service_in_notebook(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("Call API", TaskType.WEB_SERVICE)
        pkg = _make_package(tasks=[task])
        code = spark_generator._generate_notebook_code(pkg, task)
        assert "requests" in code


class TestSparkXmlPlaceholder:
    @pytest.mark.unit
    def test_xml_code(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("Parse XML", TaskType.XML, properties={"OperationType": "Validate"})
        code = spark_generator._generate_xml_task_placeholder(task)
        assert "lxml" in code or "etree" in code
        assert "Validate" in code

    @pytest.mark.unit
    def test_xml_in_notebook(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("Parse XML", TaskType.XML)
        pkg = _make_package(tasks=[task])
        code = spark_generator._generate_notebook_code(pkg, task)
        assert "etree" in code


class TestSparkWmiPlaceholder:
    @pytest.mark.unit
    def test_wmi_code(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task(
            "Watch", TaskType.WMI_EVENT_WATCHER,
            properties={"WqlQuerySource": "SELECT * FROM Win32_Process"},
        )
        code = spark_generator._generate_wmi_placeholder(task)
        assert "Win32_Process" in code

    @pytest.mark.unit
    def test_wmi_data_reader_in_notebook(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("Read WMI", TaskType.WMI_DATA_READER)
        pkg = _make_package(tasks=[task])
        code = spark_generator._generate_notebook_code(pkg, task)
        assert "WMI" in code


class TestSparkHeaderAnnotations:
    @pytest.mark.unit
    def test_header_includes_annotations(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("T1", TaskType.EXECUTE_SQL)
        pkg = _make_package(annotations=["Author: John", "Version: 3"])
        header = spark_generator._generate_header(pkg, task)
        assert "Author: John" in header
        assert "Version: 3" in header

    @pytest.mark.unit
    def test_header_includes_transaction(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("T1", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.REQUIRED)
        pkg = _make_package()
        header = spark_generator._generate_header(pkg, task)
        assert "Required" in header

    @pytest.mark.unit
    def test_header_omits_default_transaction(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("T1", TaskType.EXECUTE_SQL, transaction_option=TransactionOption.SUPPORTED)
        pkg = _make_package()
        header = spark_generator._generate_header(pkg, task)
        assert "Transaction:" not in header

    @pytest.mark.unit
    def test_header_includes_description(self, spark_generator: SparkNotebookGenerator) -> None:
        task = _make_task("T1", TaskType.EXECUTE_SQL)
        pkg = _make_package(description="ETL for sales data")
        header = spark_generator._generate_header(pkg, task)
        assert "ETL for sales data" in header


# =====================================================================
# Migration Engine tests
# =====================================================================


class TestLoggingConfigOutput:
    @pytest.mark.unit
    def test_logging_config_generated(self, config: MigrationConfig, tmp_path: Path) -> None:
        engine = MigrationEngine(config)
        pkg = _make_package(
            logging_providers=[
                LogProvider(name="SQL Log", provider_type="DTS.LogProviderSQLServer", connection_manager_ref="LogDB"),
                LogProvider(name="Text Log", provider_type="DTS.LogProviderTextFile"),
            ]
        )
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        engine._generate_logging_config([pkg], output_dir)

        path = output_dir / "logging_config.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data["logging_providers"]) == 2
        assert data["logging_providers"][0]["name"] == "SQL Log"
        assert data["logging_providers"][0]["connection_manager_ref"] == "LogDB"

    @pytest.mark.unit
    def test_logging_config_deduplicates(self, config: MigrationConfig, tmp_path: Path) -> None:
        engine = MigrationEngine(config)
        lp = LogProvider(name="Shared Log", provider_type="DTS.LogProviderSQLServer")
        pkg1 = _make_package(name="Pkg1", logging_providers=[lp])
        pkg2 = _make_package(name="Pkg2", logging_providers=[lp])
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        engine._generate_logging_config([pkg1, pkg2], output_dir)

        data = json.loads((output_dir / "logging_config.json").read_text(encoding="utf-8"))
        assert len(data["logging_providers"]) == 1

    @pytest.mark.unit
    def test_logging_config_skipped_when_empty(self, config: MigrationConfig, tmp_path: Path) -> None:
        engine = MigrationEngine(config)
        pkg = _make_package()
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        engine._generate_logging_config([pkg], output_dir)

        assert not (output_dir / "logging_config.json").exists()


class TestTaskLevelCheckpoint:
    @pytest.mark.unit
    def test_state_includes_completed_tasks(self, config: MigrationConfig, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import _save_migration_state

        state_dir = tmp_path / ".ssis2fabric"
        state_dir.mkdir()
        state = {
            "TestPkg": {
                "hash": "abc123",
                "migrated_at": "2026-03-14T00:00:00",
                "file_path": "test.dtsx",
                "completed_tasks": ["Task1", "Task2"],
            }
        }
        _save_migration_state(state_dir, state)

        state_file = state_dir / "state.json"
        assert state_file.exists()
        loaded = json.loads(state_file.read_text(encoding="utf-8"))
        assert loaded["TestPkg"]["completed_tasks"] == ["Task1", "Task2"]


class TestCollectTasksHelper:
    @pytest.mark.unit
    def test_flat_tasks(self, df_generator: DataFactoryGenerator) -> None:
        tasks = [_make_task("A"), _make_task("B")]
        result: dict[str, ControlFlowTask] = {}
        df_generator._collect_tasks(tasks, result)
        assert len(result) == 2

    @pytest.mark.unit
    def test_nested_tasks(self, df_generator: DataFactoryGenerator) -> None:
        child = _make_task("Child")
        parent = _make_task("Parent", TaskType.SEQUENCE_CONTAINER)
        parent.child_tasks = [child]
        result: dict[str, ControlFlowTask] = {}
        df_generator._collect_tasks([parent], result)
        assert len(result) == 2
