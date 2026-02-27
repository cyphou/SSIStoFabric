"""
Unit tests for the Data Factory Pipeline Generator.
Validates the structure of generated pipeline JSON.
"""

import json
from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import (
    ControlFlowTask,
    SqlParameterBinding,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.migration_engine import TargetArtifact

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


@pytest.fixture
def generator(default_config: MigrationConfig) -> DataFactoryGenerator:
    return DataFactoryGenerator(default_config)


@pytest.fixture
def simple_package() -> SSISPackage:
    parser = DTSXParser()
    return parser.parse(FIXTURES_DIR / "simple_etl.dtsx")


class TestDataFactoryGenerator:
    """Tests for Data Factory pipeline generation."""

    @pytest.mark.unit
    def test_generate_full_pipeline(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        output = generator.generate(simple_package, None, tmp_path)
        assert output.exists()
        assert output.suffix == ".json"

    @pytest.mark.unit
    def test_pipeline_has_activities(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        assert len(activities) >= 1

    @pytest.mark.unit
    def test_execute_sql_becomes_script_activity(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        truncate = next((a for a in activities if "Truncate" in a.get("name", "")), None)
        assert truncate is not None
        assert truncate["type"] == "Script"
        # TRUNCATE is NonQuery
        assert truncate["typeProperties"]["scripts"][0]["type"] == "NonQuery"
        # Connection must be in externalReferences at the activity level
        assert "externalReferences" in truncate
        assert "connection" in truncate["externalReferences"]
        # No legacy connection inside typeProperties
        assert "connection" not in truncate["typeProperties"]

    @pytest.mark.unit
    def test_stored_proc_becomes_script_nonquery(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """EXEC statements use Script activity with NonQuery type (Fabric-native)."""
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        audit = next((a for a in activities if "Audit" in a.get("name", "")), None)
        assert audit is not None
        # In Fabric, stored procs use Script activity, not SqlServerStoredProcedure
        assert audit["type"] == "Script"
        assert audit["typeProperties"]["scripts"][0]["type"] == "NonQuery"
        # The full EXEC statement is preserved in the script text
        assert "EXEC" in audit["typeProperties"]["scripts"][0]["text"]

    @pytest.mark.unit
    def test_sql_type_classification(self, generator: DataFactoryGenerator) -> None:
        """Query vs NonQuery classification for SQL statements."""
        assert generator._classify_sql_type("SELECT * FROM t") == "Query"
        assert generator._classify_sql_type("  SELECT 1") == "Query"
        assert generator._classify_sql_type("INSERT INTO t VALUES (1)") == "NonQuery"
        assert generator._classify_sql_type("UPDATE t SET x=1") == "NonQuery"
        assert generator._classify_sql_type("DELETE FROM t") == "NonQuery"
        assert generator._classify_sql_type("EXEC sp_foo") == "NonQuery"
        assert generator._classify_sql_type("TRUNCATE TABLE t") == "NonQuery"
        assert generator._classify_sql_type("CREATE TABLE t (id int)") == "NonQuery"
        assert generator._classify_sql_type("MERGE INTO t") == "NonQuery"
        assert generator._classify_sql_type("-- comment\nSELECT 1") == "NonQuery"
        assert generator._classify_sql_type("/* block */ SELECT 1") == "Query"

    @pytest.mark.unit
    def test_sql_parameters_single_binding(self, generator: DataFactoryGenerator) -> None:
        """SQL task with one ? placeholder should emit named parameter and expression."""
        task = ControlFlowTask(
            name="Delete By LoadExecId",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="DELETE FROM [Staging].[StgCustomer] WHERE [LoadExecutionId] = ?",
            sql_parameters=[
                SqlParameterBinding(
                    parameter_name="0",
                    variable_name="$Package::LoadExecutionId",
                    direction="Input",
                    data_type=3,  # Int32
                ),
            ],
        )
        activity = generator._task_to_activity(task, [])
        assert activity is not None
        script = activity["typeProperties"]["scripts"][0]
        # ? should be replaced with @p0
        assert "?" not in script["text"]
        assert "@p0" in script["text"]
        # parameters array should exist
        assert "parameters" in script
        params = script["parameters"]
        assert len(params) == 1
        assert params[0]["name"] == "p0"
        assert params[0]["type"] == "Int32"
        assert params[0]["direction"] == "Input"
        assert params[0]["value"]["value"] == "@pipeline().parameters.LoadExecutionId"
        assert params[0]["value"]["type"] == "Expression"

    @pytest.mark.unit
    def test_sql_parameters_multiple_bindings(self, generator: DataFactoryGenerator) -> None:
        """SQL task with multiple ? placeholders should emit ordered named parameters."""
        task = ControlFlowTask(
            name="Open Load Execution",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="EXEC [SystemLog].[OpenLoadExecution] ?,?,? OUTPUT",
            sql_parameters=[
                SqlParameterBinding(
                    parameter_name="0",
                    variable_name="System::ServerExecutionID",
                    direction="Input",
                    data_type=3,
                ),
                SqlParameterBinding(
                    parameter_name="1",
                    variable_name="$Project::LoadId",
                    direction="Input",
                    data_type=3,
                ),
                SqlParameterBinding(
                    parameter_name="2",
                    variable_name="SystemLog::LoadExecutionId",
                    direction="Output",
                    data_type=3,
                ),
            ],
        )
        activity = generator._task_to_activity(task, [])
        script = activity["typeProperties"]["scripts"][0]
        # Only Input params replace ? placeholders
        assert "@p0" in script["text"]
        assert "@p1" in script["text"]
        # Output param should NOT replace a ? (only Input params are used)
        params = script["parameters"]
        assert len(params) == 2
        assert params[0]["value"]["value"] == "@pipeline().parameters.ServerExecutionID"
        assert params[1]["value"]["value"] == "@pipeline().parameters.LoadId"

    @pytest.mark.unit
    def test_sql_parameters_no_bindings(self, generator: DataFactoryGenerator) -> None:
        """SQL task without parameter bindings should have plain text, no params array."""
        task = ControlFlowTask(
            name="Truncate Table",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="TRUNCATE TABLE [Staging].[StgCustomer]",
        )
        activity = generator._task_to_activity(task, [])
        script = activity["typeProperties"]["scripts"][0]
        assert script["text"] == "TRUNCATE TABLE [Staging].[StgCustomer]"
        assert "parameters" not in script

    @pytest.mark.unit
    def test_ssis_variable_to_pipeline_param(self, generator: DataFactoryGenerator) -> None:
        """SSIS variable names should be mapped to pipeline parameter names."""
        assert generator._ssis_variable_to_pipeline_param("$Package::LoadExecutionId") == "LoadExecutionId"
        assert generator._ssis_variable_to_pipeline_param("$Project::LoadId") == "LoadId"
        assert generator._ssis_variable_to_pipeline_param("System::ServerExecutionID") == "ServerExecutionID"
        assert generator._ssis_variable_to_pipeline_param("User::MyVar") == "MyVar"
        assert generator._ssis_variable_to_pipeline_param("SystemLog::LoadExecutionId") == "LoadExecutionId"

    @pytest.mark.unit
    def test_oledb_type_mapping(self, generator: DataFactoryGenerator) -> None:
        """OLE DB data type codes should map to Fabric Script parameter types."""
        assert generator._oledb_type_to_fabric(3) == "Int32"
        assert generator._oledb_type_to_fabric(8) == "String"
        assert generator._oledb_type_to_fabric(20) == "Int64"
        assert generator._oledb_type_to_fabric(11) == "Boolean"
        assert generator._oledb_type_to_fabric(999) == "String"  # unknown defaults to String

    @pytest.mark.unit
    def test_script_activity_has_timeout(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Script activities should include a scriptBlockExecutionTimeout."""
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        script_acts = [a for a in activities if a["type"] == "Script"]
        for act in script_acts:
            assert "scriptBlockExecutionTimeout" in act["typeProperties"]

    @pytest.mark.unit
    def test_data_flow_becomes_copy_activity(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        copy_act = next((a for a in activities if a.get("type") == "Copy"), None)
        assert copy_act is not None
        assert "source" in copy_act["typeProperties"]
        assert "sink" in copy_act["typeProperties"]

    @pytest.mark.unit
    def test_activity_dependencies(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Activities should have dependency chains matching SSIS precedence."""
        output = generator.generate(simple_package, None, tmp_path)
        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # Second activity should depend on first
        if len(activities) >= 2:
            assert "dependsOn" in activities[1]

    @pytest.mark.unit
    def test_sanitize_name(self, generator: DataFactoryGenerator) -> None:
        assert generator._sanitize_name("My Package (v2)") == "My_Package_v2"
        assert generator._sanitize_name("hello--world") == "hello_world"
        assert generator._sanitize_name("  spaces  ") == "spaces"

    @pytest.mark.unit
    def test_generate_task_specific_pipeline(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Can generate a pipeline for a specific task only."""
        task = simple_package.control_flow_tasks[0]
        output = generator.generate(simple_package, task, tmp_path)
        assert output.exists()

        with open(output) as f:
            pipeline = json.load(f)
        assert len(pipeline["properties"]["activities"]) >= 1


class TestConsolidatedPipeline:
    """Tests for the generate_package_pipeline method (one pipeline per package)."""

    @pytest.fixture
    def complex_package(self) -> SSISPackage:
        parser = DTSXParser()
        return parser.parse(FIXTURES_DIR / "complex_etl.dtsx")

    @pytest.mark.unit
    def test_generates_single_pipeline(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Should produce exactly one pipeline file for the package."""
        task_routing = {
            "Truncate Staging Table": TargetArtifact.DATA_FACTORY_PIPELINE,
            "Load Customers": TargetArtifact.DATAFLOW_GEN2,
            "Update Audit Log": TargetArtifact.DATA_FACTORY_PIPELINE,
        }
        output = generator.generate_package_pipeline(simple_package, task_routing, tmp_path)
        assert output.exists()

        with open(output) as f:
            pipeline = json.load(f)
        assert "name" in pipeline
        assert "properties" in pipeline

    @pytest.mark.unit
    def test_all_tasks_as_activities(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """All tasks should become inline activities within one pipeline."""
        task_routing = {
            "Truncate Staging Table": TargetArtifact.DATA_FACTORY_PIPELINE,
            "Load Customers": TargetArtifact.DATAFLOW_GEN2,
            "Update Audit Log": TargetArtifact.DATA_FACTORY_PIPELINE,
        }
        output = generator.generate_package_pipeline(simple_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        assert len(activities) == 3

    @pytest.mark.unit
    def test_dataflow_gen2_becomes_dataflow_activity(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Tasks routed to DATAFLOW_GEN2 should become Dataflow activities."""
        task_routing = {
            "Truncate Staging Table": TargetArtifact.DATA_FACTORY_PIPELINE,
            "Load Customers": TargetArtifact.DATAFLOW_GEN2,
            "Update Audit Log": TargetArtifact.DATA_FACTORY_PIPELINE,
        }
        output = generator.generate_package_pipeline(simple_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        dataflow_acts = [a for a in activities if a["type"] == "Dataflow"]
        assert len(dataflow_acts) == 1
        assert "dataflowId" in dataflow_acts[0]["typeProperties"]

    @pytest.mark.unit
    def test_spark_notebook_becomes_trident_notebook(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Tasks routed to SPARK_NOTEBOOK should become TridentNotebook activities."""
        task_routing = {
            "Truncate Staging Table": TargetArtifact.DATA_FACTORY_PIPELINE,
            "Load Customers": TargetArtifact.SPARK_NOTEBOOK,
            "Update Audit Log": TargetArtifact.DATA_FACTORY_PIPELINE,
        }
        output = generator.generate_package_pipeline(simple_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        notebook_acts = [a for a in activities if a["type"] == "TridentNotebook"]
        assert len(notebook_acts) == 1
        assert "notebookId" in notebook_acts[0]["typeProperties"]

    @pytest.mark.unit
    def test_dependency_chain(
        self, generator: DataFactoryGenerator, simple_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Activities should have sequential dependsOn chains."""
        task_routing = {
            "Truncate Staging Table": TargetArtifact.DATA_FACTORY_PIPELINE,
            "Load Customers": TargetArtifact.DATAFLOW_GEN2,
            "Update Audit Log": TargetArtifact.DATA_FACTORY_PIPELINE,
        }
        output = generator.generate_package_pipeline(simple_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # First activity has no dependencies
        assert "dependsOn" not in activities[0]
        # Second depends on first
        assert activities[1]["dependsOn"][0]["activity"] == activities[0]["name"]
        # Third depends on second
        assert activities[2]["dependsOn"][0]["activity"] == activities[1]["name"]

    @pytest.mark.unit
    def test_sequence_container_flattened(
        self, generator: DataFactoryGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Sequence containers should be flattened — children become top-level activities."""
        # Route everything to DF pipeline for simplicity
        task_routing: dict[str, TargetArtifact] = {}
        for task in complex_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(complex_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # "Initialize" container should be flattened, its child "Set Audit Start"
        # should appear as a top-level activity
        names = [a["name"] for a in activities]
        assert any("Set_Audit_Start" in n or "Audit" in n for n in names), (
            f"Expected flattened child from Sequence Container, got: {names}"
        )
        # No activity should be named "Initialize" (the container itself)
        assert not any(n == "Initialize" for n in names), (
            f"Sequence container should be flattened, not appear as an activity: {names}"
        )


class TestExecuteProcessConversion:
    """Tests for Execute Process → ExecutePipeline conversion."""

    @pytest.mark.unit
    def test_execute_process_becomes_execute_pipeline(self, generator: DataFactoryGenerator) -> None:
        """Execute Process tasks should become ExecutePipeline activities."""
        task = ControlFlowTask(
            name="Run Data Quality Checks",
            task_type=TaskType.EXECUTE_PROCESS,
            description="Run external DQ tool: C:\\Tools\\dq_check.exe --batch",
        )
        activity = generator._task_to_activity(task, [])
        assert activity is not None
        assert activity["type"] == "ExecutePipeline"
        assert activity["typeProperties"]["pipeline"]["type"] == "PipelineReference"
        assert "proc_" in activity["typeProperties"]["pipeline"]["referenceName"]
        assert "TODO" in activity["description"]
        assert "Execute Process" in activity["description"]

    @pytest.mark.unit
    def test_execute_process_routed_to_pipeline(self, generator: DataFactoryGenerator) -> None:
        """Execute Process routing should target DATA_FACTORY_PIPELINE."""
        task = ControlFlowTask(
            name="Run External Tool",
            task_type=TaskType.EXECUTE_PROCESS,
        )
        activity = generator._convert_task_for_package(task, [], {task.name: TargetArtifact.DATA_FACTORY_PIPELINE})
        assert activity is not None
        assert activity["type"] == "ExecutePipeline"


class TestSendMailConversion:
    """Tests for Send Mail → Office365Outlook conversion."""

    @pytest.mark.unit
    def test_send_mail_becomes_outlook_activity(self, generator: DataFactoryGenerator) -> None:
        """Send Mail tasks should become Office365Outlook activities."""
        task = ControlFlowTask(
            name="Send Success Notification",
            task_type=TaskType.SEND_MAIL,
            description="Send batch completion email",
            properties={
                "ToLine": "data-team@company.com",
                "Subject": "ETL Batch Complete",
                "MessageSource": "The nightly ETL has completed.",
            },
        )
        activity = generator._task_to_activity(task, [])
        assert activity is not None
        assert activity["type"] == "Office365Outlook"
        assert activity["typeProperties"]["method"] == "sendmail"
        assert activity["typeProperties"]["to"] == "data-team@company.com"
        assert activity["typeProperties"]["subject"] == "ETL Batch Complete"
        assert activity["typeProperties"]["body"] == "The nightly ETL has completed."

    @pytest.mark.unit
    def test_send_mail_with_cc(self, generator: DataFactoryGenerator) -> None:
        """CC line should be included when present."""
        task = ControlFlowTask(
            name="Send Alert",
            task_type=TaskType.SEND_MAIL,
            properties={
                "ToLine": "oncall@company.com",
                "Subject": "Alert",
                "MessageSource": "Error occurred",
                "CCLine": "manager@company.com",
            },
        )
        activity = generator._task_to_activity(task, [])
        assert activity["typeProperties"]["cc"] == "manager@company.com"

    @pytest.mark.unit
    def test_send_mail_defaults_when_no_properties(self, generator: DataFactoryGenerator) -> None:
        """Send Mail with no parsed properties uses sensible defaults."""
        task = ControlFlowTask(
            name="Send Email",
            task_type=TaskType.SEND_MAIL,
            description="Notification email",
        )
        activity = generator._task_to_activity(task, [])
        assert activity["type"] == "Office365Outlook"
        assert "TODO" in activity["typeProperties"]["to"]
        assert "cc" not in activity["typeProperties"]

    @pytest.mark.unit
    def test_send_mail_parsed_from_dtsx(self) -> None:
        """Parser should extract Send Mail properties from SSIS packages."""
        parser = DTSXParser()
        pkg = parser.parse(
            Path(__file__).parent.parent
            / "fixtures"
            / "sample_packages"
            / ".."
            / ".."
            / ".."
            / "examples"
            / "12_parent_child_packages"
            / "master_orchestrator.dtsx"
        )
        # Find the Send Mail task
        mail_tasks = [t for t in pkg.control_flow_tasks if t.task_type == TaskType.SEND_MAIL]
        assert len(mail_tasks) >= 1
        mail_task = mail_tasks[0]
        assert mail_task.properties.get("ToLine") == "data-team@company.com"
        assert "Subject" in mail_task.properties
        assert "MessageSource" in mail_task.properties


class TestDeployerConnectionInjection:
    """Tests for the deployer's connection injection and SP→Script conversion.

    ``_update_pipeline_definition`` reads the JSON from *json_path*, applies
    transforms in a local copy, then sends the result as a base64 payload via
    ``_api_call``.  To verify the in-memory transforms we set ``dry_run=False``,
    mock ``_api_call`` to return HTTP 200, and decode the base64 payload from
    the captured call.
    """

    @staticmethod
    def _make_pipeline(activities: list[dict]) -> dict:
        return {"name": "test", "properties": {"activities": activities}}

    @staticmethod
    def _make_deployer(connection_id: str | None = None, connection_map: dict | None = None):
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        deployer = FabricDeployer.__new__(FabricDeployer)
        deployer.default_connection_id = connection_id
        deployer._connection_map = connection_map or {}
        deployer._existing_items = {"DataPipeline::test": {"id": "fake-id"}}
        deployer.workspace_id = "fake-ws"
        deployer.dry_run = False  # so _api_call is invoked (we mock it)
        deployer.skip_existing = False
        return deployer

    @staticmethod
    def _decode_payload(mock_api_call) -> dict:
        """Extract and decode the pipeline JSON from the mocked _api_call."""
        import base64

        _args, kwargs = mock_api_call.call_args
        body = kwargs.get("json_body") or _args[2]
        b64 = body["definition"]["parts"][0]["payload"]
        return json.loads(base64.b64decode(b64).decode("utf-8"))

    # ------------------------------------------------------------------
    # _has_script_activity  (static – no mocking needed)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_has_script_activity_detects_sp(self) -> None:
        """_has_script_activity detects both Script and SqlServerStoredProcedure."""
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        assert FabricDeployer._has_script_activity(
            self._make_pipeline([{"name": "a", "type": "Script"}])
        ) is True
        assert FabricDeployer._has_script_activity(
            self._make_pipeline([{"name": "b", "type": "SqlServerStoredProcedure"}])
        ) is True
        assert FabricDeployer._has_script_activity(
            self._make_pipeline([{"name": "c", "type": "Wait"}])
        ) is False

    # ------------------------------------------------------------------
    # SP → Script conversion
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_sp_activity_converted_to_script(self, tmp_path: Path) -> None:
        """SqlServerStoredProcedure is converted to Script with EXEC text."""
        from unittest.mock import MagicMock

        pipeline = self._make_pipeline([
            {
                "name": "Run_SP",
                "type": "SqlServerStoredProcedure",
                "typeProperties": {"storedProcedureName": "dbo.sp_MyProc"},
            }
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = self._make_deployer("aaaa-bbbb-cccc")
        mock_resp = MagicMock(status_code=200)
        deployer._api_call = MagicMock(return_value=mock_resp)

        result = deployer._update_pipeline_definition("test", json_path)
        assert result.status == "success"

        transformed = self._decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert act["type"] == "Script"
        assert act["typeProperties"]["scripts"][0]["text"] == "EXEC dbo.sp_MyProc"
        assert act["typeProperties"]["scripts"][0]["type"] == "NonQuery"
        # Connection must be in externalReferences (Fabric-native format)
        assert act["externalReferences"]["connection"] == "aaaa-bbbb-cccc"
        assert "connection" not in act["typeProperties"]

    # ------------------------------------------------------------------
    # All three connection-injection scenarios in one pipeline
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_connection_injection_scenarios(self, tmp_path: Path) -> None:
        """TODO placeholder, missing connection, and SP all get the default ID."""
        from unittest.mock import MagicMock

        pipeline = self._make_pipeline([
            {
                "name": "With_TODO",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "TRUNCATE TABLE foo"}],
                    "connection": {
                        "referenceName": "cmgr_DW  -- TODO: replace with Fabric connection id"
                    },
                },
            },
            {
                "name": "No_Connection",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "DELETE FROM bar"}],
                },
            },
            {
                "name": "SP_Activity",
                "type": "SqlServerStoredProcedure",
                "typeProperties": {"storedProcedureName": "dbo.sp_Test"},
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = self._make_deployer("aaaa-bbbb-cccc")
        mock_resp = MagicMock(status_code=200)
        deployer._api_call = MagicMock(return_value=mock_resp)

        result = deployer._update_pipeline_definition("test", json_path)
        assert result.status == "success"

        transformed = self._decode_payload(deployer._api_call)
        acts = transformed["properties"]["activities"]

        # Scenario a: TODO placeholder → replaced (legacy format migrated)
        assert acts[0]["externalReferences"]["connection"] == "aaaa-bbbb-cccc"
        assert "connection" not in acts[0]["typeProperties"]
        # Scenario b: No connection → injected
        assert acts[1]["externalReferences"]["connection"] == "aaaa-bbbb-cccc"
        # Scenario c: SP → Script + connection injected
        assert acts[2]["type"] == "Script"
        assert acts[2]["typeProperties"]["scripts"][0]["text"] == "EXEC dbo.sp_Test"
        assert acts[2]["externalReferences"]["connection"] == "aaaa-bbbb-cccc"
        assert "connection" not in acts[2]["typeProperties"]

    # ------------------------------------------------------------------
    # No default_connection_id — TODO block stripped, no injection
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_no_connection_id_strips_todo(self, tmp_path: Path) -> None:
        """Without a default_connection_id, TODO placeholders are stripped."""
        from unittest.mock import MagicMock

        pipeline = self._make_pipeline([
            {
                "name": "Run_SQL",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "SELECT 1"}],
                    "connection": {
                        "referenceName": "cmgr_DW  -- TODO: replace with Fabric connection id"
                    },
                },
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = self._make_deployer(connection_id=None)
        mock_resp = MagicMock(status_code=200)
        deployer._api_call = MagicMock(return_value=mock_resp)

        result = deployer._update_pipeline_definition("test", json_path)
        assert result.status == "success"

        transformed = self._decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        # Legacy connection block removed and no externalReferences injected
        assert "connection" not in act["typeProperties"]
        assert "externalReferences" not in act

    # ------------------------------------------------------------------
    # Connection map resolves per-activity by SSIS name
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_connection_map_resolves_by_ssis_name(self, tmp_path: Path) -> None:
        """Connection map entries are used instead of default_connection_id."""
        from unittest.mock import MagicMock

        pipeline = self._make_pipeline([
            {
                "name": "Script_A",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "SELECT 1"}],
                },
                "externalReferences": {
                    "connection": "cmgr_DW  -- TODO: replace with Fabric connection id",
                },
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = self._make_deployer(
            connection_id=None,
            connection_map={"cmgr_DW": "map-conn-1111"},
        )
        mock_resp = MagicMock(status_code=200)
        deployer._api_call = MagicMock(return_value=mock_resp)

        result = deployer._update_pipeline_definition("test", json_path)
        assert result.status == "success"

        transformed = self._decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert act["externalReferences"]["connection"] == "map-conn-1111"

    # ------------------------------------------------------------------
    # Connection map fallback: any entry when SSIS name not found
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_connection_map_fallback_any(self, tmp_path: Path) -> None:
        """When SSIS name is missing, the first map entry is used."""
        from unittest.mock import MagicMock

        pipeline = self._make_pipeline([
            {
                "name": "Script_B",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "TRUNCATE TABLE x"}],
                },
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = self._make_deployer(
            connection_id=None,
            connection_map={"some_conn": "fallback-conn-2222"},
        )
        mock_resp = MagicMock(status_code=200)
        deployer._api_call = MagicMock(return_value=mock_resp)

        result = deployer._update_pipeline_definition("test", json_path)
        assert result.status == "success"

        transformed = self._decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert act["externalReferences"]["connection"] == "fallback-conn-2222"


class TestConnectionManifestGeneration:
    """Tests for connection manifest generation in the migration engine."""

    @pytest.mark.unit
    def test_generates_connection_manifests(self, tmp_path: Path) -> None:
        """Connection manifests are written to connections/ folder."""
        from ssis_to_fabric.analyzer.models import ConnectionManager, ConnectionType, SSISPackage
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            connection_managers=[
                ConnectionManager(
                    id="guid-1",
                    name="cmgr_DW",
                    connection_type=ConnectionType.OLEDB,
                    server="myserver",
                    database="mydb",
                    provider="SQLNCLI11.1",
                ),
                ConnectionManager(
                    id="guid-2",
                    name="cmgr_Flat",
                    connection_type=ConnectionType.FLAT_FILE,
                    server="",
                    database="",
                    provider="",
                ),
            ],
        )

        config = MigrationConfig(project_name="test", output_dir=tmp_path)
        engine = MigrationEngine(config)
        engine._generate_connection_manifests([pkg], tmp_path)

        conn_dir = tmp_path / "connections"
        assert conn_dir.exists()
        files = sorted(f.name for f in conn_dir.glob("*.json"))
        assert "cmgr_DW.json" in files
        assert "cmgr_Flat.json" in files

        dw = json.loads((conn_dir / "cmgr_DW.json").read_text())
        assert dw["ssis_name"] == "cmgr_DW"
        assert dw["fabric_target_type"] == "Warehouse"
        assert dw["server"] == "myserver"
        assert dw["database"] == "mydb"

        flat = json.loads((conn_dir / "cmgr_Flat.json").read_text())
        assert flat["fabric_target_type"] == "Lakehouse"

    @pytest.mark.unit
    def test_deduplicates_connections_across_packages(self, tmp_path: Path) -> None:
        """Same connection name across packages produces one manifest."""
        from ssis_to_fabric.analyzer.models import ConnectionManager, ConnectionType, SSISPackage
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        cm = ConnectionManager(
            id="guid-1", name="cmgr_DW", connection_type=ConnectionType.OLEDB,
        )
        pkg1 = SSISPackage(name="Pkg1", file_path="p1.dtsx", connection_managers=[cm])
        pkg2 = SSISPackage(name="Pkg2", file_path="p2.dtsx", connection_managers=[cm])

        config = MigrationConfig(project_name="test", output_dir=tmp_path)
        engine = MigrationEngine(config)
        engine._generate_connection_manifests([pkg1, pkg2], tmp_path)

        conn_dir = tmp_path / "connections"
        files = list(conn_dir.glob("*.json"))
        assert len(files) == 1


class TestConnectionResolution:
    """Tests for Phase 0 connection auto-resolution in the deployer."""

    @staticmethod
    def _make_deployer(connection_id: str | None = None, *, dry_run: bool = False):
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        deployer = FabricDeployer.__new__(FabricDeployer)
        deployer.workspace_id = "fake-ws"
        deployer.default_connection_id = connection_id
        deployer._connection_map = {}
        deployer._token = "fake-token"
        deployer._credential = None
        deployer._existing_items = None
        deployer.dry_run = dry_run
        deployer.skip_existing = True
        return deployer

    @pytest.mark.unit
    def test_override_maps_all_connections(self, tmp_path: Path) -> None:
        """When --connection-id is set, all SSIS connections map to it."""
        conn_dir = tmp_path / "connections"
        conn_dir.mkdir()
        (conn_dir / "cmgr_DW.json").write_text(json.dumps({
            "ssis_name": "cmgr_DW", "ssis_type": "OLEDB", "fabric_target_type": "Warehouse",
        }))
        (conn_dir / "cmgr_Staging.json").write_text(json.dumps({
            "ssis_name": "cmgr_Staging", "ssis_type": "OLEDB", "fabric_target_type": "Warehouse",
        }))

        deployer = self._make_deployer("override-guid")
        deployer._resolve_connections(tmp_path)

        assert deployer._connection_map == {
            "cmgr_DW": "override-guid",
            "cmgr_Staging": "override-guid",
        }

    @pytest.mark.unit
    def test_no_connections_dir_is_noop(self, tmp_path: Path) -> None:
        """Missing connections/ folder is handled gracefully."""
        deployer = self._make_deployer()
        deployer._resolve_connections(tmp_path)
        assert deployer._connection_map == {}

    @pytest.mark.unit
    def test_auto_discover_warehouse(self, tmp_path: Path) -> None:
        """Auto-discover finds existing connections matched by display name."""
        from unittest.mock import MagicMock

        conn_dir = tmp_path / "connections"
        conn_dir.mkdir()
        (conn_dir / "cmgr_DW.json").write_text(json.dumps({
            "ssis_name": "cmgr_DW", "ssis_type": "OLEDB",
            "server": "myserver.database.windows.net", "database": "MyDB",
        }))

        deployer = self._make_deployer()

        # Mock the API to return a connection whose displayName matches SSIS name
        def fake_api(method, url, **kwargs):
            resp = MagicMock()
            if "/connections" in url and "workspaces" not in url:
                resp.status_code = 200
                resp.json.return_value = {
                    "value": [
                        {
                            "id": "discovered-conn-id",
                            "displayName": "cmgr_DW",
                            "connectivityType": "ShareableCloud",
                            "connectionDetails": {"type": "SQL"},
                        }
                    ]
                }
            else:
                resp.status_code = 404
            return resp

        deployer._api_call = fake_api
        deployer._resolve_connections(tmp_path)

        assert deployer._connection_map == {"cmgr_DW": "discovered-conn-id"}

    @pytest.mark.unit
    def test_auto_create_when_no_existing(self, tmp_path: Path) -> None:
        """Auto-create SQL connection from SSIS manifest server/database."""
        from unittest.mock import MagicMock

        conn_dir = tmp_path / "connections"
        conn_dir.mkdir()
        (conn_dir / "cmgr_DW.json").write_text(json.dumps({
            "ssis_name": "cmgr_DW", "ssis_type": "OLEDB",
            "server": "myserver.database.windows.net", "database": "MyDB",
        }))

        deployer = self._make_deployer()
        call_log: list[tuple] = []

        def fake_api(method, url, **kwargs):
            call_log.append((method, url))
            resp = MagicMock()
            if method == "GET" and "/connections" in url and "workspaces" not in url:
                # No existing connections
                resp.status_code = 200
                resp.json.return_value = {"value": []}
            elif method == "POST" and "/connections" in url:
                # Create connection succeeds
                resp.status_code = 201
                resp.json.return_value = {"id": "new-conn-id"}
            else:
                resp.status_code = 404
            return resp

        deployer._api_call = fake_api
        deployer._resolve_connections(tmp_path)

        assert deployer._connection_map == {"cmgr_DW": "new-conn-id"}
        # Verify the create call was made
        create_calls = [(m, u) for m, u in call_log if m == "POST" and "/connections" in u]
        assert len(create_calls) == 1


class TestFolderOrganization:
    """Tests for the Phase 4 folder organization feature."""

    @staticmethod
    def _make_deployer():
        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        deployer = FabricDeployer.__new__(FabricDeployer)
        deployer.workspace_id = "fake-ws"
        deployer.dry_run = False
        deployer.skip_existing = True
        deployer.default_connection_id = None
        deployer._connection_map = {}
        deployer._folder_map = {}
        deployer._folder_names_sorted = []
        deployer._existing_items = None
        deployer._token = "fake-token"
        deployer._credential = None
        return deployer

    @pytest.mark.unit
    def test_list_folders(self) -> None:
        """_list_folders returns a name→id mapping from the API response."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()

        def fake_api(method, url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "value": [
                    {"displayName": "FolderA", "id": "aaa"},
                    {"displayName": "FolderB", "id": "bbb"},
                ]
            }
            return resp

        deployer._api_call = fake_api
        result = deployer._list_folders()
        assert result == {"FolderA": "aaa", "FolderB": "bbb"}

    @pytest.mark.unit
    def test_ensure_folder_reuses_existing(self) -> None:
        """_ensure_folder returns the existing folder ID without API call."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()
        existing = {"MyFolder": "existing-id"}

        # Should NOT call the API
        deployer._api_call = MagicMock(side_effect=AssertionError("should not be called"))
        result = deployer._ensure_folder("MyFolder", existing)
        assert result == "existing-id"

    @pytest.mark.unit
    def test_ensure_folder_creates_new(self) -> None:
        """_ensure_folder creates a folder when it doesn't exist."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()
        existing: dict[str, str] = {}

        def fake_api(method, url, **kwargs):
            resp = MagicMock()
            resp.status_code = 201
            resp.json.return_value = {"id": "new-folder-id", "displayName": "NewFolder"}
            return resp

        deployer._api_call = fake_api
        result = deployer._ensure_folder("NewFolder", existing)
        assert result == "new-folder-id"
        assert existing["NewFolder"] == "new-folder-id"

    @pytest.mark.unit
    def test_move_item_to_folder(self) -> None:
        """_move_item_to_folder calls the move API correctly."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()
        call_log: list[tuple] = []

        def fake_api(method, url, **kwargs):
            call_log.append((method, url, kwargs.get("json_body")))
            resp = MagicMock()
            resp.status_code = 200
            return resp

        deployer._api_call = fake_api
        result = deployer._move_item_to_folder("item-123", "folder-456", "MyPipeline")
        assert result is True
        assert len(call_log) == 1
        assert call_log[0][0] == "POST"
        assert "item-123/move" in call_log[0][1]
        assert call_log[0][2] == {"targetFolderId": "folder-456"}

    @pytest.mark.unit
    def test_organize_into_folders_full_flow(self, tmp_path: Path) -> None:
        """End-to-end: creates folders and moves pipelines + notebooks."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()

        # Create output structure with 2 pipelines and 2 notebooks
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "StgAddress.json").write_text("{}", encoding="utf-8")
        (pipelines_dir / "StgProduct.json").write_text("{}", encoding="utf-8")

        call_log: list[tuple] = []

        def fake_api(method, url, **kwargs):
            call_log.append((method, url, kwargs.get("json_body")))
            resp = MagicMock()

            if method == "GET" and "/items" in url:
                resp.status_code = 200
                resp.json.return_value = {
                    "value": [
                        {"id": "pl-1", "type": "DataPipeline", "displayName": "StgAddress"},
                        {"id": "pl-2", "type": "DataPipeline", "displayName": "StgProduct"},
                        {"id": "nb-1", "type": "Notebook", "displayName": "StgAddress_dft_Staging_StgAddress"},
                        {"id": "nb-2", "type": "Notebook", "displayName": "StgProduct_dft_Staging_StgProducts"},
                    ]
                }
            elif method == "GET" and "/folders" in url:
                resp.status_code = 200
                resp.json.return_value = {"value": []}
            elif method == "POST" and "/folders" in url:
                folder_name = kwargs.get("json_body", {}).get("displayName", "unknown")
                folder_id = f"folder-{folder_name}"
                resp.status_code = 201
                resp.json.return_value = {"id": folder_id, "displayName": folder_name}
            elif method == "POST" and "/move" in url:
                resp.status_code = 200
            else:
                resp.status_code = 404

            return resp

        deployer._api_call = fake_api
        deployer._organize_into_folders(tmp_path)

        # Verify folders were created
        folder_creates = [c for c in call_log if c[0] == "POST" and "/folders" in c[1] and "/move" not in c[1]]
        assert len(folder_creates) == 2
        folder_names = {c[2]["displayName"] for c in folder_creates}
        assert folder_names == {"StgAddress", "StgProduct"}

        # Verify items were moved (2 pipelines + 2 notebooks = 4 moves)
        moves = [c for c in call_log if c[0] == "POST" and "/move" in c[1]]
        assert len(moves) == 4

    @pytest.mark.unit
    def test_organize_skips_when_no_pipelines_dir(self, tmp_path: Path) -> None:
        """No crash when pipelines directory doesn't exist."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()
        deployer._api_call = MagicMock(side_effect=AssertionError("should not be called"))
        deployer._organize_into_folders(tmp_path)  # should return silently

    @pytest.mark.unit
    def test_organize_longest_prefix_match(self, tmp_path: Path) -> None:
        """Notebook 'StgProductCategory_x' matches 'StgProductCategory', not 'StgProduct'."""
        from unittest.mock import MagicMock

        deployer = self._make_deployer()

        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "StgProduct.json").write_text("{}", encoding="utf-8")
        (pipelines_dir / "StgProductCategory.json").write_text("{}", encoding="utf-8")

        move_targets: dict[str, str] = {}  # item_id → folder_id

        def fake_api(method, url, **kwargs):
            resp = MagicMock()

            if method == "GET" and "/items" in url:
                resp.status_code = 200
                resp.json.return_value = {
                    "value": [
                        {"id": "pl-1", "type": "DataPipeline", "displayName": "StgProduct"},
                        {"id": "pl-2", "type": "DataPipeline", "displayName": "StgProductCategory"},
                        {"id": "nb-1", "type": "Notebook", "displayName": "StgProduct_dft_StgProducts"},
                        {"id": "nb-2", "type": "Notebook", "displayName": "StgProductCategory_dft_StgProductCategory"},
                    ]
                }
            elif method == "GET" and "/folders" in url:
                resp.status_code = 200
                resp.json.return_value = {"value": []}
            elif method == "POST" and "/folders" in url:
                name = kwargs.get("json_body", {}).get("displayName", "")
                resp.status_code = 201
                resp.json.return_value = {"id": f"folder-{name}", "displayName": name}
            elif method == "POST" and "/move" in url:
                # Extract item_id from URL: .../items/{item_id}/move
                item_id = url.split("/items/")[1].split("/move")[0]
                folder_id = kwargs.get("json_body", {}).get("targetFolderId", "")
                move_targets[item_id] = folder_id
                resp.status_code = 200
            else:
                resp.status_code = 404

            return resp

        deployer._api_call = fake_api
        deployer._organize_into_folders(tmp_path)

        # nb-1 (StgProduct_dft_StgProducts) → folder-StgProduct
        assert move_targets.get("nb-1") == "folder-StgProduct"
        # nb-2 (StgProductCategory_dft_...) → folder-StgProductCategory
        assert move_targets.get("nb-2") == "folder-StgProductCategory"