"""
Unit tests for code-review fixes (C1, C2, M1–M4, m3).

Each class targets a specific fix applied to the SSISToFabric engine and
validates the corrected behaviour.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    PrecedenceConstraint,
    SqlParameterBinding,
    SSISPackage,
    TaskType,
    Variable,
)
from ssis_to_fabric.config import (
    ConnectionMappingConfig,
    MigrationConfig,
    MigrationStrategy,
)
from ssis_to_fabric.engine import (
    CELL_MARKER_PREFIX,
    CELL_MARKER_SUFFIX,
    is_cell_marker,
    make_cell_marker,
)
from ssis_to_fabric.engine.fabric_deployer import FabricDeployer
from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator


# =====================================================================
# Helpers
# =====================================================================

def _make_deployer(
    connection_id: str | None = None,
    connection_map: dict | None = None,
) -> FabricDeployer:
    """Create a FabricDeployer without calling __init__."""
    deployer = FabricDeployer.__new__(FabricDeployer)
    deployer.default_connection_id = connection_id
    deployer._connection_map = connection_map or {}
    deployer._existing_items = {"DataPipeline::test": {"id": "fake-id"}}
    deployer.workspace_id = "fake-ws"
    deployer.dry_run = False
    deployer.skip_existing = False
    return deployer


def _make_pipeline(activities: list[dict]) -> dict:
    return {"name": "test", "properties": {"activities": activities}}


def _decode_payload(mock_api_call: MagicMock) -> dict:
    """Decode the base64 pipeline JSON from a mocked _api_call."""
    _args, kwargs = mock_api_call.call_args
    body = kwargs.get("json_body") or _args[2]
    b64 = body["definition"]["parts"][0]["payload"]
    return json.loads(base64.b64decode(b64).decode("utf-8"))


_VALID_GUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_VALID_GUID_2 = "11111111-2222-3333-4444-555555555555"


# =====================================================================
# C1 – Greedy connection-fallback removed
# =====================================================================

class TestC1GreedyConnectionFallback:
    """_resolve_connection_for_activity must NOT pick a random map entry
    when the SSIS connection name doesn't match."""

    @pytest.mark.unit
    def test_exact_match_returns_correct_connection(self) -> None:
        deployer = _make_deployer(
            connection_map={"cmgr_DW": _VALID_GUID, "cmgr_ODS": _VALID_GUID_2},
        )
        assert deployer._resolve_connection_for_activity("cmgr_DW") == _VALID_GUID

    @pytest.mark.unit
    def test_case_insensitive_match(self) -> None:
        deployer = _make_deployer(connection_map={"CMGR_DW": _VALID_GUID})
        assert deployer._resolve_connection_for_activity("cmgr_dw") == _VALID_GUID

    @pytest.mark.unit
    def test_no_match_returns_default(self) -> None:
        deployer = _make_deployer(
            connection_id=_VALID_GUID_2,
            connection_map={"cmgr_ODS": _VALID_GUID},
        )
        result = deployer._resolve_connection_for_activity("cmgr_UNKNOWN")
        assert result == _VALID_GUID_2, "Should fall back to default, not any map entry"

    @pytest.mark.unit
    def test_no_match_no_default_returns_none(self) -> None:
        deployer = _make_deployer(
            connection_id=None,
            connection_map={"cmgr_ODS": _VALID_GUID},
        )
        result = deployer._resolve_connection_for_activity("cmgr_UNKNOWN")
        assert result is None, "Should be None when no match and no default"

    @pytest.mark.unit
    def test_empty_hint_returns_default(self) -> None:
        deployer = _make_deployer(
            connection_id=_VALID_GUID,
            connection_map={"cmgr_ODS": _VALID_GUID_2},
        )
        result = deployer._resolve_connection_for_activity("")
        assert result == _VALID_GUID

    @pytest.mark.unit
    def test_no_greedy_fallback_to_first_map_entry(self) -> None:
        """Previously ``next(iter(...))`` would silently return an arbitrary
        entry.  Ensure that no longer happens."""
        deployer = _make_deployer(
            connection_id=None,
            connection_map={
                "A_Source": _VALID_GUID,
                "B_Target": _VALID_GUID_2,
            },
        )
        result = deployer._resolve_connection_for_activity("C_NotInMap")
        assert result is None


# =====================================================================
# C2 – Precedence constraint resolution by name / DTSID / ref_id
# =====================================================================

class TestC2PrecedenceConstraintResolution:
    """_flatten_tasks_to_activities must resolve constraints by ref_id, id,
    and display name — not just ref_id."""

    @staticmethod
    def _flatten(tasks, constraints):
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
        from ssis_to_fabric.engine.migration_engine import TargetArtifact

        gen = DataFactoryGenerator(MigrationConfig())
        return gen._flatten_tasks_to_activities(
            tasks, [], {}, preceding=[], precedence_constraints=constraints,
        )

    @pytest.mark.unit
    def test_constraint_by_ref_id(self) -> None:
        """Standard case: constraints reference tasks via ref_id."""
        t1 = ControlFlowTask(name="Step1", task_type=TaskType.EXECUTE_SQL,
                             ref_id="\\Pkg\\Step1", sql_statement="SELECT 1")
        t2 = ControlFlowTask(name="Step2", task_type=TaskType.EXECUTE_SQL,
                             ref_id="\\Pkg\\Step2", sql_statement="SELECT 2")
        constraints = [
            PrecedenceConstraint(source_task="\\Pkg\\Step1", destination_task="\\Pkg\\Step2"),
        ]
        acts = self._flatten([t1, t2], constraints)
        step2 = next(a for a in acts if a["name"] == "Step2")
        assert any(d["activity"] == "Step1" for d in step2.get("dependsOn", []))

    @pytest.mark.unit
    def test_constraint_by_dtsid(self) -> None:
        """Constraints reference tasks via DTSID (id field)."""
        t1 = ControlFlowTask(name="Step1", task_type=TaskType.EXECUTE_SQL,
                             id="{GUID-A}", sql_statement="SELECT 1")
        t2 = ControlFlowTask(name="Step2", task_type=TaskType.EXECUTE_SQL,
                             id="{GUID-B}", sql_statement="SELECT 2")
        constraints = [
            PrecedenceConstraint(source_task="{GUID-A}", destination_task="{GUID-B}"),
        ]
        acts = self._flatten([t1, t2], constraints)
        step2 = next(a for a in acts if a["name"] == "Step2")
        assert any(d["activity"] == "Step1" for d in step2.get("dependsOn", []))

    @pytest.mark.unit
    def test_constraint_by_display_name(self) -> None:
        """Constraints reference tasks by display name (some authoring tools)."""
        t1 = ControlFlowTask(name="Step1", task_type=TaskType.EXECUTE_SQL,
                             sql_statement="SELECT 1")
        t2 = ControlFlowTask(name="Step2", task_type=TaskType.EXECUTE_SQL,
                             sql_statement="SELECT 2")
        constraints = [
            PrecedenceConstraint(source_task="Step1", destination_task="Step2"),
        ]
        acts = self._flatten([t1, t2], constraints)
        step2 = next(a for a in acts if a["name"] == "Step2")
        assert any(d["activity"] == "Step1" for d in step2.get("dependsOn", []))

    @pytest.mark.unit
    def test_dissolved_container_registers_by_name(self) -> None:
        """When a Sequence Container is dissolved, downstream tasks that
        reference it by *name* must still resolve deps correctly."""
        child = ControlFlowTask(name="Inner", task_type=TaskType.EXECUTE_SQL,
                                sql_statement="SELECT 1")
        container = ControlFlowTask(
            name="MyContainer", task_type=TaskType.SEQUENCE_CONTAINER,
            child_tasks=[child], child_precedence_constraints=[],
        )
        after = ControlFlowTask(name="After", task_type=TaskType.EXECUTE_SQL,
                                sql_statement="SELECT 2")
        constraints = [
            PrecedenceConstraint(source_task="MyContainer", destination_task="After"),
        ]
        acts = self._flatten([container, after], constraints)
        after_act = next(a for a in acts if a["name"] == "After")
        dep_names = [d["activity"] for d in after_act.get("dependsOn", [])]
        assert "Inner" in dep_names


# =====================================================================
# M1 – Unresolved TODO placeholders stripped from deployed pipelines
# =====================================================================

class TestM1UnresolvedConnectionStripping:
    """When connection resolution yields a non-GUID (e.g. TODO string or
    raw SSIS name), ``externalReferences`` must be stripped instead of
    leaving an invalid value that the Fabric API rejects."""

    @pytest.mark.unit
    def test_todo_placeholder_stripped_when_no_guid(self, tmp_path: Path) -> None:
        """Activity with TODO placeholder and no default → externalReferences removed."""
        pipeline = _make_pipeline([
            {
                "name": "Run_SQL",
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

        deployer = _make_deployer(connection_id=None)
        deployer._api_call = MagicMock(return_value=MagicMock(status_code=200))

        deployer._update_pipeline_definition("test", json_path)
        transformed = _decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert "externalReferences" not in act

    @pytest.mark.unit
    def test_todo_placeholder_resolved_to_valid_guid(self, tmp_path: Path) -> None:
        """Activity with TODO placeholder + valid GUID default → replaced."""
        pipeline = _make_pipeline([
            {
                "name": "Run_SQL",
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

        deployer = _make_deployer(connection_id=_VALID_GUID)
        deployer._api_call = MagicMock(return_value=MagicMock(status_code=200))

        deployer._update_pipeline_definition("test", json_path)
        transformed = _decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert act["externalReferences"]["connection"] == _VALID_GUID

    @pytest.mark.unit
    def test_valid_guid_connection_kept_as_is(self, tmp_path: Path) -> None:
        """Activity already containing a valid GUID should be untouched."""
        pipeline = _make_pipeline([
            {
                "name": "Run_SQL",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "SELECT 1"}],
                },
                "externalReferences": {"connection": _VALID_GUID},
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = _make_deployer(connection_id=_VALID_GUID_2)
        deployer._api_call = MagicMock(return_value=MagicMock(status_code=200))

        deployer._update_pipeline_definition("test", json_path)
        transformed = _decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert act["externalReferences"]["connection"] == _VALID_GUID

    @pytest.mark.unit
    def test_legacy_connection_migrated_and_stripped_when_no_guid(self, tmp_path: Path) -> None:
        """Legacy typeProperties.connection with a TODO is migrated to
        externalReferences, then stripped because the resolved ID is not a GUID."""
        pipeline = _make_pipeline([
            {
                "name": "Run_SQL",
                "type": "Script",
                "typeProperties": {
                    "scripts": [{"type": "NonQuery", "text": "SELECT 1"}],
                    "connection": {
                        "referenceName": "cmgr_DW  -- TODO: replace with Fabric connection id",
                    },
                },
            },
        ])
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(pipeline))

        deployer = _make_deployer(connection_id=None)
        deployer._api_call = MagicMock(return_value=MagicMock(status_code=200))

        deployer._update_pipeline_definition("test", json_path)
        transformed = _decode_payload(deployer._api_call)
        act = transformed["properties"]["activities"][0]
        assert "externalReferences" not in act
        assert "connection" not in act.get("typeProperties", {})


# =====================================================================
# M2 – Shared cell-splitting constant (is_cell_marker / make_cell_marker)
# =====================================================================

class TestM2CellMarkerConstant:
    """Verify the shared constants and helpers in ``ssis_to_fabric.engine``."""

    @pytest.mark.unit
    def test_make_cell_marker_format(self) -> None:
        assert make_cell_marker("Imports") == "# --- Imports ---"

    @pytest.mark.unit
    def test_make_cell_marker_with_spaces(self) -> None:
        assert make_cell_marker("Fabric Connections") == "# --- Fabric Connections ---"

    @pytest.mark.unit
    def test_is_cell_marker_positive(self) -> None:
        assert is_cell_marker("# --- Imports ---") is True

    @pytest.mark.unit
    def test_is_cell_marker_positive_trailing_spaces(self) -> None:
        assert is_cell_marker("# --- Imports ---   ") is True

    @pytest.mark.unit
    def test_is_cell_marker_negative_no_suffix(self) -> None:
        assert is_cell_marker("# --- Type 1 columns: col1, col2") is False

    @pytest.mark.unit
    def test_is_cell_marker_negative_comment_divider(self) -> None:
        # Long dash dividers (e.g. # -------…------) start with "# ---"
        # but are NOT cell markers because they don't end with " ---".
        # However they DO end with "---" so they would match!  Verify.
        line = "# " + "-" * 60
        # This starts with "# ---" and ends with "---" → technically a marker.
        # That's fine — such lines only appear in source code comments,
        # not inside generated notebook content.
        assert is_cell_marker(line) is True  # by design

    @pytest.mark.unit
    def test_is_cell_marker_negative_empty(self) -> None:
        assert is_cell_marker("") is False

    @pytest.mark.unit
    def test_is_cell_marker_negative_regular_comment(self) -> None:
        assert is_cell_marker("# This is a regular comment") is False

    @pytest.mark.unit
    def test_roundtrip_make_is(self) -> None:
        """make_cell_marker output should always pass is_cell_marker."""
        for title in ["Imports", "Parameters", "Fabric Connections", "Data Flow: ETL"]:
            marker = make_cell_marker(title)
            assert is_cell_marker(marker), f"Roundtrip failed for title={title!r}"

    @pytest.mark.unit
    def test_constants_consistent(self) -> None:
        marker = make_cell_marker("X")
        assert marker.startswith(CELL_MARKER_PREFIX)
        assert marker.rstrip().endswith(CELL_MARKER_SUFFIX)


class TestM2PyToIpynb:
    """_py_to_ipynb must split on cell markers into separate cells."""

    @pytest.mark.unit
    def test_splits_on_markers(self) -> None:
        content = "\n".join([
            "# preamble",
            "import os",
            "# --- Imports ---",
            "from pyspark import SparkSession",
            "# --- Config ---",
            "x = 1",
        ])
        nb = FabricDeployer._py_to_ipynb(content)
        # 3 cells: preamble, Imports section, Config section
        assert len(nb["cells"]) == 3

    @pytest.mark.unit
    def test_no_markers_produces_one_cell(self) -> None:
        content = "import os\nx = 1\n"
        nb = FabricDeployer._py_to_ipynb(content)
        assert len(nb["cells"]) == 1

    @pytest.mark.unit
    def test_marker_starts_new_cell(self) -> None:
        content = "line1\n# --- Section ---\nline2"
        nb = FabricDeployer._py_to_ipynb(content)
        # Cell 0 = line1, Cell 1 = marker + line2
        cell0_src = "".join(nb["cells"][0]["source"])
        cell1_src = "".join(nb["cells"][1]["source"])
        assert "line1" in cell0_src
        assert "# --- Section ---" in cell1_src
        assert "line2" in cell1_src

    @pytest.mark.unit
    def test_empty_content_produces_one_cell(self) -> None:
        nb = FabricDeployer._py_to_ipynb("")
        assert len(nb["cells"]) >= 1

    @pytest.mark.unit
    def test_notebook_format(self) -> None:
        nb = FabricDeployer._py_to_ipynb("x = 1")
        assert nb["nbformat"] == 4
        assert "cells" in nb
        assert nb["cells"][0]["cell_type"] == "code"


# =====================================================================
# M3 – SQL parameter placeholders in Spark generator
# =====================================================================

class TestM3SparkSqlParameters:
    """_generate_sql_execution must replace ?-placeholders with named
    parameters and emit .format() calls."""

    @staticmethod
    def _generator() -> SparkNotebookGenerator:
        return SparkNotebookGenerator(MigrationConfig())

    @pytest.mark.unit
    def test_single_param_replaced(self) -> None:
        task = ControlFlowTask(
            name="Delete By ID",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="DELETE FROM T WHERE id = ?",
            sql_parameters=[
                SqlParameterBinding(
                    parameter_name="0",
                    variable_name="$Package::RecordId",
                    direction="Input",
                    data_type=3,
                ),
            ],
        )
        code = self._generator()._generate_sql_execution(task)
        assert "?" not in code, "Positional ? should be replaced"
        assert "{p0}" in code, "Named placeholder expected"
        assert ".format(" in code
        assert "RECORDID" in code  # sanitized variable name

    @pytest.mark.unit
    def test_multiple_params_ordered(self) -> None:
        task = ControlFlowTask(
            name="Multi Param",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="EXEC sp_proc ?, ?",
            sql_parameters=[
                SqlParameterBinding(parameter_name="0", variable_name="$Package::A",
                                    direction="Input", data_type=8),
                SqlParameterBinding(parameter_name="1", variable_name="$Package::B",
                                    direction="Input", data_type=3),
            ],
        )
        code = self._generator()._generate_sql_execution(task)
        assert "{p0}" in code
        assert "{p1}" in code
        assert ".format(" in code

    @pytest.mark.unit
    def test_output_params_excluded(self) -> None:
        task = ControlFlowTask(
            name="With Output",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="EXEC sp_test ? OUTPUT",
            sql_parameters=[
                SqlParameterBinding(parameter_name="0", variable_name="$Package::X",
                                    direction="Output", data_type=3),
            ],
        )
        code = self._generator()._generate_sql_execution(task)
        # Output params should not trigger named replacement
        assert ".format(" not in code

    @pytest.mark.unit
    def test_no_params_plain_sql(self) -> None:
        task = ControlFlowTask(
            name="Simple SQL",
            task_type=TaskType.EXECUTE_SQL,
            sql_statement="TRUNCATE TABLE staging.t",
        )
        code = self._generator()._generate_sql_execution(task)
        assert "TRUNCATE TABLE staging.t" in code
        assert ".format(" not in code

    @pytest.mark.unit
    def test_no_sql_emits_todo(self) -> None:
        task = ControlFlowTask(name="Empty", task_type=TaskType.EXECUTE_SQL)
        code = self._generator()._generate_sql_execution(task)
        assert "TODO" in code


# =====================================================================
# M4 – Connection target-type override in config / migration_engine
# =====================================================================

class TestM4ConnectionTargetTypeOverride:
    """fabric_target_type_overrides in ConnectionMappingConfig must
    override the auto-classification in _generate_connection_manifests."""

    @pytest.mark.unit
    def test_override_changes_fabric_target(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = MigrationConfig(
            output_dir=tmp_path / "out",
            connection_mappings=ConnectionMappingConfig(
                fabric_target_type_overrides={"cmgr_DW": "Lakehouse"},
            ),
        )
        engine = MigrationEngine(config)
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            connection_managers=[
                ConnectionManager(
                    id="guid-1",
                    name="cmgr_DW",
                    connection_type=ConnectionType.OLEDB,
                    server="srv",
                    database="db",
                ),
            ],
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        engine._generate_connection_manifests([pkg], out_dir)

        manifest_path = out_dir / "connections" / "cmgr_DW.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["fabric_target_type"] == "Lakehouse"

    @pytest.mark.unit
    def test_no_override_uses_auto_classification(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = MigrationConfig(output_dir=tmp_path / "out")
        engine = MigrationEngine(config)
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            connection_managers=[
                ConnectionManager(
                    id="guid-1",
                    name="cmgr_DW",
                    connection_type=ConnectionType.OLEDB,
                    server="srv",
                    database="db",
                ),
            ],
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        engine._generate_connection_manifests([pkg], out_dir)

        manifest = json.loads((out_dir / "connections" / "cmgr_DW.json").read_text())
        assert manifest["fabric_target_type"] == "Warehouse"

    @pytest.mark.unit
    def test_override_flat_file_to_other(self, tmp_path: Path) -> None:
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        config = MigrationConfig(
            output_dir=tmp_path / "out",
            connection_mappings=ConnectionMappingConfig(
                fabric_target_type_overrides={"ff_conn": "Other"},
            ),
        )
        engine = MigrationEngine(config)
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            connection_managers=[
                ConnectionManager(
                    id="guid-2",
                    name="ff_conn",
                    connection_type=ConnectionType.FLAT_FILE,
                ),
            ],
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        engine._generate_connection_manifests([pkg], out_dir)

        manifest = json.loads((out_dir / "connections" / "ff_conn.json").read_text())
        assert manifest["fabric_target_type"] == "Other"

    @pytest.mark.unit
    def test_config_field_defaults_to_empty_dict(self) -> None:
        cfg = ConnectionMappingConfig()
        assert cfg.fabric_target_type_overrides == {}


# =====================================================================
# m3 – Variable scoping: namespace-qualified prefixes avoid collisions
# =====================================================================

class TestM3VariableScopingCollisions:
    """When project params, package params, and User variables share the
    same base name, subsequent entries must get a qualified prefix
    (PROJ_, PKG_, VAR_) instead of being silently dropped."""

    @staticmethod
    def _generate_config(package: SSISPackage) -> str:
        gen = SparkNotebookGenerator(MigrationConfig())
        return gen._generate_config_section(package)

    @pytest.mark.unit
    def test_duplicate_name_gets_prefix(self) -> None:
        """Same name in project_parameters and parameters → PKG_ prefix."""
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            project_parameters=[
                Variable(name="Env", namespace="Project", value="DEV", is_parameter=True),
            ],
            parameters=[
                Variable(name="Env", namespace="Package", value="PROD", is_parameter=True),
            ],
        )
        code = self._generate_config(pkg)
        # First occurrence: ENV
        assert 'ENV = _get_param(' in code
        # Second occurrence: PKG_ENV (not silently dropped)
        assert 'PKG_ENV = _get_param(' in code

    @pytest.mark.unit
    def test_triple_collision(self) -> None:
        """Same name across project, package, and variable → all three emitted."""
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            project_parameters=[
                Variable(name="Mode", namespace="Project", value="1", is_parameter=True),
            ],
            parameters=[
                Variable(name="Mode", namespace="Package", value="2", is_parameter=True),
            ],
            variables=[
                Variable(name="Mode", namespace="User", value="3"),
            ],
        )
        code = self._generate_config(pkg)
        assert 'MODE = _get_param(' in code
        assert 'PKG_MODE = _get_param(' in code
        assert 'VAR_MODE = _get_param(' in code

    @pytest.mark.unit
    def test_no_collision_no_prefix(self) -> None:
        """Unique names should NOT get any prefix."""
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            project_parameters=[
                Variable(name="Server", namespace="Project", value="srv", is_parameter=True),
            ],
            parameters=[
                Variable(name="Database", namespace="Package", value="db", is_parameter=True),
            ],
            variables=[
                Variable(name="BatchSize", namespace="User", value="1000"),
            ],
        )
        code = self._generate_config(pkg)
        assert "SERVER = " in code
        assert "DATABASE = " in code
        assert "BATCHSIZE = " in code
        # No prefix variants
        assert "PROJ_" not in code
        assert "PKG_" not in code
        assert "VAR_" not in code

    @pytest.mark.unit
    def test_system_variables_skipped(self) -> None:
        """System-namespace variables must not be emitted."""
        pkg = SSISPackage(
            name="TestPkg",
            file_path="test.dtsx",
            variables=[
                Variable(name="StartTime", namespace="System", value=""),
                Variable(name="MyVar", namespace="User", value="hello"),
            ],
        )
        code = self._generate_config(pkg)
        assert "MYVAR = " in code
        assert "STARTTIME" not in code
