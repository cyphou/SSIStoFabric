"""Tests for Phase 30 — Migration Completeness & Gap Closure."""

from __future__ import annotations

import pytest

from ssis_to_fabric.analyzer.models import (
    ConnectionManager,
    ConnectionType,
    ControlFlowTask,
    DataFlowComponent,
    DataFlowComponentType,
    EventHandler,
    MigrationComplexity,
    SSISPackage,
    TaskType,
)
from ssis_to_fabric.engine.gap_closure import (
    build_post_execute_activity,
    build_pre_execute_activity,
    build_progress_activity,
    build_warning_activity,
    generate_data_tap,
    generate_recordset_destination,
    generate_scd2_merge,
    generate_scd3_merge,
)


# =====================================================================
# SCD Type 2 — Proper row splitting
# =====================================================================


class TestSCDType2:
    """SCD Type 2 generates proper expired + new row logic."""

    def test_scd2_merge_contains_expired_rows(self):
        code = generate_scd2_merge("dim_customer", "dim_customer", ["cust_id"], ["name", "email"])
        assert "df_expired" in code
        assert "ExpirationDate" in code
        assert "IsCurrent" in code
        assert "F.lit(False)" in code  # expire old row

    def test_scd2_merge_contains_new_rows(self):
        code = generate_scd2_merge("dim_customer", "dim_customer", ["cust_id"], ["name"])
        assert "df_new" in code
        assert "EffectiveDate" in code
        assert "F.lit(True)" in code  # new row is current

    def test_scd2_merge_contains_unchanged_rows(self):
        code = generate_scd2_merge("dim_customer", "dim_customer", ["cust_id"], ["name"])
        assert "df_unchanged" in code

    def test_scd2_merge_union_all_sets(self):
        code = generate_scd2_merge("dim_product", "dim_product", ["sku"], ["price", "description"])
        assert "unionByName" in code

    def test_scd2_merge_writes_delta(self):
        code = generate_scd2_merge("dim_product", "dim_product", ["sku"], ["price"])
        assert 'saveAsTable("dim_product")' in code

    def test_scd2_merge_change_detection(self):
        code = generate_scd2_merge("dim_t", "dim_t", ["id"], ["a", "b"])
        assert '(F.col("src.a") != F.col("tgt.a"))' in code
        assert '(F.col("src.b") != F.col("tgt.b"))' in code

    def test_scd2_merge_filters_current(self):
        code = generate_scd2_merge("dim_t", "dim_t", ["id"], ["val"])
        assert "IsCurrent" in code
        assert "df_current" in code


# =====================================================================
# SCD Type 3 — Previous value column
# =====================================================================


class TestSCDType3:
    """SCD Type 3 generates Previous_{col} tracking columns."""

    def test_scd3_merge_adds_previous_columns(self):
        code = generate_scd3_merge("dim_emp", "dim_emp", ["emp_id"], ["salary", "title"])
        assert "Previous_salary" in code
        assert "Previous_title" in code

    def test_scd3_merge_preserves_business_keys(self):
        code = generate_scd3_merge("dim_emp", "dim_emp", ["emp_id"], ["salary"])
        assert '"emp_id"' in code

    def test_scd3_merge_adds_last_modified(self):
        code = generate_scd3_merge("dim_emp", "dim_emp", ["emp_id"], ["salary"])
        assert "LastModifiedDate" in code

    def test_scd3_merge_writes_delta(self):
        code = generate_scd3_merge("dim_emp", "dim_emp", ["emp_id"], ["salary"])
        assert 'saveAsTable("dim_emp")' in code


# =====================================================================
# SCD in spark_generator (integration)
# =====================================================================


class TestSCDSparkGenerator:
    """Test SCD generation through the spark generator."""

    @pytest.fixture()
    def _config(self):
        from ssis_to_fabric.config import MigrationConfig
        return MigrationConfig()

    def test_scd_type1_generates_overwrite(self, _config):
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator
        gen = SparkNotebookGenerator(_config)
        comp = DataFlowComponent(
            name="SCD_Customer",
            component_type=DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
            properties={
                "_business_keys": ["CustomerID"],
                "_scd1_columns": ["Phone", "Address"],
            },
        )
        code = gen._gen_scd(comp)
        assert "Type 1 (overwrite)" in code
        assert "F.coalesce" in code

    def test_scd_type2_uses_gap_closure(self, _config):
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator
        gen = SparkNotebookGenerator(_config)
        comp = DataFlowComponent(
            name="SCD_Product",
            component_type=DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
            properties={
                "_business_keys": ["ProductSKU"],
                "_scd2_columns": ["Price", "Category"],
            },
        )
        code = gen._gen_scd(comp)
        assert "SCD Type 2" in code
        assert "expired" in code.lower()
        assert "EffectiveDate" in code

    def test_scd_type3_uses_gap_closure(self, _config):
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator
        gen = SparkNotebookGenerator(_config)
        comp = DataFlowComponent(
            name="SCD_Employee",
            component_type=DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
            properties={
                "_business_keys": ["EmpID"],
                "_scd3_columns": ["Salary"],
            },
        )
        code = gen._gen_scd(comp)
        assert "Previous_Salary" in code
        assert "LastModified" in code

    def test_scd_mixed_types(self, _config):
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator
        gen = SparkNotebookGenerator(_config)
        comp = DataFlowComponent(
            name="SCD_Mixed",
            component_type=DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
            properties={
                "_business_keys": ["ID"],
                "_scd1_columns": ["Name"],
                "_scd2_columns": ["Status"],
                "_scd3_columns": ["Dept"],
            },
        )
        code = gen._gen_scd(comp)
        assert "Type 1 (overwrite)" in code
        assert "SCD Type 2" in code
        assert "Previous_Dept" in code


# =====================================================================
# Recordset Destination
# =====================================================================


class TestRecordsetDestination:
    """Recordset Destination generates TempView pattern."""

    def test_generates_temp_view(self):
        code = generate_recordset_destination("MyRecordset")
        assert "createOrReplaceTempView" in code

    def test_uses_variable_name(self):
        code = generate_recordset_destination("MyRecordset", "User::rsResults")
        assert "User::rsResults" in code

    def test_default_view_name(self):
        code = generate_recordset_destination("My Recordset")
        assert "recordset_my_recordset" in code.lower()

    def test_includes_read_hint(self):
        code = generate_recordset_destination("RS1")
        assert "spark.sql" in code

    def test_recordset_in_spark_generator(self):
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        gen = SparkNotebookGenerator(MigrationConfig())
        comp = DataFlowComponent(
            name="RecordsetDest",
            component_type=DataFlowComponentType.RECORDSET_DESTINATION,
            properties={"VariableName": "audit_results"},
        )
        code = gen._generate_destination_write(comp)
        assert "createOrReplaceTempView" in code
        assert "audit_results" in code


# =====================================================================
# Data Tap
# =====================================================================


class TestDataTap:
    """Data Tap generates debug/sampling code."""

    def test_generates_sample_code(self):
        code = generate_data_tap("TapOutput")
        assert "df.limit(1000)" in code

    def test_generates_display(self):
        code = generate_data_tap("TapOutput")
        assert "display()" in code

    def test_with_output_path(self):
        code = generate_data_tap("TapOutput", output_path="/debug/tap.delta")
        assert "debug_tap" in code
        assert "saveAsTable" in code

    def test_without_output_path(self):
        code = generate_data_tap("TapOutput")
        assert "saveAsTable" not in code


# =====================================================================
# Event Handlers
# =====================================================================


class TestEventHandlerActivities:
    """Event handler activities are properly typed."""

    def test_pre_execute_is_script(self):
        act = build_pre_execute_activity("OnPreExecute", 2)
        assert act["type"] == "Script"
        assert "Pre-flight validation" in act["description"]
        assert act["name"] == "EventHandler_OnPreExecute"

    def test_post_execute_has_dependencies(self):
        act = build_post_execute_activity("OnPostExecute", 1, ["Task_A", "Task_B"])
        assert act["type"] == "Script"
        assert len(act["dependsOn"]) == 2
        assert act["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]

    def test_post_execute_no_dependencies(self):
        act = build_post_execute_activity("OnPostExecute", 1, [])
        assert "dependsOn" not in act

    def test_warning_is_web_activity(self):
        act = build_warning_activity("OnWarning", 3)
        assert act["type"] == "WebActivity"
        assert "WEBHOOK_URL" in act["typeProperties"]["url"]

    def test_progress_is_set_variable(self):
        act = build_progress_activity("OnProgress", 1)
        assert act["type"] == "SetVariable"
        assert "migration_progress" in act["typeProperties"]["variableName"]


class TestEventHandlerIntegration:
    """Event handler conversion through the DataFactoryGenerator."""

    @pytest.fixture()
    def _gen(self):
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
        return DataFactoryGenerator(MigrationConfig())

    def _make_package(self, event_types: list[str]) -> SSISPackage:
        handlers = []
        for et in event_types:
            handlers.append(EventHandler(
                event_type=et,
                tasks=[
                    ControlFlowTask(
                        name=f"{et}_Task1",
                        task_type=TaskType.EXECUTE_SQL,
                        sql_statement="SELECT 1",
                    )
                ],
            ))
        return SSISPackage(
            name="TestPkg",
            control_flow_tasks=[
                ControlFlowTask(name="MainTask", task_type=TaskType.EXECUTE_SQL, sql_statement="SELECT 1")
            ],
            event_handlers=handlers,
            connection_managers=[],
        )

    def test_on_error_generates_failed_dependency(self, _gen):
        pkg = self._make_package(["OnError"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["dependsOn"][0]["dependencyConditions"] == ["Failed"]

    def test_on_pre_execute_generates_script(self, _gen):
        pkg = self._make_package(["OnPreExecute"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["type"] == "Script"
        assert "Pre-flight" in activities[0]["description"]

    def test_on_post_execute_generates_script(self, _gen):
        pkg = self._make_package(["OnPostExecute"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["type"] == "Script"
        assert activities[0]["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]

    def test_on_warning_generates_web_activity(self, _gen):
        pkg = self._make_package(["OnWarning"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["type"] == "WebActivity"

    def test_on_progress_generates_set_variable(self, _gen):
        pkg = self._make_package(["OnProgress"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["type"] == "SetVariable"

    def test_unknown_event_still_generates_wait(self, _gen):
        pkg = self._make_package(["OnQueryCancel"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 1
        assert activities[0]["type"] == "Wait"

    def test_multiple_event_types(self, _gen):
        pkg = self._make_package(["OnError", "OnPreExecute", "OnPostExecute", "OnWarning", "OnProgress"])
        activities = _gen._event_handlers_to_activities(pkg)
        assert len(activities) == 5
        types = {a["type"] for a in activities}
        assert types == {"Script", "WebActivity", "SetVariable"}  # Script from OnError + Pre + Post


# =====================================================================
# Complexity Re-scoring
# =====================================================================


class TestComplexityRescoring:
    """Components with proper implementations have reduced complexity."""

    def test_scd_is_medium(self):
        from ssis_to_fabric.analyzer.dtsx_parser import COMPONENT_COMPLEXITY
        assert COMPONENT_COMPLEXITY[DataFlowComponentType.SLOWLY_CHANGING_DIMENSION] == MigrationComplexity.MEDIUM

    def test_recordset_is_medium(self):
        from ssis_to_fabric.analyzer.dtsx_parser import COMPONENT_COMPLEXITY
        assert COMPONENT_COMPLEXITY[DataFlowComponentType.RECORDSET_DESTINATION] == MigrationComplexity.MEDIUM

    def test_fuzzy_lookup_still_high(self):
        from ssis_to_fabric.analyzer.dtsx_parser import COMPONENT_COMPLEXITY
        assert COMPONENT_COMPLEXITY[DataFlowComponentType.FUZZY_LOOKUP] == MigrationComplexity.HIGH

    def test_script_component_still_high(self):
        from ssis_to_fabric.analyzer.dtsx_parser import COMPONENT_COMPLEXITY
        assert COMPONENT_COMPLEXITY[DataFlowComponentType.SCRIPT_COMPONENT] == MigrationComplexity.HIGH
