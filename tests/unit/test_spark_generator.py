"""
Unit tests for the Spark Notebook Generator.
"""

from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import (
    Column,
    DataFlowComponent,
    DataFlowComponentType,
    SSISPackage,
)
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


@pytest.fixture
def generator(default_config: MigrationConfig) -> SparkNotebookGenerator:
    return SparkNotebookGenerator(default_config)


@pytest.fixture
def complex_package() -> SSISPackage:
    parser = DTSXParser()
    return parser.parse(FIXTURES_DIR / "complex_etl.dtsx")


class TestSparkNotebookGenerator:
    """Tests for Spark notebook generation."""

    @pytest.mark.unit
    def test_generate_notebook_for_data_flow(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        assert output.exists()
        assert output.suffix == ".py"

    @pytest.mark.unit
    def test_notebook_has_spark_imports(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "pyspark" in content.lower() or "spark" in content.lower()
        assert "from pyspark.sql" in content

    @pytest.mark.unit
    def test_notebook_has_source_read(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "spark.sql" in content or "spark.read" in content

    @pytest.mark.unit
    def test_notebook_has_destination_write(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "write" in content.lower() or "saveAsTable" in content

    @pytest.mark.unit
    def test_notebook_has_lookup_code(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "Lookup" in content or "join" in content.lower()

    @pytest.mark.unit
    def test_notebook_has_script_component_todo(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        """Script Components should generate TODO markers."""
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "TODO" in content
        assert "Script Component" in content

    @pytest.mark.unit
    def test_generate_script_task_notebook(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        script_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.SCRIPT)
        output = generator.generate(complex_package, script_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        assert "NotImplementedError" in content or "TODO" in content

    @pytest.mark.unit
    def test_generate_orchestrator_notebook(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        output = generator.generate(complex_package, None, tmp_path)

        assert output.exists()
        content = output.read_text(encoding="utf-8")
        assert "Orchestrator" in content or "orchestrator" in content.name

    @pytest.mark.unit
    def test_notebook_connection_config(
        self, generator: SparkNotebookGenerator, complex_package: SSISPackage, tmp_path: Path
    ) -> None:
        from ssis_to_fabric.analyzer.models import TaskType

        df_task = next(t for t in complex_package.control_flow_tasks if t.task_type == TaskType.DATA_FLOW)
        output = generator.generate(complex_package, df_task, tmp_path)

        content = output.read_text(encoding="utf-8")
        # Should reference connection manager info
        assert "src-server" in content or "SalesDB" in content or "Configuration" in content


# =========================================================================
# New transformation generators – unit tests
# =========================================================================


def _make_comp(comp_type: DataFlowComponentType, **kwargs) -> DataFlowComponent:
    """Helper: build a minimal DataFlowComponent for generator tests."""
    defaults = {
        "id": "test-id",
        "name": f"Test {comp_type.value}",
        "component_type": comp_type,
        "columns": [],
        "properties": {},
    }
    defaults.update(kwargs)
    return DataFlowComponent(**defaults)


class TestNewTransformGenerators:
    """Tests for the 13 newly-added transformation generators."""

    def test_gen_scd(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.SLOWLY_CHANGING_DIMENSION,
            table_name="dim_customer",
            properties={"_business_keys": ["CustomerID"], "_scd1_columns": ["Email"], "_scd2_columns": ["Address"]},
        )
        code = generator._gen_scd(comp)
        assert "Slowly Changing Dimension" in code
        assert "dim_customer" in code
        assert "df_existing" in code
        assert "SCD merge completed" in code

    def test_gen_fuzzy_lookup(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.FUZZY_LOOKUP, table_name="ref_products")
        code = generator._gen_fuzzy_lookup(comp)
        assert "Fuzzy Lookup" in code
        assert "levenshtein" in code.lower()
        assert "ref_products" in code

    def test_gen_fuzzy_grouping(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.FUZZY_GROUPING)
        code = generator._gen_fuzzy_grouping(comp)
        assert "Fuzzy Grouping" in code
        assert "soundex" in code.lower()

    def test_gen_term_lookup(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.TERM_LOOKUP, table_name="terms_ref")
        code = generator._gen_term_lookup(comp)
        assert "Term Lookup" in code
        assert "terms_ref" in code

    def test_gen_copy_column_with_columns(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.COPY_COLUMN,
            columns=[Column(name="Name_Copy", source_column="Name")],
        )
        code = generator._gen_copy_column(comp)
        assert "Name_Copy" in code
        assert 'F.col("Name")' in code

    def test_gen_copy_column_no_columns(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.COPY_COLUMN)
        code = generator._gen_copy_column(comp)
        assert "TODO" in code

    def test_gen_character_map_upper(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.CHARACTER_MAP,
            columns=[Column(name="City", expression="uppercase")],
        )
        code = generator._gen_character_map(comp)
        assert "F.upper" in code

    def test_gen_character_map_lower(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.CHARACTER_MAP,
            columns=[Column(name="City", expression="lowercase")],
        )
        code = generator._gen_character_map(comp)
        assert "F.lower" in code

    def test_gen_audit(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.AUDIT)
        code = generator._gen_audit(comp)
        assert "Audit" in code
        assert "AuditTimestamp" in code or "current_timestamp" in code

    def test_gen_merge(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.MERGE)
        code = generator._gen_merge(comp)
        assert "Merge" in code
        assert "unionByName" in code

    def test_gen_cdc_splitter(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.CDC_SPLITTER)
        code = generator._gen_cdc_splitter(comp)
        assert "CDC Splitter" in code
        assert "df_inserts" in code
        assert "df_updates" in code
        assert "df_deletes" in code

    def test_gen_percentage_sampling(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.PERCENTAGE_SAMPLING,
            properties={"SamplingValue": "25", "SamplingSeed": "123"},
        )
        code = generator._gen_percentage_sampling(comp)
        assert "sample" in code
        assert "25" in code

    def test_gen_row_sampling(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(
            DataFlowComponentType.ROW_SAMPLING,
            properties={"SamplingValue": "500"},
        )
        code = generator._gen_row_sampling(comp)
        assert "sample" in code
        assert "500" in code

    def test_gen_balanced_data_distributor(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.BALANCED_DATA_DISTRIBUTOR)
        code = generator._gen_balanced_data_distributor(comp)
        assert "repartition" in code

    def test_gen_cache_transform(self, generator: SparkNotebookGenerator) -> None:
        comp = _make_comp(DataFlowComponentType.CACHE_TRANSFORM)
        code = generator._gen_cache_transform(comp)
        assert "cache()" in code


# =========================================================================
# SSIS Expression to PySpark – unit tests
# =========================================================================


class TestSSISExprToPySpark:
    """Tests for the expanded _ssis_expr_to_pyspark static method."""

    @staticmethod
    def e(expr: str) -> str:
        return SparkNotebookGenerator._ssis_expr_to_pyspark(expr)

    # --- Date functions ---

    def test_getdate(self) -> None:
        assert "current_timestamp" in self.e("GETDATE()")

    def test_dateadd_day(self) -> None:
        r = self.e('DATEADD("dd", 7, OrderDate)')
        assert "date_add" in r
        assert "OrderDate" in r

    def test_dateadd_month(self) -> None:
        r = self.e('DATEADD("mm", 3, StartDate)')
        assert "add_months" in r

    def test_dateadd_year(self) -> None:
        r = self.e('DATEADD("yyyy", 1, HireDate)')
        assert "add_months" in r
        assert "12" in r

    def test_datediff_day(self) -> None:
        r = self.e('DATEDIFF("dd", StartDate, EndDate)')
        assert "datediff" in r

    def test_datediff_month(self) -> None:
        r = self.e('DATEDIFF("mm", StartDate, EndDate)')
        assert "months_between" in r

    def test_datepart_year(self) -> None:
        r = self.e('DATEPART("yyyy", OrderDate)')
        assert "year" in r

    def test_datepart_quarter(self) -> None:
        r = self.e('DATEPART("qq", OrderDate)')
        assert "quarter" in r

    def test_year(self) -> None:
        assert "year" in self.e("YEAR(OrderDate)")

    def test_month(self) -> None:
        assert "month" in self.e("MONTH(OrderDate)")

    def test_day(self) -> None:
        assert "dayofmonth" in self.e("DAY(OrderDate)")

    # --- String functions ---

    def test_upper(self) -> None:
        assert "upper" in self.e("UPPER(Name)")

    def test_lower(self) -> None:
        assert "lower" in self.e("LOWER(Name)")

    def test_trim(self) -> None:
        assert "trim" in self.e("TRIM(Name)")

    def test_ltrim(self) -> None:
        assert "ltrim" in self.e("LTRIM(Name)")

    def test_rtrim(self) -> None:
        assert "rtrim" in self.e("RTRIM(Name)")

    def test_len(self) -> None:
        assert "length" in self.e("LEN(Name)")

    def test_left(self) -> None:
        r = self.e("LEFT(Code, 3)")
        assert "substring" in r
        assert "1" in r and "3" in r

    def test_right(self) -> None:
        r = self.e("RIGHT(Code, 4)")
        assert "substring" in r

    def test_findstring(self) -> None:
        r = self.e('FINDSTRING(Name, "test")')
        assert "locate" in r

    def test_replace(self) -> None:
        r = self.e('REPLACE(Name, "old", "new")')
        assert "regexp_replace" in r

    def test_substring(self) -> None:
        r = self.e("SUBSTRING(Name, 1, 5)")
        assert "substring" in r

    def test_reverse(self) -> None:
        r = self.e("REVERSE(Code)")
        assert "reverse" in r

    def test_string_concatenation(self) -> None:
        r = self.e('"Hello" + Name + "!"')
        assert "concat" in r.lower()

    # --- Null functions ---

    def test_isnull(self) -> None:
        assert "isNull" in self.e("ISNULL(col)")

    def test_replacenull(self) -> None:
        r = self.e("REPLACENULL(Amount, 0)")
        assert "coalesce" in r

    def test_null_wstr(self) -> None:
        r = self.e("NULL(DT_WSTR, 50)")
        assert "None" in r and "string" in r

    def test_null_i4(self) -> None:
        r = self.e("NULL(DT_I4)")
        assert "None" in r and "int" in r

    # --- Math functions ---

    def test_abs(self) -> None:
        assert "abs" in self.e("ABS(Amount)")

    def test_ceiling(self) -> None:
        assert "ceil" in self.e("CEILING(Price)")

    def test_floor(self) -> None:
        assert "floor" in self.e("FLOOR(Price)")

    def test_round(self) -> None:
        r = self.e("ROUND(Price, 2)")
        assert "round" in r

    def test_power(self) -> None:
        r = self.e("POWER(Base, 3)")
        assert "pow" in r

    def test_sqrt(self) -> None:
        assert "sqrt" in self.e("SQRT(Value)")

    def test_sign(self) -> None:
        assert "signum" in self.e("SIGN(Delta)")

    def test_square(self) -> None:
        r = self.e("SQUARE(Value)")
        assert "pow" in r and "2" in r

    # --- Type casts ---

    def test_cast_dt_str(self) -> None:
        r = self.e("(DT_STR, 50, 1252) Amount")
        assert 'cast("string")' in r

    def test_cast_dt_wstr(self) -> None:
        r = self.e("(DT_WSTR, 100) Name")
        assert 'cast("string")' in r

    def test_cast_dt_i4(self) -> None:
        r = self.e("(DT_I4) Quantity")
        assert 'cast("int")' in r

    def test_cast_dt_i2(self) -> None:
        r = self.e("(DT_I2) SmallVal")
        assert 'cast("smallint")' in r

    def test_cast_dt_i8(self) -> None:
        r = self.e("(DT_I8) BigVal")
        assert 'cast("bigint")' in r

    def test_cast_dt_r4(self) -> None:
        r = self.e("(DT_R4) FloatVal")
        assert 'cast("float")' in r

    def test_cast_dt_r8(self) -> None:
        r = self.e("(DT_R8) DoubleVal")
        assert 'cast("double")' in r

    def test_cast_dt_bool(self) -> None:
        r = self.e("(DT_BOOL) Flag")
        assert 'cast("boolean")' in r

    def test_cast_dt_date(self) -> None:
        r = self.e("(DT_DATE) DateCol")
        assert 'cast("date")' in r

    def test_cast_dt_dbtimestamp(self) -> None:
        r = self.e("(DT_DBTIMESTAMP) TimestampCol")
        assert 'cast("timestamp")' in r

    def test_cast_dt_decimal(self) -> None:
        r = self.e("(DT_DECIMAL, 2) Price")
        assert "decimal" in r

    def test_cast_dt_numeric(self) -> None:
        r = self.e("(DT_NUMERIC, 18, 4) Amount")
        assert "decimal(18,4)" in r

    def test_cast_dt_guid(self) -> None:
        r = self.e("(DT_GUID) UniqueId")
        assert "string" in r

    def test_cast_dt_bytes(self) -> None:
        r = self.e("(DT_BYTES, 16) BinaryCol")
        assert "binary" in r

    def test_cast_dt_cy(self) -> None:
        r = self.e("(DT_CY) MoneyCol")
        assert "decimal(19,4)" in r

    # --- Ternary / logical ---

    def test_ternary(self) -> None:
        r = self.e("x > 0 ? x : 0")
        assert "F.when" in r
        assert "otherwise" in r

    # --- Variable references ---

    def test_package_var(self) -> None:
        r = self.e("@[$Package::StartDate]")
        assert "STARTDATE" in r

    def test_user_var(self) -> None:
        r = self.e("@[User::Counter]")
        assert "COUNTER" in r

    # --- Fallback ---

    def test_fallback_expr(self) -> None:
        r = self.e("some_complex + thing")
        assert "F.expr" in r
