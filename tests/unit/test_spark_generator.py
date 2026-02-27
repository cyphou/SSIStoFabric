"""
Unit tests for the Spark Notebook Generator.
"""

from pathlib import Path

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.analyzer.models import SSISPackage
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
