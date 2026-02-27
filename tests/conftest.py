"""Test configuration and shared fixtures."""

from pathlib import Path

import pytest

from ssis_to_fabric.config import DataflowType, MigrationConfig, MigrationStrategy

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_PACKAGES_DIR = FIXTURES_DIR / "sample_packages"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def sample_packages_dir() -> Path:
    return SAMPLE_PACKAGES_DIR


@pytest.fixture
def default_config(tmp_path: Path) -> MigrationConfig:
    """Provide a default migration config for testing."""
    return MigrationConfig(
        project_name="test-migration",
        strategy=MigrationStrategy.HYBRID,
        output_dir=tmp_path / "output",
    )


@pytest.fixture
def df_only_config(tmp_path: Path) -> MigrationConfig:
    """Config for Data Factory only strategy."""
    return MigrationConfig(
        project_name="test-df-only",
        strategy=MigrationStrategy.DATA_FACTORY,
        output_dir=tmp_path / "output",
    )


@pytest.fixture
def spark_only_config(tmp_path: Path) -> MigrationConfig:
    """Config for Spark only strategy."""
    return MigrationConfig(
        project_name="test-spark-only",
        strategy=MigrationStrategy.SPARK,
        output_dir=tmp_path / "output",
    )


@pytest.fixture
def dataflow_gen2_config(tmp_path: Path) -> MigrationConfig:
    """Config for hybrid strategy with Dataflow Gen2 output."""
    return MigrationConfig(
        project_name="test-dataflow-gen2",
        strategy=MigrationStrategy.HYBRID,
        dataflow_type=DataflowType.DATAFLOW_GEN2,
        output_dir=tmp_path / "output",
    )
