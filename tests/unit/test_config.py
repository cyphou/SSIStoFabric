"""
Unit tests for configuration management.
"""

from pathlib import Path

import pytest

from ssis_to_fabric.config import (
    MigrationConfig,
    MigrationStrategy,
    SourceConfig,
    load_config,
)


class TestMigrationConfig:
    """Tests for configuration loading and validation."""

    @pytest.mark.unit
    def test_default_config(self) -> None:
        config = MigrationConfig()
        assert config.project_name == "ssis-migration"
        assert config.strategy == MigrationStrategy.HYBRID

    @pytest.mark.unit
    def test_config_from_yaml(self, tmp_path: Path) -> None:
        yaml_content = """
project_name: test-project
strategy: spark
source:
  catalog_server: my-server.database.windows.net
  catalog_database: SSISDB
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml_content)

        config = MigrationConfig.from_yaml(config_path)
        assert config.project_name == "test-project"
        assert config.strategy == MigrationStrategy.SPARK
        assert config.source.catalog_server == "my-server.database.windows.net"

    @pytest.mark.unit
    def test_config_env_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SSIS_CATALOG_SERVER", "env-server.database.windows.net")
        monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws-12345")

        config = MigrationConfig.from_yaml(tmp_path / "nonexistent.yaml")
        assert config.source.catalog_server == "env-server.database.windows.net"
        assert config.fabric.workspace_id == "ws-12345"

    @pytest.mark.unit
    def test_config_missing_file_uses_defaults(self) -> None:
        config = load_config(Path("nonexistent.yaml"))
        assert config.project_name == "ssis-migration"

    @pytest.mark.unit
    def test_packages_path_resolution(self) -> None:
        config = SourceConfig(packages_path="./test/packages")
        assert config.packages_path is not None
        assert config.packages_path.is_absolute()

    @pytest.mark.unit
    def test_packages_path_none(self) -> None:
        config = SourceConfig(packages_path=None)
        assert config.packages_path is None
