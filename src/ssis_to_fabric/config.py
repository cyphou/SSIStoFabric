"""
Configuration management for the migration tool.
Loads settings from environment, YAML config files, and CLI overrides.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class MigrationStrategy(str, Enum):
    """Strategy for translating SSIS components."""

    DATA_FACTORY = "data_factory"
    SPARK = "spark"
    HYBRID = "hybrid"  # Use DF where possible, Spark for complex transforms


class DataflowType(str, Enum):
    """How Data Flow tasks are rendered under the hybrid strategy.

    - ``notebook``: Generate PySpark notebooks (default).
    - ``dataflow_gen2``: Generate Dataflow Gen2 (Power Query / M code).
    """

    NOTEBOOK = "notebook"
    DATAFLOW_GEN2 = "dataflow_gen2"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class SourceConfig(BaseModel):
    """SSIS source configuration."""

    catalog_server: str = Field(default="", description="SQL Server hosting SSISDB")
    catalog_database: str = Field(default="SSISDB")
    catalog_user: str = Field(default="")
    catalog_password: str = Field(default="")
    packages_path: Path | None = Field(default=None, description="Local path to .dtsx files (alternative to SSISDB)")

    @field_validator("packages_path", mode="before")
    @classmethod
    def resolve_path(cls, v: str | None) -> Path | None:
        if v is None or v == "":
            return None
        return Path(v).resolve()


class FabricConfig(BaseModel):
    """Microsoft Fabric target configuration."""

    workspace_id: str = Field(default="")
    workspace_name: str = Field(default="")
    capacity_id: str = Field(default="")


class AzureConfig(BaseModel):
    """Azure authentication and resource configuration."""

    tenant_id: str = Field(default="")
    client_id: str = Field(default="")
    client_secret: str = Field(default="")
    subscription_id: str = Field(default="")
    resource_group: str = Field(default="")


class DataFactoryConfig(BaseModel):
    """Data Factory specific configuration."""

    factory_name: str = Field(default="")
    linked_service_name: str = Field(default="")


class RegressionConfig(BaseModel):
    """Non-regression testing configuration."""

    source_db_connection: str = Field(default="")
    target_db_connection: str = Field(default="")
    baseline_dir: Path = Field(default=Path("tests/regression/baselines"))
    tolerance_row_count: float = Field(default=0.0, description="Allowed % difference in row counts")
    tolerance_numeric: float = Field(default=0.0001, description="Allowed numeric precision difference")
    sample_size: int = Field(default=10000, description="Max rows to compare per table")


class EnvironmentProfile(BaseModel):
    """Per-environment deployment configuration (dev / staging / prod).

    Example YAML::

        environments:
          dev:
            workspace_id: "aaaa-bbbb-cccc-dddd"
            connection_mappings:
              OLEDB_Source: "conn-id-for-dev"
          prod:
            workspace_id: "1111-2222-3333-4444"
            connection_mappings:
              OLEDB_Source: "conn-id-for-prod"
    """

    workspace_id: str = Field(default="")
    workspace_name: str = Field(default="")
    connection_mappings: dict[str, str] = Field(default_factory=dict)


class ConnectionMappingConfig(BaseModel):
    """Maps SSIS connection manager names to Fabric connection IDs.

    Example YAML::

        connection_mappings:
          mappings:
            OLEDB_Source: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            ADO_Warehouse: "11111111-2222-3333-4444-555555555555"
          fabric_target_type_overrides:
            ADO_Warehouse: "Lakehouse"
    """

    mappings: dict[str, str] = Field(
        default_factory=dict,
        description="SSIS connection name → Fabric connection ID / reference name",
    )
    fabric_target_type_overrides: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "SSIS connection name → Fabric target type (Warehouse, Lakehouse, Other). "
            "Overrides the automatic classification based on connection type."
        ),
    )


class RetryConfig(BaseModel):
    """Configurable retry settings for Fabric REST API calls."""

    max_retries: int = Field(default=5, description="Maximum number of retry attempts")
    base_delay: float = Field(default=2.0, description="Base delay in seconds for exponential backoff")
    max_delay: float = Field(default=60.0, description="Maximum delay cap in seconds")
    default_retry_after: int = Field(default=30, description="Fallback Retry-After value in seconds")


class MigrationConfig(BaseModel):
    """Root configuration for the migration project."""

    project_name: str = Field(default="ssis-migration")
    strategy: MigrationStrategy = Field(default=MigrationStrategy.HYBRID)
    dataflow_type: DataflowType = Field(
        default=DataflowType.NOTEBOOK,
        description=(
            "How Data Flow tasks are rendered under the hybrid strategy. "
            "'notebook' (default) generates PySpark notebooks; "
            "'dataflow_gen2' generates Dataflow Gen2 / Power Query M code."
        ),
    )
    output_dir: Path = Field(default=Path("output"))
    log_level: LogLevel = Field(default=LogLevel.INFO)
    parallel_workers: int = Field(
        default=1,
        ge=1,
        description="Number of parallel worker threads for multi-agent migration (1 = sequential).",
    )

    source: SourceConfig = Field(default_factory=SourceConfig)
    fabric: FabricConfig = Field(default_factory=FabricConfig)
    azure: AzureConfig = Field(default_factory=AzureConfig)
    data_factory: DataFactoryConfig = Field(default_factory=DataFactoryConfig)
    regression: RegressionConfig = Field(default_factory=RegressionConfig)
    connection_mappings: ConnectionMappingConfig = Field(default_factory=ConnectionMappingConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    # Named environment profiles (dev / staging / prod) for multi-workspace support
    environments: dict[str, EnvironmentProfile] = Field(
        default_factory=dict,
        description="Named environment profiles for multi-workspace deployments",
    )

    def get_environment(self, env_name: str) -> EnvironmentProfile:
        """Return the profile for *env_name*, or a default profile if not configured."""
        if env_name in self.environments:
            return self.environments[env_name]
        # Fall back to the root fabric config as a single environment
        return EnvironmentProfile(
            workspace_id=self.fabric.workspace_id,
            workspace_name=self.fabric.workspace_name,
        )

    @classmethod
    def from_yaml(cls, path: Path) -> MigrationConfig:
        """Load configuration from a YAML file, with env var overrides."""
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}
        else:
            data = {}

        # Override with environment variables
        data = cls._apply_env_overrides(data)
        return cls(**data)

    @classmethod
    def _apply_env_overrides(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Apply environment variable overrides to config data."""
        env_map = {
            "source": {
                "catalog_server": "SSIS_CATALOG_SERVER",
                "catalog_database": "SSIS_CATALOG_DATABASE",
                "catalog_user": "SSIS_CATALOG_USER",
                "catalog_password": "SSIS_CATALOG_PASSWORD",
                "packages_path": "SSIS_PACKAGES_PATH",
            },
            "fabric": {
                "workspace_id": "FABRIC_WORKSPACE_ID",
                "workspace_name": "FABRIC_WORKSPACE_NAME",
                "capacity_id": "FABRIC_CAPACITY_ID",
            },
            "azure": {
                "tenant_id": "AZURE_TENANT_ID",
                "client_id": "AZURE_CLIENT_ID",
                "client_secret": "AZURE_CLIENT_SECRET",
                "subscription_id": "AZURE_SUBSCRIPTION_ID",
                "resource_group": "AZURE_RESOURCE_GROUP",
            },
            "data_factory": {
                "factory_name": "ADF_FACTORY_NAME",
                "linked_service_name": "ADF_LINKED_SERVICE_NAME",
            },
            "regression": {
                "source_db_connection": "SOURCE_DB_CONNECTION_STRING",
                "target_db_connection": "TARGET_DB_CONNECTION_STRING",
            },
        }

        for section, mappings in env_map.items():
            if section not in data:
                data[section] = {}
            for key, env_var in mappings.items():
                val = os.environ.get(env_var)
                if val:
                    data[section][key] = val

        # Top-level overrides
        if os.environ.get("LOG_LEVEL"):
            data["log_level"] = os.environ["LOG_LEVEL"]

        # Connection-mappings section (flat dict in YAML)
        if "connection_mappings" in data and isinstance(data["connection_mappings"], dict):
            raw = data["connection_mappings"]
            # If user supplied a flat dict (name: id), wrap in {"mappings": ...}
            if "mappings" not in raw:
                data["connection_mappings"] = {"mappings": raw}

        return data


def load_config(config_path: Path | None = None) -> MigrationConfig:
    """Load migration config from YAML file with env overrides."""
    path = config_path or Path("migration_config.yaml")
    return MigrationConfig.from_yaml(path)
