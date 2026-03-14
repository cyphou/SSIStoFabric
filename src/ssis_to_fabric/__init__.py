"""SSIStoFabric – Migrate SSIS packages to Microsoft Fabric."""

from ssis_to_fabric.api import SSISMigrator
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy, load_config
from ssis_to_fabric.engine.migration_engine import (
    MigrationEngine,
    MigrationItem,
    MigrationPlan,
    TargetArtifact,
)

__all__ = [
    "SSISMigrator",
    "MigrationConfig",
    "MigrationStrategy",
    "MigrationEngine",
    "MigrationItem",
    "MigrationPlan",
    "TargetArtifact",
    "load_config",
]
