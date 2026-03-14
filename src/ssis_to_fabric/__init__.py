"""SSIS to Microsoft Fabric migration tool."""

from ssis_to_fabric.api import SSISMigrator
from ssis_to_fabric.config import MigrationConfig, MigrationStrategy, load_config
from ssis_to_fabric.engine.agents import AgentOrchestrator, DeployAgent
from ssis_to_fabric.engine.migration_engine import (
    AssessmentReport,
    MigrationEngine,
    MigrationItem,
    MigrationPlan,
    PackageAssessment,
    TargetArtifact,
)
from ssis_to_fabric.engine.plugin_registry import (
    ComponentRegistry,
    ExpressionRule,
    HookManager,
    TransformationStrategy,
    component_handler,
    discover_plugins,
    get_component_registry,
    get_hook_manager,
    hook,
    register_expression_rule,
)

__all__ = [
    "AgentOrchestrator",
    "AssessmentReport",
    "ComponentRegistry",
    "DeployAgent",
    "ExpressionRule",
    "HookManager",
    "SSISMigrator",
    "MigrationConfig",
    "MigrationStrategy",
    "MigrationEngine",
    "MigrationItem",
    "MigrationPlan",
    "TargetArtifact",
    "TransformationStrategy",
    "component_handler",
    "discover_plugins",
    "get_component_registry",
    "get_hook_manager",
    "hook",
    "load_config",
    "register_expression_rule",
]
