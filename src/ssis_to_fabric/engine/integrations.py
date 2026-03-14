"""
Integrations & Ecosystem
==========================
Phase 14 integration modules:

- **Key Vault**: resolve ``keyvault://`` secrets in configuration
- **Power BI**: generate thin dataset definitions from lineage graph
- **dbt**: scaffold dbt models from generated Spark notebooks
- **Webhooks**: notify external services on migration events
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path  # noqa: TC003 (used at runtime)
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# =====================================================================
# Azure Key Vault Secret Resolution
# =====================================================================

_KV_PATTERN = re.compile(r"^keyvault://(?P<vault>[^/]+)/(?P<secret>[^/]+)$")


@dataclass
class KeyVaultConfig:
    """Configuration for Azure Key Vault integration."""

    vault_url: str = ""
    credential_type: str = "default"  # "default", "client_secret", "managed_identity"
    cache_secrets: bool = True


def resolve_keyvault_reference(ref: str, kv_config: KeyVaultConfig | None = None) -> str | None:
    """Resolve a ``keyvault://vault-name/secret-name`` reference.

    Returns the secret value or ``None`` if not resolvable.
    In non-interactive environments, returns a placeholder.
    """
    match = _KV_PATTERN.match(ref)
    if not match:
        return None

    vault_name = match.group("vault")
    secret_name = match.group("secret")

    logger.info("keyvault_resolve", vault=vault_name, secret=secret_name)

    # Attempt real resolution if azure-identity is available
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        vault_url = f"https://{vault_name}.vault.azure.net/"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        secret = client.get_secret(secret_name)
        return secret.value
    except ImportError:
        logger.warning("keyvault_sdk_not_installed", hint="pip install azure-keyvault-secrets")
    except Exception as e:
        logger.warning("keyvault_resolve_failed", error=str(e))

    return f"<keyvault:{vault_name}/{secret_name}>"


def is_keyvault_reference(value: str) -> bool:
    """Check if a string is a Key Vault reference."""
    return bool(_KV_PATTERN.match(value))


def resolve_config_secrets(
    config_dict: dict[str, Any],
    kv_config: KeyVaultConfig | None = None,
) -> dict[str, Any]:
    """Walk a config dict and resolve any keyvault:// references."""
    resolved = {}
    for key, value in config_dict.items():
        if isinstance(value, str) and is_keyvault_reference(value):
            resolved[key] = resolve_keyvault_reference(value, kv_config) or value
        elif isinstance(value, dict):
            resolved[key] = resolve_config_secrets(value, kv_config)
        else:
            resolved[key] = value
    return resolved


# =====================================================================
# Power BI Dataset Generation
# =====================================================================


@dataclass
class PowerBITable:
    """A table definition for a Power BI thin dataset."""

    name: str
    source_type: str = "DirectQuery"
    columns: list[dict[str, str]] = field(default_factory=list)


@dataclass
class PowerBIDataset:
    """A Power BI thin dataset definition."""

    name: str
    tables: list[PowerBITable] = field(default_factory=list)
    relationships: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "defaultMode": "DirectQuery",
            "tables": [
                {
                    "name": t.name,
                    "source_type": t.source_type,
                    "columns": t.columns,
                }
                for t in self.tables
            ],
            "relationships": self.relationships,
        }


def generate_powerbi_dataset_from_lineage(
    lineage_dict: dict[str, Any],
    dataset_name: str = "Migration Dataset",
) -> PowerBIDataset:
    """Generate a Power BI dataset definition from a lineage graph.

    Uses destination tables from the lineage graph as the dataset tables.
    """
    dataset = PowerBIDataset(name=dataset_name)

    writers = lineage_dict.get("writers", {})
    for table_name in sorted(writers.keys()):
        pbi_table = PowerBITable(name=table_name)
        dataset.tables.append(pbi_table)

    logger.info("powerbi_dataset_generated", tables=len(dataset.tables))
    return dataset


def write_powerbi_dataset(dataset: PowerBIDataset, output_dir: Path) -> Path:
    """Write a Power BI dataset definition to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{dataset.name.replace(' ', '_')}.dataset.json"
    path.write_text(json.dumps(dataset.to_dict(), indent=2), encoding="utf-8")
    return path


# =====================================================================
# dbt Model Scaffolding
# =====================================================================


@dataclass
class DbtModel:
    """A scaffold for a dbt model."""

    name: str
    sql: str
    description: str = ""
    materialized: str = "table"
    tags: list[str] = field(default_factory=list)


def scaffold_dbt_models_from_notebooks(notebooks_dir: Path) -> list[DbtModel]:
    """Scaffold dbt model stubs from generated Spark notebooks.

    Extracts table references and SQL patterns from notebook code
    to generate initial dbt model SQL and YAML entries.
    """
    models: list[DbtModel] = []

    if not notebooks_dir.exists():
        return models

    for nb_file in sorted(notebooks_dir.glob("*.py")):
        content = nb_file.read_text(encoding="utf-8")
        model_name = nb_file.stem.lower().replace(" ", "_")

        # Extract SQL queries from spark.sql(...) calls
        sql_matches = re.findall(r'spark\.sql\(\s*"""(.*?)"""', content, re.DOTALL)
        if not sql_matches:
            sql_matches = re.findall(r"spark\.sql\(\s*'([^']+)'", content)

        if sql_matches:
            sql = sql_matches[0].strip()
        else:
            # Generate a stub SELECT from source tables found in the notebook
            read_matches = re.findall(r'\.read\.\w+\("([^"]+)"\)', content)
            if read_matches:
                sql = f"SELECT * FROM {{{{ source('raw', '{read_matches[0]}') }}}}"
            else:
                sql = f"-- TODO: Convert notebook {nb_file.name} to SQL\nSELECT 1"

        model = DbtModel(
            name=model_name,
            sql=sql,
            description=f"Migrated from SSIS notebook: {nb_file.name}",
            tags=["ssis_migration"],
        )
        models.append(model)

    logger.info("dbt_models_scaffolded", count=len(models))
    return models


def write_dbt_models(models: list[DbtModel], output_dir: Path) -> list[Path]:
    """Write dbt model files to disk."""
    models_dir = output_dir / "models" / "ssis_migration"
    models_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for model in models:
        # Write SQL model file
        header = (
            "-- dbt model scaffolded from SSIS migration\n"
            f"-- Source: {model.description}\n"
            f"-- Materialization: {model.materialized}\n\n"
            f"{{{{ config(materialized='{model.materialized}') }}}}\n\n"
        )
        sql_path = models_dir / f"{model.name}.sql"
        sql_path.write_text(header + model.sql + "\n", encoding="utf-8")
        paths.append(sql_path)

    # Write schema.yml
    schema = {
        "version": 2,
        "models": [
            {
                "name": m.name,
                "description": m.description,
                "config": {"tags": m.tags},
            }
            for m in models
        ],
    }
    schema_path = models_dir / "schema.yml"
    import yaml

    schema_path.write_text(yaml.dump(schema, default_flow_style=False), encoding="utf-8")
    paths.append(schema_path)

    return paths


# =====================================================================
# Webhook Notifications
# =====================================================================


@dataclass
class WebhookConfig:
    """Configuration for webhook notifications."""

    url: str
    headers: dict[str, str] = field(default_factory=dict)
    events: list[str] = field(default_factory=lambda: ["migration_complete", "deployment_complete", "error"])


class WebhookNotifier:
    """Send notifications to external services via webhooks."""

    def __init__(self, configs: list[WebhookConfig] | None = None) -> None:
        self._configs = configs or []

    def add_webhook(self, config: WebhookConfig) -> None:
        self._configs.append(config)

    def notify(self, event: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Send a notification to all registered webhooks that listen for this event.

        Returns a list of response summaries.
        """
        results = []
        for config in self._configs:
            if event not in config.events:
                continue

            body = {
                "event": event,
                "source": "ssis2fabric",
                **payload,
            }

            try:
                import requests

                resp = requests.post(
                    config.url,
                    json=body,
                    headers=config.headers,
                    timeout=10,
                )
                results.append({
                    "url": config.url,
                    "status": resp.status_code,
                    "success": resp.ok,
                })
                logger.info("webhook_sent", url=config.url, status=resp.status_code)
            except Exception as e:
                results.append({
                    "url": config.url,
                    "status": 0,
                    "success": False,
                    "error": str(e),
                })
                logger.warning("webhook_failed", url=config.url, error=str(e))

        return results

    @property
    def webhook_count(self) -> int:
        return len(self._configs)

    def to_dict(self) -> list[dict[str, Any]]:
        return [
            {"url": c.url, "events": c.events}
            for c in self._configs
        ]


# =====================================================================
# Slack / Teams Message Formatting
# =====================================================================


def format_slack_message(event: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Format a webhook payload as a Slack message."""
    text = f"*[ssis2fabric]* {event}"
    if "project_name" in payload:
        text += f" — {payload['project_name']}"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
    ]

    details = payload.get("summary", payload)
    if isinstance(details, dict):
        fields_text = "\n".join(f"• *{k}*: {v}" for k, v in list(details.items())[:10])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": fields_text},
        })

    return {"blocks": blocks, "text": text}


def format_teams_message(event: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Format a webhook payload as a Microsoft Teams Adaptive Card."""
    facts = []
    details = payload.get("summary", payload)
    if isinstance(details, dict):
        facts = [{"name": k, "value": str(v)} for k, v in list(details.items())[:10]]

    return {
        "@type": "MessageCard",
        "summary": f"ssis2fabric: {event}",
        "themeColor": "0076D7",
        "title": f"ssis2fabric — {event}",
        "sections": [
            {
                "facts": facts,
                "markdown": True,
            },
        ],
    }


# =====================================================================
# Init Config Generator
# =====================================================================

_DEFAULT_CONFIG_TEMPLATE = """\
# SSIS to Fabric Migration Configuration
# Generated by: ssis2fabric init
# Documentation: https://github.com/cyphou/SSIStoFabric

project_name: {project_name}
strategy: hybrid                      # data_factory | spark | hybrid
dataflow_type: notebook               # notebook | dataflow_gen2
output_dir: output
log_level: INFO

source:
  catalog_server: ""
  catalog_database: SSISDB
  catalog_user: ""
  catalog_password: ""                # or keyvault://vault-name/secret-name
  packages_path: "{packages_path}"

fabric:
  workspace_id: "{workspace_id}"
  workspace_name: ""
  capacity_id: ""

azure:
  tenant_id: ""
  client_id: ""
  client_secret: ""                   # or keyvault://vault-name/secret-name
  subscription_id: ""
  resource_group: ""

data_factory:
  factory_name: ""
  linked_service_name: ""

connection_mappings: {{}}

retry:
  max_retries: 3
  base_delay: 1.0
  max_delay: 60.0

regression:
  baseline_dir: tests/regression/baselines
  tolerance_row_count: 0.0
  tolerance_numeric: 0.0001
  sample_size: 10000
"""


def generate_init_config(
    project_name: str = "my-ssis-migration",
    packages_path: str = "",
    workspace_id: str = "",
) -> str:
    """Generate a starter migration_config.yaml content."""
    return _DEFAULT_CONFIG_TEMPLATE.format(
        project_name=project_name,
        packages_path=packages_path,
        workspace_id=workspace_id,
    )


def write_init_config(
    output_path: Path,
    project_name: str = "my-ssis-migration",
    packages_path: str = "",
    workspace_id: str = "",
) -> Path:
    """Write a starter migration_config.yaml to disk."""
    content = generate_init_config(project_name, packages_path, workspace_id)
    output_path.write_text(content, encoding="utf-8")
    logger.info("init_config_written", path=str(output_path))
    return output_path
