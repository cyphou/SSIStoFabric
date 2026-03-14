"""Phase 14 – Integrations & Ecosystem tests.

Tests cover:
- Key Vault reference resolution
- Power BI dataset generation
- dbt model scaffolding
- Webhook notifications
- Slack/Teams message formatting
- Init config generation
- CLI commands (init, dbt-scaffold, powerbi-dataset)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from ssis_to_fabric.engine.integrations import (
    DbtModel,
    PowerBIDataset,
    PowerBITable,
    WebhookConfig,
    WebhookNotifier,
    format_slack_message,
    format_teams_message,
    generate_init_config,
    generate_powerbi_dataset_from_lineage,
    is_keyvault_reference,
    resolve_config_secrets,
    resolve_keyvault_reference,
    scaffold_dbt_models_from_notebooks,
    write_dbt_models,
    write_init_config,
    write_powerbi_dataset,
)

if TYPE_CHECKING:
    from pathlib import Path


# ── Key Vault ────────────────────────────────────────────────────


class TestKeyVault:
    def test_is_keyvault_reference_valid(self):
        assert is_keyvault_reference("keyvault://myvault/mysecret")

    def test_is_keyvault_reference_invalid(self):
        assert not is_keyvault_reference("not-a-ref")
        assert not is_keyvault_reference("keyvault://")
        assert not is_keyvault_reference("")

    def test_resolve_non_reference(self):
        assert resolve_keyvault_reference("plain-string") is None

    def test_resolve_returns_placeholder(self):
        # Without real Azure credentials, returns a placeholder
        result = resolve_keyvault_reference("keyvault://vault/secret")
        assert result is not None
        assert "vault" in result
        assert "secret" in result

    def test_resolve_config_secrets(self):
        config = {
            "source": {
                "password": "keyvault://myvault/dbpass",
                "server": "myserver.database.windows.net",
            },
            "azure": {
                "client_secret": "keyvault://myvault/appsecret",
            },
            "output_dir": "output",
        }
        resolved = resolve_config_secrets(config)
        assert resolved["source"]["server"] == "myserver.database.windows.net"
        assert resolved["output_dir"] == "output"
        # keyvault refs should be resolved (to placeholder without real credentials)
        assert "keyvault://" not in str(resolved["source"]["password"])
        assert "keyvault://" not in str(resolved["azure"]["client_secret"])

    def test_resolve_no_keyvault_refs(self):
        config = {"key": "value", "nested": {"a": "b"}}
        assert resolve_config_secrets(config) == config


# ── Power BI Dataset ─────────────────────────────────────────────


class TestPowerBIDataset:
    def test_generate_from_lineage(self):
        lineage = {
            "writers": {
                "dbo.FactSales": ["Package1"],
                "dbo.DimCustomer": ["Package2"],
            },
            "readers": {
                "staging.Sales": ["Package1"],
            },
        }
        dataset = generate_powerbi_dataset_from_lineage(lineage, "TestDS")
        assert dataset.name == "TestDS"
        assert len(dataset.tables) == 2
        assert {t.name for t in dataset.tables} == {"dbo.DimCustomer", "dbo.FactSales"}

    def test_generate_empty_lineage(self):
        dataset = generate_powerbi_dataset_from_lineage({})
        assert len(dataset.tables) == 0

    def test_to_dict(self):
        dataset = PowerBIDataset(
            name="Test",
            tables=[PowerBITable(name="T1")],
        )
        d = dataset.to_dict()
        assert d["name"] == "Test"
        assert d["defaultMode"] == "DirectQuery"
        assert len(d["tables"]) == 1

    def test_write_dataset(self, tmp_path: Path):
        dataset = PowerBIDataset(name="My Dataset", tables=[PowerBITable(name="T1")])
        path = write_powerbi_dataset(dataset, tmp_path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["name"] == "My Dataset"


# ── dbt Scaffolding ──────────────────────────────────────────────


class TestDbtScaffolding:
    def test_scaffold_from_notebooks(self, tmp_path: Path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "load_sales.py").write_text(
            '# Notebook\n'
            'df = spark.sql("""SELECT * FROM staging.sales""")\n'
            'df.write.saveAsTable("dbo.fact_sales")\n'
        )
        models = scaffold_dbt_models_from_notebooks(nb_dir)
        assert len(models) == 1
        assert models[0].name == "load_sales"
        assert "staging.sales" in models[0].sql

    def test_scaffold_with_read_pattern(self, tmp_path: Path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "etl.py").write_text(
            'df = spark.read.format("delta").load("raw_data")\n'
        )
        models = scaffold_dbt_models_from_notebooks(nb_dir)
        assert len(models) == 1
        # Should attempt a source reference
        assert "source(" in models[0].sql or "raw_data" in models[0].sql

    def test_scaffold_fallback(self, tmp_path: Path):
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "cleanup.py").write_text("# Just a cleanup script\nprint('done')\n")
        models = scaffold_dbt_models_from_notebooks(nb_dir)
        assert len(models) == 1
        assert "TODO" in models[0].sql

    def test_scaffold_empty_dir(self, tmp_path: Path):
        models = scaffold_dbt_models_from_notebooks(tmp_path / "nonexistent")
        assert models == []

    def test_write_dbt_models(self, tmp_path: Path):
        models = [
            DbtModel(name="m1", sql="SELECT 1", description="test model"),
            DbtModel(name="m2", sql="SELECT 2"),
        ]
        paths = write_dbt_models(models, tmp_path)
        # Should have 2 SQL files + 1 schema.yml
        assert len(paths) == 3
        assert any(p.name == "m1.sql" for p in paths)
        assert any(p.name == "schema.yml" for p in paths)

        # Verify SQL content
        m1 = next(p for p in paths if p.name == "m1.sql")
        content = m1.read_text()
        assert "SELECT 1" in content
        assert "config(" in content

        # Verify schema.yml
        schema = next(p for p in paths if p.name == "schema.yml")
        import yaml

        data = yaml.safe_load(schema.read_text())
        assert data["version"] == 2
        assert len(data["models"]) == 2


# ── Webhook Notifications ────────────────────────────────────────


class TestWebhookNotifier:
    def test_add_webhook(self):
        notifier = WebhookNotifier()
        notifier.add_webhook(WebhookConfig(url="https://example.com/hook"))
        assert notifier.webhook_count == 1

    def test_notify_filters_events(self):
        notifier = WebhookNotifier([
            WebhookConfig(url="https://example.com/hook", events=["error"]),
        ])

        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.status_code = 200
            mock_post.return_value = mock_resp

            # This event is not subscribed
            results = notifier.notify("migration_complete", {"status": "ok"})
            assert len(results) == 0
            mock_post.assert_not_called()

    def test_notify_sends_to_matching(self):
        notifier = WebhookNotifier([
            WebhookConfig(url="https://example.com/hook", events=["migration_complete"]),
        ])

        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.status_code = 200
            mock_post.return_value = mock_resp

            results = notifier.notify("migration_complete", {"packages": 5})
            assert len(results) == 1
            assert results[0]["success"] is True

    def test_notify_handles_failure(self):
        notifier = WebhookNotifier([
            WebhookConfig(url="https://example.com/hook"),
        ])

        with patch("requests.post", side_effect=ConnectionError("timeout")):
            results = notifier.notify("error", {"msg": "fail"})
            assert len(results) == 1
            assert results[0]["success"] is False

    def test_to_dict(self):
        notifier = WebhookNotifier([
            WebhookConfig(url="https://a.com", events=["e1"]),
        ])
        d = notifier.to_dict()
        assert len(d) == 1
        assert d[0]["url"] == "https://a.com"


# ── Slack / Teams Formatting ─────────────────────────────────────


class TestMessageFormatting:
    def test_slack_message(self):
        msg = format_slack_message("migration_complete", {
            "project_name": "TestProject",
            "summary": {"packages": 5, "pipelines": 3},
        })
        assert "blocks" in msg
        assert "text" in msg
        assert "TestProject" in msg["text"]

    def test_slack_message_no_project(self):
        msg = format_slack_message("error", {"detail": "something failed"})
        assert "ssis2fabric" in msg["text"]

    def test_teams_message(self):
        msg = format_teams_message("deployment_complete", {
            "summary": {"workspace": "ws1", "items": 10},
        })
        assert msg["@type"] == "MessageCard"
        assert "deployment_complete" in msg["title"]
        assert len(msg["sections"][0]["facts"]) == 2

    def test_teams_message_empty_payload(self):
        msg = format_teams_message("test", {})
        assert msg["@type"] == "MessageCard"


# ── Init Config ──────────────────────────────────────────────────


class TestInitConfig:
    def test_generate_default(self):
        content = generate_init_config()
        assert "project_name: my-ssis-migration" in content
        assert "strategy: hybrid" in content
        assert "keyvault://" in content  # reference comment

    def test_generate_custom(self):
        content = generate_init_config(
            project_name="myproj",
            packages_path="/path/to/packages",
            workspace_id="ws-123",
        )
        assert "project_name: myproj" in content
        assert "packages_path: \"/path/to/packages\"" in content
        assert "workspace_id: \"ws-123\"" in content

    def test_write_init_config(self, tmp_path: Path):
        path = tmp_path / "config.yaml"
        result = write_init_config(path)
        assert result == path
        assert path.exists()
        content = path.read_text()
        assert "project_name:" in content

    def test_parseable_yaml(self, tmp_path: Path):
        import yaml

        path = tmp_path / "config.yaml"
        write_init_config(path, project_name="test")
        data = yaml.safe_load(path.read_text())
        assert data["project_name"] == "test"
        assert data["strategy"] == "hybrid"


# ── CLI Commands ─────────────────────────────────────────────────


class TestCLIIntegrationCommands:
    def test_init_command_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["init", "--help"])
        assert result.exit_code == 0
        assert "starter migration_config.yaml" in result.output

    def test_init_creates_file(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        out = str(tmp_path / "config.yaml")
        runner = CliRunner()
        result = runner.invoke(main, ["init", "--output", out, "--project-name", "test"])
        assert result.exit_code == 0
        assert "Created" in result.output

    def test_init_refuses_overwrite(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        out = tmp_path / "config.yaml"
        out.write_text("existing")

        runner = CliRunner()
        result = runner.invoke(main, ["init", "--output", str(out)])
        assert result.exit_code == 1
        assert "already exists" in result.output

    def test_dbt_scaffold_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["dbt-scaffold", "--help"])
        assert result.exit_code == 0
        assert "dbt models" in result.output

    def test_dbt_scaffold_no_notebooks(self, tmp_path: Path):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["dbt-scaffold", str(tmp_path)])
        assert result.exit_code == 0
        assert "No notebooks" in result.output

    def test_powerbi_dataset_registered(self):
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["powerbi-dataset", "--help"])
        assert result.exit_code == 0
        assert "Power BI" in result.output
