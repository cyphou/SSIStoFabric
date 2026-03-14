"""Tests for Phase 25 — Enterprise Connectors."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.connectors import (
    ConnectorType,
    FabricConnectorType,
    SSISConnection,
    detect_connector_type,
    generate_shortcut,
    migrate_connection,
    write_connector_report,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestConnectorDetection:
    def test_oracle_provider(self):
        assert detect_connector_type("", "OraOLEDB.Oracle") == ConnectorType.ORACLE

    def test_teradata_provider(self):
        assert detect_connector_type("", "TDOLEDB") == ConnectorType.TERADATA

    def test_db2_provider(self):
        assert detect_connector_type("", "IBMOLEDB.DB2") == ConnectorType.DB2

    def test_sap_provider(self):
        assert detect_connector_type("", "SAPNWRFC") == ConnectorType.SAP

    def test_oracle_connection_string(self):
        assert detect_connector_type("Data Source=Oracle;") == ConnectorType.ORACLE

    def test_s3_connection_string(self):
        assert detect_connector_type("s3://my-bucket/data") == ConnectorType.S3

    def test_gcs_connection_string(self):
        assert detect_connector_type("gs://my-bucket/data") == ConnectorType.GCS

    def test_bigquery(self):
        assert detect_connector_type("bigquery://project") == ConnectorType.BIGQUERY

    def test_rest_api(self):
        assert detect_connector_type("https://api.example.com/data") == ConnectorType.REST_API

    def test_salesforce(self):
        assert detect_connector_type("salesforce://login") == ConnectorType.SALESFORCE

    def test_default_oledb(self):
        assert detect_connector_type("Server=db;Database=test") == ConnectorType.OLEDB


class TestConnectionMigration:
    def test_oracle_migration(self):
        conn = SSISConnection(name="ora_src", connection_type=ConnectorType.ORACLE, server="orahost")
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.ORACLE
        assert len(fabric.notes) > 0

    def test_sap_migration(self):
        conn = SSISConnection(name="sap_src", connection_type=ConnectorType.SAP)
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.SAP_TABLE
        assert fabric.credential_type == "service_principal"

    def test_salesforce_migration(self):
        conn = SSISConnection(name="sf", connection_type=ConnectorType.SALESFORCE)
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.SALESFORCE

    def test_s3_migration(self):
        conn = SSISConnection(name="s3_data", connection_type=ConnectorType.S3)
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.S3
        assert any("OneLake" in n for n in fabric.notes)

    def test_rest_api_migration(self):
        conn = SSISConnection(name="api", connection_type=ConnectorType.REST_API, server="https://api.test.com")
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.REST

    def test_oledb_default(self):
        conn = SSISConnection(name="sql", connection_type=ConnectorType.OLEDB, server="sqlhost", database="db")
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.SQL_SERVER

    def test_azure_sql_detection(self):
        conn = SSISConnection(
            name="azsql", connection_type=ConnectorType.OLEDB,
            connection_string="Server=mydb.database.azure.windows.net",
        )
        fabric = migrate_connection(conn)
        assert fabric.connector_type == FabricConnectorType.AZURE_SQL
        assert fabric.credential_type == "managed_identity"


class TestOneLakeShortcut:
    def test_s3_shortcut(self):
        conn = SSISConnection(name="s3", connection_type=ConnectorType.S3)
        shortcut = generate_shortcut(conn)
        assert shortcut is not None
        assert shortcut.source_type == "S3"

    def test_gcs_shortcut(self):
        conn = SSISConnection(name="gcs", connection_type=ConnectorType.GCS)
        shortcut = generate_shortcut(conn)
        assert shortcut is not None
        assert shortcut.source_type == "GCS"

    def test_no_shortcut_for_oledb(self):
        conn = SSISConnection(name="sql", connection_type=ConnectorType.OLEDB)
        assert generate_shortcut(conn) is None


class TestConnectorReport:
    def test_write_report(self, tmp_path: Path):
        conn = SSISConnection(name="c1", connection_type=ConnectorType.ORACLE)
        fabric = migrate_connection(conn)
        path = write_connector_report([(conn, fabric)], tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["total_connections"] == 1


class TestCLI:
    def test_connector_map_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["connector-map", "--help"])
        assert result.exit_code == 0
