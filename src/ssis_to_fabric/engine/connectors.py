"""
Enterprise Connectors
======================
Phase 25: SAP, Salesforce, Oracle/Teradata/DB2, cloud-native (S3/GCS/BigQuery),
and REST API migration support.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Connector Types
# =====================================================================


class ConnectorType(str, Enum):
    SAP = "sap"
    SALESFORCE = "salesforce"
    ORACLE = "oracle"
    TERADATA = "teradata"
    DB2 = "db2"
    S3 = "s3"
    GCS = "gcs"
    BIGQUERY = "bigquery"
    REST_API = "rest_api"
    OLEDB = "oledb"
    ODBC = "odbc"
    FLAT_FILE = "flat_file"
    EXCEL = "excel"
    UNKNOWN = "unknown"


class FabricConnectorType(str, Enum):
    """Target Fabric connector types."""
    SAP_HANA = "SapHana"
    SAP_TABLE = "SapTable"
    SALESFORCE = "Salesforce"
    DATAVERSE = "Dataverse"
    ORACLE = "Oracle"
    TERADATA = "Teradata"
    DB2 = "Db2"
    S3 = "AmazonS3"
    GCS = "GoogleCloudStorage"
    BIGQUERY = "GoogleBigQuery"
    REST = "RestService"
    HTTP = "HttpServer"
    ONELAKE = "OneLake"
    SQL_SERVER = "SqlServer"
    AZURE_SQL = "AzureSqlDatabase"
    LAKEHOUSE = "Lakehouse"


# =====================================================================
# Connection Mapping
# =====================================================================


@dataclass
class SSISConnection:
    """An SSIS connection manager to be migrated."""

    name: str
    connection_type: ConnectorType
    connection_string: str = ""
    server: str = ""
    database: str = ""
    provider: str = ""
    properties: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "connection_type": self.connection_type.value,
            "server": self.server,
            "database": self.database,
            "provider": self.provider,
        }


@dataclass
class FabricConnection:
    """A target Fabric connection/linked service."""

    name: str
    connector_type: FabricConnectorType
    config: dict[str, Any] = field(default_factory=dict)
    credential_type: str = "key"  # key, service_principal, managed_identity
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "connector_type": self.connector_type.value,
            "config": self.config,
            "credential_type": self.credential_type,
            "notes": self.notes,
        }


# =====================================================================
# Provider Detection
# =====================================================================


_PROVIDER_MAP: dict[str, ConnectorType] = {
    "sqlncli": ConnectorType.OLEDB,
    "msoledbsql": ConnectorType.OLEDB,
    "sqloledb": ConnectorType.OLEDB,
    "oraoledb": ConnectorType.ORACLE,
    "msdaora": ConnectorType.ORACLE,
    "oraodbc": ConnectorType.ORACLE,
    "tdoledb": ConnectorType.TERADATA,
    "ibmda400": ConnectorType.DB2,
    "ibmoledb": ConnectorType.DB2,
    "db2oledb": ConnectorType.DB2,
    "sapnwrfc": ConnectorType.SAP,
    "saphana": ConnectorType.SAP,
}

_FABRIC_CONNECTOR_MAP: dict[ConnectorType, FabricConnectorType] = {
    ConnectorType.OLEDB: FabricConnectorType.SQL_SERVER,
    ConnectorType.ORACLE: FabricConnectorType.ORACLE,
    ConnectorType.TERADATA: FabricConnectorType.TERADATA,
    ConnectorType.DB2: FabricConnectorType.DB2,
    ConnectorType.SAP: FabricConnectorType.SAP_TABLE,
    ConnectorType.SALESFORCE: FabricConnectorType.SALESFORCE,
    ConnectorType.S3: FabricConnectorType.S3,
    ConnectorType.GCS: FabricConnectorType.GCS,
    ConnectorType.BIGQUERY: FabricConnectorType.BIGQUERY,
    ConnectorType.REST_API: FabricConnectorType.REST,
    ConnectorType.FLAT_FILE: FabricConnectorType.LAKEHOUSE,
    ConnectorType.EXCEL: FabricConnectorType.LAKEHOUSE,
}


def detect_connector_type(connection_string: str, provider: str = "") -> ConnectorType:
    """Detect the connector type from connection string and provider."""
    provider_lower = provider.lower()
    for key, ct in _PROVIDER_MAP.items():
        if key in provider_lower:
            return ct

    cs_lower = connection_string.lower()
    if "oracle" in cs_lower:
        return ConnectorType.ORACLE
    if "teradata" in cs_lower:
        return ConnectorType.TERADATA
    if "db2" in cs_lower or "iseries" in cs_lower:
        return ConnectorType.DB2
    if "sap" in cs_lower:
        return ConnectorType.SAP
    if "salesforce" in cs_lower or "sfdc" in cs_lower:
        return ConnectorType.SALESFORCE
    if "s3://" in cs_lower or "amazonaws.com" in cs_lower:
        return ConnectorType.S3
    if "gs://" in cs_lower or "storage.googleapis.com" in cs_lower:
        return ConnectorType.GCS
    if "bigquery" in cs_lower:
        return ConnectorType.BIGQUERY
    if "http://" in cs_lower or "https://" in cs_lower:
        return ConnectorType.REST_API

    return ConnectorType.OLEDB


# =====================================================================
# Connection Migration
# =====================================================================


def migrate_connection(ssis_conn: SSISConnection) -> FabricConnection:
    """Migrate an SSIS connection to a Fabric connection."""
    target_type = _FABRIC_CONNECTOR_MAP.get(ssis_conn.connection_type, FabricConnectorType.SQL_SERVER)
    config: dict[str, Any] = {}
    notes: list[str] = []
    credential_type = "key"

    if ssis_conn.connection_type == ConnectorType.ORACLE:
        config = {
            "host": ssis_conn.server or "${ORACLE_HOST}",
            "port": 1521,
            "serviceName": ssis_conn.database or "${ORACLE_SID}",
        }
        notes.append("Install Oracle client driver on Fabric gateway")
        credential_type = "key"
    elif ssis_conn.connection_type == ConnectorType.SAP:
        config = {
            "server": ssis_conn.server or "${SAP_SERVER}",
            "systemNumber": "00",
            "clientId": "${SAP_CLIENT}",
        }
        notes.append("Configure SAP RFC connection via on-premises data gateway")
        credential_type = "service_principal"
    elif ssis_conn.connection_type == ConnectorType.SALESFORCE:
        config = {
            "environmentUrl": "https://login.salesforce.com",
            "apiVersion": "58.0",
        }
        notes.append("Use OAuth2 for Salesforce authentication")
        credential_type = "service_principal"
    elif ssis_conn.connection_type == ConnectorType.TERADATA:
        config = {
            "server": ssis_conn.server or "${TERADATA_HOST}",
            "database": ssis_conn.database,
        }
        notes.append("Install Teradata ODBC driver on gateway")
    elif ssis_conn.connection_type == ConnectorType.DB2:
        config = {
            "server": ssis_conn.server or "${DB2_HOST}",
            "database": ssis_conn.database,
            "port": 50000,
        }
        notes.append("Install IBM DB2 driver on gateway")
    elif ssis_conn.connection_type == ConnectorType.S3:
        config = {
            "bucketName": "${S3_BUCKET}",
            "region": "us-east-1",
        }
        target_type = FabricConnectorType.S3
        notes.append("Consider OneLake shortcut instead of direct S3 access")
        credential_type = "key"
    elif ssis_conn.connection_type == ConnectorType.GCS:
        config = {
            "bucketName": "${GCS_BUCKET}",
            "projectId": "${GCP_PROJECT}",
        }
        notes.append("Consider OneLake shortcut for GCS integration")
        credential_type = "service_principal"
    elif ssis_conn.connection_type == ConnectorType.BIGQUERY:
        config = {
            "projectId": "${GCP_PROJECT}",
            "datasetId": "${BQ_DATASET}",
        }
        credential_type = "service_principal"
    elif ssis_conn.connection_type == ConnectorType.REST_API:
        config = {
            "url": ssis_conn.server or "${API_BASE_URL}",
            "authenticationType": "Anonymous",
        }
        notes.append("Review API authentication requirements")
    else:
        config = {
            "server": ssis_conn.server,
            "database": ssis_conn.database,
        }
        if "azure" in (ssis_conn.connection_string or "").lower():
            target_type = FabricConnectorType.AZURE_SQL
            credential_type = "managed_identity"

    return FabricConnection(
        name=f"fabric_{ssis_conn.name}",
        connector_type=target_type,
        config=config,
        credential_type=credential_type,
        notes=notes,
    )


# =====================================================================
# OneLake Shortcut Generation
# =====================================================================


@dataclass
class OneLakeShortcut:
    """A Fabric OneLake shortcut definition."""

    name: str
    source_type: str  # S3, GCS, ADLS
    source_path: str
    target_lakehouse: str = ""
    target_path: str = ""
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "sourceType": self.source_type,
            "sourcePath": self.source_path,
            "targetLakehouse": self.target_lakehouse,
            "targetPath": self.target_path or f"Files/{self.name}",
            "config": self.config,
        }


def generate_shortcut(ssis_conn: SSISConnection) -> OneLakeShortcut | None:
    """Generate an OneLake shortcut for cloud storage connections."""
    if ssis_conn.connection_type == ConnectorType.S3:
        return OneLakeShortcut(
            name=f"shortcut_{ssis_conn.name}",
            source_type="S3",
            source_path="${S3_BUCKET}",
            config={"region": "us-east-1"},
        )
    elif ssis_conn.connection_type == ConnectorType.GCS:
        return OneLakeShortcut(
            name=f"shortcut_{ssis_conn.name}",
            source_type="GCS",
            source_path="${GCS_BUCKET}",
        )
    return None


# =====================================================================
# Report
# =====================================================================


def write_connector_report(
    mappings: list[tuple[SSISConnection, FabricConnection]],
    output_path: Path,
) -> Path:
    """Write connector migration report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_connections": len(mappings),
        "connectors_by_type": {},
        "mappings": [],
    }
    for ssis_c, fabric_c in mappings:
        ct = ssis_c.connection_type.value
        report["connectors_by_type"][ct] = report["connectors_by_type"].get(ct, 0) + 1
        report["mappings"].append({
            "source": ssis_c.to_dict(),
            "target": fabric_c.to_dict(),
        })
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("connector_report_written", path=str(output_path))
    return output_path
