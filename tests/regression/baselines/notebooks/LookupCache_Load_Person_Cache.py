# Fabric Notebook
# Migrated from SSIS Package: LookupCache
# Task: Load Person Cache
# Task Type: DATA_FLOW
# Generated: 2026-03-14 06:05:45 UTC
# Migration Complexity: MEDIUM
#
# NOTE: Review all TODO comments before running in production.
# This notebook was auto-generated and may require adjustments.


# --- Imports ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Fabric-specific utilities (available in Fabric notebook runtime)
try:
    import notebookutils
except ImportError:
    notebookutils = None  # type: ignore[assignment]


# --- Parameters (from pipeline or Variable Library) ---
# When run from a pipeline, parameters are injected by the
# TridentNotebook activity.  When run standalone, defaults are used.

try:
    _ctx = notebookutils.notebook.getContext()
    _params = _ctx.get('currentRunConfig', {}).get('parameters', {})
except Exception:
    _params = {}


def _get_param(name: str, default: str = '') -> str:
    """Read a parameter from the pipeline context, falling back to the default."""
    return _params.get(name, default)


# --- Fabric Connections ---
# Map SSIS connection managers to Fabric connection IDs.
# Update the IDs below to match your Fabric workspace connections.
# (same IDs used by Data Factory pipeline externalReferences)
_FABRIC_CONNECTIONS = {
    "cmgr_DW": "cmgr_DW  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2016
    "cmgr_Reference": "cmgr_Reference  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorks2014
    "cmgr_Source": "cmgr_Source  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLT
    "cmgr_SourceFile": "cmgr_SourceFile  -- TODO: replace with Fabric connection id",  # FLAT_FILE
    "PersonCache": "PersonCache  -- TODO: replace with Fabric connection id",  # UNKNOWN
    "cmgr_AsureStorage_ssiscookbook": "cmgr_AsureStorage_ssiscookbook  -- TODO: replace with Fabric connection id",  # UNKNOWN
    "cmgr_MDS_DB": "cmgr_MDS_DB  -- TODO: replace with Fabric connection id",  # OLEDB / . / MDS_DB
}


def _get_connection_id(conn_name: str) -> str:
    """Get a Fabric connection ID for an SSIS connection manager."""
    conn_id = _FABRIC_CONNECTIONS.get(conn_name, "")
    if not conn_id or "TODO" in conn_id:
        raise ValueError(
            f"Fabric connection not configured for: {conn_name}. "
            f"Update _FABRIC_CONNECTIONS dict with the Fabric connection ID."
        )
    return conn_id


def _jdbc_url_for(conn_name: str) -> str:
    """Get JDBC connection string from a Fabric connection."""
    conn_id = _get_connection_id(conn_name)
    return notebookutils.credentials.getConnectionStringOrCreds(conn_id)


# === Source Data Reads ===

# Source: Person Data
# Uses Fabric connection (same as pipeline externalReferences)
df_source_person_data = spark.read.format("jdbc") \
    .option("url", _jdbc_url_for("cmgr_Reference")) \
    .option("query", """SELECT CAST(Person.LastName + COALESCE(N' ' + Person.MiddleName, N'') + N', ' + Person.FirstName AS NVARCHAR(256)) as ClientName
       ,Person.BusinessEntityID AS BusinessEntityID
	   ,CAST(1 AS BIT) AS IsPerson
  FROM Person.Person;""") \
    .load()
logger.info(f"Read {{count}} rows from Person Data", count=df_source_person_data.count())

df = df_source_person_data

# === Transformations ===

# Unknown Transform: Person Cache (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'TreatDuplicateKeysAsError': 'false', 'CacheColumnName': 'IsPerson'}


# === Completion ===
logger.info("Notebook completed: Load Person Cache")
print("Migration notebook execution complete: Load Person Cache")
