# Fabric Notebook
# Migrated from SSIS Package: StgAddress
# Task: dft_Staging_StgAddress
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

LOADEXECUTIONID = _get_param("LoadExecutionId", "0")

# --- Fabric Connections ---
# Map SSIS connection managers to Fabric connection IDs.
# Update the IDs below to match your Fabric workspace connections.
# (same IDs used by Data Factory pipeline externalReferences)
_FABRIC_CONNECTIONS = {
    "cmgr_AsureStorage_ssiscookbook": "cmgr_AsureStorage_ssiscookbook  -- TODO: replace with Fabric connection id",  # UNKNOWN
    "cmgr_DW": "cmgr_DW  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2012
    "cmgr_MDS_DB": "cmgr_MDS_DB  -- TODO: replace with Fabric connection id",  # OLEDB / . / MDS_DB
    "cmgr_Source": "cmgr_Source  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2012
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

# Source: ole_src_SalesLT_Address
# Uses Fabric connection (same as pipeline externalReferences)
df_source_ole_src_saleslt_address = spark.read.format("jdbc") \
    .option("url", _jdbc_url_for("cmgr_Source")) \
    .option("query", """SELECT        AddressID, AddressLine1, AddressLine2, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
FROM            SalesLT.Address
ORDER BY AddressID""") \
    .load()
logger.info(f"Read {{count}} rows from ole_src_SalesLT_Address", count=df_source_ole_src_saleslt_address.count())

df = df_source_ole_src_saleslt_address

# === Transformations ===

# Data Conversion: dcnv_Unicode_Strings
# TODO: Cast columns to target data types
# df = df.withColumn("AddressLine1", df["AddressLine1"].cast("str"))
# df = df.withColumn("AddressLine2", df["AddressLine2"].cast("str"))
# df = df.withColumn("City", df["City"].cast("str"))
# df = df.withColumn("StateProvince", df["StateProvince"].cast("str"))
# df = df.withColumn("CountryRegion", df["CountryRegion"].cast("str"))
# df = df.withColumn("PostalCode", df["PostalCode"].cast("str"))

# Derived Column: der_LoadExecutionId_PostalCode
df = df.withColumn("LoadExecutionId", F.lit(LOADEXECUTIONID))  # SSIS: @[$Package::LoadExecutionId]

# === Destination Writes ===

# Destination: ole_dst_Staging_StgAddress -> Table: [Staging].[StgAddress]
# Connection: cmgr_DW (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("cmgr_DW")) \
    .option("dbtable", "[Staging].[StgAddress]") \
    .save()
logger.info("Data written to [Staging].[StgAddress] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: dft_Staging_StgAddress")
print("Migration notebook execution complete: dft_Staging_StgAddress")
