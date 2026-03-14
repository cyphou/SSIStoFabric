# Fabric Notebook
# Migrated from SSIS Package: ProcessingExcel
# Task: ProcessExcelOrders
# Task Type: DATA_FLOW
# Generated: 2026-03-14 06:05:45 UTC
# Migration Complexity: MEDIUM
#
# NOTE: Review all TODO comments before running in production.
# This notebook was auto-generated and may require adjustments.


# --- Imports ---
import logging

from pyspark.sql.types import *

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
    "Excel_Ch07_Orders": "Excel_Ch07_Orders  -- TODO: replace with Fabric connection id",  # EXCEL / C:\SSIS2016Cookbook\Chapter07\Files\Ch07_Orders.xls
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

# Source: Excel Source (Excel)
df_source_excel_source = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("inferSchema", "true") \
    .load("TODO_FILE_PATH")
logger.info(f"Read {count} rows from Excel sheet Sheet1", count=df_source_excel_source.count())


df = df_source_excel_source

# === Transformations ===

# Multicast: Multicast
# In Spark, simply reuse the same DataFrame for multiple outputs
df_output_1 = df  # Branch 1
df_output_2 = df  # Branch 2


# Unknown Transform: Script Component (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'SourceCode': '\n                  ', 'BinaryCode': '\n                  ', 'VSTAProjectName': 'SC_e79bd37d76a140e690088c852af9ee3f', 'ScriptLanguage': 'CSharp', 'ReadOnlyVariables': '', 'ReadWriteVariables': '', 'BreakpointCollection': '\n                  ', 'MetadataChecksum': '0585368d16f252e8d2305c29a8de9a54', 'MetadataShaChecksum': '1737ee7f37998a3b734093e4c2f985914754c425', 'UserComponentTypeName': 'Microsoft.ScriptComponentHost', 'MetadataChecksum140': 'D0'}


# === Completion ===
logger.info("Notebook completed: ProcessExcelOrders")
print("Migration notebook execution complete: ProcessExcelOrders")
