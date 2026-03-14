# Fabric Notebook
# Migrated from SSIS Package: ExecutionTrees
# Task: dft_DW_DimOrderProvenance
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

LOADDATE = _get_param("LoadDate", "1/1/1900")
LOADID = _get_param("LoadID", "0")
SOURCELOADID = _get_param("SourceLoadID", "0")

# --- Fabric Connections ---
# Map SSIS connection managers to Fabric connection IDs.
# Update the IDs below to match your Fabric workspace connections.
# (same IDs used by Data Factory pipeline externalReferences)
_FABRIC_CONNECTIONS = {
    "cmgr_DW": "cmgr_DW  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2016
    "cmgr_AsureStorage_ssiscookbook": "cmgr_AsureStorage_ssiscookbook  -- TODO: replace with Fabric connection id",  # UNKNOWN
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


# === Transformations ===

# Derived Column: der_LoadID
df = df.withColumn("LoadID", F.lit(INTLOADID))  # SSIS: @[SystemLog::intLoadID]

# Lookup: lkp_DW_DimOrderProvenance_allcols
df_lookup = spark.sql("SELECT * FROM lookup_table")  # TODO: verify lookup query
df = df.join(
    df_lookup,
    on=(df["Description"] == df_lookup["ProvenanceDescription"]) & (df["IDOrderProvenance"] == df_lookup["IDOrderProvenance"]),
    how="left"       # Match type: left = partial match, inner = full match
)


# Lookup: lkp_DW_DimOrderProvenance_code
df_lookup = spark.sql("SELECT * FROM lookup_table")  # TODO: verify lookup query
df = df.join(
    df_lookup,
    on=(df["Code"] == df_lookup["ProvenanceCode"]),
    how="left"       # Match type: left = partial match, inner = full match
)


            # OLE DB Command: ole_cmd_upd_Type1Cols
            # TODO: Translate row-by-row command execution to batch Spark operation
            # Original SQL: UPDATE [DW].[DimOrderProvenance]
SET [ProvenanceDescription] = ?
WHERE [IDOrderProvenance] = ?
            # Consider using DataFrame operations instead of row-by-row processing
            df.createOrReplaceTempView("temp_ole_cmd_upd_type1cols")
            spark.sql("""
                UPDATE [DW].[DimOrderProvenance]
SET [ProvenanceDescription] = ?
WHERE [IDOrderProvenance] = ?
            """)


# Unknown Transform: scr_src_OrderProvenance (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'SourceCode': '\n                  ', 'BinaryCode': '\n                  ', 'VSTAProjectName': 'SC_cf93d7c1fe964f5fb6ade407b2d79551', 'ScriptLanguage': 'CSharp', 'ReadOnlyVariables': '', 'ReadWriteVariables': '', 'BreakpointCollection': '\n                  ', 'MetadataChecksum': 'ebf5b2f547ef9f7f6c165095247e8cff', 'UserComponentTypeName': 'Microsoft.ScriptComponentHost', 'MetadataShaChecksum': 'fd3f409473669f432cfc17025d3e09293e0cb0c3', 'MetadataChecksum140': '18'}


# === Destination Writes ===

# Destination: ole_dst_DW_DimOrderProvenance -> Table: [DW].[DimOrderProvenance]
# Connection: cmgr_DW (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("cmgr_DW")) \
    .option("dbtable", "[DW].[DimOrderProvenance]") \
    .save()
logger.info("Data written to [DW].[DimOrderProvenance] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: dft_DW_DimOrderProvenance")
print("Migration notebook execution complete: dft_DW_DimOrderProvenance")
