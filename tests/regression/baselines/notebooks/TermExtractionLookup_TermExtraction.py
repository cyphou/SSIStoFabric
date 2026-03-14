# Fabric Notebook
# Migrated from SSIS Package: TermExtractionLookup
# Task: TermExtraction
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
    "(LOCAL).AdventureWorksDW2014": "(LOCAL).AdventureWorksDW2014  -- TODO: replace with Fabric connection id",  # OLEDB / (LOCAL) / AdventureWorksDW2014
    "BlogsTxt": "BlogsTxt  -- TODO: replace with Fabric connection id",  # FLAT_FILE
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

# Source: Flat File Source (Flat File)
df_source_flat_file_source = spark.read.csv(
    "TODO_FILE_PATH",
    header=True,
    inferSchema=True,
)
logger.info(f"Read {count} rows from flat file", count=df_source_flat_file_source.count())


df = df_source_flat_file_source

# === Transformations ===

# Multicast: MulticastBlogs
# In Spark, simply reuse the same DataFrame for multiple outputs
df_output_1 = df  # Branch 1
df_output_2 = df  # Branch 2


# Unknown Transform: Term Extraction (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'NeedReferenceData': 'false', 'OutTermTable': '', 'OutTermColumn': '', 'WordOrPhrase': '2', 'ScoreType': '0', 'FrequencyThreshold': '2', 'MaxLengthOfTerm': '12', 'IsCaseSensitive': 'false'}


# === Destination Writes ===

# Destination: Blogs -> Table: [Blogs]
# Connection: (LOCAL).AdventureWorksDW2014 (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("dbtable", "[Blogs]") \
    .save()
logger.info("Data written to [Blogs] via Fabric connection")

# Destination: Terms -> Table: [Terms]
# Connection: (LOCAL).AdventureWorksDW2014 (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("dbtable", "[Terms]") \
    .save()
logger.info("Data written to [Terms] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: TermExtraction")
print("Migration notebook execution complete: TermExtraction")
