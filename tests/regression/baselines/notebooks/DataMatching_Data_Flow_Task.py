# Fabric Notebook
# Migrated from SSIS Package: DataMatching
# Task: Data Flow Task
# Task Type: DATA_FLOW
# Generated: 2026-03-14 06:05:45 UTC
# Migration Complexity: HIGH
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
    "localhost.DQS_STAGING_DATA": "localhost.DQS_STAGING_DATA  -- TODO: replace with Fabric connection id",  # OLEDB / localhost / DQS_STAGING_DATA
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

# Source: CustomersDirty
# TODO: Configure source data read
df_source_customersdirty = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")


df = df_source_customersdirty

# === Transformations ===

# Lookup: ExactMatches
df_lookup = spark.sql("SELECT * FROM lookup_table")  # TODO: verify lookup query
df = df.join(
    df_lookup,
    on=['FullName', 'StreetAddress', 'CleanCustomerKey'],
    how="left"       # Match type: left = partial match, inner = full match
)


# Fuzzy Lookup: FuzzyMatches
# Reference table: reference_table
# IMPORTANT: PySpark has no built-in fuzzy join.
# Options:
#   1. Use Soundex / Levenshtein for approximate matching
#   2. Use a dedicated library (e.g. spark-fuzzy-matching)
df_ref = spark.read.format("delta").table("reference_table")

# Example using Levenshtein distance (edit distance ≤ threshold)
_match_col = "match_column"  # TODO: Set the column to fuzzy-match on
_threshold = 3  # TODO: Adjust threshold (original similarity: 0)
df = df.alias("src").crossJoin(df_ref.alias("ref")).filter(
    F.levenshtein(F.col("src." + _match_col), F.col("ref." + _match_col)) <= _threshold
)
logger.info("Fuzzy lookup completed for FuzzyMatches")


# Multicast: Match
# In Spark, simply reuse the same DataFrame for multiple outputs
df_output_1 = df  # Branch 1
df_output_2 = df  # Branch 2


# Multicast: NoMatch
# In Spark, simply reuse the same DataFrame for multiple outputs
df_output_1 = df  # Branch 1
df_output_2 = df  # Branch 2


# Union All: Union All
# TODO: Union all input DataFrames
df = df_input_1.unionByName(df_input_2, allowMissingColumns=True)


# === Destination Writes ===

# Destination: CustomersDirtyMatch -> Table: [dbo].[CustomersDirtyMatch]
# Connection: localhost.DQS_STAGING_DATA (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("localhost.DQS_STAGING_DATA")) \
    .option("dbtable", "[dbo].[CustomersDirtyMatch]") \
    .save()
logger.info("Data written to [dbo].[CustomersDirtyMatch] via Fabric connection")

# Destination: CustomersDirtyNoMatch -> Table: [dbo].[CustomersDirtyNoMatch]
# Connection: localhost.DQS_STAGING_DATA (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("localhost.DQS_STAGING_DATA")) \
    .option("dbtable", "[dbo].[CustomersDirtyNoMatch]") \
    .save()
logger.info("Data written to [dbo].[CustomersDirtyNoMatch] via Fabric connection")

# Destination: FuzzyMatchingResults -> Table: [dbo].[FuzzyMatchingResults]
# Connection: localhost.DQS_STAGING_DATA (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("localhost.DQS_STAGING_DATA")) \
    .option("dbtable", "[dbo].[FuzzyMatchingResults]") \
    .save()
logger.info("Data written to [dbo].[FuzzyMatchingResults] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: Data Flow Task")
print("Migration notebook execution complete: Data Flow Task")
