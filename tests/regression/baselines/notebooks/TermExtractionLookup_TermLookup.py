# Fabric Notebook
# Migrated from SSIS Package: TermExtractionLookup
# Task: TermLookup
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

# Source: Blogs
# TODO: Configure source data read
df_source_blogs = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")


df = df_source_blogs

# === Transformations ===

# Term Lookup: Term Lookup
# Looks up terms/phrases from a reference table and scores matches.
df_terms = spark.read.format("delta").table("term_reference_table")  # TODO: verify reference table
_input_col = "text_column"  # TODO: Set the text column to scan
_term_col = "Term"  # Column in reference table containing terms

# Cross-join and check if term appears in the text
df = df.alias("src").crossJoin(df_terms.alias("terms")).filter(
    F.col("src." + _input_col).contains(F.col("terms." + _term_col))
)
logger.info("Term lookup completed for Term Lookup")


# === Destination Writes ===

# Destination: TermsInBlogs -> Table: [TermsInBlogs]
# Connection: (LOCAL).AdventureWorksDW2014 (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("dbtable", "[TermsInBlogs]") \
    .save()
logger.info("Data written to [TermsInBlogs] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: TermLookup")
print("Migration notebook execution complete: TermLookup")
