# Fabric Notebook
# Migrated from SSIS Package: SplitData
# Task: SplitData
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
    "(LOCAL).AdventureWorksDW2014": "(LOCAL).AdventureWorksDW2014  -- TODO: replace with Fabric connection id",  # OLEDB / (LOCAL) / AdventureWorksDW2014
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

# Source: AW_DW_Source
# TODO: Configure source data read
df_source_aw_dw_source = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")


df = df_source_aw_dw_source

# === Transformations ===

# Derived Column: IdentifyTestSet
df = df.withColumn("TrainTest", F.expr("2"))  # SSIS: 2

# Derived Column: IdentifyTrainingSet
df = df.withColumn("TrainTest", F.expr("1"))  # SSIS: 1

# Unknown Transform: SplitData (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'SamplingValue': '70', 'SamplingSeed': '0', 'Selected': 'false'}


# === Destination Writes ===

# Destination: TMTestSet -> Table: [TMTestSet]
# Connection: (LOCAL).AdventureWorksDW2014 (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("dbtable", "[TMTestSet]") \
    .save()
logger.info("Data written to [TMTestSet] via Fabric connection")

# Destination: TMTrainingSet -> Table: [TMTrainingSet]
# Connection: (LOCAL).AdventureWorksDW2014 (OLEDB → Fabric Connection)
df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("dbtable", "[TMTrainingSet]") \
    .save()
logger.info("Data written to [TMTrainingSet] via Fabric connection")

# === Completion ===
logger.info("Notebook completed: SplitData")
print("Migration notebook execution complete: SplitData")
