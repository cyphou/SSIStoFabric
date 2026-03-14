# Fabric Notebook
# Migrated from SSIS Package: UsingVariables
# Task: VariableDispenser
# Task Type: SCRIPT
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


# === Script Task: VariableDispenser ===
# IMPORTANT: This was a C#/VB.NET script task in SSIS.
# TODO: Manually translate the script logic.
#
# Description: Script Task
# Properties: {}
#
def execute_script_task():
    """
    TODO: Implement the script task logic in Python.
    Original SSIS Script Task needed manual translation.
    """
    raise NotImplementedError("Script task requires manual migration")

execute_script_task()


# === Completion ===
logger.info("Notebook completed: VariableDispenser")
print("Migration notebook execution complete: VariableDispenser")
