"""
Shared Generator Utilities
===========================
Common helpers used by multiple generators (Data Factory, Dataflow Gen2,
Spark Notebook).  Centralised here to eliminate duplication and ensure
consistent classification of SSIS data flow components.
"""

from __future__ import annotations

import re
from typing import Any

from ssis_to_fabric.analyzer.models import DataFlowComponent, DataFlowComponentType

# -------------------------------------------------------------------------
# Component type sets
# -------------------------------------------------------------------------

SOURCE_COMPONENT_TYPES = frozenset(
    {
        DataFlowComponentType.OLE_DB_SOURCE,
        DataFlowComponentType.ADO_NET_SOURCE,
        DataFlowComponentType.FLAT_FILE_SOURCE,
        DataFlowComponentType.EXCEL_SOURCE,
        DataFlowComponentType.ODBC_SOURCE,
        DataFlowComponentType.XML_SOURCE,
        DataFlowComponentType.RAW_FILE_SOURCE,
        DataFlowComponentType.CDC_SOURCE,
    }
)

DEST_COMPONENT_TYPES = frozenset(
    {
        DataFlowComponentType.OLE_DB_DESTINATION,
        DataFlowComponentType.ADO_NET_DESTINATION,
        DataFlowComponentType.FLAT_FILE_DESTINATION,
        DataFlowComponentType.EXCEL_DESTINATION,
        DataFlowComponentType.ODBC_DESTINATION,
        DataFlowComponentType.RAW_FILE_DESTINATION,
        DataFlowComponentType.RECORDSET_DESTINATION,
        DataFlowComponentType.SQL_SERVER_DESTINATION,
        DataFlowComponentType.DATA_READER_DESTINATION,
    }
)


# -------------------------------------------------------------------------
# Classification helpers
# -------------------------------------------------------------------------


def is_source(comp: DataFlowComponent) -> bool:
    """Return ``True`` if *comp* is a data-flow source component."""
    return comp.component_type in SOURCE_COMPONENT_TYPES


def is_destination(comp: DataFlowComponent) -> bool:
    """Return ``True`` if *comp* is a data-flow destination component."""
    return comp.component_type in DEST_COMPONENT_TYPES


def is_transform(comp: DataFlowComponent) -> bool:
    """Return ``True`` if *comp* is a data-flow transformation (not source/dest)."""
    return not (is_source(comp) or is_destination(comp))


# -------------------------------------------------------------------------
# Name sanitisation
# -------------------------------------------------------------------------

_UNSAFE_RE = re.compile(r"[^a-zA-Z0-9_]")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")


def sanitize_name(name: str, *, max_length: int = 260) -> str:
    """Sanitise *name* for use in artifact identifiers.

    Replaces non-alphanumeric characters with underscores, collapses
    sequences, strips leading/trailing underscores, and truncates.
    """
    sanitized = _UNSAFE_RE.sub("_", name)
    sanitized = _MULTI_UNDERSCORE_RE.sub("_", sanitized)
    return sanitized.strip("_")[:max_length]


# -------------------------------------------------------------------------
# Error-column filtering
# -------------------------------------------------------------------------

_ERROR_COLUMN_NAMES = frozenset({"ErrorCode", "ErrorColumn", ""})


def filter_error_columns(columns: list[Any]) -> list[Any]:
    """Remove SSIS error-output columns (ErrorCode, ErrorColumn, empty)."""
    return [c for c in columns if c.name not in _ERROR_COLUMN_NAMES]
