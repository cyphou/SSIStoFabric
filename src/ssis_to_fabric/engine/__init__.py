"""
Migration Engine
================
Orchestrates the conversion from SSIS packages to Fabric artifacts.
Routes components to the appropriate generator (Data Factory or Spark)
based on the migration strategy and component complexity.
"""

# ---------------------------------------------------------------------------
# Shared constants for notebook cell splitting
# ---------------------------------------------------------------------------
# Spark notebooks are generated as plain .py files.  Cell boundaries are
# delimited by lines that match: ``# --- <section title> ---``
# The deployer (``fabric_deployer._py_to_ipynb``) later splits on these
# markers to produce Jupyter cells.  Keeping the pattern in one place avoids
# the two modules drifting out of sync.

CELL_MARKER_PREFIX = "# ---"
"""Prefix that starts every cell-boundary marker line."""

CELL_MARKER_SUFFIX = "---"
"""Suffix that ends every cell-boundary marker line (after stripping whitespace)."""


def is_cell_marker(line: str) -> bool:
    """Return *True* if *line* is a notebook cell-boundary marker."""
    return line.startswith(CELL_MARKER_PREFIX) and line.rstrip().endswith(CELL_MARKER_SUFFIX)


def make_cell_marker(title: str) -> str:
    """Build a cell-boundary marker with the given *title*.

    >>> make_cell_marker("Imports")
    '# --- Imports ---'
    """
    return f"{CELL_MARKER_PREFIX} {title} {CELL_MARKER_SUFFIX}"
