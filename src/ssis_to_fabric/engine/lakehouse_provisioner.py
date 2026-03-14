"""
Lakehouse Provisioner
=====================
Reads ``*.destinations.json`` sidecar manifests (produced by the migration
engine's connection-manifest generation step) and generates the SQL DDL or
Spark DDL needed to create target Fabric Lakehouse / Warehouse tables.

Sidecar manifest format (example)::

    {
        "ssis_name": "OLE_DB_Destination",
        "ssis_type": "OLEDB",
        "server": "myserver.database.windows.net",
        "database": "DW",
        "table_name": "dbo.FactSales",
        "columns": [
            {"name": "SalesKey", "data_type": "DT_I4"},
            {"name": "Amount",   "data_type": "DT_DECIMAL,10,2"},
            {"name": "LoadDate", "data_type": "DT_DBDATE"}
        ],
        "fabric_target_type": "Lakehouse"
    }

Usage::

    from ssis_to_fabric.engine.lakehouse_provisioner import LakehouseProvisioner
    provisioner = LakehouseProvisioner()
    ddl_files = provisioner.provision(output_dir=Path("output"))
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# SSIS DT_* data-type → Spark Delta Lake / SQL type mapping
# ---------------------------------------------------------------------------
_SSIS_TO_SPARK: dict[str, str] = {
    "DT_BOOL": "BOOLEAN",
    "DT_I1": "TINYINT",
    "DT_I2": "SMALLINT",
    "DT_I4": "INT",
    "DT_I8": "BIGINT",
    "DT_UI1": "SMALLINT",
    "DT_UI2": "INT",
    "DT_UI4": "BIGINT",
    "DT_UI8": "BIGINT",
    "DT_R4": "FLOAT",
    "DT_R8": "DOUBLE",
    "DT_CY": "DECIMAL(19,4)",
    "DT_DATE": "TIMESTAMP",
    "DT_DBDATE": "DATE",
    "DT_DBTIME": "STRING",
    "DT_DBTIMESTAMP": "TIMESTAMP",
    "DT_DBTIMESTAMP2": "TIMESTAMP",
    "DT_DBTIMESTAMPOFFSET": "TIMESTAMP",
    "DT_STR": "STRING",
    "DT_WSTR": "STRING",
    "DT_TEXT": "STRING",
    "DT_NTEXT": "STRING",
    "DT_IMAGE": "BINARY",
    "DT_BYTES": "BINARY",
    "DT_GUID": "STRING",
    "DT_FILETIME": "BIGINT",
    "DT_NUMERIC": "DECIMAL",
    "DT_DECIMAL": "DECIMAL",
    "UNKNOWN": "STRING",
}

_SSIS_TO_SQL: dict[str, str] = {
    "DT_BOOL": "BIT",
    "DT_I1": "TINYINT",
    "DT_I2": "SMALLINT",
    "DT_I4": "INT",
    "DT_I8": "BIGINT",
    "DT_UI1": "SMALLINT",
    "DT_UI2": "INT",
    "DT_UI4": "BIGINT",
    "DT_UI8": "BIGINT",
    "DT_R4": "REAL",
    "DT_R8": "FLOAT",
    "DT_CY": "DECIMAL(19,4)",
    "DT_DATE": "DATETIME2",
    "DT_DBDATE": "DATE",
    "DT_DBTIME": "TIME",
    "DT_DBTIMESTAMP": "DATETIME2",
    "DT_DBTIMESTAMP2": "DATETIME2",
    "DT_DBTIMESTAMPOFFSET": "DATETIMEOFFSET",
    "DT_STR": "VARCHAR",
    "DT_WSTR": "NVARCHAR",
    "DT_TEXT": "VARCHAR(MAX)",
    "DT_NTEXT": "NVARCHAR(MAX)",
    "DT_IMAGE": "VARBINARY(MAX)",
    "DT_BYTES": "VARBINARY",
    "DT_GUID": "UNIQUEIDENTIFIER",
    "DT_FILETIME": "BIGINT",
    "DT_NUMERIC": "DECIMAL",
    "DT_DECIMAL": "DECIMAL",
    "UNKNOWN": "NVARCHAR(MAX)",
}


class LakehouseProvisioner:
    """Generates Lakehouse / Warehouse DDL from sidecar destination manifests."""

    def __init__(self, dialect: str = "spark") -> None:
        """
        Parameters
        ----------
        dialect:
            ``"spark"`` (default) generates Spark Delta Lake ``CREATE TABLE``
            syntax; ``"sql"`` generates T-SQL ``CREATE TABLE`` syntax.
        """
        self.dialect = dialect.lower()
        self._type_map = _SSIS_TO_SPARK if self.dialect == "spark" else _SSIS_TO_SQL

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def provision(self, output_dir: Path) -> list[Path]:
        """Scan *output_dir* for ``*.destinations.json`` files and generate DDL.

        Writes one ``<table_name>.sql`` file per destination manifest into
        ``output_dir/lakehouse/``.  Returns the list of generated file paths.
        """
        output_dir = Path(output_dir)
        manifests = list(output_dir.rglob("*.destinations.json"))
        if not manifests:
            logger.info("no_destination_manifests_found", path=str(output_dir))
            return []

        ddl_dir = output_dir / "lakehouse"
        ddl_dir.mkdir(parents=True, exist_ok=True)

        generated: list[Path] = []
        for manifest_path in manifests:
            try:
                ddl_path = self._generate_from_manifest(manifest_path, ddl_dir)
                if ddl_path:
                    generated.append(ddl_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("provision_manifest_error", file=str(manifest_path), error=str(exc))

        logger.info("lakehouse_ddl_generated", count=len(generated), path=str(ddl_dir))
        return generated

    def generate_ddl(self, manifest: dict[str, Any]) -> str:
        """Generate a ``CREATE TABLE`` DDL statement from a manifest dict."""
        table_name = manifest.get("table_name", "unknown_table")
        columns: list[dict[str, Any]] = manifest.get("columns", [])

        col_defs = []
        for col in columns:
            col_name = col.get("name", "col")
            raw_type = col.get("data_type", "UNKNOWN").upper()
            sql_type = self._map_type(raw_type)
            col_defs.append(f"    {col_name} {sql_type}")

        col_block = ",\n".join(col_defs) if col_defs else "    -- no columns defined"

        if self.dialect == "spark":
            return (
                f"-- Auto-generated Spark Delta Lake DDL\n"
                f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
                f"{col_block}\n"
                f") USING DELTA;\n"
            )
        else:
            return (
                f"-- Auto-generated T-SQL DDL\n"
                f"IF OBJECT_ID(N'{table_name}', N'U') IS NULL\n"
                f"CREATE TABLE {table_name} (\n"
                f"{col_block}\n"
                f");\n"
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _generate_from_manifest(self, manifest_path: Path, ddl_dir: Path) -> Path | None:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        ddl = self.generate_ddl(manifest)
        table_name = manifest.get("table_name", manifest_path.stem).replace(".", "_").replace(" ", "_")
        out_path = ddl_dir / f"{table_name}.sql"
        out_path.write_text(ddl, encoding="utf-8")
        logger.info("ddl_written", table=table_name, path=str(out_path))
        return out_path

    def _map_type(self, ssis_type: str) -> str:
        """Map an SSIS DT_* type string to the target dialect SQL type."""
        # Handle parameterised types: DT_WSTR,50 / DT_DECIMAL,10,2 / DT_NUMERIC,18,4
        base = ssis_type.split(",")[0].strip()
        parts = [p.strip() for p in ssis_type.split(",")]

        if base in ("DT_WSTR", "DT_STR") and len(parts) >= 2:
            length = parts[1]
            return f"NVARCHAR({length})" if self.dialect == "sql" else "STRING"
        if base in ("DT_NUMERIC", "DT_DECIMAL") and len(parts) >= 3:
            prec, scale = parts[1], parts[2]
            return f"DECIMAL({prec},{scale})"
        if base in ("DT_BYTES",) and len(parts) >= 2:
            length = parts[1]
            return f"VARBINARY({length})" if self.dialect == "sql" else "BINARY"

        return self._type_map.get(base, "STRING" if self.dialect == "spark" else "NVARCHAR(MAX)")
