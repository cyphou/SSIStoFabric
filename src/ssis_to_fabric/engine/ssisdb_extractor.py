"""
SSISDB Catalog Extractor
=========================
Connects to a SQL Server hosting SSISDB and extracts deployed
.dtsx packages to local files for offline migration.

Requires: pyodbc (pip install pyodbc)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)

# SQL to catalog deployed packages
_LIST_PACKAGES_SQL = """\
SELECT
    f.name   AS folder_name,
    pj.name  AS project_name,
    pk.name  AS package_name,
    pk.package_id
FROM [SSISDB].[catalog].[packages] pk
JOIN [SSISDB].[catalog].[projects] pj ON pk.project_id = pj.project_id
JOIN [SSISDB].[catalog].[folders]  f  ON pj.folder_id  = f.folder_id
WHERE 1 = 1
{folder_filter}
{project_filter}
ORDER BY f.name, pj.name, pk.name
"""

# SQL to extract the .dtsx blob for a given package_id
_EXTRACT_PACKAGE_SQL = """\
SELECT
    pk.name AS package_name,
    CAST(pk.package_data AS VARBINARY(MAX)) AS package_data
FROM [SSISDB].[internal].[packages] pk
WHERE pk.package_id = ?
"""

# Fallback: use catalog.get_project and extract from project zip
_EXTRACT_VIA_PROJECT_SQL = """\
EXEC [SSISDB].[catalog].[get_project] @folder_name = ?, @project_name = ?
"""


class SSISDBExtractor:
    """Extract .dtsx packages from an SSISDB catalog."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self._conn = None

    def connect(self) -> None:
        """Establish a connection to the SQL Server hosting SSISDB."""
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc is required for SSISDB extraction. Install it with: pip install pyodbc") from None

        self._conn = pyodbc.connect(self.connection_string, autocommit=True)
        logger.info("ssisdb_connected", server=self.connection_string[:40])

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def list_packages(
        self,
        folder_name: str | None = None,
        project_name: str | None = None,
    ) -> list[dict]:
        """List all deployed packages in SSISDB.

        Returns:
            List of dicts with keys: folder, project, name, package_id
        """
        if not self._conn:
            raise RuntimeError("Not connected. Call connect() first.")

        folder_filter = f"AND f.name = '{folder_name}'" if folder_name else ""
        project_filter = f"AND pj.name = '{project_name}'" if project_name else ""
        sql = _LIST_PACKAGES_SQL.format(
            folder_filter=folder_filter,
            project_filter=project_filter,
        )

        cursor = self._conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()

        packages = []
        for row in rows:
            packages.append(
                {
                    "folder": row.folder_name,
                    "project": row.project_name,
                    "name": row.package_name,
                    "package_id": row.package_id,
                }
            )

        logger.info("ssisdb_packages_listed", count=len(packages))
        return packages

    def extract_package(self, pkg_info: dict, output_dir: Path) -> Path:
        """Extract a single package to a .dtsx file.

        Args:
            pkg_info: Dict from list_packages() with folder, project, name, package_id
            output_dir: Root directory for extracted files

        Returns:
            Path to the written .dtsx file.
        """
        if not self._conn:
            raise RuntimeError("Not connected. Call connect() first.")

        # Try direct extraction from internal.packages first
        try:
            data = self._extract_direct(pkg_info["package_id"])
        except Exception:
            # Fallback: extract project zip and find the package inside
            data = self._extract_from_project(pkg_info["folder"], pkg_info["project"], pkg_info["name"])

        if data is None:
            raise RuntimeError(f"Could not extract package {pkg_info['name']} from project {pkg_info['project']}")

        # Write to: output_dir / folder / project / package.dtsx
        pkg_dir = output_dir / pkg_info["folder"] / pkg_info["project"]
        pkg_dir.mkdir(parents=True, exist_ok=True)
        output_path = pkg_dir / pkg_info["name"]
        if not output_path.suffix:
            output_path = output_path.with_suffix(".dtsx")

        output_path.write_bytes(data)
        logger.info(
            "ssisdb_package_extracted",
            package=pkg_info["name"],
            path=str(output_path),
        )
        return output_path

    def _extract_direct(self, package_id: int) -> bytes | None:
        """Try reading the package blob from internal.packages."""
        cursor = self._conn.cursor()
        try:
            cursor.execute(_EXTRACT_PACKAGE_SQL, (package_id,))
            row = cursor.fetchone()
            if row and row.package_data:
                return bytes(row.package_data)
        finally:
            cursor.close()
        return None

    def _extract_from_project(self, folder: str, project: str, package_name: str) -> bytes | None:
        """Fallback: use catalog.get_project to download the project zip,
        then extract the specific .dtsx from inside it."""
        import io
        import zipfile

        cursor = self._conn.cursor()
        try:
            cursor.execute(_EXTRACT_VIA_PROJECT_SQL, (folder, project))
            row = cursor.fetchone()
            if not row:
                return None
            project_data = bytes(row[0])
        finally:
            cursor.close()

        # The project data is a ZIP; extract the requested package
        with zipfile.ZipFile(io.BytesIO(project_data)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(package_name.lower()):
                    return zf.read(name)
                # Also try without path
                if name.split("/")[-1].lower() == package_name.lower():
                    return zf.read(name)

        return None
