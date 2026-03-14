"""
Developer Experience
=====================
Phase 16 utilities that improve the developer and user experience:

- **JSON Schema**: auto-generate from Pydantic ``MigrationConfig``
- **ADR generation**: create Architecture Decision Records
- **Migration cookbook**: common SSIS pattern reference with before/after code
- **Decision tree**: interactive HTML wizard for strategy selection
- **Sphinx conf**: generate ``docs/conf.py`` for API documentation
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path  # noqa: TC003 (used at runtime)
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# =====================================================================
# JSON Schema Export
# =====================================================================


def generate_config_json_schema() -> dict[str, Any]:
    """Generate a JSON Schema from the Pydantic ``MigrationConfig`` model.

    This enables external tools (VS Code, pre-commit, CI) to validate
    ``migration_config.yaml`` without running Python.
    """
    from ssis_to_fabric.config import MigrationConfig

    schema = MigrationConfig.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = "SSIS to Fabric Migration Configuration"
    schema["description"] = (
        "Configuration file schema for ssis2fabric. "
        "Validates migration_config.yaml structure and types."
    )
    return schema


def write_json_schema(output_path: Path) -> Path:
    """Write the MigrationConfig JSON Schema to a file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = generate_config_json_schema()
    output_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    logger.info("json_schema_written", path=str(output_path))
    return output_path


# =====================================================================
# Architecture Decision Records (ADRs)
# =====================================================================


@dataclass
class ADR:
    """An Architecture Decision Record."""

    number: int
    title: str
    status: str = "Accepted"  # Proposed, Accepted, Deprecated, Superseded
    context: str = ""
    decision: str = ""
    consequences: str = ""
    date: str = ""

    def __post_init__(self) -> None:
        if not self.date:
            self.date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    def to_markdown(self) -> str:
        return (
            f"# ADR-{self.number:04d}: {self.title}\n\n"
            f"**Date:** {self.date}\n\n"
            f"**Status:** {self.status}\n\n"
            f"## Context\n\n{self.context}\n\n"
            f"## Decision\n\n{self.decision}\n\n"
            f"## Consequences\n\n{self.consequences}\n"
        )

    @property
    def filename(self) -> str:
        slug = self.title.lower().replace(" ", "-").replace("/", "-")
        return f"adr-{self.number:04d}-{slug}.md"


# Pre-defined ADRs for this project
_PROJECT_ADRS: list[ADR] = [
    ADR(
        number=1,
        title="Use Pydantic for configuration validation",
        context=(
            "The migration tool needs a robust configuration system that "
            "validates user input, supports defaults, and enables environment-specific overrides."
        ),
        decision=(
            "Use Pydantic v2 BaseModel for all configuration classes. "
            "This gives us type validation, JSON Schema generation, and serialization for free."
        ),
        consequences=(
            "Adds pydantic>=2.0 as a dependency. Config errors are caught at load time "
            "with descriptive messages. JSON Schema can be exported for external tooling."
        ),
    ),
    ADR(
        number=2,
        title="Click for CLI framework",
        context="The tool needs a user-friendly command-line interface with subcommands, options, and help text.",
        decision="Use Click with Rich for formatted output. All commands are flat under a single group.",
        consequences="Simple extension model: new features add new @main.command() functions.",
    ),
    ADR(
        number=3,
        title="lxml for DTSX parsing",
        context="SSIS .dtsx packages are XML files with complex namespaces and nested structures.",
        decision="Use lxml for XPath-based XML parsing with namespace-aware queries.",
        consequences="Requires lxml C extension. Parsing is fast and reliable for large XML files.",
    ),
    ADR(
        number=4,
        title="Hybrid migration strategy",
        context="SSIS packages contain both control flow (orchestration) and data flow (transformations).",
        decision=(
            "Default to hybrid strategy: Data Factory pipelines for control flow, "
            "Spark notebooks for data flow transformations."
        ),
        consequences="Leverages strengths of both platforms. More complex but more faithful migration.",
    ),
    ADR(
        number=5,
        title="Plugin architecture for extensibility",
        context="Different organizations have custom SSIS components and expression patterns.",
        decision=(
            "Use entry_points-based plugin system with component handler registry, "
            "custom expression rules, and hook points."
        ),
        consequences="Third-party packages can extend behavior without forking the project.",
    ),
]


def generate_adrs(output_dir: Path) -> list[Path]:
    """Write all project ADRs to a docs/adr/ directory."""
    adr_dir = output_dir / "docs" / "adr"
    adr_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for adr in _PROJECT_ADRS:
        path = adr_dir / adr.filename
        path.write_text(adr.to_markdown(), encoding="utf-8")
        paths.append(path)

    # Write index
    index_lines = ["# Architecture Decision Records\n\n"]
    for adr in _PROJECT_ADRS:
        index_lines.append(f"- [ADR-{adr.number:04d}: {adr.title}]({adr.filename})\n")
    (adr_dir / "README.md").write_text("".join(index_lines), encoding="utf-8")
    paths.append(adr_dir / "README.md")

    logger.info("adrs_generated", count=len(_PROJECT_ADRS))
    return paths


# =====================================================================
# Migration Cookbook
# =====================================================================


@dataclass
class CookbookEntry:
    """A single entry in the migration cookbook."""

    title: str
    ssis_pattern: str
    fabric_pattern: str
    description: str = ""
    complexity: str = "medium"  # low, medium, high
    tags: list[str] = field(default_factory=list)


# Common SSIS-to-Fabric migration patterns
_COOKBOOK_ENTRIES: list[CookbookEntry] = [
    CookbookEntry(
        title="Simple Data Copy",
        description="Copy data from source to destination without transformation.",
        ssis_pattern=(
            "Data Flow Task → OLE DB Source → OLE DB Destination"
        ),
        fabric_pattern=(
            "Data Factory Copy Activity with source and sink datasets"
        ),
        complexity="low",
        tags=["copy", "data-flow", "basic"],
    ),
    CookbookEntry(
        title="Derived Column Transform",
        description="Add or modify columns using expressions.",
        ssis_pattern=(
            "Data Flow Task → Source → Derived Column → Destination\n"
            "Expression: UPPER([CustomerName])"
        ),
        fabric_pattern=(
            "Spark notebook: df.withColumn('CustomerName', F.upper(F.col('CustomerName')))"
        ),
        complexity="low",
        tags=["expression", "transform", "derived-column"],
    ),
    CookbookEntry(
        title="Lookup Join",
        description="Enrich data by looking up values from a reference table.",
        ssis_pattern=(
            "Data Flow Task → Source → Lookup (reference table) → Destination"
        ),
        fabric_pattern=(
            "Spark notebook: df.join(lookup_df, on='KeyColumn', how='left')"
        ),
        complexity="medium",
        tags=["lookup", "join"],
    ),
    CookbookEntry(
        title="Conditional Split",
        description="Route rows to different destinations based on conditions.",
        ssis_pattern=(
            "Data Flow Task → Source → Conditional Split → Dest1 / Dest2"
        ),
        fabric_pattern=(
            "Spark notebook: df.filter(condition) for each branch\n"
            "Or: Data Factory If Condition / Switch activity"
        ),
        complexity="medium",
        tags=["conditional", "routing", "split"],
    ),
    CookbookEntry(
        title="SCD Type 2 (Slowly Changing Dimension)",
        description="Track historical changes to dimension records.",
        ssis_pattern=(
            "Data Flow Task → SCD Wizard → Historical / Current attribute routing"
        ),
        fabric_pattern=(
            "Spark notebook with MERGE INTO using Delta Lake:\n"
            "WHEN MATCHED AND changed THEN UPDATE SET EndDate = current_date\n"
            "WHEN NOT MATCHED THEN INSERT"
        ),
        complexity="high",
        tags=["scd", "dimension", "history"],
    ),
    CookbookEntry(
        title="Execute SQL Task",
        description="Run ad-hoc SQL statements (DDL, DML, stored procedures).",
        ssis_pattern="Execute SQL Task with SQL statement or stored procedure",
        fabric_pattern=(
            "Data Factory Script activity or Spark notebook spark.sql()\n"
            "Stored procedures: Fabric Warehouse stored procedure activity"
        ),
        complexity="low",
        tags=["sql", "ddl", "stored-procedure"],
    ),
    CookbookEntry(
        title="For Each Loop Container",
        description="Iterate over a collection and process each item.",
        ssis_pattern="For Each Loop Container → inner tasks",
        fabric_pattern="Data Factory ForEach activity with inner pipeline activities",
        complexity="medium",
        tags=["loop", "iteration", "foreach"],
    ),
    CookbookEntry(
        title="Execute Package Task",
        description="Call child packages from a master orchestrator.",
        ssis_pattern="Execute Package Task referencing child .dtsx",
        fabric_pattern="Data Factory Invoke Pipeline activity referencing child pipeline",
        complexity="medium",
        tags=["orchestration", "parent-child", "package"],
    ),
    CookbookEntry(
        title="Aggregate Transform",
        description="Group data and compute aggregates (SUM, COUNT, AVG).",
        ssis_pattern="Data Flow → Source → Aggregate → Destination",
        fabric_pattern="Spark notebook: df.groupBy('col').agg(F.sum('amount'))",
        complexity="medium",
        tags=["aggregate", "group-by"],
    ),
    CookbookEntry(
        title="Error Handling with Event Handlers",
        description="Handle errors at package or task level.",
        ssis_pattern="OnError event handler → Send Mail / Log task",
        fabric_pattern=(
            "Data Factory pipeline failure path → Web activity (webhook)\n"
            "Or: Spark try/except with notebookutils error handling"
        ),
        complexity="high",
        tags=["error-handling", "events"],
    ),
]


def generate_cookbook(output_dir: Path) -> Path:
    """Generate the migration cookbook as a Markdown file."""
    cookbook_dir = output_dir / "docs"
    cookbook_dir.mkdir(parents=True, exist_ok=True)
    path = cookbook_dir / "migration_cookbook.md"

    lines = [
        "# SSIS to Fabric Migration Cookbook\n\n",
        "Common SSIS patterns and their Fabric equivalents.\n\n",
        "---\n\n",
    ]

    for entry in _COOKBOOK_ENTRIES:
        lines.append(f"## {entry.title}\n\n")
        if entry.description:
            lines.append(f"{entry.description}\n\n")
        lines.append(f"**Complexity:** {entry.complexity}\n\n")
        lines.append(f"**Tags:** {', '.join(entry.tags)}\n\n")
        lines.append("### SSIS Pattern\n\n")
        lines.append(f"```\n{entry.ssis_pattern}\n```\n\n")
        lines.append("### Fabric Equivalent\n\n")
        lines.append(f"```\n{entry.fabric_pattern}\n```\n\n")
        lines.append("---\n\n")

    path.write_text("".join(lines), encoding="utf-8")
    logger.info("cookbook_generated", entries=len(_COOKBOOK_ENTRIES))
    return path


def get_cookbook_entries() -> list[CookbookEntry]:
    """Return all cookbook entries for programmatic access."""
    return list(_COOKBOOK_ENTRIES)


# =====================================================================
# Interactive Decision Tree (HTML)
# =====================================================================


def generate_decision_tree_html() -> str:
    """Generate an interactive HTML decision tree for strategy selection.

    Returns a self-contained HTML string with inline CSS and JavaScript.
    """
    return """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>SSIS to Fabric — Migration Strategy Wizard</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 700px; margin: 40px auto; padding: 0 20px; color: #333; }
  h1 { color: #0078d4; }
  .step { display: none; padding: 20px; border: 1px solid #ddd; border-radius: 8px;
          margin: 20px 0; background: #fafafa; }
  .step.active { display: block; }
  button { background: #0078d4; color: #fff; border: none; padding: 10px 24px;
           border-radius: 4px; cursor: pointer; margin: 4px; font-size: 14px; }
  button:hover { background: #005a9e; }
  .result { padding: 20px; background: #e6f7e6; border: 2px solid #28a745;
            border-radius: 8px; margin: 20px 0; }
  .result h2 { color: #28a745; margin-top: 0; }
</style>
</head>
<body>
<h1>Migration Strategy Wizard</h1>
<p>Answer a few questions to determine the best migration strategy.</p>

<div id="q1" class="step active">
  <h3>Do your SSIS packages primarily contain data flow transformations?</h3>
  <button onclick="next('q2a')">Yes — heavy transforms</button>
  <button onclick="next('q2b')">No — mostly orchestration</button>
  <button onclick="next('q2c')">Mix of both</button>
</div>

<div id="q2a" class="step">
  <h3>Do you need to preserve complex expression logic?</h3>
  <button onclick="result('spark')">Yes — need PySpark expressions</button>
  <button onclick="result('hybrid')">Some — hybrid approach</button>
</div>

<div id="q2b" class="step">
  <h3>Are your packages mainly Execute SQL and control flow?</h3>
  <button onclick="result('data_factory')">Yes — Data Factory pipelines</button>
  <button onclick="result('hybrid')">Also some data flows</button>
</div>

<div id="q2c" class="step">
  <h3>Do you have more than 20 packages?</h3>
  <button onclick="result('hybrid')">Yes — hybrid for scalability</button>
  <button onclick="result('hybrid')">No — hybrid still recommended</button>
</div>

<div id="result-spark" class="step result">
  <h2>Recommended: Spark Strategy</h2>
  <p>Use <code>strategy: spark</code> in your config. All data flows will be
     converted to PySpark notebooks with full expression transpilation.</p>
  <pre>ssis2fabric init --project-name my-migration
# Edit migration_config.yaml: strategy: spark
ssis2fabric migrate</pre>
</div>

<div id="result-data_factory" class="step result">
  <h2>Recommended: Data Factory Strategy</h2>
  <p>Use <code>strategy: data_factory</code>. Packages will be converted to
     Data Factory pipeline JSON with Copy, Script, and ForEach activities.</p>
  <pre>ssis2fabric init --project-name my-migration
# Edit migration_config.yaml: strategy: data_factory
ssis2fabric migrate</pre>
</div>

<div id="result-hybrid" class="step result">
  <h2>Recommended: Hybrid Strategy (default)</h2>
  <p>Use <code>strategy: hybrid</code>. Control flow becomes Data Factory
     pipelines; data flows become Spark notebooks. Best of both worlds.</p>
  <pre>ssis2fabric init --project-name my-migration
# Edit migration_config.yaml: strategy: hybrid
ssis2fabric migrate</pre>
</div>

<script>
function next(id) {
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}
function result(strategy) {
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById('result-' + strategy).classList.add('active');
}
</script>
</body>
</html>"""


def write_decision_tree(output_dir: Path) -> Path:
    """Write the interactive decision tree HTML file."""
    docs_dir = output_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    path = docs_dir / "strategy_wizard.html"
    path.write_text(generate_decision_tree_html(), encoding="utf-8")
    logger.info("decision_tree_written", path=str(path))
    return path


# =====================================================================
# Sphinx Configuration Generator
# =====================================================================


def generate_sphinx_conf(project_name: str = "SSIS to Fabric") -> str:
    """Generate a ``docs/conf.py`` for Sphinx API documentation."""
    return f'''\
# Sphinx configuration for {project_name}
# Auto-generated by ssis2fabric

project = "{project_name}"
copyright = "2026, Migration Team"
author = "Migration Team"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build"]

html_theme = "furo"
html_static_path = ["_static"]

intersphinx_mapping = {{
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
}}

autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False
'''


def write_sphinx_conf(output_dir: Path, project_name: str = "SSIS to Fabric") -> Path:
    """Write Sphinx docs/conf.py and docs/index.rst."""
    docs_dir = output_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    # conf.py
    conf_path = docs_dir / "conf.py"
    conf_path.write_text(generate_sphinx_conf(project_name), encoding="utf-8")

    # index.rst
    index_path = docs_dir / "index.rst"
    index_content = f"""\
{project_name}
{'=' * len(project_name)}

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api/modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
"""
    index_path.write_text(index_content, encoding="utf-8")

    logger.info("sphinx_conf_written", path=str(conf_path))
    return conf_path
