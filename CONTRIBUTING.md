# Contributing to SSIS to Microsoft Fabric Migration Tool

Thank you for your interest in contributing! This guide covers the development setup, coding standards, and contribution workflow.

---

## 🛠️ Development Setup

### Prerequisites

- Python 3.10+ (tested on 3.10–3.13)
- Git

### Getting Started

```bash
# Clone the repository
git clone <repo-url>
cd SSISToFabric

# Create a virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/Mac

# Install development dependencies
pip install -e ".[dev]"
pre-commit install

# Run tests
pytest tests/ -v
```

### 📁 Project Structure

See the [README](README.md) for a detailed architecture overview.

```
src/ssis_to_fabric/
├── analyzer/         → SSIS package parsing (.dtsx XML → models)
│   ├── models.py    →   Data models (SSISPackage, Variable, Task, DataFlowComponent, etc.)
│   └── dtsx_parser.py →  .dtsx XML parser + Project.params reader
├── engine/           → 34 migration & analysis modules
│   ├── migration_engine.py    → Orchestration, routing & plan generation
│   ├── data_factory_generator.py → ADF pipeline JSON generation
│   ├── dataflow_generator.py     → Dataflow Gen2 (Power Query M)
│   ├── spark_generator.py        → PySpark notebook generation
│   ├── expression_transpiler.py  → Unified expression transpiler facade
│   ├── fabric_deployer.py        → Fabric REST API deployment
│   ├── csharp_transpiler.py      → C# Script Task → Python transpiler
│   ├── script_transpiler.py      → AST-based C# parser, .NET BCL mapping
│   ├── lineage.py / column_lineage.py → Table & column lineage graphs
│   ├── data_quality.py           → Column profiling, validation rules
│   ├── gitops.py                 → Git-sync, artifact versioning
│   ├── policy_engine.py          → Governance rules, PII detection
│   ├── connectors.py             → Enterprise connector mapping
│   ├── intelligence.py           → AI pattern recognition, NL queries
│   └── + 10 more modules         → See README for full list
├── testing/          → Non-regression baseline validation
│   └── regression_runner.py
├── cli.py            → 35 CLI commands (ssis2fabric)
├── api.py            → Public Python API (SSISMigrator facade)
├── config.py         → Configuration management (Pydantic)
└── logging_config.py → Structured logging (structlog)
tests/                → 1932 tests (40 unit files + 14 regression)
examples/             → 12 scenarios + 28 real SSIS packages
```

---

## ✅ Code Quality

This project enforces code quality through:

- **ruff** — Linting and formatting (line length: 120, target: Python 3.10+)
- **mypy** — Static type checking in strict mode
- **pre-commit** — Automatic checks before each commit

```bash
ruff check src/ tests/          # Lint
ruff format src/ tests/         # Format
mypy src/ssis_to_fabric/ --ignore-missing-imports  # Type check
```

### Style Guidelines

- Follow PEP 8 with `ruff` (max line length: 120)
- Use type hints where practical (validated with `mypy --strict`)
- Private methods prefixed with `_`
- Constants as `UPPER_SNAKE_CASE`

---

## 🧪 Testing

```bash
# All tests (1932+)
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# With coverage
pytest tests/ --cov=ssis_to_fabric --cov-report=html
```

### Test Categories

| Marker | Description |
|--------|-------------|
| `unit` | Fast tests, no external dependencies |
| `regression` | Non-regression baseline comparison |
| `integration` | Requires database connections |
| `e2e` | End-to-end migration tests |

### Writing Tests

- Use `pytest` conventions
- Tests write to `tempfile.mkdtemp()` and clean up
- No mocking of file I/O — tests use real temp directories
- Each test should be independent and self-contained

---

## 🔧 Adding New SSIS Components

1. Add the component type to `DataFlowComponentType` in `src/ssis_to_fabric/analyzer/models.py`
2. Add the class ID mapping in `COMPONENT_CLASS_MAP` in `src/ssis_to_fabric/analyzer/dtsx_parser.py`
3. Add metadata extraction in `_extract_component_metadata()`
4. Add generation logic in the appropriate generator
5. Add unit tests and update regression baselines

---

## 🤝 Contribution Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Follow the coding standards above
- Add tests for any new functionality
- Update documentation if adding new features

### 3. Run Tests

```bash
pytest tests/ -v
```

All existing tests must pass. New features should include tests.

### 4. Submit a Pull Request

- Provide a clear description of the change
- Reference any related issues
- Include before/after screenshots for visual changes

---

## 💬 Commit Messages

Use conventional commit format:

```
feat: add Excel source support to Dataflow Gen2 generator
fix: resolve connection injection for Script activities
test: add regression tests for CDC source parsing
docs: update README with new component support table
```

---

## 🎯 Areas for Contribution

See the [Roadmap](README.md#-roadmap) for the full roadmap.

### ✅ Recently Completed

- **Phase 33 — Auto-Remediation & Self-Healing (v5.2.0)**: TODO stub scanning, 33 remediation templates, fuzzy connection matching, confidence scoring, dry-run, JSON reports
- **Phase 38 — Legacy Modernization Patterns (v5.2.0)**: Medallion blueprint, 5-pattern recommendation, technical-debt scoring (0–100), wave planner, modernization reports
- **Phase 31 — Fabric Native API (v5.1.0)**: OneLake shortcuts, Lakehouse provisioning, refresh scheduling, semantic model deploy, Git integration
- **Phase 30 — Gap Closure (v5.0.0)**: SCD Types 1/2/3, full event handlers, Recordset Dest, Data Tap, complexity re-scoring
- **Phase 29 — Metadata Catalog (v4.3.0)**: unified metadata store, full-text search, auto-tagging, dependency matrix, Purview/Data Catalog export, `catalog` CLI command

### 🔥 Next Up

See the [Roadmap](README.md#-roadmap) for the full phase list (Phases 34–41).

### Phase 31: Fabric Native API Integration (v5.1.0)

- OneLake Shortcut deployment: POST `/v1/workspaces/{id}/items/{id}/shortcuts` for S3/GCS/ADLS sources
- Lakehouse table provisioning via OneLake Tables API (create Delta tables programmatically)
- Refresh scheduling: POST `/v1/workspaces/{id}/items/{id}/schedules` for pipeline triggers
- Semantic Link: generate DirectLake semantic model definition (`.bim` JSON) from lakehouse schema
- Workspace Git integration: auto-commit generated artifacts via Fabric Git REST API
- Access control: assign workspace roles and item-level sharing during deployment
- Environment provisioning: create Spark environments with library dependencies
- `ssis2fabric deploy --provision-lakehouse` and `--provision-shortcuts` flags
- Enhanced `fabric_deployer.py` with new API surface
- Tests for each new API integration with mock HTTP responses

### Phase 32: Multi-Cloud Target Support (v5.2.0)

- Target abstraction layer: decouple code generation from Fabric-specific output
- `TargetGenerator` base class with `FabricTarget`, `DatabricksTarget`, `GlueTarget`, `DataflowTarget`
- Databricks generator: Delta Live Tables notebooks, Unity Catalog integration, Workflow JSON
- AWS Glue generator: PySpark Glue ETL scripts, Glue Crawler configs, Step Function orchestration
- GCP Dataflow generator: Apache Beam pipelines, Dataflow Flex Templates
- Target selection via `--target fabric|databricks|glue|dataflow` CLI flag
- Shared transformation library for cross-platform expression compatibility
- Target-specific connection mapping (OLEDB → Databricks JDBC, Glue Connections, etc.)
- New engine module: `multi_cloud.py`
- Tests for each target generator + cross-target parity tests

### Phase 33: Auto-Remediation & Self-Healing (v5.3.0)

- Template-based `# TODO` stub resolution with 30+ code templates
- Fuzzy connection mapping: match SSIS connection managers to target catalog entries
- Expression fallback decomposition: break complex expressions into simpler sub-steps
- Confidence scoring per remediated artifact (HIGH/MEDIUM/LOW)
- Self-healing loop: detect generation failures → apply fix strategy → retry
- `ssis2fabric remediate` CLI command with `--dry-run` and `--min-confidence` flags
- Remediation report: before/after diff, confidence breakdown, manual-review list
- New engine module: `remediation.py`

### Phase 34: Migration Analytics & Reporting (v5.4.0)

- `MigrationHistory` store: track every migration run with timestamps, metrics, outcomes
- Longitudinal trend tracking: success rate, complexity distribution, velocity over time
- Team comparison: per-developer/per-team migration throughput and quality metrics
- Power BI `.pbix` template export for migration analytics dashboard
- Burndown chart: remaining packages vs. projected completion
- `ssis2fabric analytics` CLI command with `--export pbi|csv|json`
- Weekly digest report (HTML email template)
- New engine module: `analytics.py`

### Phase 35: Disaster Recovery & High Availability (v5.5.0)

- Cloud-backed checkpoints: save migration state to Azure Blob / S3 / GCS
- Resumable migrations: restart from last successful checkpoint after failure
- Point-in-time rollback: revert to any prior checkpoint (not just the last)
- Artifact backup: snapshot all generated artifacts before overwrite
- DR runbooks: auto-generated step-by-step recovery documentation
- `ssis2fabric checkpoint` and `ssis2fabric restore` CLI commands
- Heartbeat monitoring: detect stalled migrations and alert
- New engine module: `disaster_recovery.py`

### Phase 36: Compliance Frameworks (v6.0.0)

- HIPAA rule set: PHI field detection, encryption-at-rest validation, access logging
- PCI-DSS rule set: cardholder data flow tracking, network segmentation checks
- ISO 27001 rule set: information classification, risk assessment, control mapping
- SOX rule set: change management audit trail, segregation of duties validation
- Evidence package generator: bundled proof of compliance per framework
- SIEM integration: export audit events to Splunk / Sentinel / Elastic
- `ssis2fabric compliance` CLI with `--framework hipaa|pci|iso27001|sox`
- New engine module: `compliance.py`

### Phase 37: API-First Architecture & SDK (v6.1.0)

- REST API server (FastAPI): expose all migration operations as HTTP endpoints
- Python SDK: `ssis2fabric.sdk` package with typed client for programmatic access
- Webhook system: configurable callbacks for migration events (start, complete, fail)
- GraphQL endpoint: flexible querying of migration state, history, and catalog
- OpenAPI 3.1 spec auto-generation with interactive Swagger UI
- API key + OAuth2 authentication with role-based scoping
- Rate limiting and request throttling for multi-user environments
- `ssis2fabric serve` CLI command with `--port` and `--workers`
- New engine module: `api_server.py`

### Phase 38: Legacy Modernization Patterns (v6.2.0)

- Medallion architecture generator: auto-structure Bronze → Silver → Gold layers
- 20+ modernization patterns: SCD Type 2, Slowly Changing Dimension, Hub-and-Spoke, Star Schema
- Wave planner: cluster packages by dependency and complexity into migration waves
- Technical debt scoring: quantify modernization effort per package
- Before/after architecture diagrams (Mermaid + SVG)
- Data mesh integration: domain-aligned data product generation
- `ssis2fabric modernize` CLI command with `--pattern medallion|star|mesh`
- New engine module: `modernization.py`

### Phase 39: Interactive Migration Studio (v6.3.0)

- Web-based visual migration editor (React + FastAPI backend)
- Drag-and-drop SSIS component → Fabric artifact mapping canvas
- Live preview: see generated pipeline/notebook/dataflow as you configure
- Visual expression editor with syntax highlighting and auto-complete
- Side-by-side source (SSIS) ↔ target (Fabric) comparison view
- Override manager: pin manual overrides that survive re-migration
- Collaboration: multi-user editing with conflict resolution
- `ssis2fabric studio` CLI command to launch the local web UI
- New engine module: `studio.py`

### Phase 40: Migration Marketplace & Community (v6.4.0)

- Shareable migration template registry (local + remote)
- Community connector plugins: publish/install custom source/target handlers
- Pattern marketplace: browse and install pre-built transformation patterns
- Version-controlled template packages with semantic versioning
- Template rating, tagging, and search across the marketplace
- `ssis2fabric marketplace search|install|publish` CLI commands
- Import/export migration profiles as portable bundles
- New engine module: `marketplace.py`

### Phase 41: Autonomous Migration Agent (v6.5.0)

- Fully autonomous end-to-end migration orchestrator (zero-touch mode)
- Self-learning pattern library: learn from past migrations to improve future ones
- LLM-powered decision engine: choose strategy, resolve ambiguity, handle edge cases
- Adaptive retry with progressive escalation (template → LLM → human-in-the-loop)
- Migration confidence dashboard: real-time scoring with go/no-go recommendation
- Anomaly detection: flag unusual patterns, data drift, or regression risks
- `ssis2fabric auto-migrate` CLI command with `--approval manual|auto`
- Feedback loop: human corrections improve future autonomous decisions
- New engine module: `autonomous.py`

---

## 📋 Release Process

1. Update `CHANGELOG.md` with the new version
2. Run full test suite: `pytest tests/ -v`
3. Validate all sample migrations
4. Bump version in `pyproject.toml`
5. Create a Git tag: `git tag v4.x.x`
6. Push to main: `git push origin main --tags`
