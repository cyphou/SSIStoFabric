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
├── engine/           → 29 migration & analysis modules
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
├── cli.py            → 30 CLI commands (ssis2fabric)
├── api.py            → Public Python API (SSISMigrator facade)
├── config.py         → Configuration management (Pydantic)
└── logging_config.py → Structured logging (structlog)
tests/                → 1508 tests (36 unit files + 14 regression)
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
# All tests (1508+)
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

See the [Roadmap](README.md#-roadmap) for the full Phase 27–36 plan.

### High Priority (Phase 27–29)

- Package decomposition: auto-split monolithic SSIS packages into smaller Fabric pipelines
- Migration validation framework: test harness generation, schema drift detection
- Metadata catalog: searchable index of all packages, tasks, connections, lineage

### Medium Priority (Phase 30–33)

- Multi-cloud target support: Databricks, AWS Glue, GCP Dataflow generators
- Automated remediation: resolve TODO stubs with template-based code gen
- Migration analytics: longitudinal tracking, trend dashboards, Power BI export
- Disaster recovery: cloud-backed checkpoints, resumable migrations

### Low Priority (Phase 34–36)

- Compliance frameworks: HIPAA/PCI-DSS/ISO 27001 rule sets, evidence packages
- API-first architecture: REST server, Python SDK, webhooks, GraphQL
- Legacy modernization patterns: medallion architecture, wave planner

---

## 📋 Release Process

1. Update `CHANGELOG.md` with the new version
2. Run full test suite: `pytest tests/ -v`
3. Validate all sample migrations
4. Bump version in `pyproject.toml`
5. Create a Git tag: `git tag v4.x.x`
6. Push to main: `git push origin main --tags`
