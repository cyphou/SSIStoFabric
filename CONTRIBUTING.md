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
├── engine/           → Migration generators & deployment
│   ├── migration_engine.py    → Orchestration, routing & plan generation
│   ├── data_factory_generator.py → ADF pipeline JSON generation
│   ├── dataflow_generator.py     → Dataflow Gen2 (Power Query M) + expression transpiler
│   ├── spark_generator.py        → PySpark notebook + expression transpiler
│   ├── expression_transpiler.py  → Unified expression transpiler facade
│   ├── utils.py                  → Shared generator utilities
│   ├── fabric_deployer.py        → Fabric REST API deployment
│   ├── csharp_transpiler.py      → C# Script Task → Python transpiler
│   ├── lineage.py                → Data lineage graph builder
│   ├── report_generator.py       → HTML migration report
│   ├── lakehouse_provisioner.py  → DDL generation for Lakehouse/Warehouse
│   ├── ssisdb_extractor.py       → SSISDB catalog .dtsx extraction
│   └── agents.py                 → Multi-agent parallel orchestration
├── testing/          → Non-regression baseline validation
│   └── regression_runner.py
├── cli.py            → CLI entry point (ssis2fabric)
├── api.py            → Public Python API (SSISMigrator facade)
├── config.py         → Configuration management (Pydantic)
└── logging_config.py → Structured logging (structlog)
tests/                → 806 tests (792 unit + 14 regression)
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
# All tests (806+)
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

See the [Roadmap](README.md#-roadmap) for the full Phase 7–16 plan.

### High Priority (Phase 7–8)

- OpenTelemetry tracing and correlation IDs for agent orchestration
- Bitwise operator support (`BITAND`, `BITOR`, `BITXOR`) in expression transpilers
- C# transpiler AST mode (tree-walk parser replacing regex for complex scripts)
- Interactive charts in HTML migration report

### Medium Priority (Phase 9–11)

- Plugin architecture: `TransformationStrategy` protocol for custom generators
- Transaction scope support for SSIS `Required`/`Supported` semantics
- Column-level lineage tracking through transformations
- WMI Event Watcher, Web Service, and XML Task generation

### Low Priority (Phase 12–16)

- Blue-green deployment and rollback CLI command
- Property-based testing (Hypothesis) for expression transpiler fuzzing
- Azure Key Vault secret resolution for connection credentials
- Power BI dataset generation from lineage graph
- VS Code extension for inline migration assessment
- Sphinx-generated API documentation

---

## 📋 Release Process

1. Update `CHANGELOG.md` with the new version
2. Run full test suite: `pytest tests/ -v`
3. Validate all sample migrations
4. Bump version in `pyproject.toml`
5. Create a Git tag: `git tag v1.x.x`
6. Push to main: `git push origin main --tags`
