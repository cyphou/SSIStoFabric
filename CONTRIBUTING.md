# Contributing

## Development Setup

```bash
git clone <repo-url>
cd SSISToFabric
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/Mac
pip install -e ".[dev]"
pre-commit install
```

## Code Quality

This project enforces code quality through:

- **ruff** — Linting and formatting (line length: 120, target: Python 3.10+)
- **mypy** — Static type checking in strict mode
- **pre-commit** — Automatic checks before each commit

Run checks locally:

```bash
ruff check src/ tests/          # Lint
ruff format src/ tests/         # Format
mypy src/ssis_to_fabric/ --ignore-missing-imports  # Type check
```

## Testing

```bash
# All tests
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

## Adding New SSIS Components

1. Add the component type to `DataFlowComponentType` in `src/ssis_to_fabric/analyzer/models.py`
2. Add the class ID mapping in `COMPONENT_CLASS_MAP` in `src/ssis_to_fabric/analyzer/dtsx_parser.py`
3. Add metadata extraction in `_extract_component_metadata()`
4. Add generation logic in the appropriate generator
5. Add unit tests and update regression baselines

## Commit Messages

Use conventional commit format:

```
feat: add Excel source support to Dataflow Gen2 generator
fix: resolve connection injection for Script activities
test: add regression tests for CDC source parsing
docs: update README with new component support table
```
