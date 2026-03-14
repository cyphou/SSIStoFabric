# 📝 Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] — Roadmap

> The following phases are planned. Each will be released as a minor or major version.

### Phase 10 — Advanced SSIS Features *(v1.8.0)*
- Transaction scope support (`Required`/`Supported` → pipeline activity groups with error handling)
- Checkpoint/restart (idempotent re-runs with checkpoint state persistence and resume)
- Disabled task handling (skip generation or emit commented-out code)
- Logging provider migration (SSIS SQL/XML/text log providers → Fabric monitoring config)
- WMI Event Watcher, Web Service, and XML Task generation (currently routed to MANUAL)
- Package annotation preservation in generated artifacts as comments

### Phase 11 — Column-Level Lineage *(v1.9.0)*
- Column-level lineage tracking (source column → transformation → destination column)
- Transformation semantics in lineage graph (JOIN, FILTER, DERIVE, AGGREGATE)
- Column-level impact analysis (`lineage impact --column dbo.Sales.Amount`)
- Interactive lineage visualization (D3.js Sankey diagrams in HTML report)
- Cross-package column tracing (Execute Package parameter → child column mapping)

### Phase 12 — Deployment Hardening *(v2.0.0)*
- Blue-green deployment (deploy to staging folder, validate, then swap)
- `ssis2fabric rollback` CLI command (revert to previous deployment snapshot)
- Pre-deploy workspace validation (permissions, capacity tier, naming conflicts)
- Adaptive rate limiting and throttling for Fabric REST API
- Deployment state machine (`PENDING` → `QUEUED` → `IN_PROGRESS` → `COMMITTED` / `ROLLED_BACK`)
- Pipeline scheduling (configure triggers on deployed pipelines)

### Phase 13 — Testing & Quality *(v2.1.0)*
- Property-based testing (Hypothesis) for expression transpiler fuzzing
- Mutation testing (mutmut) integration in CI
- E2E test harness (Docker Compose with SQL Server + sample SSISDB + automated migration)
- Generated code validation (PySpark `ast.parse()`, M syntax checker)
- Performance benchmarks (pytest-benchmark) with regression tracking
- Visual regression for HTML reports (screenshot comparison)

### Phase 14 — Integrations & Ecosystem *(v2.2.0)*
- Azure Key Vault secret resolution for connection credentials
- Power BI dataset generation from lineage graph
- dbt model scaffolding from generated Spark notebooks
- Webhook notifications (Slack, Microsoft Teams, email) on migration events
- GitHub Actions marketplace action (`ssis-to-fabric-action`)
- `ssis2fabric init` command to generate starter `migration_config.yaml`

### Phase 15 — Enterprise Scale *(v2.3.0)*
- RBAC support (role-based package-level access control with Azure AD groups)
- Multi-tenant migration (parallel deployment to multiple workspace targets)
- Queue-based migration orchestration (async job processing with progress tracking)
- Horizontal scaling (distribute work across machines via Redis/Azure Queue)
- Cost estimation (Fabric CU consumption projections per generated artifact)
- Compliance reporting (data flow audit trail for SOC2/GDPR)

### Phase 16 — Developer Experience *(v2.4.0)*
- Sphinx API documentation auto-generated from docstrings
- Architecture Decision Records (ADRs) for all major design choices
- Interactive migration decision tree (HTML wizard for strategy selection)
- Migration cookbook (common SSIS patterns with before/after code comparisons)
- JSON Schema export for `migration_config.yaml` validation by external tools
- VS Code extension (inline assessment, syntax highlighting for generated artifacts)

---

## [1.7.0] - 2026-03-14

### ✨ Phase 9 — Plugin Architecture

#### TransformationStrategy Protocol
- `TransformationStrategy` runtime-checkable protocol: `can_handle()`, `generate_pyspark()`, `generate_m()`
- Custom handlers override built-in generator logic for any SSIS data flow component type
- Context dict passes `generator`, `config` to handler for full access

#### Component Handler Registry
- `ComponentRegistry` class: `register()`, `unregister()`, `get()`, `has()`, `clear()`
- `@component_handler("ComponentType")` class decorator for declarative registration
- Global singleton via `get_component_registry()`
- Custom handlers take precedence; built-in generator falls through when absent

#### Lifecycle Hook System
- `HookManager` with 8 lifecycle events: `pre_parse`, `post_parse`, `pre_plan`, `post_plan`, `pre_generate`, `post_generate`, `pre_deploy`, `post_deploy`
- Hooks invoked in registration order; return value chains through subsequent callbacks
- `@hook("event_name")` function decorator for declarative registration
- Exceptions in hooks are logged but do not halt migration
- `MigrationEngine.create_plan()` fires `pre_plan` / `post_plan`
- `MigrationEngine.execute()` fires `pre_generate` / `post_generate`
- `SSISMigrator.analyze()` fires `pre_parse` / `post_parse`
- `SSISMigrator.deploy()` fires `pre_deploy` / `post_deploy`

#### Custom Expression Transpiler Rules
- `register_expression_rule(pattern, replacement, target, priority)` for user-supplied regex rules
- Rules can target `"pyspark"`, `"m"`, or `"both"` transpilers
- Priority-ordered: higher priority rules applied first
- Callable replacements supported: `(re.Match) -> str`
- Rules applied at the start of transpiler before built-in patterns

#### Plugin Discovery via entry_points
- `discover_plugins(group="ssis_to_fabric.plugins")` loads third-party pip packages
- Each plugin receives `(registry, hooks, add_expr_rule)` for full extensibility
- `[project.entry-points."ssis_to_fabric.plugins"]` in `pyproject.toml`
- Failed plugins logged and skipped without halting

#### Infrastructure
- `reset_all()` helper clears all registries (test isolation)
- 49 new tests (1013 total)

---

## [1.6.0] - 2026-03-14

### ✨ Phase 8 — Expression & Transpiler Completeness

#### Bitwise Operators
- `BITAND(a, b)`, `BITOR(a, b)`, `BITXOR(a, b)` in PySpark (`.bitwiseAND/OR/XOR()`)
- `BITAND`, `BITOR`, `BITXOR` in Power Query M (`Number.BitwiseAnd/Or/Xor`)

#### Recursive Nested Expression Handling
- PySpark transpiler rewritten with recursive descent parser (mirrors M architecture)
- Nested calls now handled at arbitrary depth: `UPPER(SUBSTRING(REPLACE(...), 1, 5))`
- Internal helpers: `_find_close_paren()`, `_split_args()`, `_conv()` with string-aware parsing
- Type casts `(DT_*)` now recurse into their operand expressions

#### New SSIS Functions
- **Date**: `GETUTCDATE()` → `F.to_utc_timestamp()` / `DateTimeZone.UtcNow()`
- **Trig**: `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2` in both PySpark and M
- **Math**: `LOG2()`, `PI()`, `RAND()` in both targets
- **String**: `CONCATENATE()` function in PySpark

#### Expression Validation & Error Reporting
- `ExpressionTranspiler.validate(expr)` returns `ValidationResult` with typed issues
- Detects: unbalanced parentheses, unknown functions, unrecognised `DT_*` type casts
- `ExpressionIssue` dataclass with severity (`error`/`warning`/`info`), message, position
- `ValidationResult.is_valid` property (True when no errors)
- `_KNOWN_FUNCTIONS` registry covering all 40+ supported SSIS functions

#### C# Transpiler AST Mode
- Two-pass transpilation: AST structure extraction → line-rule conversion
- `_extract_structure()` extracts `usings`, `namespace`, `classes`, `methods` from C# source
- Class declarations → Python `class` stubs, method signatures → `def name(params):`
- `using` directives stripped from body, mapped to Python imports via `_USING_TO_IMPORT`
- Namespace wrappers automatically removed

#### Power Query M Parity
- Fixed `_split_args()` to respect quoted strings (commas inside `"..."` no longer split)
- Added `GETUTCDATE`, trig functions, `LOG2`, `PI`, `RAND`, bitwise ops to M transpiler

---

## [1.5.0] - 2026-03-14

### ✨ Phase 7 — Observability & Diagnostics

#### Correlation IDs
- UUID-based correlation IDs (`ssis2fabric-<uuid4>`) bound via `contextvars` and threaded through structlog
- `bind_correlation_id()`, `get_correlation_id()`, `clear_correlation_id()` in `logging_config.py`
- Correlation ID attached to every log entry and included in migration plan / HTML report

#### Per-Artifact Timing Metrics
- `MigrationItem`: new `started_at`, `completed_at`, `elapsed_ms` fields — per-item wall-clock timing
- `MigrationPlan`: new `completed_at`, `total_elapsed_ms`, `correlation_id` fields
- `OrchestratorStats`: new `total_elapsed_ms`, `phase1_elapsed_ms`, `phase2_elapsed_ms` fields
- All timing fields serialized in `to_dict()` and `migration_report.json`

#### Progress Callbacks
- `ProgressCallback` type alias (`Callable[[str, dict], None]`) for event hooks
- `MigrationEngine.execute()`: emits `item_started`, `item_completed`, `phase_completed`, `migration_completed` events
- `AgentOrchestrator.run()`: emits same events through parallel phases
- `SSISMigrator.migrate()` / `run()`: `progress_callback` parameter threaded to engine / orchestrator
- CLI: Rich progress bar with spinner, task counter, and elapsed time during migration

#### Audit Logging
- JSON-lines audit trail via `configure_audit_log()` / `write_audit_entry()`
- Structured entries: `{timestamp, correlation_id, action, detail}`
- Audit entries written at migration start and completion in both engine and orchestrator

#### Enhanced HTML Report
- Timing header: total elapsed time and correlation ID displayed at top of report
- Inline SVG donut chart for status distribution (completed / errors / manual / other)
- Per-item "Time" column showing elapsed_ms for each migration artifact

---

## [1.4.0] - 2026-03-14

### ✨ Phase 6 — Fidelity & Scale

#### Security
- **SQL injection in `SSISDBExtractor.list_packages()`** — f-string interpolation replaced with parameterised queries (`cursor.execute(sql, params)`)

#### Code Quality
- **Shared generator utilities** (`engine/utils.py`) — extracted `sanitize_name()`, `is_source()`, `is_destination()`, `is_transform()`, `filter_error_columns()`, `SOURCE_COMPONENT_TYPES`, `DEST_COMPONENT_TYPES` from 3 generators to eliminate duplication
- Generators now delegate to `engine/utils.py` via thin wrappers

#### Performance
- **Assessment O(N) fix** — `assess()` now calls `create_plan()` once for all packages, then indexes by package name. Previously called it N+1 times (once per package + once per `_assess_package`)

#### Bug Fixes
- **Incremental mode selective cleanup** — `shutil.rmtree` no longer deletes all output in incremental mode. Only artifacts for changed packages are cleaned; unchanged packages' outputs are preserved. Fixed in both `MigrationEngine.execute()` and `AgentOrchestrator.run()`

#### Features
- **DAG-aware Spark generation** — when `DataFlowPath` routing is available (multicast, conditional split), the Spark generator now produces code in topological order following the actual data flow DAG instead of assuming linear source→transform→dest flow
- **Unified expression transpiler** (`engine/expression_transpiler.py`) — single `ExpressionTranspiler` class with `to_pyspark()` and `to_m()` methods for clean access to both transpilers
- **New SSIS functions**: `CODEPOINT()` and `TOKENCOUNT()` in PySpark; `CODEPOINT()`, `TOKENCOUNT()`, `HEX()` in Power Query M

#### CI/CD
- **Python 3.13** — added classifier and Azure DevOps pipeline matrix strategy (3.10, 3.11, 3.12, 3.13)
- Removed stale `build/` directory from tracking

#### Tests
- **58 new tests** (748 → 806) in `tests/unit/test_phase6_features.py`:
  - `TestSharedUtils` (7) — sanitize_name, is_source/dest/transform, filter_error_columns
  - `TestLineageGraph` (10) — build, impact, Mermaid, SQL extraction, JSON export
  - `TestReportGenerator` (5) — HTML generation, errors, edge cases, escaping
  - `TestFabricDeployer` (14) — auth, API retry, delete, deployment report, connections, folders
  - `TestExpressionTranspiler` (12) — new functions, unified facade, DT casts
  - `TestDAGAwareSpark` (3) — linear fallback, multicast, transform chains
  - `TestIncrementalCleanup` (2) — selective vs full cleanup
  - `TestSSISDBExtractorSQL` (4) — parameterised queries, injection safety
  - `TestAssessPerformance` (1) — verify single plan creation

---

## [1.3.0] - 2026-03-14

### ✨ Phase 5 — Quality & Completeness

#### Event Handler Migration
- **SSIS `OnError` event handlers** are now converted to pipeline failure-path activities with `dependsOn: ["Failed"]` conditions wired to all top-level activities
- **Other event handlers** (`OnPreExecute`, `OnPostExecute`, `OnWarning`, etc.) emit placeholder Wait activities with TODO comments for manual review
- Event handler activities are automatically included in consolidated package pipelines

#### Pre-Migration Assessment
- **`ssis2fabric assess <path>`** CLI command — produces a readiness score (0–100), per-package effort estimates (person-hours), risk flags, unsupported component list, and connection inventory
- **`MigrationEngine.assess()`** method — programmatic API for pre-migration assessment
- **`SSISMigrator.assess()`** facade method — one-call assessment via the public API
- **`AssessmentReport` / `PackageAssessment`** data models with `to_dict()` serialization
- Effort estimation uses weighted scoring: LOW=0.5h, MEDIUM=2h, HIGH=8h, MANUAL=16h per task
- Risk flags for Script Tasks, Script Components, non-OnError event handlers, and partial-parse packages

#### Config Validation
- **`ssis2fabric validate-config`** CLI command — validates the loaded configuration, checks UUID format in connection mappings, verifies output directory writability, and reports warnings for missing workspace IDs

#### Migrate Dry-Run
- **`ssis2fabric migrate --dry-run`** flag — shows the migration plan (packages, tasks, targets, complexity) without generating any files

#### Bug Fixes (from bug bash)
- **C# `IsNullOrWhiteSpace` transpilation** — fixed logic error (`or` → `and`) that gave wrong results for whitespace-only strings
- **C# `.Contains()` / `.ContainsKey()` transpilation** — fixed reversed operand order (`x.Contains(y)` → `y in x`)
- **C# File I/O resource leaks** — transpiled `open(x).read()` patterns now use `Path(x).read_text()` to avoid leaked file handles
- **Path traversal in SSISDB extractor** — folder/project names from SSISDB are now validated against directory traversal attacks
- **SQL injection in regression runner** — table names and column names are now bracket-escaped in SQL queries

#### Tests
- **40 new tests** (708 → 748) in `tests/unit/test_phase5_features.py` covering event handler conversion, assessment scoring, CLI commands, and API facade

#### Documentation
- README badges updated to v1.3.0 / 748 tests
- New CLI commands documented: `assess`, `validate-config`, `migrate --dry-run`

---

## [1.2.0] - 2026-03-14

### ✨ Phase 1 — Hardening

#### Expression Transpiler Parity
- **Complete Power Query M parity** — `_ssis_expr_to_m()` is now fully recursive and handles nested SSIS expressions like `UPPER(TRIM(SUBSTRING(...)))`
- **Date functions**: `DATEADD` → `Date.AddDays`/`Date.AddMonths`/`Date.AddYears`; `DATEDIFF` → `Duration.Days`/`Duration.TotalHours`/`Duration.TotalMinutes`/`Duration.TotalSeconds`; `DATEPART` → `Date.Year`/`Date.Month`/`Date.Day`/etc.; `YEAR`/`MONTH`/`DAY` direct mappings
- **String functions**: `REPLACENULL` → `if … = null then … else …`; `LEFT` → `Text.Start`; `RIGHT` → `Text.End`; `FINDSTRING` → `Text.PositionOf`; `TOKEN` → `List.ItemAt(Text.Split(…))`; `REVERSE` → `Text.Reverse`; `LTRIM`/`RTRIM` → `Text.TrimStart`/`Text.TrimEnd`
- **Math functions**: `ABS` → `Number.Abs`; `CEILING` → `Number.RoundUp`; `FLOOR` → `Number.RoundDown`; `ROUND` → `Number.Round`; `POWER` → `Number.Power`; `SQRT` → `Number.Sqrt`; `SIGN` → `Number.Sign`; `EXP`/`LOG`/`LOG10`
- **Type casts**: `(DT_BOOL)` → `Logical.From`; `(DT_DECIMAL,s)` / `(DT_NUMERIC,p,s)` → `Decimal.From`; `(DT_CY)` → `Currency.From`; `(DT_GUID)` → `Text.From`; `(DT_BYTES,n)` → `Binary.From`; `(DT_I2)` → `Int32.From`; `(DT_I8)` → `Int64.From`; `(DT_R4)`/`(DT_R8)` → `Number.From`

#### Graceful Error Handling
- **Malformed `.dtsx` files** — `DTSXParser.parse()` now catches `XMLSyntaxError` and `OSError`, returning a partial `SSISPackage` with `status="partial"` and descriptive `warnings` instead of crashing
- **`SSISPackage.status`** — new field (`"ok"` / `"partial"`) on the model
- **`_parse_section_safe()`** — internal helper that wraps each parse sub-section so a corrupt connection manager or event handler does not abort the entire parse

#### Deployment Retry
- **Exponential backoff with jitter** for HTTP 429 (rate-limit) and 5xx (server error) responses in `FabricDeployer._api_call()`
- **`RetryConfig`** — new Pydantic model in `config.py` exposed as `MigrationConfig.retry` with `max_retries`, `base_delay`, `max_delay`, `default_retry_after`
- Added `math` and `random` imports to `fabric_deployer.py`

#### Structured Error Reporting
- **`MigrationError` dataclass** — `source`, `severity`, `message`, `suggested_fix` fields
- **`MigrationPlan.errors`** — list of `MigrationError` populated from parser warnings, generation failures, and pipeline errors
- **`migration_report.json`** — now contains an `"errors"` array alongside existing fields

#### Tests
- **78 new tests** (478 → 556) in `tests/unit/test_new_features.py` covering all Phase 1–3 features
- Fixed failing `test_import_ssismigrator` by adding `src/ssis_to_fabric/__init__.py` that exports the public API surface

### ✨ Phase 2 — Ecosystem & DX

- **`LakehouseProvisioner`** (`engine/lakehouse_provisioner.py`) — reads `*.destinations.json` sidecar manifests and generates Spark Delta Lake or T-SQL `CREATE TABLE` DDL
- **`ReportGenerator`** (`engine/report_generator.py`) — generates a self-contained HTML dashboard from `migration_report.json` with summary cards, complexity breakdown, item table, and errors section
- **`ssis2fabric report <output_dir>`** CLI command
- **`ssis2fabric lineage <path>`** CLI command — builds data lineage graph, supports `--table` for impact analysis, `--output` to write `lineage.json`
- **PyPI release workflow** (`.github/workflows/release.yml`) — triggered on `v*` tags; builds with `python -m build` and publishes via `twine`
- **Dockerfile** — multi-stage build (builder + runtime), `ssis2fabric` CLI as entrypoint
- **`.dockerignore`** added
- **`[project.urls]`** added to `pyproject.toml` for proper PyPI metadata

### ✨ Phase 3 — Enterprise

- **`LineageGraph`** (`engine/lineage.py`) — directed table-dependency graph across all packages; `build()`, `impact()`, `to_mermaid()`, `to_dict()`, `write_json()` methods
- **`CSharpTranspiler`** (`engine/csharp_transpiler.py`) — converts simple C# Script Task code to Python equivalents; unsupported patterns emit `# TODO: Manual conversion required` comments
- **Multi-workspace environment support** — `EnvironmentProfile` model and `MigrationConfig.environments` dict; `config.get_environment(env_name)` lookup
- **Incremental/delta migration** — SHA-256 hashing of `.dtsx` files; state persisted in `.ssis2fabric/state.json`; `execute(incremental=True)` skips unchanged packages

---



### ✨ Added

- **Fabric connection pattern for notebooks** — generated PySpark notebooks now use `notebookutils.credentials.getConnectionStringOrCreds()` to resolve JDBC URLs from Fabric connection IDs at runtime, matching how Data Factory pipelines use `externalReferences.connection`
- **`_FABRIC_CONNECTIONS` dict** emitted in each notebook mapping SSIS connection manager names to Fabric connection IDs
- **`_get_connection_id()` / `_jdbc_url_for()` helpers** generated in each notebook for clean JDBC URL resolution
- **13 new transformation generators**: SCD, Fuzzy Lookup, Fuzzy Grouping, Term Lookup, Copy Column, Character Map, Audit, Merge, CDC Splitter, Percentage Sampling, Row Sampling, Balanced Data Distributor, Cache Transform
- **30+ new SSIS expression patterns** in `_ssis_expr_to_pyspark`: DATEADD, DATEDIFF, DATEPART, YEAR/MONTH/DAY, REPLACENULL, NULL(), LEFT/RIGHT, FINDSTRING, TOKEN, REVERSE, ABS, CEILING, FLOOR, ROUND, POWER, SIGN, SQUARE, SQRT, EXP, LOG, LOG10, HEX, plus 15 DT_* type cast patterns
- **82 new tests** (410 → 492) covering transformation generators, expression patterns, and generated Python validity

### 🐛 Fixed

- **JDBC read chain broken by inline comment** — `# Fabric Connection: ...` comment after `.option("url", ...)` absorbed the `\` line continuation, breaking the method chain
- **`df` undefined in notebooks** — source reads produced `df_source_xxx` but transformations/destinations used bare `df`; added `df = df_source_xxx` bridge assignment
- **Empty column names in data conversion** — `withColumn("", ...)` emitted for columns with empty names; now filtered out
- **Fallback expression broke `withColumn()` parens** — `_ssis_expr_to_pyspark` fallback returned `F.expr(...)  # TODO: ...` where the `#` comment swallowed the closing `)` of `withColumn()`; removed all inline comments from expression return values
- **Generated notebooks now pass `ast.parse()`** — all emitted Python is syntactically valid

---

## [1.0.0] - 2026-02-26

### ✨ Added

- **Core migration engine** with three strategies: `hybrid`, `data_factory`, `spark`
- **DTSX parser** supporting 28+ SSIS component types, connection managers, parameters, and variables
- **Data Factory pipeline generator** producing Fabric-compatible JSON with Script, ExecutePipeline, ForEach, Until, Copy, Dataflow, and Notebook activities
- **Dataflow Gen2 generator** producing Power Query M code with `Sql.Database()`, `Csv.Document()`, `Excel.Workbook()`, `Odbc.DataSource()`, `Xml.Tables()`, and CDC source support
- **Spark notebook generator** producing PySpark `.py` files for complex transforms (Lookup, SCD, Merge Join, Fuzzy operations)
- **SSIS expression transpiler** converting expressions to both Power Query M and PySpark
- **Fabric deployer** with multi-phase deployment (connections → folders → notebooks → leaf pipelines → referencing pipelines), automatic connection resolution by server/database, workspace folder organization, and dry-run
- **SSISDB extractor** for pulling `.dtsx` packages from SQL Server catalog via pyodbc
- **Destination sidecar manifests** (`.destinations.json`) for downstream schema provisioning
- **CLI** (`ssis2fabric`) with `analyze`, `plan`, `migrate`, `deploy`, `verify`, `validate`, and `extract-ssisdb` commands
- **Python API** (`SSISMigrator` facade) for programmatic usage
- **Non-regression framework** with file-based, structural, and data validation
- **410 unit and regression tests** with full component coverage
- **CI/CD pipelines** for both Azure DevOps and GitHub Actions
- **28 real SSIS example packages** from PacktPublishing (MIT license)
- **Connection mapping** via `migration_config.yaml` for SSIS → Fabric connection resolution
- **Hierarchical parameter support** for project parameters, package parameters, and Execute Package bindings
