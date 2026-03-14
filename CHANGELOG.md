# 📝 Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [4.0.0] - 2025-07-24

### 🧠 Phase 26 — Intelligent Migration
- AI-assisted pattern recognition: auto-classify packages (SIMPLE_COPY, ORCHESTRATOR, CDC, SCD, etc.)
- Natural language migration queries with table/task/keyword matching
- Automated validation test generation from package semantics
- Strategy recommendation engine with confidence scoring and effort estimation
- Built-in knowledge base (8 entries) with searchable migration patterns
- `ssis2fabric smart-analyze` CLI command with Rich table output

### 🔌 Phase 25 — Enterprise Connectors
- 14 connector types: SAP, Salesforce, Oracle, Teradata, DB2, S3, GCS, BigQuery, REST, Kafka, MQTT, OLEDB, ODBC
- Automatic provider-based and connection-string-based connector detection
- Type-specific Fabric connection configuration (managed identity, service principal)
- OneLake shortcut generation for S3/GCS/ADLS cloud sources
- Azure SQL auto-detection from `.database.azure.windows.net` connection strings
- `ssis2fabric connector-map` CLI command

### ⚡ Phase 24 — Performance Optimization
- `MigrationProfiler` with start/stop timing, artifact/line counting, and summary report
- Spark notebook optimizer: broadcast join hints, cache suggestions, coalesce/repartition advice
- Pipeline parallelism advisor: dependency graph analysis, max parallelism detection
- Fabric capacity auto-tuning: SKU recommendation based on pipeline/notebook workload
- `ssis2fabric benchmark` CLI command with comparison report

### 🛡️ Phase 23 — Policy Engine & Governance
- Declarative policy rules with severity levels (ERROR/WARNING/INFO)
- Policy categories: NAMING, SECURITY, COMPLIANCE, QUALITY
- Pre-deployment gates: naming validation, forbidden pattern detection, required tag checks
- Environment promotion workflows: DEV → STAGING → PROD with automatic policy evaluation
- Data classification: auto-detect PII, PHI, financial data via 9 regex patterns
- `ssis2fabric policy-check` CLI with `--strict` fail mode

### 📅 Phase 22 — Orchestration & Scheduling
- SQL Server Agent schedule extraction with automatic cron expression generation
- Fabric Scheduled Trigger generation from extracted schedules
- Cross-pipeline dependency graph with topological ordering
- SLA definitions with max duration and priority tracking
- External scheduler adapters: Airflow DAG and Azure Logic App generation
- `ssis2fabric schedule-export` CLI command

### 🔬 Phase 21 — Advanced Script Transpilation
- AST-based C# parsing with 18 node types (class, method, if/else, loop, try/catch, etc.)
- 60+ .NET BCL method mappings across System, System.IO, System.Data, System.Net, System.Text
- LINQ expression → PySpark chain transpilation
- Confidence scoring per transpiled function (auto vs manual review)
- Side-by-side diff view: original C# vs generated Python
- `ssis2fabric transpile` support via script_transpiler engine

### 📡 Phase 20 — Streaming & Real-Time
- Streaming source detection: Event Hub, Kafka, CDC, Service Bus, MQTT
- Connection manager and task inspection for streaming protocol identification
- Fabric Eventstream artifact generation with configurable windowing
- Streaming readiness scoring (0-100) with actionable recommendations
- `ssis2fabric streaming-assess` CLI command with HTML report

### 🌐 Phase 19 — Web Dashboard
- 12 REST API route definitions mirroring all CLI commands
- OpenAPI 3.0.3 spec generation for API documentation
- Server-Sent Events (SSE) progress streaming with typed events
- Package browser view with task/connection/variable counts
- Self-contained dashboard HTML generation (no external dependencies)
- `ssis2fabric dashboard` CLI command

### 📦 Phase 18 — GitOps & Artifact Versioning
- Change tracking with `ChangeRecord` and JSON-based `ChangeLog`
- `.fabricignore` parser with glob pattern support
- Artifact diff: compare pipeline JSON across migration runs
- Git operations: init, branch creation, add-and-commit
- Artifact sync to git with branch-per-migration strategy
- `ssis2fabric git-sync` CLI command

### 🔍 Phase 17 — Data Quality Framework
- Column-level profiling: nulls, cardinality, min/max, patterns
- 6 validation rule types: NOT_NULL, UNIQUE, RANGE, REGEX, REFERENTIAL, CUSTOM
- Pre-migration risk scoring (LOW/MEDIUM/HIGH/CRITICAL)
- Post-migration reconciliation: row count and column profile comparison
- HTML quality report writer
- `ssis2fabric data-quality` CLI command

---

## [2.4.0] - 2026-03-14

### 🛠️ Phase 16 — Developer Experience

#### JSON Schema Export
- `generate_config_json_schema()`: auto-generates JSON Schema from Pydantic `MigrationConfig`
- `write_json_schema()`: writes schema file for external validation (VS Code, CI, pre-commit)
- `ssis2fabric schema-export` CLI command with configurable output path
- Schema includes `$schema` draft 2020-12 reference and descriptive title

#### Architecture Decision Records (ADRs)
- `ADR` dataclass: number, title, status, context, decision, consequences, date
- 5 pre-defined project ADRs: Pydantic config, Click CLI, lxml parsing, hybrid strategy, plugin architecture
- `generate_adrs()`: writes all ADRs + README index to `docs/adr/`
- Markdown format with status tracking (Proposed, Accepted, Deprecated, Superseded)

#### Migration Cookbook
- 10 common SSIS-to-Fabric patterns with before/after code comparisons
- Covers: data copy, derived columns, lookups, conditional splits, SCD Type 2, SQL tasks, loops, parent-child, aggregates, error handling
- Each entry: title, description, complexity level, tags, SSIS pattern, Fabric equivalent
- `generate_cookbook()`: writes `docs/migration_cookbook.md`
- `get_cookbook_entries()`: programmatic access for tooling

#### Interactive Decision Tree
- HTML wizard for migration strategy selection (Spark, Data Factory, Hybrid)
- Self-contained: inline CSS + JavaScript, no external dependencies
- `write_decision_tree()`: outputs `docs/strategy_wizard.html`

#### Sphinx Documentation
- `generate_sphinx_conf()`: creates `docs/conf.py` with autodoc, napoleon, viewcode, intersphinx
- `write_sphinx_conf()`: generates conf.py + index.rst with API module references
- Configured for furo theme with Google-style docstring support

#### CLI Commands
- `ssis2fabric schema-export`: export JSON Schema for config validation
- `ssis2fabric generate-docs`: generate cookbook, ADRs, wizard, and Sphinx config (each toggleable)

---

## [2.3.0] - 2026-03-14

### 🏢 Phase 15 — Enterprise Scale

#### RBAC — Role-Based Access Control
- `MigrationRole` enum: `ADMIN`, `OPERATOR`, `VIEWER`, `DEPLOYER` with granular permissions
- `RBACPolicy` dataclass: principal (Azure AD group), role, scope (package or wildcard)
- `RBACConfig`: enable/disable, default role fallback, policy list with `authorize()` method
- Scope-based filtering: restrict roles to specific packages or grant wildcard access
- Serializable: `to_dict()` / `from_dict()` for YAML/JSON configuration

#### Multi-Tenant Migration
- `TenantTarget` dataclass: tenant_id, workspace_id, connection_mappings, environment
- `MultiTenantConfig`: list of tenant targets with configurable parallelism
- `plan_multi_tenant_deployment()`: generates batched deployment plans across tenants
- `TenantDeploymentResult`: per-tenant success/failure tracking

#### Queue-Based Migration Orchestration
- `MigrationQueue` class: in-process job queue with priority ordering
- `MigrationJob` dataclass: job lifecycle (PENDING → RUNNING → COMPLETED/FAILED/CANCELLED)
- `submit()`, `claim()`, `complete()`, `fail()`, `cancel()` job lifecycle methods
- `retry_failed()`: re-queue failed jobs within max attempt limits
- `save()` / `load()`: persist queue state to JSON for crash recovery
- Worker identity: `generate_worker_id()` for distributed scenarios

#### Cost Estimation
- `estimate_migration_cost()`: projects Fabric CU consumption per artifact
- Complexity-weighted: pipeline activity count and notebook line count
- Configurable daily execution multiplier
- `ssis2fabric cost-estimate` CLI command with formatted table output
- `write_cost_report()`: JSON report with per-artifact and aggregate totals

#### Compliance Reporting
- `ComplianceTracker` class: records audit trail for SOC2/GDPR compliance
- `AuditEntry` dataclass: timestamp, action, principal, package, correlation_id
- `data_lineage_report()`: aggregated compliance summary
- `write_audit_log()`: persists audit entries + summary to JSON
- `ssis2fabric compliance-report` CLI command
- Filter queries: `entries_for_package()`, `entries_for_principal()`

---

## [2.2.0] - 2026-03-14

### 🔌 Phase 14 — Integrations & Ecosystem

#### Azure Key Vault Secret Resolution
- `keyvault://vault-name/secret-name` reference pattern for config values
- `resolve_keyvault_reference()`: resolves secrets via Azure SDK (with fallback placeholder)
- `resolve_config_secrets()`: walks config dict and resolves all Key Vault references
- `is_keyvault_reference()`: detection helper
- `KeyVaultConfig` dataclass: vault URL, credential type, caching options

#### Power BI Dataset Generation
- `generate_powerbi_dataset_from_lineage()`: creates thin dataset from lineage graph
- Uses destination tables (writers) from lineage as dataset tables
- `PowerBIDataset` / `PowerBITable` dataclasses with `to_dict()` serialization
- `ssis2fabric powerbi-dataset` CLI command: generates `.dataset.json` from lineage

#### dbt Model Scaffolding
- `scaffold_dbt_models_from_notebooks()`: extracts SQL and table references from notebooks
- Generates dbt SQL model files with `{{ config() }}` blocks
- Generates `schema.yml` with model metadata and tags
- `ssis2fabric dbt-scaffold` CLI command: scaffolds from `notebooks/` directory
- `DbtModel` dataclass: name, sql, description, materialization, tags

#### Webhook Notifications
- `WebhookNotifier` class: sends JSON payloads to registered webhook URLs
- Event filtering: only notifies webhooks subscribed to specific events
- `WebhookConfig`: URL, custom headers, event subscription list
- `format_slack_message()`: Slack Block Kit message formatting
- `format_teams_message()`: Microsoft Teams Adaptive Card formatting

#### Init Command
- `ssis2fabric init` CLI command: generates starter `migration_config.yaml`
- Options: `--project-name`, `--packages-path`, `--workspace-id`, `--output`
- Refuses to overwrite existing config files
- Generated YAML includes Key Vault reference comments and all config sections

---

## [2.1.0] - 2026-03-14

### 🧪 Phase 13 — Testing & Quality

#### Generated Code Validation
- `validate_python_syntax()`: validates generated PySpark notebooks via `ast.parse()`
- `validate_notebook_dir()`: batch-validates all `.py` notebooks in output directory
- `validate_pipeline_json()`: validates pipeline JSON structure and required keys
- Heuristic warnings for TODO/FIXME markers and `F.expr()` fallbacks
- `CodeValidationResult` dataclass: file_path, is_valid, errors, warnings, line_count

#### Expression Transpiler Fuzzing
- `generate_random_ssis_expression()`: random SSIS expression generator for fuzz testing
- Builds syntactically plausible expressions: functions, casts, ternaries, nested calls
- Fuzz-test round-trip: generate → transpile → `ast.parse()` validation
- 10 seed-based parametrized fuzz tests cover expression transpiler edge cases

#### M Syntax Validation
- `validate_m_syntax()`: basic Power Query M structural validation
- Checks balanced brackets, `let...in` structure, known M keywords
- Severity-based reporting (errors vs warnings)

#### Report Validation
- `validate_report_json()`: validates migration report JSON for expected keys (`summary`, `packages`)
- `validate_report_html()`: validates HTML reports for `<html>`, `<title>`, and expected content
- Integration tests validate actual project output when available

#### Benchmark Helpers
- `benchmark()` function: measures execution time with iteration support
- `BenchmarkResult` dataclass with `ops_per_second` calculation
- `write_benchmark_results()`: serializes benchmark data to JSON
- `benchmark` pytest marker for performance test identification

#### CI/CD Quality Gate
- `QualityGate` job added to Azure Pipelines Build stage
- Mutation testing with `mutmut` (informational, non-blocking)
- `[quality]` optional dependency group: hypothesis, mutmut, pytest-benchmark
- `[tool.mutmut]` configuration in pyproject.toml

---

## [2.0.0] - 2026-03-14

### 🚀 Phase 12 — Deployment Hardening

#### Deployment State Machine
- `DeploymentState` enum: `PENDING`, `QUEUED`, `IN_PROGRESS`, `VALIDATING`, `COMMITTED`, `ROLLED_BACK`, `FAILED`
- Valid transition enforcement with `InvalidStateTransitionError` exception
- Full transition history recording with timestamps for audit trails

#### Deployment Snapshots & Rollback
- `DeploymentSnapshot` dataclass: tracks deployment lifecycle, items deployed, and state history
- Snapshot persistence: `save_snapshot()` / `load_snapshots()` / `load_latest_snapshot()` (JSON-based)
- `ssis2fabric rollback` CLI command: reverts latest or specific deployment by ID
- `--deployment-id` option: target a specific deployment for rollback

#### Pre-Deploy Validation
- `ssis2fabric validate-deploy` CLI command: validates migration output before deployment
- Pipeline JSON validity checks (detects malformed JSON)
- Notebook file existence and non-empty content validation
- Workspace naming conflict detection (warns when items already exist)
- `ValidationResult` with severity-based issue classification (error/warning/info)

#### Blue-Green Deployment
- `BlueGreenDeployment` class: deploy to staging, validate, then swap to production
- Configurable staging suffix (`_staging` default)
- `swap_plan()` generates rename operations from staging → production names
- `can_swap()` readiness check before executing swap

#### Adaptive Rate Limiting
- `AdaptiveRateLimiter`: dynamically adjusts concurrency based on API responses
- Halves concurrency on 429 throttle responses with exponential backoff
- Gradual recovery: concurrency increases by 1 after 10 consecutive successes
- Configurable min/max concurrency bounds

#### Pipeline Scheduling
- `ScheduleTrigger` dataclass: daily, hourly, weekly, or cron schedule definitions
- `to_trigger_payload()`: Fabric-compatible schedule trigger JSON generation
- `generate_schedule_config()`: writes `schedule_triggers.json` to output directory

---

## [1.9.0] - 2026-03-14

### ✨ Phase 11 — Column-Level Lineage

#### Column-Level Lineage Tracking
- `ColumnLineageGraph` class: builds column-level DAG from parsed SSIS packages
- `ColumnEdge` dataclass: source_table, source_column → destination_table, destination_column
- BFS-based column tracing through data flow component chains (source → transforms → destination)
- Column name matching: source output columns tracked through intermediate transforms to destination inputs

#### Transformation Semantics
- `TransformationType` enum: `PASSTHROUGH`, `DERIVE`, `JOIN`, `AGGREGATE`, `FILTER`, `LOOKUP`, `SORT`, `UNION`, `PIVOT`, `UNPIVOT`, `CONVERT`, `UNKNOWN`
- Component type → transformation mapping: Derived Column → `DERIVE`, Lookup → `LOOKUP`, Merge Join → `JOIN`, Aggregate → `AGGREGATE`, Conditional Split → `FILTER`, etc.
- Derived Column expressions tracked: `[Price] * [Qty]` → source column reference extraction

#### Column-Level Impact Analysis
- `column_impact(fqn)` method: returns upstream and downstream column edges transitively
- BFS traversal for multi-hop lineage: A.X → B.X → C.X chains
- CLI: `ssis2fabric lineage --column dbo.Sales.Amount` shows upstream/downstream edges

#### Interactive D3.js Sankey Visualization
- `to_sankey_data()` exports nodes and links for D3.js Sankey diagrams
- Embedded in HTML migration report via `_render_column_lineage()`
- Color-coded by transformation type and source table
- Auto-sized canvas based on node count

#### Cross-Package Column Tracing
- Execute Package parameter bindings traced as column lineage edges
- Parent variable expressions mapped to child parameter names
- Enables multi-package column flow tracking

#### Export Formats
- `column_lineage.json` output alongside existing `lineage.json`
- Summary statistics: total edges, total columns, transformation type counts
- Mermaid flowchart export for column lineage
- Sankey data embedded in `migration_report.json` for HTML report

#### Infrastructure
- 32 new tests (1101 total)

---

## [1.8.0] - 2026-03-14

### ✨ Phase 10 — Advanced SSIS Features

#### Transaction Scope Support
- `TransactionOption` enum (`NOT_SUPPORTED`, `SUPPORTED`, `REQUIRED`) on `ControlFlowTask`
- Parser extracts `DTS:TransactionOption` (0/1/2) and `IsolationLevel` attributes
- Data Factory: `Required` transactions annotated with `[Transaction: REQUIRED]` and fail-fast policy (retry=0)
- Spark notebooks: transaction option shown in header when non-default

#### Task-Level Checkpoint / Restart
- Incremental state now tracks `completed_tasks` per package in `state.json`
- Individual tasks completed in a prior run (same hash) are skipped on re-run
- Enables fine-grained resume after partial failures

#### Logging Provider Migration
- `LogProvider` model: name, provider_type, connection_manager_ref, description, properties
- Parser extracts `DTS:LogProviders/DTS:LogProvider` elements
- `logging_config.json` manifest generated alongside connection manifests
- Log provider names included in pipeline description

#### WMI, Web Service & XML Task Generation
- `WMI_EVENT_WATCHER` and `WMI_DATA_READER` task types added to `TaskType` enum
- `TASK_CLASS_MAP` entries for `Microsoft.WebServiceTask`, `Microsoft.XMLTask`, `Microsoft.WmiEventWatcherTask`, `Microsoft.WmiDataReaderTask`
- Data Factory: WebService → `WebActivity`, XML → `Script` placeholder, WMI → `Wait` placeholder
- Spark: Python `requests` template, `lxml`/`etree` template, WQL information placeholder

#### Package Annotation Preservation
- Parser extracts `DTS:PackageAnnotations/DTS:Annotation` elements
- Annotations added to pipeline `annotations` array and Spark notebook header
- Package description included in both pipeline and notebook metadata

#### Disabled Task Handling
- Disabled tasks (`task.disabled == True`) skip generation in Data Factory (return `None`)
- Confirmed across both generators with regression tests

#### Infrastructure
- 56 new tests (1069 total)

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
