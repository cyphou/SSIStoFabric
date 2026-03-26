# Task Plan — SSIS to Fabric Migration Tool

> This file tracks the current development plan. Updated by Copilot and reviewed
> by the developer. See `.github/copilot-instructions.md` for workflow rules.

---

## Current Phase: Post-Phase 33+38 Stabilization

### Completed — v5.2.0 (Phase 33 — Auto-Remediation & Phase 38 — Legacy Modernization)

- [x] `RemediationEngine`: scan and auto-patch TODO stubs in generated artifacts
- [x] Template library: 33 templates (config, expression, manual, verification, connection)
- [x] Fuzzy connection matching via Levenshtein with confidence scoring
- [x] Dry-run mode, JSON remediation report, min-confidence filter
- [x] `ModernizationEngine`: Bronze/Silver/Gold classification, 5-pattern scoring, debt scoring, wave planner
- [x] Technical debt scoring (0–100) with complexity, TODO, manual-review, unsupported components
- [x] Wave planner: dependency-aware migration waves ordered by increasing debt
- [x] 2 new CLI commands: `ssis2fabric remediate`, `ssis2fabric modernize`
- [x] 2 new `SSISMigrator` API methods: `remediate()`, `modernize()`
- [x] 136 new tests (1932 total, 0 failures)

### Completed — v5.1.0 (Phase 31 — Fabric Native API)

- [x] `OneLakeShortcutManager`: Deploy S3/GCS/ADLS/OneLake shortcuts
- [x] `LakehouseProvisioner`: Create Lakehouse items + provision Delta tables
- [x] `RefreshScheduler`: Attach scheduled triggers (Daily/Weekly) to items
- [x] `SemanticModelDeployer`: Deploy DirectLake semantic models from TMDL
- [x] `WorkspaceGitManager`: Git connect, initialize, commit, pull
- [x] Shortcut auto-deployment in `deploy_all()` Phase 0.6
- [x] 5 CLI commands: create-lakehouse, schedule-refresh, deploy-semantic-model, git-connect, git-commit
- [x] 5 `SSISMigrator` API methods
- [x] 64 new tests (1796 total, 0 failures)

### Completed — v5.0.0 (Phase 30 — Gap Closure)

- [x] SCD Type 2: Delta MERGE with row expiration/insertion via `generate_scd2_merge()`
- [x] SCD Type 3: Previous value column tracking via `generate_scd3_merge()`
- [x] SCD Type 1: Overwrite logic with F.coalesce()
- [x] Recordset Destination → Spark TempView pattern
- [x] Data Tap → Debug sampling cell
- [x] Event handler expansion: OnPreExecute/OnPostExecute/OnWarning/OnProgress
- [x] Complexity rescoring: SCD & Recordset Dest → MEDIUM
- [x] `gap_closure.py` engine module
- [x] 40 new tests

### Completed — v4.3.0 (Phase 29 — Metadata Catalog)

- [x] `MetadataCatalog` class with build, search, browse, dependency matrix
- [x] Full-text search across packages, tables, SQL, expressions, connections
- [x] 15 auto-tagging rules (STAGING, DIMENSION, FACT, LOOKUP, SCD, CDC, etc.)
- [x] Purview + Data Catalog export formats
- [x] `ssis2fabric catalog` CLI command
- [x] `SSISMigrator.catalog()` API method
- [x] 69 new tests (1677 total, 0 failures)

### Completed — Bug Fixes (merge resolution)

- [x] Recursive PySpark expression transpiler (nested functions, bitwise, trig)
- [x] Timing fields on `MigrationItem` / `MigrationPlan`
- [x] `progress_callback` support in `execute()` and `migrate()`
- [x] `add_error()` structured error reporting on `MigrationPlan`
- [x] `_generate_logging_config()` logging provider extraction
- [x] WEB_SERVICE / XML / WMI placeholder generators
- [x] Header annotations, transaction option, package description
- [x] DAG-aware data flow generation
- [x] Plugin registry integration in `_generate_transformation()`
- [x] Expression rules integration in PySpark transpiler
- [x] Lifecycle hooks (`pre_plan`, `post_plan`, `pre_generate`, `post_generate`)
- [x] `AssessmentReport.connection_inventory` and `auto_migrate_pct`
- [x] `_save_migration_state` path fix
- [x] All 84 merge-related test failures fixed

---

## Next Up — Phase 30: Migration Completeness & Gap Closure (v5.0.0)

- [ ] SCD Type 2 fix: split merged DataFrame into `df_expired` (set EndDate) + `df_new` (set StartDate, current flag)
- [ ] SCD Type 3: add PreviousValue column pattern with before/after tracking
- [ ] Event Handler — OnPreExecute: generate pre-flight validation activity (check connections, verify tables)
- [ ] Event Handler — OnPostExecute: generate post-flight logging activity (row counts, duration, status)
- [ ] Event Handler — OnWarning: generate alert/notification activity (Teams/Slack webhook or log)
- [ ] Event Handler — OnProgress: generate progress-tracking metadata update
- [ ] Recordset Destination: `createOrReplaceTempView()` for in-memory DataFrame passing between cells
- [ ] Data Tap component: generate debug/sampling cells with `df.sample()` + optional file output
- [ ] Advanced Row Sampling: exact count, percentage, and stratified sampling modes
- [ ] Re-score component complexity ratings after gap closure
- [ ] Update Known Limitations table in README
- [ ] New engine module: `gap_closure.py`
- [ ] Tests for SCD Types 1/2/3, all event handler types, Recordset, Data Tap

## Up Next — Phase 31: Fabric Native API Integration (v5.1.0)

- [ ] OneLake Shortcut deployment: POST `/v1/workspaces/{id}/items/{id}/shortcuts`
- [ ] Lakehouse table provisioning via OneLake Tables API (create Delta tables programmatically)
- [ ] Refresh scheduling: POST `/v1/workspaces/{id}/items/{id}/schedules` for pipeline triggers
- [ ] Semantic Link: generate DirectLake semantic model definition (`.bim` JSON) from lakehouse
- [ ] Workspace Git integration: auto-commit generated artifacts via Fabric Git REST API
- [ ] Access control: assign workspace roles and item-level sharing during deployment
- [ ] Environment provisioning: create Spark environments with library dependencies
- [ ] `ssis2fabric deploy --provision-lakehouse` and `--provision-shortcuts` flags
- [ ] Enhanced `fabric_deployer.py` with new API surface
- [ ] Tests for each new API integration with mock HTTP responses

## Backlog — Phases 32–41

### Phase 32: Multi-Cloud Target Support (v5.2.0)

- [ ] Target abstraction layer: decouple code generation from Fabric-specific output
- [ ] `TargetGenerator` base class with `FabricTarget`, `DatabricksTarget`, `GlueTarget`, `DataflowTarget`
- [ ] Databricks generator: Delta Live Tables notebooks, Unity Catalog, Workflow JSON
- [ ] AWS Glue generator: PySpark Glue ETL scripts, Glue Crawler configs
- [ ] GCP Dataflow generator: Apache Beam pipelines, Dataflow Flex Templates
- [ ] `--target fabric|databricks|glue|dataflow` CLI flag
- [ ] New engine module: `multi_cloud.py`
- [ ] Tests for each target generator + cross-target parity tests

### Phase 33: Auto-Remediation & Self-Healing (v5.3.0)

- [ ] Template library: 30+ code templates for common `# TODO` stub patterns
- [ ] Fuzzy connection mapping: match SSIS connection managers to target catalog entries
- [ ] Expression fallback decomposition: break complex expressions into simpler sub-steps
- [ ] Confidence scoring per remediated artifact (HIGH/MEDIUM/LOW)
- [ ] Self-healing loop: detect generation failures → apply fix strategy → retry
- [ ] `ssis2fabric remediate` CLI command with `--dry-run` and `--min-confidence` flags
- [ ] New engine module: `remediation.py`
- [ ] Tests for remediation templates and self-healing loop

### Phase 34: Migration Analytics & Reporting (v5.4.0)

- [ ] `MigrationHistory` store: track every migration run with timestamps, metrics, outcomes
- [ ] Longitudinal trend tracking: success rate, complexity distribution, velocity over time
- [ ] Power BI `.pbix` template export for migration analytics dashboard
- [ ] Burndown chart: remaining packages vs. projected completion
- [ ] `ssis2fabric analytics` CLI command with `--export pbi|csv|json`
- [ ] New engine module: `analytics.py`
- [ ] Tests for history tracking and analytics export

### Phase 35: Disaster Recovery & High Availability (v5.5.0)

- [ ] Cloud-backed checkpoints: save migration state to Azure Blob / S3 / GCS
- [ ] Resumable migrations: restart from last successful checkpoint after failure
- [ ] Point-in-time rollback: revert to any prior checkpoint (not just the last)
- [ ] DR runbooks: auto-generated step-by-step recovery documentation
- [ ] `ssis2fabric checkpoint` and `ssis2fabric restore` CLI commands
- [ ] New engine module: `disaster_recovery.py`
- [ ] Tests for checkpoint/restore and resumable migration

### Phase 36: Compliance Frameworks (v6.0.0)

- [ ] HIPAA/PCI-DSS/ISO 27001/SOX rule sets
- [ ] Evidence package generator: bundled proof of compliance per framework
- [ ] SIEM integration: export audit events to Splunk / Sentinel / Elastic
- [ ] `ssis2fabric compliance` CLI with `--framework hipaa|pci|iso27001|sox`
- [ ] New engine module: `compliance.py`
- [ ] Tests for each compliance rule set

### Phase 37: API-First Architecture & SDK (v6.1.0)

- [ ] REST API server (FastAPI): expose all migration operations as HTTP endpoints
- [ ] Python SDK: `ssis2fabric.sdk` package with typed client
- [ ] Webhook system: configurable callbacks for migration events
- [ ] GraphQL endpoint + OpenAPI 3.1 spec
- [ ] `ssis2fabric serve` CLI command with `--port` and `--workers`
- [ ] New engine module: `api_server.py`
- [ ] Tests for API endpoints and SDK client

### Phase 38: Legacy Modernization Patterns (v6.2.0)

- [ ] Medallion architecture generator: Bronze → Silver → Gold layers
- [ ] 20+ modernization patterns, wave planner, technical debt scoring
- [ ] Data mesh integration: domain-aligned data product generation
- [ ] `ssis2fabric modernize` CLI command
- [ ] New engine module: `modernization.py`
- [ ] Tests for modernization pattern generation

### Phase 39: Interactive Migration Studio (v6.3.0)

- [ ] Web-based visual migration editor (React + FastAPI backend)
- [ ] Drag-and-drop mapping, live preview, expression editor
- [ ] Override manager: pin manual overrides that survive re-migration
- [ ] `ssis2fabric studio` CLI command
- [ ] New engine module: `studio.py`
- [ ] Tests for studio API and override persistence

### Phase 40: Migration Marketplace & Community (v6.4.0)

- [ ] Shareable migration template registry (local + remote)
- [ ] Community connector plugins, pattern marketplace
- [ ] `ssis2fabric marketplace search|install|publish` CLI commands
- [ ] New engine module: `marketplace.py`
- [ ] Tests for template registry and marketplace operations

### Phase 41: Autonomous Migration Agent (v6.5.0)

- [ ] Fully autonomous end-to-end migration orchestrator (zero-touch mode)
- [ ] Self-learning pattern library, LLM-powered decision engine
- [ ] Adaptive retry with progressive escalation
- [ ] `ssis2fabric auto-migrate` CLI command with `--approval manual|auto`
- [ ] New engine module: `autonomous.py`
- [ ] Tests for autonomous decision engine and feedback loop

---

## Review Notes

_Updated: 2026-03-14 — All tests pass (1677 passed, 3 skipped). Version 4.3.0._
