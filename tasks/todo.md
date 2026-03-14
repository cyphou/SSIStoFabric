# Task Plan — SSIS to Fabric Migration Tool

> This file tracks the current development plan. Updated by Copilot and reviewed
> by the developer. See `.github/copilot-instructions.md` for workflow rules.

---

## Current Phase: Post-Phase 29 Stabilization

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

## Next Up — Phase 30: Multi-Cloud Target Support (v5.0.0)

- [ ] Design target abstraction layer (`FabricTarget`, `DatabricksTarget`, `GlueTarget`)
- [ ] Generator plugin interface per target (extending Phase 9 plugin architecture)
- [ ] Databricks notebook + Workflow JSON generator
- [ ] AWS Glue PySpark ETL script generator
- [ ] GCP Dataflow Apache Beam pipeline generator
- [ ] `--target fabric|databricks|glue|dataflow` CLI flag
- [ ] Target-specific connection mapping
- [ ] Tests for each target generator

## Backlog — Phases 31–36

| Phase | Focus | Version |
|-------|-------|---------|
| 31 | Auto-Remediation & Self-Healing | v5.1.0 |
| 32 | Migration Analytics & Reporting | v5.2.0 |
| 33 | Disaster Recovery & HA | v5.3.0 |
| 34 | Compliance Frameworks | v5.4.0 |
| 35 | API-First & SDK | v5.5.0 |
| 36 | Legacy Modernization Patterns | v6.0.0 |

---

## Review Notes

_Updated: 2026-03-14 — All tests pass (1677 passed, 3 skipped). Version 4.3.0._
