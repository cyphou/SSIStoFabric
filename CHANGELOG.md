# 📝 Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
