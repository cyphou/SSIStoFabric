# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-27

### Added

- **Fabric connection pattern for notebooks** — generated PySpark notebooks now use `notebookutils.credentials.getConnectionStringOrCreds()` to resolve JDBC URLs from Fabric connection IDs at runtime, matching how Data Factory pipelines use `externalReferences.connection`
- **`_FABRIC_CONNECTIONS` dict** emitted in each notebook mapping SSIS connection manager names to Fabric connection IDs
- **`_get_connection_id()` / `_jdbc_url_for()` helpers** generated in each notebook for clean JDBC URL resolution
- **13 new transformation generators**: SCD, Fuzzy Lookup, Fuzzy Grouping, Term Lookup, Copy Column, Character Map, Audit, Merge, CDC Splitter, Percentage Sampling, Row Sampling, Balanced Data Distributor, Cache Transform
- **30+ new SSIS expression patterns** in `_ssis_expr_to_pyspark`: DATEADD, DATEDIFF, DATEPART, YEAR/MONTH/DAY, REPLACENULL, NULL(), LEFT/RIGHT, FINDSTRING, TOKEN, REVERSE, ABS, CEILING, FLOOR, ROUND, POWER, SIGN, SQUARE, SQRT, EXP, LOG, LOG10, HEX, plus 15 DT_* type cast patterns
- **82 new tests** (410 → 492) covering transformation generators, expression patterns, and generated Python validity

### Fixed

- **JDBC read chain broken by inline comment** — `# Fabric Connection: ...` comment after `.option("url", ...)` absorbed the `\` line continuation, breaking the method chain
- **`df` undefined in notebooks** — source reads produced `df_source_xxx` but transformations/destinations used bare `df`; added `df = df_source_xxx` bridge assignment
- **Empty column names in data conversion** — `withColumn("", ...)` emitted for columns with empty names; now filtered out
- **Fallback expression broke `withColumn()` parens** — `_ssis_expr_to_pyspark` fallback returned `F.expr(...)  # TODO: ...` where the `#` comment swallowed the closing `)` of `withColumn()`; removed all inline comments from expression return values
- **Generated notebooks now pass `ast.parse()`** — all emitted Python is syntactically valid

## [1.0.0] - 2026-02-26

### Added

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
