# SSIS to Microsoft Fabric Migration Tool

Production-ready framework for migrating SQL Server Integration Services (SSIS) packages to Microsoft Fabric using **Data Factory pipelines**, **Dataflow Gen2**, and **Spark notebooks**.

> **410 tests** | **28 pipelines** | **28 notebooks** generated from the included example projects.

---

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────────────┐
│  SSIS Packages   │────▶│   Analyzer    │────▶│      Migration Engine       │
│  (.dtsx files)   │     │  DTSXParser   │     │   (Routing & Orchestration) │
│  + Project.params│     │              │     └──────────┬──────────────────┘
└─────────────────┘     └──────────────┘                │
        ▲                                 ┌─────────────┼──────────────┬──────────────┐
        │                                 ▼             ▼              ▼              ▼
┌───────┴─────────┐            ┌──────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────┐
│ SSISDB Extractor │            │ Data Factory │ │ Dataflow   │ │   Spark    │ │  Manual  │
│ (catalog → .dtsx)│            │ Generator    │ │ Gen2       │ │ Notebook   │ │  Review  │
└─────────────────┘            │ (Pipelines)  │ │ Generator  │ │ Generator  │ │  Report  │
                               └──────┬───────┘ └──┬───┬─────┘ └──┬───┬────┘ └──────────┘
                                      ▼            ▼   ▼           ▼   ▼
                                pipeline.json  df.json  sidecar  nb.py  sidecar
                                                     .destinations.json
```

### Output Artifacts

| Artifact | Format | Description |
|----------|--------|-------------|
| **Pipelines** | Fabric JSON | One consolidated pipeline per SSIS package + orchestrator |
| **Dataflows** | Dataflow Gen2 JSON | Power Query M for source + transformations (no destination — configured in Fabric UI) |
| **Notebooks** | PySpark `.py` | Complex transformations (lookups, scripts, SCD) |
| **Destination Manifests** | Sidecar JSON | Per-dataflow/notebook `*.destinations.json` with target table, columns, and connection ref |
| **Migration Report** | JSON | Full traceability: task → artifact mapping |

---

## Features

### SSIS Component Support

| SSIS Component | Fabric Target | Notes |
|----------------|---------------|-------|
| Execute SQL Task | Script Activity | Query/NonQuery auto-classified; SSIS connection ref mapped; timeout set |
| Data Flow (OLE DB / ADO.NET) | Dataflow Gen2 | `Sql.Database()` / `Value.NativeQuery()` in M; `spark.sql()` in PySpark |
| Data Flow (Flat File) | Dataflow Gen2 | `Csv.Document()` in M; `spark.read.csv()` in PySpark |
| Data Flow (Excel) | Dataflow Gen2 | `Excel.Workbook()` in M; `spark.read.format("excel")` in PySpark |
| Data Flow (ODBC) | Dataflow Gen2 | `Odbc.DataSource()` in M; `spark.read.format("jdbc")` in PySpark |
| Data Flow (XML) | Dataflow Gen2 | `Xml.Tables()` in M; `spark.read.format("xml")` in PySpark |
| Data Flow (Raw File) | Dataflow Gen2 | Binary read in M; Parquet fallback in PySpark (no direct equivalent) |
| Data Flow (CDC Source) | Dataflow Gen2 | Reads `cdc.<table>_CT`; TODO markers for LSN / `__$operation` filtering |
| Data Flow (SQL Server Dest) | Copy Activity | Fast-load destination → `SqlSink` |
| Data Flow (Recordset Dest) | Spark Notebook | In-memory destination; HIGH complexity |
| Data Flow + Derived Column | Dataflow Gen2 / Spark | SSIS expressions auto-transpiled to M or PySpark |
| Data Flow + Lookup | Dataflow Gen2 / Spark | Join keys & ref keys extracted from SSIS XML; `Table.NestedJoin` or `.join()` |
| Data Flow + Fuzzy Lookup | Spark Notebook | HIGH complexity; placeholder with metadata |
| Data Flow + Merge Join | Spark Notebook | Join type + left/right key columns fully extracted |
| Data Flow + Merge | Dataflow Gen2 / Spark | Sorted merge of two inputs |
| Data Flow + Aggregate | Dataflow Gen2 / Spark | Group-by + aggregations (sum/count/avg/min/max/count_distinct) extracted |
| Data Flow + Sort | Dataflow Gen2 / Spark | Sort columns + direction (asc/desc) extracted |
| Data Flow + Conditional Split | Dataflow Gen2 / Spark | Conditions extracted; `Table.SelectRows` or `df.filter()` generated |
| Data Flow + Copy Column | Dataflow Gen2 / Spark | Simple column duplication |
| Data Flow + Character Map | Dataflow Gen2 / Spark | Unicode case/width transforms |
| Data Flow + Audit | Dataflow Gen2 / Spark | System variable columns (PackageName, ExecutionTime, etc.) |
| Data Flow + SCD | Spark Notebook | SCD Type 2 merge pattern |
| Data Flow + CDC Splitter | Spark Notebook | Routes CDC rows by operation type; HIGH complexity |
| Data Flow + Fuzzy Grouping | Spark Notebook | HIGH complexity; placeholder with metadata |
| Data Flow + Percentage/Row Sampling | Dataflow Gen2 / Spark | Sampling transforms |
| Script Component (C#) | Spark Notebook (TODO stub) | Placeholder with original script reference |
| Execute Process Task | ExecutePipeline Activity | Placeholder sub-pipeline preserving original command |
| Send Mail Task | Office365Outlook Activity | To/CC/Subject/Body extracted from SSIS properties |
| Execute Package Task | ExecutePipeline Activity | Parameter bindings forwarded; waitOnCompletion = true |
| File System Task | TridentNotebook Activity | Operation mapped to `mssparkutils.fs.*` (cp/mv/rm/mkdirs) |
| FTP Task | Copy Activity / TridentNotebook | Send/Receive → Copy activity; other ops → Notebook placeholder |
| ForEach Loop Container | ForEach Activity | Enumerator type parsed (File/Item/ADO/NodeList/SMO/FromVariable); `items` expression auto-derived |
| For Loop Container | Until Activity | Loop expression converted |
| Sequence Container | Flattened with dependency graph | Children promoted to top-level with correct `dependsOn` |
| Precedence Constraints | `dependsOn` chains | Success/failure/completion semantics preserved |

### Data Source & Connection Support

| Source/Dest Type | Dataflow (M) | Spark (PySpark) | Copy Activity |
|------------------|-------------|-----------------|---------------|
| OLE DB | `Sql.Database()` | `spark.sql()` | `SqlSource` / `SqlSink` |
| ADO.NET | `Sql.Database()` | `spark.sql()` | `SqlSource` / `SqlSink` |
| Flat File | `Csv.Document()` | `spark.read.csv()` | `DelimitedTextSource` / `DelimitedTextSink` |
| Excel | `Excel.Workbook()` | `spark.read.format("excel")` | `ExcelSource` / `DelimitedTextSink` |
| ODBC | `Odbc.DataSource()` | `spark.read.format("jdbc")` | `OdbcSource` / `OdbcSink` |
| XML | `Xml.Tables()` | `spark.read.format("xml")` | `XmlSource` |
| Raw File | `Binary.Buffer()` | `spark.read.parquet()` (fallback) | — |
| CDC | `Sql.Database()` + CDC table | `spark.sql()` + CDC table | `SqlSource` + CDC query |
| SQL Server (fast) | — | — | `SqlSink` |

Connection types recognized by the parser: **OLEDB**, **ADO.NET**, **Flat File**, **Excel**, **ODBC**, **FTP**, **HTTP**, **FILE**, **SMTP**, **Oracle**, **SharePoint**, **Analysis Services**.

### Parameter & Variable Support

The framework fully handles SSIS parameterization and parent-child orchestration:

| Feature | Details |
|---------|---------|
| **Package parameters** | Both SSIS 2012+ container format (`<DTS:PackageParameters>`) and loose format |
| **Project parameters** | `Project.params` file parsed; merged into all package pipelines |
| **Parameter metadata** | `DataType`, `Required`, `Sensitive`, `Description` preserved |
| **Execute Package bindings** | `<DTS:ParameterAssignment>` parsed and converted to `ExecutePipeline` parameters |
| **ADF expressions** | `$Package::VarName` references become `@pipeline().parameters.VarName` |
| **Variables** | SSIS variables (non-System) converted to pipeline variables |

### SSIS Expression Transpiler

SSIS expressions in Derived Columns, Conditional Splits, and other transforms are automatically converted to both **Power Query M** and **PySpark**:

| SSIS Expression | Power Query M | PySpark |
|-----------------|---------------|----------|
| `GETDATE()` | `DateTime.LocalNow()` | `F.current_timestamp()` |
| `UPPER(col)` | `Text.Upper([col])` | `F.upper(F.col("col"))` |
| `LOWER(col)` | `Text.Lower([col])` | `F.lower(F.col("col"))` |
| `TRIM(col)` | `Text.Trim([col])` | `F.trim(F.col("col"))` |
| `LEN(col)` | `Text.Length([col])` | `F.length(F.col("col"))` |
| `REPLACE(col,a,b)` | `Text.Replace([col],a,b)` | `F.regexp_replace(...)` |
| `SUBSTRING(col,s,l)` | `Text.Middle([col],s-1,l)` | `F.substring(...)` |
| `ISNULL(col)` | `[col] = null` | `F.col("col").isNull()` |
| `(DT_STR,...) col` | `Text.From([col])` | `.cast("string")` |
| `(DT_I4) col` | `Int64.From([col])` | `.cast("int")` |
| `cond ? a : b` | `if cond then a else b` | `F.expr("...")` |
| `a + b` (concat) | `a & b` | — |

Complex expressions that cannot be pattern-matched are emitted with `/* TODO */` comments.

### Connection Mapping

SSIS connection manager names can be mapped to Fabric connection IDs via `migration_config.yaml`:

```yaml
connection_mappings:
  MyOLEDB_Source: "fabric-conn-guid-1"
  MyOLEDB_Dest:   "fabric-conn-guid-2"
```

When a mapping exists, the generated pipeline uses the Fabric connection ID directly instead of emitting a `TODO` placeholder.

### Destination Sidecar Manifests

Each generated dataflow and notebook gets an accompanying `*.destinations.json` sidecar:

```json
{
  "destinations": [
    {
      "table": "dw.DimCustomer",
      "connectionRef": "DW_Connection",
      "columns": [
        { "name": "CustomerKey", "dataType": "i4", "length": 0, "precision": 0, "scale": 0 },
        { "name": "CustomerName", "dataType": "str", "length": 100, "precision": 0, "scale": 0 }
      ]
    }
  ]
}
```

This enables downstream tooling (e.g. Lakehouse table creation scripts) to provision the target schema without parsing M code.

### Parallel & Sequential Semantics

Sequence containers are flattened into top-level pipeline activities with correct dependency resolution:

- **Container with no precedence constraints** → children run **in parallel** (all depend on preceding phase only)
- **Container with precedence constraints** → children follow the **constraint dependency graph**
- **Fan-in**: the next phase after a container depends on **all** leaf activities from that container

Example from the AdventureWorksDW Master ETL pipeline:

```
Initialize_ETL_Batch         → (no deps)
  Extract_Customers          → [Initialize_ETL_Batch]     ┐
  Extract_Products           → [Initialize_ETL_Batch]     ├─ parallel
  Extract_Orders             → [Initialize_ETL_Batch]     ┘
Load_DimDate                 → [all 3 extracts]           ← fan-in
  Load_DimCustomer           → [Load_DimGeography]        ┐
  Load_DimProduct            → [Load_DimGeography]        ├─ parallel
Load_FactInternetSales       → [all dim loads]            ┐
Load_FactResellerSales       → [all dim loads]            ├─ parallel
Phase_5_Post_Process         → [both fact loads]          ← fan-in
Finalize_ETL_Batch           → [Phase_5_Post_Process]
Send_Success_Notification    → [Finalize_ETL_Batch]
```

---

## Migration Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| **`hybrid`** (default) | Routes simple data flows to Dataflow Gen2, complex to Spark, control flow to pipelines | Most projects |
| **`data_factory`** | All tasks to Data Factory pipelines | Simple ETL packages |
| **`spark`** | All tasks to Spark notebooks | Complex transformation-heavy packages |

---

## Deployment Phases

The deployer uses a multi-phase strategy that handles item dependencies and workspace organization:

| Phase | Action | Details |
|-------|--------|---------|
| **0** | Resolve connections | Auto-discovers SSIS connection managers from `.conmgr` files; matches existing Fabric SQL connections by server/database or creates new ones with WorkspaceIdentity |
| **0.5** | Pre-create folders | Creates one workspace folder per SSIS package before any items are deployed |
| **1a** | Deploy Dataflow Gen2 | Dataflows deployed first (no dependencies); each moved to its folder immediately |
| **1b** | Deploy Notebooks | PySpark notebooks deployed; each moved to its folder immediately |
| **1c** | Deploy leaf pipelines | Pipelines with no cross-pipeline references (no `ExecutePipeline` or `Copy` activities) |
| **1d** | Deploy Copy pipelines | Pipelines containing Copy activities deployed as empty shells (Fabric creates linked items) |
| **2** | Create referencing shells | Empty pipeline shells for orchestrators / `ExecutePipeline` parents (establishes item IDs) |
| **3** | Update definitions | Full pipeline definitions pushed with resolved `pipelineId` references |
| **4** | Move remaining items | Catch-all: moves any async-created items (HTTP 202) that weren't moved during their deploy phase |

Each item is moved into its workspace folder immediately after creation when the item ID is available (synchronous 201 responses). Items created asynchronously (202 responses) are caught in Phase 4.

---

## Quick Start

### 1. Install

```bash
git clone <repo-url>
cd SSISToFabric

python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -e ".[dev]"
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Fabric workspace ID and credentials
# Review migration_config.yaml for connection/threshold settings
```

### 3. Python API (recommended)

All operations are available as importable classes — no CLI required:

```python
from ssis_to_fabric import SSISMigrator

# Create a migrator (strategy: "hybrid" | "data_factory" | "spark")
migrator = SSISMigrator(strategy="hybrid", output_dir="./output")

# 1. Analyze — parse SSIS packages
packages = migrator.analyze("path/to/ssis_project/")
for pkg in packages:
    print(f"{pkg.name}: {pkg.total_tasks} tasks, {pkg.total_data_flows} data flows")

# 2. Plan — generate a migration plan without executing
plan = migrator.plan(packages)
for item in plan.items:
    print(f"  {item.source_task} → {item.target_artifact.value}")

# 3. Migrate — parse, plan, and generate Fabric artifacts in one call
result = migrator.migrate("path/to/ssis_project/")
completed = [i for i in result.items if i.status == "completed"]
print(f"Generated {len(completed)} artifacts")

# 4. Deploy — push artifacts to a Fabric workspace
report = migrator.deploy(
    workspace_id="<workspace-guid>",
    clean=False,       # True = delete all items first
    dry_run=False,     # True = simulate only
)
print(f"{report.succeeded} deployed, {report.failed} failed")

# 5. Validate — compare output against approved baselines
results = migrator.validate("tests/regression/baselines/", "./output")
assert all(r["status"] == "pass" for r in results)
```

#### One-shot: migrate + deploy

```python
from ssis_to_fabric import SSISMigrator

migrator = SSISMigrator(strategy="hybrid", output_dir="./output")
migrator.run(
    "path/to/ssis_project/",
    workspace_id="<workspace-guid>",
    clean=True,
)
```

#### Using an existing configuration

```python
from ssis_to_fabric import SSISMigrator, load_config

config = load_config("migration_config.yaml")
migrator = SSISMigrator.from_config(config, strategy="hybrid")
result = migrator.migrate("path/to/ssis_project/")
```

#### Available types for advanced usage

```python
from ssis_to_fabric import (
    SSISMigrator,          # High-level facade
    MigrationConfig,       # Pydantic config model
    MigrationStrategy,     # Enum: HYBRID, DATA_FACTORY, SPARK
    MigrationEngine,       # Low-level orchestration engine
    MigrationPlan,         # Plan dataclass
    MigrationItem,         # Per-task item dataclass
    TargetArtifact,        # Enum: DATA_FACTORY_PIPELINE, DATAFLOW_GEN2, ...
    load_config,           # Config loader (YAML + env vars)
)
```

### 4. CLI (alternative)

The CLI commands remain available for scripting or one-off use:

```bash
# Analyze packages
ssis2fabric analyze path/to/ssis_project/

# Generate a plan
ssis2fabric plan path/to/ssis_project/ --strategy hybrid

# Execute full migration
ssis2fabric migrate path/to/ssis_project/ --strategy hybrid --output ./output

# Deploy to Fabric
ssis2fabric deploy ./output --workspace-id <workspace-guid>
ssis2fabric deploy ./output --workspace-id <guid> --dry-run   # simulate only

# Verify deployed artifacts exist in workspace
ssis2fabric verify ./output --workspace-id <workspace-guid>

# Validate against baselines
ssis2fabric validate tests/regression/baselines/ output/

# Extract packages from SSISDB catalog
ssis2fabric extract-ssisdb "Driver={ODBC Driver 17};Server=myserver;..." -o ./extracted
ssis2fabric extract-ssisdb "<conn-str>" --folder MyFolder --project MyProject
```

This generates:
```
output/
├── pipelines/                      # One JSON per package + Master orchestrator
├── notebooks/                      # PySpark notebooks
│   ├── MyPackage_LoadDim.py
│   └── MyPackage_LoadDim.destinations.json   # Sidecar manifest
├── connections/                    # Auto-discovered connection definitions
└── migration_report.json
```

### 5. Run Tests

```bash
# All tests (410)
pytest tests/ -v

# API tests only
pytest tests/unit/test_api.py -v

# Automation feature tests (expression transpiler, metadata wiring, etc.)
pytest tests/unit/test_automation_features.py -v

# Compare against approved baselines
ssis2fabric validate tests/regression/baselines/ output/
```

---

## Project Structure

```
SSISToFabric/
├── src/ssis_to_fabric/
│   ├── analyzer/                       # SSIS package parsing
│   │   ├── models.py                  # Data models (SSISPackage, Variable, Task, etc.)
│   │   └── dtsx_parser.py            # .dtsx XML parser + Project.params reader
│   ├── engine/                         # Migration generators
│   │   ├── migration_engine.py        # Orchestration, routing & plan generation
│   │   ├── data_factory_generator.py  # ADF pipeline JSON generation
│   │   ├── dataflow_generator.py      # Dataflow Gen2 (Power Query M) generation + expression transpiler
│   │   ├── spark_generator.py         # PySpark notebook generation + expression transpiler
│   │   ├── fabric_deployer.py         # Fabric REST API deployment, folder organization & workspace verification
│   │   └── ssisdb_extractor.py        # SSISDB catalog .dtsx extraction (pyodbc)
│   ├── testing/                        # Test framework
│   │   └── regression_runner.py       # Non-regression baseline validation
│   ├── cli.py                          # CLI entry point (ssis2fabric)
│   ├── api.py                          # Public Python API (SSISMigrator facade)
│   ├── config.py                       # Configuration management
│   └── logging_config.py              # Structured logging (structlog)
├── tests/
│   ├── unit/                           # 396 unit tests
│   │   ├── test_dtsx_parser.py        # Parser tests (26 tests)
│   │   ├── test_data_factory_generator.py  # Pipeline & folder organization tests (46 tests)
│   │   ├── test_dataflow_generator.py # Dataflow Gen2 tests (36 tests)
│   │   ├── test_spark_generator.py    # Notebook generation tests (9 tests)
│   │   ├── test_migration_engine.py   # Engine routing & orchestration tests (19 tests)
│   │   ├── test_parameters_parent_child.py  # Parameter & parent-child tests (27 tests)
│   │   ├── test_api.py               # Python API facade tests (34 tests)
│   │   ├── test_config.py            # Configuration tests (6 tests)
│   │   ├── test_automation_features.py # Expression transpiler, metadata wiring, sidecar, FS/FTP (63 tests)
│   │   └── test_data_sources.py       # ODBC/XML/CDC/Raw/type-aware generators (40 tests)
│   ├── regression/                     # Non-regression baseline tests (14 tests)
│   │   └── test_non_regression.py     # Baseline comparison tests
│   └── fixtures/                       # Sample SSIS packages
│       └── sample_packages/
│           ├── simple_etl.dtsx        # Basic ETL package
│           ├── complex_etl.dtsx       # Multi-source with lookups/scripts
│           ├── parent_child.dtsx      # Parameter bindings & containers
│           └── Project.params         # Project-level parameters
├── examples/                           # 12 migration scenarios + full project
│   ├── 01_simple_copy/                # Basic source-to-destination copy
│   ├── 02_incremental_load/           # Parameterized incremental ETL
│   ├── ...                            # (03 through 11)
│   ├── 12_parent_child_packages/      # Master → child orchestration
│   └── full_ssis_project/             # Real VS-generated SSIS packages (MIT)
│       ├── SSISCookbook/              # 13 staging packages + orchestrator
│       ├── AdventureWorksETL/         # 4 lookup & transform packages
│       └── additional_packages/       # 11 standalone packages (Excel, FTP, etc.)
├── output/                             # Generated migration artifacts (git-ignored)
│   └── full_project_migration/
│       ├── pipelines/     (28 files)  # One per SSIS package
│       ├── notebooks/     (28 files)  # PySpark notebooks
│       ├── connections/               # Auto-discovered connection definitions
│       └── migration_report.json
├── azure-pipelines.yml                 # Azure DevOps CI/CD
├── .github/workflows/ci.yml          # GitHub Actions CI/CD
├── migration_config.yaml               # Default configuration
└── pyproject.toml                      # Python project config
```

---

## Non-Regression Testing

The framework provides **three levels** of non-regression validation:

### Level 1: File-Based Comparison
Compares generated artifacts against approved baselines:
- **JSON pipelines**: Deep structural diff (ignoring timestamps)
- **Dataflow definitions**: Query structure comparison
- **Python notebooks**: Line-by-line comparison (ignoring comments)

### Level 2: Structural Validation
Validates generated artifacts meet requirements:
- Pipeline activities have required fields (`name`, `type`, `dependsOn`)
- Parameters and variables sections are well-formed
- ExecutePipeline activities pass correct parameter expressions
- Notebooks have valid Python syntax

### Level 3: Data Validation (Integration)
Compares actual data between SSIS-processed and Fabric-processed outputs:
- Row count comparison with configurable tolerance
- Schema match validation
- Sample data comparison

### Running Tests

```bash
# All tests (410)
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# API facade tests
pytest tests/unit/test_api.py -v

# Automation features (expression transpiler, metadata, FS/FTP, sidecars)
pytest tests/unit/test_automation_features.py -v

# Parameter & parent-child tests
pytest tests/unit/test_parameters_parent_child.py -v

# Regression tests only
pytest tests/regression/ -m regression -v

# With coverage report
pytest tests/ --cov=ssis_to_fabric --cov-report=html
```

---

## Real SSIS Project Examples

The `examples/full_ssis_project/` directory contains **28 real Visual Studio-generated SSIS packages**
from the [SQL Server 2017 Integration Services Cookbook](https://github.com/PacktPublishing/SQL-Server-2017-Integration-Services-Cookbook)
(MIT License). These can be opened in **Visual Studio / SSDT** and contain proper metadata
(`DTS:ExecutableType`, `DTS:refId`, `DTS:LocaleID`, `PackageFormatVersion 8`, etc.).

### SSISCookbook (13 packages + orchestrator)

Complete staging ETL project with shared connection managers (`.conmgr`), sample CSV/XML data,
and `EP_Staging.dtsx` as the Execute-Package orchestrator.

| Package | Key Components |
|---------|---------------|
| EP_Staging | Execute Package tasks (calls all Stg* packages) |
| StgCustomer | OLE DB Source → Data Conversion → OLE DB Destination |
| StgSalesOrderHeader | OLE DB Source → Derived Column → OLE DB Destination |
| StgProduct | OLE DB Source → Data Conversion → OLE DB Destination |
| ... (10 more) | Staging loads with various transforms |

### AdventureWorksETL (4 packages)

| Package | Key Components |
|---------|---------------|
| CascadingLookup | OLE DB + Flat File sources, cascading Lookups |
| ExecutionTrees | Pipeline execution tree demonstration |
| LookupCache | Lookup with Cache transform |
| LookupExpression | Lookup with expression-based connection |

### Additional Packages (11 standalone)

| Package | Key Components |
|---------|---------------|
| ProcessingExcel | Excel Source (Jet OLE DB) |
| DataMatching | Fuzzy Grouping + Fuzzy Lookup |
| TermExtractionLookup | Term Extraction + Term Lookup |
| SplitData | Conditional Split + Percentage Sampling |
| CustomWebServiceSource | Script Component as Web Service source |
| RegExValidation | Script Component with Regex |
| SecureFtpTask | FTP Task (SFTP) |
| FileSizes | Script Task + ForEach Loop |
| UsingVariables | Variable manipulation |
| DataMining | Data Mining model training |
| DataProfiling | Data Profiling task |

---

## CI/CD

### Azure DevOps (`azure-pipelines.yml`)
1. **Build**: Lint → Type check → Unit tests → Regression tests
2. **Dry Run**: Analyze & migrate sample packages
3. **Deploy**: Deploy to Fabric workspace (on `main` branch)

### GitHub Actions (`.github/workflows/ci.yml`)
1. **Lint**: ruff + mypy
2. **Test**: Unit tests across Python 3.10/3.11/3.12
3. **Regression**: Non-regression validation
4. **Dry Run**: Full migration of sample packages

---

## Extending

### Adding New SSIS Component Support

1. Add the component type to `DataFlowComponentType` in [models.py](src/ssis_to_fabric/analyzer/models.py)
2. Add the class ID mapping in `COMPONENT_CLASS_MAP` in [dtsx_parser.py](src/ssis_to_fabric/analyzer/dtsx_parser.py)
3. Add metadata extraction in `_extract_component_metadata()` in [dtsx_parser.py](src/ssis_to_fabric/analyzer/dtsx_parser.py)
4. Add generation logic in the appropriate generator (M in `dataflow_generator.py`, PySpark in `spark_generator.py`)
5. Add unit tests and update regression baselines

### Adding New SSIS Expression Patterns

The expression transpilers are in:
- `DataflowGen2Generator._ssis_expr_to_m()` — converts SSIS expressions to Power Query M
- `SparkNotebookGenerator._ssis_expr_to_pyspark()` — converts SSIS expressions to PySpark

Both use regex pattern matching. Add new patterns as `re.sub` or `re.match` blocks.

### Adding New Control-Flow Task Types

1. Add the task class name in `TASK_CLASS_MAP` in [dtsx_parser.py](src/ssis_to_fabric/analyzer/dtsx_parser.py)
2. Add a `_parse_<task>()` method to extract properties from `<ObjectData>`
3. Wire the parse call in `_parse_executable()`
4. Add a `_<task>_to_activity()` method in [data_factory_generator.py](src/ssis_to_fabric/engine/data_factory_generator.py)
5. Wire both routed and non-routed dispatch

### Connection Mapping

Map SSIS connection manager names to Fabric connection IDs in `migration_config.yaml`:

```yaml
connection_mappings:
  OLTPServer: "fabric-guid-1"
  DWServer:   "fabric-guid-2"
```

### Custom Migration Rules

Override routing logic in `MigrationEngine._route_task()` to customize
which components go to Data Factory, Dataflow Gen2, or Spark.

---

## CLI Command Reference

| Command | Description |
|---------|-------------|
| `ssis2fabric analyze <path>` | Parse SSIS packages and display migration assessment |
| `ssis2fabric plan <path>` | Generate a migration plan without executing |
| `ssis2fabric migrate <path>` | Full migration: analyze, plan, generate artifacts |
| `ssis2fabric deploy <output_dir>` | Deploy artifacts to a Fabric workspace |
| `ssis2fabric verify <output_dir>` | Verify deployed artifacts exist in workspace |
| `ssis2fabric validate <baseline> <output>` | Non-regression comparison against baselines |
| `ssis2fabric extract-ssisdb <conn_str>` | Extract .dtsx packages from SSISDB catalog |

---

## License

MIT
