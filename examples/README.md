# SSIS-to-Fabric Migration Examples

A collection of SSIS packages organized by complexity, designed to showcase
the full spectrum of migration scenarios from simple copy operations to
enterprise-grade multi-package orchestrations.

## Directory Structure

```
examples/
├── 01_simple_copy/           # Basic source-to-destination copy
├── 02_incremental_load/      # Parameterized incremental ETL
├── 03_flat_file_import/      # File-based data import
├── 04_dimension_load/        # Dimension table with SCD Type 2
├── 05_fact_table_etl/        # Multi-source fact table with lookups
├── 06_multi_destination/     # One source → many destinations
├── 07_master_data_mgmt/      # Complex MDM with merge logic
├── 08_data_warehouse_etl/    # Full DW load with star schema
├── 09_cross_system_sync/     # Multi-system orchestration
├── 10_enterprise_etl/        # Enterprise-grade with error handling
├── 11_realtime_cdc/          # CDC (Change Data Capture) pattern
└── 12_parent_child_packages/ # Master → child package orchestration
```

## Complexity Levels

| # | Example | Tasks | Data Flows | Complexity | Migration Target |
|---|---------|-------|------------|------------|------------------|
| 01 | Simple Copy | 1 | 1 | LOW | Data Factory |
| 02 | Incremental Load | 3 | 1 | LOW | Data Factory |
| 03 | Flat File Import | 4 | 1 | MEDIUM | Data Factory |
| 04 | Dimension Load | 5 | 1 | HIGH | Spark |
| 05 | Fact Table ETL | 6 | 2 | MEDIUM | Hybrid |
| 06 | Multi Destination | 4 | 1 | MEDIUM | Hybrid |
| 07 | Master Data Mgmt | 8 | 2 | HIGH | Spark |
| 08 | Data Warehouse ETL | 12 | 4 | HIGH | Hybrid |
| 09 | Cross-System Sync | 10 | 3 | HIGH | Hybrid |
| 10 | Enterprise ETL | 15+ | 5 | MANUAL | Hybrid + Manual |
| 11 | Real-time CDC | 7 | 2 | HIGH | Spark |
| 12 | Parent-Child | 6 | 0 | MEDIUM | Data Factory |

## Running Examples

```bash
# Analyze a single example
ssis2fabric analyze examples/01_simple_copy/

# Analyze all examples
ssis2fabric analyze examples/

# Generate a migration plan for a specific example
ssis2fabric plan examples/05_fact_table_etl/ --output plan.json

# Migrate all examples
ssis2fabric migrate examples/ --strategy hybrid --output output/
```
