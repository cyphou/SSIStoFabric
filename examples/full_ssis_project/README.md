# Real SSIS Project Examples

These are **real Visual Studio-generated SSIS packages** from the
[SQL Server 2017 Integration Services Cookbook](https://github.com/PacktPublishing/SQL-Server-2017-Integration-Services-Cookbook)
(MIT License, Packt Publishing / authors Dejan Sarka, Matija Lah, Miloš Radivojević).

They can be opened directly in **Visual Studio / SSDT / Azure Data Studio** and
contain proper metadata (`DTS:ExecutableType`, `DTS:refId`, `DTS:LocaleID`,
`DTS:DesignTimeProperties`, `PackageFormatVersion 8`, etc.).

## Projects

### SSISCookbook (ETL.Staging)

A complete **staging ETL project** with 13 packages, 4 shared connection managers,
sample CSV/XML data, and an Execute-Package orchestrator.

| Package | Description |
|---------|-------------|
| EP_Staging.dtsx | Orchestrator — calls all Stg* packages via Execute Package tasks |
| StgAddress.dtsx | Stage addresses with Data Conversion |
| StgAggregatedSalesFromCloudDW.dtsx | Stage aggregated sales from cloud DW |
| StgCustomer.dtsx | Stage customers with Data Conversion + Derived Column |
| StgCustomerAddress.dtsx | Stage customer-address bridge |
| StgOrderProvenance.dtsx | Stage order provenance |
| StgProduct.dtsx | Stage products |
| StgProductCategory.dtsx | Stage product categories |
| StgProductDescription.dtsx | Stage product descriptions |
| StgProductModel.dtsx | Stage product models |
| StgProductModelProductDescription.dtsx | Stage product model ↔ description |
| StgSalesOrderDetail.dtsx | Stage sales order line items |
| StgSalesOrderHeader.dtsx | Stage sales order headers |

**Connection Managers** (`.conmgr` files): `cmgr_Source`, `cmgr_DW`, `cmgr_MDS_DB`, `cmgr_AsureStorage_ssiscookbook`

### AdventureWorksETL

Focused **lookup & transform examples** with 4 packages:

| Package | Description |
|---------|-------------|
| CascadingLookup.dtsx | Cascading Lookup with OLE DB + Flat File sources |
| ExecutionTrees.dtsx | Demonstrates pipeline execution trees |
| LookupCache.dtsx | Lookup with Cache transform |
| LookupExpression.dtsx | Lookup with expression-based connection |

### additional_packages

Standalone real `.dtsx` files showcasing diverse SSIS components:

| Package | Key Components |
|---------|---------------|
| CustomWebServiceSource.dtsx | Script Component as Web Service source |
| DataMatching.dtsx | Fuzzy Grouping + Fuzzy Lookup |
| DataMining.dtsx | Data Mining model training |
| DataProfiling.dtsx | Data Profiling task |
| FileSizes.dtsx | Script Task + ForEach Loop with variables |
| ProcessingExcel.dtsx | Excel Source (Jet OLE DB) |
| RegExValidation.dtsx | Script Component with Regex validation |
| SecureFtpTask.dtsx | FTP Task (SFTP) |
| SplitData.dtsx | Conditional Split + Percentage Sampling |
| TermExtractionLookup.dtsx | Term Extraction + Term Lookup transforms |
| UsingVariables.dtsx | Variable manipulation with Script Tasks |

## Usage

```bash
# Parse all packages
ssis-to-fabric migrate examples/full_ssis_project/SSISCookbook --output output/cookbook

# Parse a single package
ssis-to-fabric migrate examples/full_ssis_project/additional_packages/ProcessingExcel.dtsx --output output/excel

# Analyze only
ssis-to-fabric analyze examples/full_ssis_project/AdventureWorksETL
```

## License

Source packages: MIT License — Copyright Packt Publishing.
