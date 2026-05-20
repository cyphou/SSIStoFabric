<!-- Copilot instructions for the SSIS to Microsoft Fabric migration project -->

# Project: SSIS to Microsoft Fabric Migration

Automated migration of SSIS artifacts to Microsoft Fabric format.

## Architecture — Pipeline

```
SSIS → Microsoft Fabric
```

## Project Structure

- **Source / Extraction**: `src/`
- **Target / Generation**: `output/`, `src/ssis_to_fabric/`, `src/ssis_to_fabric.egg-info/`
- **Tests**: `tests/` (81 test files)
- **Docs**: `docs/`

## Key Modules

- **Extraction**:
  - `output\examples\notebooks\CrossSystemSync_Extract_Oracle_Data.py`
  - `output\examples\notebooks\CrossSystemSync_Extract_SAP_Data.py`
  - `output\examples\notebooks\EnterpriseETL_Extract_CRM_Pipeline.py`
  - `output\examples\notebooks\EnterpriseETL_Extract_Financial_Trans.py`
  - `output\examples\notebooks\TermExtractionLookup_TermExtraction.py`
  - `output\examples\notebooks\TermExtractionLookup_TermLookup.py`
  - `output\full_project_migration\notebooks\TermExtractionLookup_TermExtraction.py`
  - `output\full_project_migration\notebooks\TermExtractionLookup_TermLookup.py`
  - `src\ssis_to_fabric\analyzer\dtsx_parser.py`
  - `src\ssis_to_fabric\engine\ssisdb_extractor.py`
- **Generation**:
  - `output\examples\notebooks\EnterpriseETL_Import_Exchange_Rates.py`
  - `output\examples\notebooks\FlatFileImportPackage_Import_Product_CSV.py`
  - `src\ssis_to_fabric\engine\data_factory_generator.py`
  - `src\ssis_to_fabric\engine\dataflow_generator.py`
  - `src\ssis_to_fabric\engine\report_generator.py`
  - `src\ssis_to_fabric\engine\spark_generator.py`
- **Assessment**:
  - `output\examples\notebooks\EnterpriseETL_Validate_External_Resources.py`
  - `output\examples\notebooks\RegExValidation_ReadPatterns.py`
  - `output\examples\notebooks\RegExValidation_Validate_Person_Data.py`
  - `output\full_project_migration\notebooks\RegExValidation_ReadPatterns.py`
  - `output\full_project_migration\notebooks\RegExValidation_Validate_Person_Data.py`
  - `src\ssis_to_fabric\engine\migration_validator.py`
- **Deployment**:
  - `_deploy.py`
  - `output\examples\notebooks\CascadingLookup_Resolve_Client_Data.py`
  - `output\examples\notebooks\LookupCache_Resolve_Client_Data.py`
  - `output\examples\notebooks\LookupExpression_Resolve_Client_Data.py`
  - `output\full_project_migration\notebooks\CascadingLookup_Resolve_Client_Data.py`
  - `output\full_project_migration\notebooks\LookupCache_Resolve_Client_Data.py`
  - `output\full_project_migration\notebooks\LookupExpression_Resolve_Client_Data.py`
  - `src\ssis_to_fabric\engine\deployment_hardening.py`
  - `src\ssis_to_fabric\engine\fabric_deployer.py`
- **Orchestration**:
  - `src\ssis_to_fabric\cli.py`
  - `src\ssis_to_fabric\engine\orchestration.py`
- **Utilities**:
  - `output\examples\notebooks\CrossSystemSync_Fetch_API_Data.py`
  - `output\examples\notebooks\CrossSystemSync_Send_Sync_Report.py`
  - `output\examples\notebooks\CustomWebServiceSource_Airport_Information.py`
  - `output\examples\notebooks\DataMatching_Data_Flow_Task.py`
  - `output\examples\notebooks\DataMining_Data_Flow_Task.py`
  - `output\examples\notebooks\DataWarehouseETL_Load_DimCustomer.py`
  - `output\examples\notebooks\DataWarehouseETL_Load_DimProduct.py`
  - `output\examples\notebooks\DataWarehouseETL_Load_DimStore.py`
  - `output\examples\notebooks\DataWarehouseETL_Load_Fact_Inventory.py`
  - `output\examples\notebooks\DataWarehouseETL_Load_Fact_Sales.py`
  - `output\examples\notebooks\DimensionLoadSCD2_SCD2_Customer_Load.py`
  - `output\examples\notebooks\EnterpriseETL_Load_Fact_Financial.py`
  - `output\examples\notebooks\ExecutionTrees_dft_DW_DimOrderProvenance.py`
  - `output\examples\notebooks\FactTableETL_Load_Order_Lines.py`
  - `output\examples\notebooks\FactTableETL_Load_Returns.py`
  - ... and 89 more

## Hard Constraints

1. **Read before write** — never assume file contents from memory
2. **Test after every change** — run `pytest tests/ --tb=short -q`
3. **No duplicate functions** — always search for an existing name before creating one
4. **Git hygiene** — commit only when tests pass, conventional messages (`feat:`, `fix:`, `test:`, `docs:`)

## Multi-Agent Architecture

This project uses a specialized agent architecture. See `docs/AGENTS.md` for the full
architecture diagram and `.github/agents/` for per-agent definitions.

## Workflow Rules

### 1. Plan Before Build
- For multi-step work, create a plan before starting
- If something goes sideways, STOP and re-plan

### 2. Read Before Write
- **Always read target code before editing**
- Read `copilot-instructions.md` at session start for project rules

### 3. Testing Contract
- Run `pytest tests/ --tb=short -q` after EVERY implementation change
- If tests fail → fix them before reporting completion
- New features **require** new tests
- Never weaken test assertions to make tests pass

### 4. Scope Discipline
- Only modify files directly related to the task
- No drive-by refactors
- Prefer the smallest change that solves the problem
