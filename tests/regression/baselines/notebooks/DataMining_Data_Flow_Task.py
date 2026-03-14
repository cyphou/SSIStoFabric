# Fabric Notebook
# Migrated from SSIS Package: DataMining
# Task: Data Flow Task
# Task Type: DATA_FLOW
# Generated: 2026-03-14 06:05:45 UTC
# Migration Complexity: MEDIUM
#
# NOTE: Review all TODO comments before running in production.
# This notebook was auto-generated and may require adjustments.


# --- Imports ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Fabric-specific utilities (available in Fabric notebook runtime)
try:
    import notebookutils
except ImportError:
    notebookutils = None  # type: ignore[assignment]


# --- Parameters (from pipeline or Variable Library) ---
# When run from a pipeline, parameters are injected by the
# TridentNotebook activity.  When run standalone, defaults are used.

try:
    _ctx = notebookutils.notebook.getContext()
    _params = _ctx.get('currentRunConfig', {}).get('parameters', {})
except Exception:
    _params = {}


def _get_param(name: str, default: str = '') -> str:
    """Read a parameter from the pipeline context, falling back to the default."""
    return _params.get(name, default)


# --- Fabric Connections ---
# Map SSIS connection managers to Fabric connection IDs.
# Update the IDs below to match your Fabric workspace connections.
# (same IDs used by Data Factory pipeline externalReferences)
_FABRIC_CONNECTIONS = {
    "(LOCAL).AdventureWorksDW2014": "(LOCAL).AdventureWorksDW2014  -- TODO: replace with Fabric connection id",  # OLEDB / (LOCAL) / AdventureWorksDW2014
    "localhost.Ch08NaiveBayes": "localhost.Ch08NaiveBayes  -- TODO: replace with Fabric connection id",  # ANALYSIS_SERVICES / localhost / Ch08NaiveBayes
    "cmgr_AsureStorage_ssiscookbook": "cmgr_AsureStorage_ssiscookbook  -- TODO: replace with Fabric connection id",  # UNKNOWN
    "cmgr_DW": "cmgr_DW  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2012
    "cmgr_MDS_DB": "cmgr_MDS_DB  -- TODO: replace with Fabric connection id",  # OLEDB / . / MDS_DB
    "cmgr_Source": "cmgr_Source  -- TODO: replace with Fabric connection id",  # OLEDB / . / AdventureWorksLTDW2012
}


def _get_connection_id(conn_name: str) -> str:
    """Get a Fabric connection ID for an SSIS connection manager."""
    conn_id = _FABRIC_CONNECTIONS.get(conn_name, "")
    if not conn_id or "TODO" in conn_id:
        raise ValueError(
            f"Fabric connection not configured for: {conn_name}. "
            f"Update _FABRIC_CONNECTIONS dict with the Fabric connection ID."
        )
    return conn_id


def _jdbc_url_for(conn_name: str) -> str:
    """Get JDBC connection string from a Fabric connection."""
    conn_id = _get_connection_id(conn_name)
    return notebookutils.credentials.getConnectionStringOrCreds(conn_id)


# === Source Data Reads ===

# Source: RModel
# Uses Fabric connection (same as pipeline externalReferences)
df_source_rmodel = spark.read.format("jdbc") \
    .option("url", _jdbc_url_for("(LOCAL).AdventureWorksDW2014")) \
    .option("query", """EXECUTE sys.sp_execute_external_script
  @language = N'R'
 ,@script = N'
   # Education and Occupation are ordered
   TM$Education =
     factor(TM$Education, order=TRUE, 
            levels=c("Partial High School", 
                     "High School","Partial College",
                     "Bachelors", "Graduate Degree"));
   TM$Occupation =
     factor(TM$Occupation, order=TRUE,
            levels=c("Manual", "Skilled Manual",
                     "Professional", "Clerical",
                     "Management"));
   # Split the data to the training and test set
   TMTrain <- TM[TM$TrainTest==1,];
   TMTest <- TM[TM$TrainTest==2,];
   # Package e1071 (Naive Bayes)
   library(e1071);
   # Build the Naive Bayes model
   TMNB <- naiveBayes(TMTrain[,2:11], TMTrain[,13]);
   # Data frame with predictions for all rows
   TM_PR <- as.data.frame(predict(TMNB, TMTest, type = "raw"));
   # Combine original data with predictions
   df_TM_PR <- cbind(TMTest[,-(2:12)], TM_PR);'
 ,@input_data_1 = N'
   SELECT CustomerKey, MaritalStatus, Gender,
    TotalChildren, NumberChildrenAtHome,
    EnglishEducation AS Education,
    EnglishOccupation AS Occupation,
    HouseOwnerFlag, NumberCarsOwned, CommuteDistance,
    Region, TrainTest, BikeBuyer    
   FROM dbo.TMTrainingSet
   UNION
   SELECT CustomerKey, MaritalStatus, Gender,
    TotalChildren, NumberChildrenAtHome,
    EnglishEducation AS Education,
    EnglishOccupation AS Occupation,
    HouseOwnerFlag, NumberCarsOwned, CommuteDistance,
    Region, TrainTest, BikeBuyer
   FROM dbo.TMTestSet;'
 ,@input_data_1_name =  N'TM'
 ,@output_data_1_name = N'df_TM_PR'
WITH RESULT SETS 
(
 ("CustomerKey"             INT   NOT NULL,
  "BikeBuyer"               INT   NOT NULL,
  "Predicted_0_Probability" FLOAT NOT NULL, 
  "Predicted_1_Probability" FLOAT NOT NULL)
);""") \
    .load()
logger.info(f"Read {{count}} rows from RModel", count=df_source_rmodel.count())

# Source: TMTestSet
# TODO: Configure source data read
df_source_tmtestset = spark.sql("SELECT 1 as placeholder -- TODO: Replace with actual query")


df = df_source_rmodel

# === Transformations ===

# Unknown Transform: Data Mining Query (Type: UNKNOWN)
# TODO: Manually implement this transformation
# Properties: {'ObjectRef': '', 'QueryText': 'SELECT FLATTENED\n  t.[CustomerKey],\n  t.[BikeBuyer],\n  ([TM_NB].[Bike Buyer]) as [SSASPrediction],\n  (PredictProbability([TM_NB].[Bike Buyer])) as [SSASPredictProbability]\nFrom\n  [TM_NB]\nPREDICTION JOIN\n @InputRowset AS t\nON\n  [TM_NB].[Marital Status] = t.[MaritalStatus] AND\n  [TM_NB].[Gender] = t.[Gender] AND\n  [TM_NB].[Yearly Income] = t.[YearlyIncome] AND\n  [TM_NB].[Total Children] = t.[TotalChildren] AND\n  [TM_NB].[Number Children At Home] = t.[NumberChildrenAtHome] AND\n  [TM_NB].[English Education] = t.[EnglishEducation] AND\n  [TM_NB].[English Occupation] = t.[EnglishOccupation] AND\n  [TM_NB].[House Owner Flag] = t.[HouseOwnerFlag] AND\n  [TM_NB].[Number Cars Owned] = t.[NumberCarsOwned] AND\n  [TM_NB].[Commute Distance] = t.[CommuteDistance] AND\n  [TM_NB].[Region] = t.[Region] AND\n  [TM_NB].[Age] = t.[Age] AND\n  [TM_NB].[Bike Buyer] = t.[BikeBuyer]', 'CatalogName': 'Ch08NaiveBayes', 'ModelStructureName': 'TM', 'ModelName': 'TM_NB', 'QueryBuilderSpecification': '<?xml version="1.0" encoding="utf-16"?>\n<DataminingQueryBuilderState xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n  <SelectTablesControlState>\n    <Tables>\n      <SelectedTableInfo>\n        <tableName>Data Flow Input Columns</tableName>\n        <friendlyTableName>Data Flow Input Columns</friendlyTableName>\n      </SelectedTableInfo>\n    </Tables>\n  </SelectTablesControlState>\n  <IsSingletonQuery>false</IsSingletonQuery>\n  <View>Design</View>\n  <QueryText>SELECT FLATTENED\n  t.[CustomerKey],\n  t.[BikeBuyer],\n  ([TM_NB].[Bike Buyer]) as [SSASPrediction],\n  (PredictProbability([TM_NB].[Bike Buyer])) as [SSASPredictProbability]\nFrom\n  [TM_NB]\nPREDICTION JOIN\n @InputRowset AS t\nON\n  [TM_NB].[Marital Status] = t.[MaritalStatus] AND\n  [TM_NB].[Gender] = t.[Gender] AND\n  [TM_NB].[Yearly Income] = t.[YearlyIncome] AND\n  [TM_NB].[Total Children] = t.[TotalChildren] AND\n  [TM_NB].[Number Children At Home] = t.[NumberChildrenAtHome] AND\n  [TM_NB].[English Education] = t.[EnglishEducation] AND\n  [TM_NB].[English Occupation] = t.[EnglishOccupation] AND\n  [TM_NB].[House Owner Flag] = t.[HouseOwnerFlag] AND\n  [TM_NB].[Number Cars Owned] = t.[NumberCarsOwned] AND\n  [TM_NB].[Commute Distance] = t.[CommuteDistance] AND\n  [TM_NB].[Region] = t.[Region] AND\n  [TM_NB].[Age] = t.[Age] AND\n  [TM_NB].[Bike Buyer] = t.[BikeBuyer]</QueryText>\n  <DatabaseId>Ch08NaiveBayes</DatabaseId>\n  <MiningStructureId>TM</MiningStructureId>\n  <MiningModelId>TM_NB</MiningModelId>\n  <ColumnMappings>\n    <ColumnMapping>\n      <miningModelColumnId>Marital Status</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>MaritalStatus</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Gender</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>Gender</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Yearly Income</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>YearlyIncome</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Total Children</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>TotalChildren</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Number Children At Home</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>NumberChildrenAtHome</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>English Education</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>EnglishEducation</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>English Occupation</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>EnglishOccupation</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>House Owner Flag</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>HouseOwnerFlag</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Number Cars Owned</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>NumberCarsOwned</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Commute Distance</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>CommuteDistance</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Region</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>Region</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Age</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>Age</dataColumnName>\n    </ColumnMapping>\n    <ColumnMapping>\n      <miningModelColumnId>Bike Buyer</miningModelColumnId>\n      <tableName>Data Flow Input Columns</tableName>\n      <dataColumnName>BikeBuyer</dataColumnName>\n    </ColumnMapping>\n  </ColumnMappings>\n  <GridRows>\n    <GridRow>\n      <type>DataColumn</type>\n      <parentType>DataTable</parentType>\n      <parentId>Data Flow Input Columns</parentId>\n      <field>CustomerKey</field>\n      <alias />\n      <show>true</show>\n      <group />\n      <andOr />\n      <criteria />\n    </GridRow>\n    <GridRow>\n      <type>DataColumn</type>\n      <parentType>DataTable</parentType>\n      <parentId>Data Flow Input Columns</parentId>\n      <field>BikeBuyer</field>\n      <alias />\n      <show>true</show>\n      <group />\n      <andOr />\n      <criteria />\n    </GridRow>\n    <GridRow>\n      <type>MiningColumn</type>\n      <parentType>MiningModel</parentType>\n      <parentId>TM_NB</parentId>\n      <field>Bike Buyer</field>\n      <alias>SSASPrediction</alias>\n      <show>true</show>\n      <group />\n      <andOr />\n      <criteria />\n    </GridRow>\n    <GridRow>\n      <type>PredictFunction</type>\n      <parentType>None</parentType>\n      <field>PredictProbability</field>\n      <alias>SSASPredictProbability</alias>\n      <show>true</show>\n      <group />\n      <andOr />\n      <criteria>[TM_NB].[Bike Buyer]</criteria>\n    </GridRow>\n  </GridRows>\n</DataminingQueryBuilderState>', 'QueryBuilderQueryString': 'SELECT FLATTENED\n  t.[CustomerKey],\n  t.[BikeBuyer],\n  ([TM_NB].[Bike Buyer]) as [SSASPrediction],\n  (PredictProbability([TM_NB].[Bike Buyer])) as [SSASPredictProbability]\nFrom\n  [TM_NB]\nPREDICTION JOIN\n @InputRowset AS t\nON\n  [TM_NB].[Marital Status] = t.[MaritalStatus] AND\n  [TM_NB].[Gender] = t.[Gender] AND\n  [TM_NB].[Yearly Income] = t.[YearlyIncome] AND\n  [TM_NB].[Total Children] = t.[TotalChildren] AND\n  [TM_NB].[Number Children At Home] = t.[NumberChildrenAtHome] AND\n  [TM_NB].[English Education] = t.[EnglishEducation] AND\n  [TM_NB].[English Occupation] = t.[EnglishOccupation] AND\n  [TM_NB].[House Owner Flag] = t.[HouseOwnerFlag] AND\n  [TM_NB].[Number Cars Owned] = t.[NumberCarsOwned] AND\n  [TM_NB].[Commute Distance] = t.[CommuteDistance] AND\n  [TM_NB].[Region] = t.[Region] AND\n  [TM_NB].[Age] = t.[Age] AND\n  [TM_NB].[Bike Buyer] = t.[BikeBuyer]'}


# Derived Column: Derived Column
df = df.withColumn("RPrediction", F.when(F.expr("Predicted_1_Probability >= 0.50"), F.expr("1")).otherwise(F.expr("0")))  # SSIS: Predicted_1_Probability >= 0.50 ? 1 : 0
df = df.withColumn("RPredictProbability", F.when(F.expr("Predicted_1_Probability < 0.50"), F.expr("Predicted_0_Probability")).otherwise(F.expr("Predicted_1_Probability")))  # SSIS: Predicted_1_Probability < 0.50 ? Predicted_0_Probability : Predicted_1_Probability

# Merge Join: Merge Join
# df_left and df_right should reference the appropriate source DataFrames
df = df_left.join(
    df_right,
    on="join_key"  # TODO: Set correct join key(s),
    how="full"
)


# Multicast: Multicast
# In Spark, simply reuse the same DataFrame for multiple outputs
df_output_1 = df  # Branch 1
df_output_2 = df  # Branch 2


# Sort: Sort
df = df.orderBy(
    F.col("CustomerKey").asc()
)


# Sort: Sort 1
df = df.orderBy(
    F.col("CustomerKey").asc()
)


# === Completion ===
logger.info("Notebook completed: Data Flow Task")
print("Migration notebook execution complete: Data Flow Task")
