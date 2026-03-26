"""
Gap Closure — Phase 30
=======================
Helpers for SCD Type 2/3 code generation, event handler conversion,
Recordset Destination patterns, and Data Tap generation.

These helpers are consumed by spark_generator and data_factory_generator.
"""

from __future__ import annotations

from textwrap import dedent
from typing import Any


# =====================================================================
# SCD Type 2 — Proper row splitting
# =====================================================================

def generate_scd2_merge(
    table: str,
    safe_table: str,
    business_keys: list[str],
    scd2_cols: list[str],
) -> str:
    """Generate proper SCD Type 2 PySpark code with row expiration + new row insert."""
    bk_str = ", ".join(f'"{c}"' for c in business_keys) if business_keys else '"business_key"'
    scd2_str = ", ".join(f'"{c}"' for c in scd2_cols) if scd2_cols else '"value_col"'

    change_conditions = " | ".join(
        f'(F.col("src.{c}") != F.col("tgt.{c}"))' for c in scd2_cols
    ) if scd2_cols else 'F.lit(False)'

    return dedent(f'''\
        # --- SCD Type 2 Merge: {table} ---
        # Business keys: [{bk_str}]
        # History-tracked columns: [{scd2_str}]
        df_existing = spark.read.format("delta").table("{safe_table}")

        # Join incoming to existing current rows
        df_current = df_existing.filter(F.col("IsCurrent") == True)
        df_merged = df.alias("src").join(
            df_current.alias("tgt"),
            on=[{bk_str}],
            how="full",
        )

        # Detect changed rows
        _changed = {change_conditions}
        df_merged = df_merged.withColumn("_scd2_changed", _changed)

        # Expired rows: old current rows where values changed — set EndDate, IsCurrent=False
        df_expired = (
            df_merged
            .filter(F.col("_scd2_changed") & F.col("tgt.IsCurrent").isNotNull())
            .select("tgt.*")
            .withColumn("ExpirationDate", F.current_timestamp())
            .withColumn("IsCurrent", F.lit(False))
        )

        # New rows: incoming rows with changes — set StartDate, IsCurrent=True
        df_new = (
            df_merged
            .filter(F.col("_scd2_changed") | F.col("tgt.IsCurrent").isNull())
            .select("src.*")
            .withColumn("EffectiveDate", F.current_timestamp())
            .withColumn("ExpirationDate", F.lit(None).cast("timestamp"))
            .withColumn("IsCurrent", F.lit(True))
        )

        # Unchanged rows: keep as-is
        df_unchanged = (
            df_merged
            .filter(~F.col("_scd2_changed") & F.col("tgt.IsCurrent").isNotNull())
            .select("tgt.*")
        )

        # Union all three sets and overwrite the dimension table
        df_result = df_unchanged.unionByName(df_expired, allowMissingColumns=True) \\
            .unionByName(df_new, allowMissingColumns=True)
        df_result.write.format("delta").mode("overwrite") \\
            .option("overwriteSchema", "true") \\
            .saveAsTable("{safe_table}")
        logger.info("SCD Type 2 merge completed for {safe_table}")
    ''')


# =====================================================================
# SCD Type 3 — Previous value column
# =====================================================================

def generate_scd3_merge(
    table: str,
    safe_table: str,
    business_keys: list[str],
    scd3_cols: list[str],
) -> str:
    """Generate SCD Type 3 PySpark code with PreviousValue columns."""
    bk_str = ", ".join(f'"{c}"' for c in business_keys) if business_keys else '"business_key"'

    lines = [
        f"# --- SCD Type 3 Merge: {table} ---",
        f'df_existing = spark.read.format("delta").table("{safe_table}")',
        "",
        "# Join incoming to existing on business key(s)",
        'df_merged = df.alias("src").join(',
        '    df_existing.alias("tgt"),',
        f"    on=[{bk_str}],",
        '    how="left",',
        ")",
    ]

    for col in scd3_cols:
        lines += [
            "",
            f'# Track previous value for column: {col}',
            f'df_merged = df_merged.withColumn("Previous_{col}",',
            f'    F.when(F.col("src.{col}") != F.col("tgt.{col}"), F.col("tgt.{col}"))',
            f'    .otherwise(F.col("tgt.Previous_{col}")))',
            f'df_merged = df_merged.withColumn("{col}", F.col("src.{col}"))',
        ]

    lines += [
        '',
        f'df_merged = df_merged.withColumn("LastModifiedDate", F.current_timestamp())',
        f'df_merged.write.format("delta").mode("overwrite") \\',
        f'    .option("overwriteSchema", "true") \\',
        f'    .saveAsTable("{safe_table}")',
        f'logger.info("SCD Type 3 merge completed for {safe_table}")',
    ]
    return "\n".join(lines)


# =====================================================================
# Recordset Destination → TempView pattern
# =====================================================================

def generate_recordset_destination(comp_name: str, variable_name: str | None = None) -> str:
    """Generate Recordset Destination as a Spark TempView for cross-cell sharing."""
    view_name = variable_name or f"recordset_{comp_name}".replace(" ", "_").lower()
    return dedent(f'''\
        # Recordset Destination: {comp_name}
        # SSIS stores results in an in-memory Recordset (Object variable).
        # In Spark, register as a temporary view for downstream cells.
        df.createOrReplaceTempView("{view_name}")
        logger.info("Recordset stored as temp view: {view_name} ({{df.count()}} rows)")
        # To read in a later cell: df = spark.sql("SELECT * FROM {view_name}")
    ''')


# =====================================================================
# Data Tap → Debug sampling cell
# =====================================================================

def generate_data_tap(comp_name: str, output_path: str | None = None) -> str:
    """Generate a Data Tap component as a debug/sampling cell."""
    lines = [
        f"# Data Tap: {comp_name}",
        "# In SSIS, Data Tap captures data passing through a path for debugging.",
        "# In Spark, sample and display for inspection.",
        f'df_tap_{comp_name.replace(" ", "_").lower()} = df.limit(1000)',
        f'logger.info("Data Tap [{comp_name}]: captured {{df_tap_{comp_name.replace(" ", "_").lower()}.count()}} sample rows")',
        f'df_tap_{comp_name.replace(" ", "_").lower()}.display()  # Fabric notebook display',
    ]
    if output_path:
        lines += [
            f'# Optional: persist tap data for offline review',
            f'df_tap_{comp_name.replace(" ", "_").lower()}.write.mode("overwrite") \\',
            f'    .format("delta").saveAsTable("debug_tap_{comp_name.replace(" ", "_").lower()}")',
        ]
    return "\n".join(lines)


# =====================================================================
# Event Handler activity builders (for Data Factory generator)
# =====================================================================

def build_pre_execute_activity(event_type: str, task_count: int) -> dict[str, Any]:
    """Build a pre-flight validation activity for OnPreExecute."""
    return {
        "name": f"EventHandler_{event_type}",
        "type": "Script",
        "typeProperties": {
            "scriptBlockExecutionTimeout": "00:05:00",
            "scripts": [
                {
                    "type": "Query",
                    "text": (
                        "-- Pre-flight validation (converted from SSIS OnPreExecute)\n"
                        f"-- Original handler had {task_count} task(s)\n"
                        "SELECT 1 AS validation_check\n"
                        "-- TODO: Add connection/table existence checks"
                    ),
                }
            ],
        },
        "description": (
            f"Pre-flight validation from SSIS {event_type} event handler. "
            f"Originally had {task_count} task(s)."
        ),
    }


def build_post_execute_activity(
    event_type: str,
    task_count: int,
    top_activity_names: list[str],
) -> dict[str, Any]:
    """Build a post-flight logging activity for OnPostExecute."""
    activity: dict[str, Any] = {
        "name": f"EventHandler_{event_type}",
        "type": "Script",
        "typeProperties": {
            "scriptBlockExecutionTimeout": "00:05:00",
            "scripts": [
                {
                    "type": "NonQuery",
                    "text": (
                        "-- Post-flight logging (converted from SSIS OnPostExecute)\n"
                        f"-- Original handler had {task_count} task(s)\n"
                        "INSERT INTO migration_audit_log (event_type, timestamp, status)\n"
                        f"VALUES ('{event_type}', GETDATE(), 'COMPLETED')\n"
                        "-- TODO: Configure audit table and connection"
                    ),
                }
            ],
        },
        "description": (
            f"Post-flight logging from SSIS {event_type} event handler. "
            f"Originally had {task_count} task(s)."
        ),
    }
    if top_activity_names:
        activity["dependsOn"] = [
            {"activity": name, "dependencyConditions": ["Succeeded"]}
            for name in top_activity_names
        ]
    return activity


def build_warning_activity(event_type: str, task_count: int) -> dict[str, Any]:
    """Build an alert/notification activity for OnWarning."""
    return {
        "name": f"EventHandler_{event_type}",
        "type": "WebActivity",
        "typeProperties": {
            "url": "${WEBHOOK_URL}",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {
                "event": event_type,
                "message": f"Warning event from migrated SSIS pipeline ({task_count} original tasks)",
                "severity": "warning",
                "timestamp": "@utcnow()",
            },
        },
        "description": (
            f"Alert/notification from SSIS {event_type} event handler. "
            f"Originally had {task_count} task(s). "
            "Configure WEBHOOK_URL parameter for Teams/Slack integration."
        ),
    }


def build_progress_activity(event_type: str, task_count: int) -> dict[str, Any]:
    """Build a progress metadata update activity for OnProgress."""
    return {
        "name": f"EventHandler_{event_type}",
        "type": "SetVariable",
        "typeProperties": {
            "variableName": "migration_progress",
            "value": {
                "type": "Expression",
                "value": "@concat('Progress: ', string(utcnow()))",
            },
        },
        "description": (
            f"Progress tracking from SSIS {event_type} event handler. "
            f"Originally had {task_count} task(s)."
        ),
    }
