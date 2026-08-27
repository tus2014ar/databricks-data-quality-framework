# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # dqx_crm_bronze.py
# MAGIC ## Bronze Ingestion Freshness Monitor — CRM Pipeline
# MAGIC Checks MAX(ingestion_timestamp) across all 17 CRM bronze tables
# MAGIC against freshness thresholds. Writes one finding row per table
# MAGIC per run to the bronze findings table. Custom check — DQX has no built-in
# MAGIC freshness rule.
# MAGIC
# MAGIC Load type split:
# MAGIC - Full overwrite (15 tables): 25h threshold
# MAGIC - Incremental merge (2 tables): 96h threshold
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
import uuid
from datetime import datetime, timezone

import pyspark.sql.types as T

# COMMAND ----------

# DBTITLE 1,widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("initial_full_load", "False")
dbutils.widgets.text("verbose", "False")

environment       = dbutils.widgets.get("environment").strip().lower()
initial_full_load = dbutils.widgets.get("initial_full_load").strip().lower() == "true"
verbose           = dbutils.widgets.get("verbose").strip().lower() == "true"
prefix            = "dev_" if environment == "dev" else ""

# initial_full_load gates the table setup cell below — set to True on
# first run only to create the bronze findings table. The DDL is defined
# and executed within this notebook.

assert environment in ("dev", "prod"), \
    f"Invalid environment '{environment}'. Must be 'dev' or 'prod'."

run_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc)

print(f"environment : {environment}")
print(f"prefix      : '{prefix}'")
print(f"run_id      : {run_id}")
print(f"run_ts      : {run_ts.isoformat()}")

# COMMAND ----------

# DBTITLE 1,DDL
if initial_full_load:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {prefix}crm_data.opportunities.dqx_crm_bronze (
            run_id          STRING    NOT NULL,
            run_ts          TIMESTAMP NOT NULL,
            pipeline        STRING    NOT NULL,
            layer           STRING    NOT NULL,
            table_name      STRING    NOT NULL,
            check_name      STRING    NOT NULL,
            severity        STRING    NOT NULL,
            record_type     STRING    NOT NULL,
            total_row_count BIGINT,
            fail_count      BIGINT,
            fail_rate       DOUBLE,
            message         STRING,
            row_data        STRING
        )
        USING delta
        COMMENT 'DQX freshness findings for all 17 CRM bronze tables. One finding row per table per run. Overwritten each run — reflects current state as of the latest run.'
    """)

    spark.sql(f"""
        ALTER TABLE {prefix}crm_data.opportunities.dqx_crm_bronze
        OWNER TO `data_science`
    """)

    print(f"Table created: {prefix}crm_data.opportunities.dqx_crm_bronze")

else:
    print("initial_full_load = False — skipping table setup.")

# COMMAND ----------

# DBTITLE 1,config
# ── Freshness Thresholds ──────────────────────────────────────────────────────
# Change these values here to adjust alerting.
#
# THRESHOLD_FULL_OVERWRITE_H: applied to all 15 full-overwrite reference tables.
# These tables replace all rows on every pipeline run regardless of whether
# CRM data changed — MAX(ingestion_timestamp) reliably reflects the last
# pipeline run. 25h = ~4 consecutive missed runs before alerting.
#
# THRESHOLD_INCREMENTAL_H: applied to bronze_opportunitiesview and
# bronze_opportunitystagehistoryview. These tables only write rows when CRM
# has new or changed records — MAX(ingestion_timestamp) reflects the last data
# change, not the last pipeline run. A quiet CRM period produces a stale
# timestamp on a healthy pipeline. 96h provides enough headroom to tolerate
# several quiet days before treating the situation as a genuine outage.

THRESHOLD_FULL_OVERWRITE_H = 25
THRESHOLD_INCREMENTAL_H    = 96

# ── Table Configuration ───────────────────────────────────────────────────────
# Source catalog is environment-prefixed — dev reads from dev_crm_source.jdbc,
# prod reads from crm_source.jdbc.

SOURCE_CATALOG = f"{prefix}crm_source.jdbc"
FINDINGS_TABLE = f"{prefix}crm_data.opportunities.dqx_crm_bronze"

PIPELINE = "crm_pipeline"
LAYER    = "bronze"

# Each entry: (table_name, threshold_hours)
BRONZE_TABLES = [
    # Incremental merge — threshold: THRESHOLD_INCREMENTAL_H
    ("bronze_opportunitiesview",               THRESHOLD_INCREMENTAL_H),
    ("bronze_opportunitystagehistoryview",      THRESHOLD_INCREMENTAL_H),
    # Full overwrite — threshold: THRESHOLD_FULL_OVERWRITE_H
    ("bronze_entityfirmorgs",                  THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_revenueprojection",               THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitystaffteam",            THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunityvaluelistitems",       THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitycustomvaluelistitems", THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitycustomvaluelists",     THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitytimestampednotes",     THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_personnelview",                   THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitycompetition",          THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_opportunitycustomfieldsview",     THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_officedivision",                  THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_offices",                         THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_practiceareas",                   THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_divisions",                       THRESHOLD_FULL_OVERWRITE_H),
    ("bronze_studios",                         THRESHOLD_FULL_OVERWRITE_H),
]

# COMMAND ----------

# DBTITLE 1,Read MAX(ingestion_timestamp) per table
# Read MAX(ingestion_timestamp) for all 17 bronze tables in one pass.
# Fail fast if any table returns NULL — table may be empty or inaccessible.

max_ts_map = {}

for table_name, threshold_h in BRONZE_TABLES:
    full_table = f"{SOURCE_CATALOG}.{table_name}"
    row = spark.sql(f"""
        SELECT MAX(ingestion_timestamp) AS max_ingestion_ts
        FROM {full_table}
    """).collect()[0]

    max_ts = row["max_ingestion_ts"]

    if max_ts is None:
        raise ValueError(
            f"MAX(ingestion_timestamp) returned NULL from {full_table}. "
            "Table may be empty — cannot evaluate freshness."
        )

    max_ts_map[table_name] = max_ts
    print(f"{table_name}: {max_ts}")

# COMMAND ----------

# DBTITLE 1,Evaluate Freshness Per Table
CHECK_NAME    = "ingestion_freshness"
finding_rows  = []

for table_name, threshold_h in BRONZE_TABLES:
    max_ingestion_ts = max_ts_map[table_name]

    # Coerce to UTC-aware datetime for safe arithmetic — Databricks may return
    # a naive datetime depending on cluster timezone configuration.
    if hasattr(max_ingestion_ts, "tzinfo") and max_ingestion_ts.tzinfo is None:
        max_ingestion_ts = max_ingestion_ts.replace(tzinfo=timezone.utc)

    hours_since = (run_ts - max_ingestion_ts).total_seconds() / 3600
    is_breached = hours_since > threshold_h

    fail_count = 1 if is_breached else 0
    fail_rate  = float(fail_count)
    message    = (
        f"Bronze ingestion is stale: {hours_since:.2f}h since last ingestion "
        f"(threshold: {threshold_h}h, "
        f"last ingested: {max_ingestion_ts.isoformat()})"
        if is_breached
        else None
    )

    finding_rows.append((
        run_id,
        run_ts,
        PIPELINE,
        LAYER,
        table_name,
        CHECK_NAME,
        "warn",    # freshness checks are always WARN — never ERROR
        "finding",
        None,      # total_row_count — not applicable for a binary freshness check
        fail_count,
        fail_rate,
        message,
        None,      # row_data — NULL for finding rows; only populated on quarantine rows
    ))

    print(f"{table_name}: {hours_since:.2f}h | threshold: {threshold_h}h | breached: {is_breached}")

# COMMAND ----------

# DBTITLE 1,Findings DataFrame
schema = T.StructType([
    T.StructField("run_id",          T.StringType(),    nullable=False),
    T.StructField("run_ts",          T.TimestampType(), nullable=False),
    T.StructField("pipeline",        T.StringType(),    nullable=False),
    T.StructField("layer",           T.StringType(),    nullable=False),
    T.StructField("table_name",      T.StringType(),    nullable=False),
    T.StructField("check_name",      T.StringType(),    nullable=False),
    T.StructField("severity",        T.StringType(),    nullable=False),
    T.StructField("record_type",     T.StringType(),    nullable=False),
    T.StructField("total_row_count", T.LongType(),      nullable=True),
    T.StructField("fail_count",      T.LongType(),      nullable=True),
    T.StructField("fail_rate",       T.DoubleType(),    nullable=True),
    T.StructField("message",         T.StringType(),    nullable=True),
    T.StructField("row_data",        T.StringType(),    nullable=True),
])

df_finding = spark.createDataFrame(finding_rows, schema=schema)

if verbose:
    display(df_finding)

# COMMAND ----------

# DBTITLE 1,Overwrite to findings table
(
    df_finding
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(FINDINGS_TABLE)
)

total_breached = sum(1 for r in finding_rows if r[9] == 1)

print(f"Findings written to  : {FINDINGS_TABLE}")
print(f"run_id               : {run_id}")
print(f"tables checked       : {len(finding_rows)}")
print(f"breached             : {total_breached}")
print(f"passed               : {len(finding_rows) - total_breached}")
