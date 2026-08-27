# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Gold Layer DQX Execution Notebook — CRM Pipeline
# MAGIC Loads quality checks from `dqx_crm_gold_checks.yml` and applies them against
# MAGIC all 23 gold tables in `crm_data.opportunities`. Writes one finding
# MAGIC row per check per table per run (always, including on a clean pass) and one
# MAGIC quarantine row per failing record to the gold findings table.
# MAGIC
# MAGIC Multi-table check patterns handled:
# MAGIC - `foreign_key` checks (14) — ref_table environment-prefixed at runtime
# MAGIC - Master-data list checks (6) — sql_expression against `enterprise.master_data`
# MAGIC   gold reference tables, catalog environment-prefixed at runtime
# MAGIC - `gold_gng_scd` Go/No-Go checks — pre-joined to `gold_opportunities_fact`
# MAGIC   (filtered to `current_flag = TRUE`) to bring in `stage_number`
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install DQX
# MAGIC %pip install databricks-labs-dqx==0.15.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import uuid
from datetime import datetime, timezone
from typing import Dict, List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("initial_full_load", "False")
dbutils.widgets.text("verbose", "False")
dbutils.widgets.text("yaml_path", "")

environment = dbutils.widgets.get("environment").strip().lower()
initial_full_load = dbutils.widgets.get("initial_full_load").strip().lower() == "true"
verbose = dbutils.widgets.get("verbose").strip().lower() == "true"

assert environment in ("dev", "prod"), \
    f"Invalid environment '{environment}'. Must be 'dev' or 'prod'."

prefix = "dev_" if environment == "dev" else ""
run_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc)

yaml_path = dbutils.widgets.get("yaml_path").strip()
if not yaml_path:
    notebook_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_dir = "/".join(notebook_ctx.notebookPath().get().split("/")[:-1])
    yaml_path = f"/Workspace{notebook_dir}/dqx_crm_gold_checks.yml"

print(f"environment : {environment}")
print(f"prefix      : '{prefix}'")
print(f"run_id      : {run_id}")
print(f"run_ts      : {run_ts.isoformat()}")
print(f"yaml_path   : {yaml_path}")

# COMMAND ----------

# DBTITLE 1,Table setup
FINDINGS_TABLE = f"{prefix}crm_data.opportunities.dqx_crm_gold"

if initial_full_load:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FINDINGS_TABLE} (
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
        COMMENT 'DQX gold-layer findings for all 23 CRM gold tables. One finding row per check per table per run, plus one quarantine row per failing record. Overwritten each run.'
    """)

    spark.sql(f"ALTER TABLE {FINDINGS_TABLE} OWNER TO `data_science`")

    # ── Tags (dev and prod, always) ──
    spark.sql(f"SET TAG ON TABLE {FINDINGS_TABLE} domain = crm_data")
    spark.sql(f"SET TAG ON TABLE {FINDINGS_TABLE} pipeline = crm")
    print(f"Tags applied to {FINDINGS_TABLE}")

    # ── Permissions (prod-only) ──
    if environment == "prod":
        spark.sql(f"GRANT SELECT ON TABLE {FINDINGS_TABLE} TO `crm_data_gold_user`")
        print(f"Permissions applied to {FINDINGS_TABLE}")
    else:
        print(f"Skipping table permissions — environment is '{environment}', not 'prod'.")

    print(f"Table created: {FINDINGS_TABLE}")
else:
    print("initial_full_load = False — skipping table setup.")

# COMMAND ----------

# DBTITLE 1,configuration
# Pattern 2 — gold_gng_scd needs stage_number from gold_opportunities_fact
PREJOIN_TABLES = {
    "gold_gng_scd": {
        "join_table": "gold_opportunities_fact",
        "join_key": "opportunity_id",
        "select_cols": ["stage_number"],
    },
}

ACTIVE_IND_TABLES = {"gold_opportunities_fact"}

# COMMAND ----------

# DBTITLE 1,Load, rewrite, validate
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=yaml_path))

def _apply_prefix(obj, prefix):
    if isinstance(obj, str):
        return obj.replace("__PREFIX__", prefix)
    if isinstance(obj, dict):
        return {k: _apply_prefix(v, prefix) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_apply_prefix(v, prefix) for v in obj]
    return obj

checks = _apply_prefix(checks, prefix)

EXPECTED_TABLES = {
    "gold_opportunities_fact", "gold_opportunity_fiscal_time_allocation",
    "gold_manual_fact_percent_complete", "gold_manual_revenue_projections",
    "gold_revenue_projection", "gold_revenue_projection_comparison_fiscal_year",
    "gold_revenue_projection_comparison_monthly", "gold_opportunity_history",
    "gold_gng_scd", "gold_dim_project_location", "gold_dim_studio",
    "gold_dim_market", "gold_dim_office", "gold_dim_division",
    "gold_dim_go_recommendation", "gold_dim_deliverymethod", "gold_dim_contracttype",
    "gold_dim_primarycategory", "gold_dim_servicetype", "gold_dim_selfperform",
    "gold_dim_clienttype", "gold_dim_priority", "gold_dim_custom_field",
}

checks_by_table: Dict[str, List[dict]] = {}
for check in checks:
    table = check.pop("table_name", None)
    if table is None:
        raise ValueError(f"Check '{check.get('name')}' has no 'table_name' tag.")
    checks_by_table.setdefault(table, []).append(check)

drift = set(checks_by_table) ^ EXPECTED_TABLES
assert not drift, f"table_name drift: {drift}"

status = dq_engine.validate_checks(checks)
assert not status.has_errors, f"Invalid checks in YAML: {status.errors}"

print(f"Loaded and validated {len(checks)} checks from {yaml_path}")
if verbose:
    for c in checks:
        fn = c.get("check", {}).get("function")
        if fn == "foreign_key":
            print(f"  {c['name']}: ref_table = {c['check']['arguments']['ref_table']}")
        elif fn == "sql_expression" and "enterprise.master_data." in c["check"]["arguments"].get("expression", ""):
            print(f"  {c['name']}: rewritten master-data expression confirmed")

# COMMAND ----------

# DBTITLE 1,Per-table apply loop
PIPELINE = "crm_pipeline"
LAYER = "gold"

all_results: Dict[str, dict] = {}
finding_rows: List[tuple] = []
quarantine_dfs: List = []


def _read_table(table: str):
    df = spark.read.table(f"{prefix}crm_data.opportunities.{table}")

    if table in ACTIVE_IND_TABLES and "active_ind" in df.columns:
        df = df.filter(F.col("active_ind") == 1)

    if table in PREJOIN_TABLES:
        cfg = PREJOIN_TABLES[table]
        join_df = spark.read.table(
            f"{prefix}crm_data.opportunities.{cfg['join_table']}"
        ).select(cfg["join_key"], *cfg["select_cols"])
        df = df.join(join_df, on=cfg["join_key"], how="left")

    return df


for table, table_checks in checks_by_table.items():
    print(f"  processing: {table}")
    try:
        df = _read_table(table)
        data_cols = df.columns

        result_df = dq_engine.apply_checks_by_metadata(df, table_checks)

        # Single aggregation: total row count + every check's fail_count in one pass.
        agg_exprs = [F.count(F.lit(1)).alias("__total__")]
        for check in table_checks:
            check_name = check["name"]
            severity = check["criticality"]
            issue_col = "_errors" if severity == "error" else "_warnings"
            agg_exprs.append(
                F.sum(
                    F.when(F.exists(F.col(issue_col), lambda x: x["name"] == check_name), 1).otherwise(0)
                ).alias(f"__fail__{check_name}__")
            )
        agg_row = result_df.agg(*agg_exprs).collect()[0]
        total_row_count = agg_row["__total__"]

        # Finding rows — one per check, unconditionally (fail_count=0 on a clean pass).
        for check in table_checks:
            check_name = check["name"]
            severity = check["criticality"]
            fail_count = agg_row[f"__fail__{check_name}__"] or 0
            fail_rate = float(fail_count) / total_row_count if total_row_count else 0.0

            finding_rows.append((
                run_id, run_ts, PIPELINE, LAYER, table, check_name, severity,
                "finding", total_row_count, fail_count, fail_rate, None, None,
            ))

        # Quarantine rows — built as a distributed DataFrame, no driver-side collect.
        row_data_json = F.to_json(F.struct(*[F.col(c) for c in data_cols]))
        tagged_errors = F.transform(
            F.coalesce(F.col("_errors"), F.array()),
            lambda x: F.struct(x["name"].alias("check_name"), x["message"].alias("message"), F.lit("error").alias("severity")),
        )
        tagged_warnings = F.transform(
            F.coalesce(F.col("_warnings"), F.array()),
            lambda x: F.struct(x["name"].alias("check_name"), x["message"].alias("message"), F.lit("warn").alias("severity")),
        )

        table_quarantine_df = (
            result_df
            .withColumn("__row_data__", row_data_json)
            .withColumn("__issues__", F.concat(tagged_errors, tagged_warnings))
            .filter(F.size(F.col("__issues__")) > 0)
            .withColumn("__issue__", F.explode(F.col("__issues__")))
            .select(
                F.lit(run_id).alias("run_id"),
                F.lit(run_ts).alias("run_ts"),
                F.lit(PIPELINE).alias("pipeline"),
                F.lit(LAYER).alias("layer"),
                F.lit(table).alias("table_name"),
                F.col("__issue__.check_name").alias("check_name"),
                F.col("__issue__.severity").alias("severity"),
                F.lit("quarantine").alias("record_type"),
                F.lit(None).cast(T.LongType()).alias("total_row_count"),
                F.lit(None).cast(T.LongType()).alias("fail_count"),
                F.lit(None).cast(T.DoubleType()).alias("fail_rate"),
                F.col("__issue__.message").alias("message"),
                F.col("__row_data__").alias("row_data"),
            )
        )
        quarantine_dfs.append(table_quarantine_df)

        all_results[table] = {"status": "success", "rule_count": len(table_checks)}
        print(f"  [OK]  {table}: {len(table_checks)} checks, {total_row_count} rows")

    except Exception as exc:
        all_results[table] = {"status": "error", "error": str(exc)}
        print(f"  [ERR] {table}: {str(exc)[:200]}")

# COMMAND ----------

# DBTITLE 1,Write in Dataframe
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

findings_df = spark.createDataFrame(finding_rows, schema=schema)

if quarantine_dfs:
    quarantine_df = quarantine_dfs[0]
    for qdf in quarantine_dfs[1:]:
        quarantine_df = quarantine_df.unionByName(qdf)
else:
    quarantine_df = spark.createDataFrame([], schema=schema)

df_out = findings_df.unionByName(quarantine_df)

if verbose:
    display(df_out)

df_out.write.format("delta").mode("overwrite").saveAsTable(FINDINGS_TABLE)

quarantine_row_count = quarantine_df.count()

print(f"Findings written to : {FINDINGS_TABLE}")
print(f"run_id               : {run_id}")
print(f"finding rows         : {len(finding_rows)}")
print(f"quarantine rows      : {quarantine_row_count}")

# COMMAND ----------

# DBTITLE 1,Summary
success = [(t, r) for t, r in all_results.items() if r["status"] == "success"]
failed  = [(t, r) for t, r in all_results.items() if r["status"] == "error"]

error_violations = sum(1 for r in finding_rows if r[6] == "error" and (r[9] or 0) > 0)
warn_violations  = sum(1 for r in finding_rows if r[6] == "warn"  and (r[9] or 0) > 0)

print("=" * 60)
print("RUN SUMMARY")
print("=" * 60)
print(f"Tables processed              : {len(all_results)}")
print(f"Succeeded                     : {len(success)}")
print(f"Failed                        : {len(failed)}")
print(f"Total checks run              : {sum(r['rule_count'] for _, r in success)}")
print(f"Finding rows                  : {len(finding_rows)}")
print(f"Quarantine rows               : {quarantine_row_count}")
print(f"Checks with error violations  : {error_violations}")
print(f"Checks with warn violations   : {warn_violations}")

if failed:
    print()
    print("FAILURES:")
    for table, result in failed:
        print(f"  [ERR] {table}: {result['error'][:150]}")
