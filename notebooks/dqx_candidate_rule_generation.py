# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "databricks-labs-dqx[llm]==0.15.0",
# ]
# ///
# MAGIC %md
# MAGIC # Candidate Rule Generation (DQX AI-Assisted Profiler)
# MAGIC
# MAGIC Profiles gold tables in `crm_data.opportunities` using the DQX
# MAGIC AI-assisted profiler and generates candidate DQ rule sets for each table.
# MAGIC
# MAGIC **Inputs:** Unity Catalog gold tables (fact + dimension)
# MAGIC **Output:** Rule counts and candidates printed in the final summary cell
# MAGIC
# MAGIC **Run once per review cycle. Results feed into `dqx_01_write_checks_yaml`.**

# COMMAND ----------

# DBTITLE 1,Install
# MAGIC %pip install 'databricks-labs-dqx[llm]==0.15.0'
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import re
import json
import os
from typing import Dict, List, Tuple

import databricks
import site

import dspy
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import InputConfig

# Patches the databricks namespace so DQX sub-packages resolve correctly
# when installed alongside databricks-sdk in the same cluster environment.
# Without this, importing DQProfiler and DQGenerator fails with a ModuleNotFoundError
# because pip places them in a different site-packages path than the SDK.
for _p in site.getsitepackages():
    _db = os.path.join(_p, "databricks")
    if os.path.isdir(_db) and _db not in list(databricks.__path__):
        databricks.__path__.append(_db)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("environment",   "dev",                                  "Environment (dev / prod)")
dbutils.widgets.text("verbose",       "False",                                 "Verbose output (True / False)")
dbutils.widgets.text("lm_model",      "databricks/databricks-claude-opus-4-8", "LM endpoint model string")
dbutils.widgets.text("lm_max_tokens", "8000",                                  "LM max output tokens")

environment   = dbutils.widgets.get("environment").strip().lower()
verbose       = dbutils.widgets.get("verbose").strip().lower() == "true"
lm_model      = dbutils.widgets.get("lm_model").strip()
lm_max_tokens = int(dbutils.widgets.get("lm_max_tokens").strip())

# prefix targets the catalog per databricks/architecture-catalog-guide.md.
# dev  → dev_crm_data.opportunities.gold_*
# prod → crm_data.opportunities.gold_*
# If dev_crm_data catalog does not exist, run with environment=prod.
prefix = "dev_" if environment == "dev" else ""

# Fail fast — surface missing config before any compute runs
assert lm_model,          "lm_model widget must not be empty"
assert lm_max_tokens > 0, "lm_max_tokens must be a positive integer"

if verbose:
    print(f"environment:   {environment}")
    print(f"lm_model:      {lm_model}")
    print(f"lm_max_tokens: {lm_max_tokens}")
    print(f"prefix:        {prefix!r}")

# COMMAND ----------

# DBTITLE 1,Initialise clients
ws        = WorkspaceClient()
profiler  = DQProfiler(workspace_client=ws)
generator = DQGenerator(workspace_client=ws)

# DSPy is configured once here using widget values from Cell 3.
# All profiling and generation cells share this single lm instance.
# The prior version initialised dspy.LM twice with hardcoded values —
# once in the main loop cell and again in the gold_opportunities_fact bypass cell.
lm = dspy.LM(model=lm_model, max_tokens=lm_max_tokens)
dspy.configure(lm=lm)

# COMMAND ----------

# DBTITLE 1,Table lists
# prefix targets the catalog, not the table name.
# dev  → dev_crm_data.opportunities.gold_*
# prod → crm_data.opportunities.gold_*
# Per databricks/architecture-catalog-guide.md §The Environment Parameter

FACT_TABLES: List[str] = [
    f"{prefix}crm_data.opportunities.gold_opportunities_fact",
    f"{prefix}crm_data.opportunities.gold_opportunity_fiscal_time_allocation",
    f"{prefix}crm_data.opportunities.gold_manual_fact_percent_complete",
    f"{prefix}crm_data.opportunities.gold_manual_revenue_projections",
    f"{prefix}crm_data.opportunities.gold_revenue_projection",
    f"{prefix}crm_data.opportunities.gold_revenue_projection_comparison_fiscal_year",
    f"{prefix}crm_data.opportunities.gold_revenue_projection_comparison_monthly",
    f"{prefix}crm_data.opportunities.gold_opportunity_history",
    f"{prefix}crm_data.opportunities.gold_gng_scd",
]

DIM_TABLES: List[str] = [
    f"{prefix}crm_data.opportunities.gold_dim_project_location",
    f"{prefix}crm_data.opportunities.gold_dim_studio",
    f"{prefix}crm_data.opportunities.gold_dim_market",
    f"{prefix}crm_data.opportunities.gold_dim_office",
    f"{prefix}crm_data.opportunities.gold_dim_division",
    f"{prefix}crm_data.opportunities.gold_dim_go_recommendation",
    f"{prefix}crm_data.opportunities.gold_dim_deliverymethod",
    f"{prefix}crm_data.opportunities.gold_dim_contracttype",
    f"{prefix}crm_data.opportunities.gold_dim_primarycategory",
    f"{prefix}crm_data.opportunities.gold_dim_servicetype",
    f"{prefix}crm_data.opportunities.gold_dim_selfperform",
    f"{prefix}crm_data.opportunities.gold_dim_clienttype",
    f"{prefix}crm_data.opportunities.gold_dim_priority",
    f"{prefix}crm_data.opportunities.gold_dim_custom_field",
]

# COMMAND ----------

# DBTITLE 1,Prompt builders:fetches context dynamically
def _schema_block(df) -> str:
    """Returns formatted column names and types from the DataFrame schema."""
    return "\n".join(
        f"  - {f.name} ({f.dataType.simpleString()})"
        for f in df.schema.fields
    )


def _stats_block(summary_stats: dict) -> str:
    """
    Formats DQX summary_stats into a human-readable block.

    summary_stats is already computed by profiler.profile(df). Making the
    min/max/null information explicitly visible in the prompt means the LLM
    uses it to set realistic thresholds and flag anomalies rather than
    discovering them implicitly from raw statistics.
    """
    if not summary_stats:
        return ""

    lines = []
    for col_name, stats in summary_stats.items():
        parts = []
        if stats.get("min") is not None:
            parts.append(f"min={stats['min']}")
        if stats.get("max") is not None:
            parts.append(f"max={stats['max']}")
        if stats.get("null_count") is not None:
            parts.append(f"nulls={stats['null_count']}")
        if stats.get("distinct_count") is not None:
            parts.append(f"distinct={stats['distinct_count']}")
        if parts:
            lines.append(f"  - {col_name}: {', '.join(parts)}")

    if not lines:
        return ""

    return (
        "\nCOLUMN STATISTICS (from profiler — use to identify anomalies and set realistic thresholds):\n"
        + "\n".join(lines)
    )


def _table_context_block(table: str) -> str:
    """
    Fetches table-level context from information_schema.

    Pulls the table comment, primary key constraints, and column comments so
    the LLM understands the table's documented intent and grain without
    relying on hardcoded metadata in the notebook. Failures are caught and
    silently skipped — missing metadata degrades prompt quality but does not
    block rule generation.
    """
    short              = table.split(".")[-1]
    catalog, schema, _ = table.split(".")

    # Table comment — documents purpose and grain when set by the pipeline author
    try:
        comment_row = spark.sql(f"""
            SELECT comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name   = '{short}'
        """).collect()
        table_comment = (
            comment_row[0]["comment"]
            if comment_row and comment_row[0]["comment"]
            else ""
        )
    except Exception:
        table_comment = ""

    # Primary key constraints — identifies the grain of the table
    try:
        pk_rows = spark.sql(f"""
            SELECT column_name
            FROM {catalog}.information_schema.constraint_column_usage
            WHERE table_schema    = '{schema}'
              AND table_name      = '{short}'
              AND constraint_type = 'PRIMARY KEY'
            ORDER BY ordinal_position
        """).collect()
        pk_cols = [r["column_name"] for r in pk_rows]
    except Exception:
        pk_cols = []

    # Column comments — captures per-column business definitions
    try:
        col_rows = spark.sql(f"""
            SELECT column_name, comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name   = '{short}'
              AND comment IS NOT NULL
              AND comment      != ''
            ORDER BY ordinal_position
        """).collect()
        col_comments = {r["column_name"]: r["comment"] for r in col_rows}
    except Exception:
        col_comments = {}

    lines = []
    if table_comment:
        lines.append(f"  - Table description: {table_comment}")
    if pk_cols:
        lines.append(f"  - Primary key: {', '.join(pk_cols)}")
    if col_comments:
        lines.append("  - Column definitions:")
        for col, comment in col_comments.items():
            lines.append(f"      {col}: {comment}")

    return (
        "\nTABLE METADATA (from information_schema):\n" + "\n".join(lines)
        if lines
        else ""
    )


def build_fact_prompt(table: str, df, summary_stats: dict) -> str:
    """
    Builds a rich, table-aware prompt for fact and projection tables.

    Context is fetched dynamically from information_schema and summary_stats —
    no hardcoded metadata in the notebook. The LLM receives:
      1. Table metadata   — comment, primary key, column definitions
      2. Column statistics — min/max/nulls to identify anomalies
      3. Actual schema    — all column names and types
      4. Business rules   — what to check and how
    """
    schema        = _schema_block(df)
    stats         = _stats_block(summary_stats)
    table_context = _table_context_block(table)

    prompt = f"""Generate data quality checks for: {table}
{table_context}
{stats}

ACTUAL TABLE SCHEMA:
{schema}

BUSINESS RULES (only generate rules for columns confirmed in the schema above):

IDENTITY:
- opportunity_id must be not null and unique (single PK fact tables only).
- opportunity_name and client must be not null and not empty (trim whitespace).

DATES:
- stage_date_created and last_modified_date_time must be not null.
- anticipated_award_date: CASE WHEN stage IN ('1','2','3','4','5')
  THEN anticipated_award_date > CURRENT_DATE ELSE TRUE END
- anticipated_booking_date must be > CURRENT_DATE.
- project_finish_date must be > project_start_date.

PROBABILITY:
- win_probability and project_probability must be not null.
- Range 0.0-1.0 decimal or 0-100 percentage — check actual column values to determine scale.

PERCENT COMPLETE:
- All _percent_complete fields must be 0.0-1.0 decimal.
- Cumulative by FY: fy2 >= fy1, fy3 >= fy2, fy4 >= fy3 (sql_expression, allow nulls).
- Co-population: if any fy_percent_complete is not null, all must be not null (sql_expression).

JV OWNERSHIP (co-population sql_expression):
  (jv_ownership_percent IS NULL AND project_ownership IS NULL)
  OR (jv_ownership_percent IS NOT NULL AND project_ownership IS NOT NULL)

FINANCIAL: construction_value, opportunity_revenue, opportunity_fee,
factored_fee, trade_labor_hours >= 0. duration_months >= 0.
"""

    if verbose:
        print("-" * 60)
        print(f"PROMPT: {table.split('.')[-1]}")
        print("-" * 60)
        print(prompt)
        print("-" * 60)

    return prompt


def build_dim_prompt(table: str, df, summary_stats: dict) -> str:
    """
    Builds a schema-aware prompt for dimension tables.

    DQX summary_stats only covers numeric columns. Injecting the full schema
    ensures the LLM generates not-null and not-empty checks for string columns
    (name, code, label) that never appear in numeric summary_stats.
    """
    schema        = _schema_block(df)
    stats         = _stats_block(summary_stats)
    table_context = _table_context_block(table)

    prompt = f"""Generate data quality checks for dimension table: {table}
{table_context}
{stats}

ACTUAL TABLE SCHEMA ({table.split('.')[-1]}):
{schema}

DIMENSION TABLE RULES:
- Every ID column must be not null and unique.
- Every name, label, code, or description string column must be not null and not empty (trim whitespace).
- Every code column used for fact table joins must be not null, not empty, and unique.
- If opportunity_id exists as a foreign key it must be not null.
- If a last_modified or updated_at timestamp exists it must be not null and not in future.
- Any numeric fields must be within reasonable ranges based on the data (warn level).
"""

    if verbose:
        print("-" * 60)
        print(f"PROMPT: {table.split('.')[-1]}")
        print("-" * 60)
        print(prompt)
        print("-" * 60)

    return prompt

# COMMAND ----------

# DBTITLE 1,Profile and generate rules header
# MAGIC %md
# MAGIC ## Profile and generate rules
# MAGIC
# MAGIC Two strategies depending on table characteristics:
# MAGIC
# MAGIC 1. **Standard path** — DQGenerator.generate_dq_rules_ai_assisted() with enriched prompt. Used for most tables.
# MAGIC 2. **Direct LM path** — used for tables where the DQX generator internal max_tokens=1000 cap
# MAGIC    causes truncation. Calls the already-configured lm directly, bypassing the cap.

# COMMAND ----------

# DBTITLE 1,Run loop
# Tables that require the direct LM path due to the DQX generator internal
# max_tokens=1000 cap. gold_opportunities_fact has a large enough schema that
# the generator response is truncated and returns an empty JSON object.
# gold_revenue_projection added after the generator wrapped its response in a
# JSON key {"quality_rules": "[..."} instead of a bare array — same root cause.
# Add a short table name here if the same truncation is observed on another table.
LARGE_SCHEMA_TABLES = {
    "gold_opportunities_fact",
    "gold_revenue_projection",
}

_REQUIRED_KEYS       = {"criticality", "check"}
_REQUIRED_CHECK_KEYS = {"function", "arguments"}
_VALID_CRITICALITY   = {"error", "warn"}


def _validate_dqx_rule(rule: dict, idx: int, table: str) -> bool:
    """
    Returns True if rule matches DQX schema {criticality, check:{function, arguments}}.
    Logs a warning and returns False if not — rule is dropped rather than passed downstream.
    """
    if not isinstance(rule, dict):
        print(f"  [WARN] {table} rule[{idx}]: not a dict — dropped")
        return False
    missing = _REQUIRED_KEYS - rule.keys()
    if missing:
        print(f"  [WARN] {table} rule[{idx}]: missing keys {missing} — dropped")
        return False
    if rule["criticality"] not in _VALID_CRITICALITY:
        print(f"  [WARN] {table} rule[{idx}]: invalid criticality '{rule['criticality']}' — dropped")
        return False
    check = rule.get("check", {})
    if not isinstance(check, dict):
        print(f"  [WARN] {table} rule[{idx}]: 'check' is not a dict — dropped")
        return False
    missing_check = _REQUIRED_CHECK_KEYS - check.keys()
    if missing_check:
        print(f"  [WARN] {table} rule[{idx}]: check missing keys {missing_check} — dropped")
        return False
    return True


def _extract_and_validate_rules(response_text: str, table: str) -> list:
    """
    Extracts the first well-formed JSON array from LM response text and validates
    each rule against the DQX schema. Returns only rules that pass validation.

    Uses a non-greedy match anchored to the first [ ... ] block to avoid grabbing
    prose that wraps the array. Falls back to greedy and then bare text if needed.
    """
    match = re.search(r"\[.*?\]", response_text, re.DOTALL)
    if not match:
        try:
            candidate = json.loads(response_text.strip())
            if isinstance(candidate, list):
                raw_rules = candidate
            else:
                raise ValueError("Response is not a JSON array")
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(
                f"LM returned no parseable JSON array. "
                f"Response preview: {response_text[:300]}"
            ) from exc
    else:
        try:
            raw_rules = json.loads(match.group())
        except json.JSONDecodeError:
            # Non-greedy match stopped too early — try greedy fallback
            greedy = re.search(r"\[.*\]", response_text, re.DOTALL)
            if not greedy:
                raise ValueError(
                    f"LM returned no parseable JSON array. "
                    f"Response preview: {response_text[:300]}"
                )
            raw_rules = json.loads(greedy.group())

    if not isinstance(raw_rules, list):
        raise ValueError(f"Parsed JSON is not a list — got {type(raw_rules).__name__}")

    valid = [r for i, r in enumerate(raw_rules) if _validate_dqx_rule(r, i, table)]

    if not valid:
        raise ValueError(
            f"LM returned {len(raw_rules)} rules but none passed DQX schema validation"
        )

    return valid


all_results: Dict = {}


def _profile_and_generate(
    table: str,
    df,
    prompt_fn,
) -> Tuple[List, List, dict, str]:
    """
    Profiles a table and generates DQ rule candidates.

    Returns (checks, profiles, summary_stats, strategy).
    Raises on unrecoverable errors — caller handles logging.
    """
    short = table.split(".")[-1]

    # Tables that carry active_ind — profile only active records so candidate
    # thresholds reflect live data. Including soft-deleted rows (active_ind = 2)
    # skews min/max/null/distinct stats and distorts the ranges the LLM proposes.
    # Matches the filter applied in the archived dqx_05_data_profile.py.
    ACTIVE_IND_TABLES = {
        "gold_opportunities_fact",
    }
    df_to_profile = (
        df.filter(F.col("active_ind") == 1)
        if short in ACTIVE_IND_TABLES and "active_ind" in df.columns
        else df
    )

    summary_stats, profiles = profiler.profile(df_to_profile)
    prompt = prompt_fn(table, df_to_profile, summary_stats)

    if short in LARGE_SCHEMA_TABLES:
        # Direct LM path — bypasses the DQX generator internal 1000-token cap.
        # lm was configured in Cell 4 from widget parameters.
        # Output is validated against DQX schema {criticality, check:{function, arguments}}
        # so both paths produce the same shape downstream.
        full_prompt   = prompt + "\n\nReturn ONLY a valid JSON array of DQX rules. Each rule must have this exact shape: {\"criticality\": \"error\" or \"warn\", \"check\": {\"function\": \"<dqx_function_name>\", \"arguments\": {<args>}}}. No markdown, no explanation, no wrapper keys."
        response      = lm(full_prompt)
        response_text = response[0] if isinstance(response, list) else str(response)
        checks        = _extract_and_validate_rules(response_text, short)
        strategy      = "direct_lm"
    else:
        checks = generator.generate_dq_rules_ai_assisted(
            user_input=prompt,
            summary_stats=summary_stats,
            input_config=InputConfig(location=table),
        )
        strategy = "generator"

    return checks, profiles, summary_stats, strategy


def _run_tables(tables: List[str], prompt_fn) -> None:
    """
    Profiles each table, generates rules, and accumulates results in all_results.
    Errors are caught per table so a single failure does not stop the full run.
    """
    for table in tables:
        short = table.split(".")[-1]
        print(f"  processing: {short}")
        try:
            df = spark.read.table(table)
            checks, profiles, summary_stats, strategy = _profile_and_generate(
                table, df, prompt_fn
            )
            all_results[table] = {
                "status":        "success",
                "strategy":      strategy,
                "rule_count":    len(checks),
                "checks":        checks,
                "summary_stats": summary_stats,
                "profiles":      [str(p) for p in profiles],
            }
            print(f"  [OK]  {short}: {len(checks)} rules ({strategy})")

        except Exception as exc:
            all_results[table] = {"status": "error", "error": str(exc)}
            print(f"  [ERR] {short}: {str(exc)[:120]}")


print("=" * 60)
print("FACT AND PROJECTION TABLES")
print("=" * 60)
_run_tables(FACT_TABLES, build_fact_prompt)

print()
print("=" * 60)
print("DIMENSION TABLES")
print("=" * 60)
_run_tables(DIM_TABLES, build_dim_prompt)

# COMMAND ----------

# DBTITLE 1,Summary
success = [(t, r) for t, r in all_results.items() if r["status"] == "success"]
failed  = [(t, r) for t, r in all_results.items() if r["status"] == "error"]

print("=" * 60)
print("RUN SUMMARY")
print("=" * 60)
print(f"Tables processed : {len(all_results)}")
print(f"Succeeded        : {len(success)}")
print(f"Failed           : {len(failed)}")
print(f"Total rules      : {sum(r['rule_count'] for _, r in success)}")

if failed:
    print()
    print("FAILURES:")
    for table, result in failed:
        print(f"  [ERR] {table.split('.')[-1]}: {result['error'][:100]}")

print()
print("RULES PER TABLE:")
for table, result in success:
    short      = table.split(".")[-1]
    strategy   = result.get("strategy", "")
    checks     = result.get("checks", [])
    err_count  = sum(1 for c in checks if c.get("criticality") == "error")
    warn_count = sum(1 for c in checks if c.get("criticality") == "warn")
    print(f"  {short}")
    print(f"    rules: {result['rule_count']}  errors: {err_count}  warnings: {warn_count}  strategy: {strategy}")

if verbose:
    print()
    print("FULL RULE DETAIL:")
    for table, result in success:
        short = table.split(".")[-1]
        print(f"\n  --- {short} ---")
        for check in result.get("checks", []):
            if "check" in check:
                fn   = check["check"]["function"]
                args = check["check"]["arguments"]
                crit = check["criticality"].upper()
            else:
                fn   = check.get("check_type", check.get("rule_name", "unknown"))
                args = {
                    "column": check.get("column"),
                    "sql_expression": check.get("sql_expression"),
                }
                crit = check.get("severity", "warn").upper()
            print(f"  {crit:5} | {fn} | {args}")

