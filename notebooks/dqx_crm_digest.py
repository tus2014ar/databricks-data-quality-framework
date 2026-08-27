# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # dqx_crm_digest.py
# MAGIC ## Weekly DQ Digest Notification — CRM Pipeline
# MAGIC Builds and delivers a digest of the latest DQX findings from
# MAGIC `dqx_crm_bronze` and `dqx_crm_gold`. Reflects the most recent
# MAGIC pipeline run only — both source tables are overwrite-per-run.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
import concurrent.futures
import html as html_lib
import json
from datetime import datetime, timezone

import requests
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("test_mode", "True")
dbutils.widgets.dropdown("send_mode", "render_only", ["render_only", "webhook"])
dbutils.widgets.text("verbose", "False")

environment = dbutils.widgets.get("environment").strip().lower()
test_mode   = dbutils.widgets.get("test_mode").strip().lower() == "true"
send_mode   = dbutils.widgets.get("send_mode").strip().lower()
verbose     = dbutils.widgets.get("verbose").strip().lower() == "true"

assert environment in ("dev", "prod"), \
    f"Invalid environment '{environment}'. Must be 'dev' or 'prod'."
assert send_mode in ("render_only", "webhook"), \
    f"Invalid send_mode '{send_mode}'. Must be 'render_only' or 'webhook'."

prefix = "dev_" if environment == "dev" else ""
run_ts = datetime.now(timezone.utc)

print(f"environment : {environment}")
print(f"prefix      : '{prefix}'")
print(f"run_ts      : {run_ts.isoformat()}")
print(f"test_mode   : {test_mode}")
print(f"send_mode   : {send_mode}")

# COMMAND ----------

# DBTITLE 1,Fixed recipients
# Dial-in phase: the digest goes to just these 5 people while it's being
# validated, before expanding to broader distribution. All 5 receive the same
# single shared digest — no per-recipient filtering.
FIXED_RECIPIENTS = [
    "stakeholder1@example.com",
    "stakeholder2@example.com",
    "stakeholder3@example.com",
    "stakeholder4@example.com",
    "stakeholder5@example.com",
]

# test_mode=True (the widget default) routes here for validation runs, not to
# the 5 named stakeholders. Flip test_mode=False only once the content has been
# reviewed and approved, which routes to all 5 stakeholders above.
TEST_RECIPIENTS = [
    "test.recipient1@example.com",
    "stakeholder4@example.com",
]

# BU/BD leader recipient group (crm_source.reference.static_bu_bd_leaders)
# is intentionally not wired up yet — that static table doesn't exist. Add the
# existence check + recipient resolution here once it's uploaded.

# COMMAND ----------

# DBTITLE 1,Read latest findings
bronze_findings = spark.read.table(f"{prefix}crm_data.opportunities.dqx_crm_bronze")
gold_findings    = spark.read.table(f"{prefix}crm_data.opportunities.dqx_crm_gold")

bronze_finding_rows = bronze_findings.filter("record_type = 'finding'")
gold_finding_rows   = gold_findings.filter("record_type = 'finding'")

finding_rows_df = bronze_finding_rows.unionByName(gold_finding_rows)
quarantine_df   = gold_findings.filter("record_type = 'quarantine'")

total_quarantine_rows = quarantine_df.count()

print(f"bronze finding rows : {bronze_finding_rows.count()}")
print(f"gold finding rows   : {gold_finding_rows.count()}")
print(f"quarantine rows     : {total_quarantine_rows}")

# COMMAND ----------

# DBTITLE 1,Record-owner resolution
# Reused directly from earlier dashboard work: extract opportunity_id
# from quarantine rows' row_data JSON, join to gold_opportunities_fact for
# created_by_email/last_modified_by_email. stage_number and
# last_modified_date_time are also pulled here for the "Immediate Priorities"
# active/recency filter below - same join, no extra read.
fact_contacts = (
    spark.read.table(f"{prefix}crm_data.opportunities.gold_opportunities_fact")
    .select(
        F.col("opportunity_id").cast("string").alias("opportunity_id"),
        "created_by_email",
        "last_modified_by_email",
        "stage_number",
        "last_modified_date_time",
    )
    # opportunity_id is expected to be unique (opportunity_id_unique DQX check),
    # but that check reports failures rather than preventing them — if it's
    # currently failing, a left join on this column would fan out and inflate
    # every downstream count. Cheap guard regardless of whether it's failing.
    .dropDuplicates(["opportunity_id"])
)

# Denominator for the header's "N of TOTAL" framing — fact_contacts is already
# deduplicated by opportunity_id, so its count is the total distinct opportunity
# universe without a separate table read.
total_opportunity_records = fact_contacts.count()

quarantine_with_owner = (
    quarantine_df
    .withColumn("opportunity_id", F.get_json_object("row_data", "$.opportunity_id"))
    .join(fact_contacts, on="opportunity_id", how="left")
    .withColumn("resolved_contact", F.coalesce("last_modified_by_email", "created_by_email"))
    # No .cache() — PERSIST TABLE isn't supported on serverless compute
    # (NOT_SUPPORTED_WITH_SERVERLESS). Not functionally required, just a
    # performance optimization we can't use here.
)

resolved_df   = quarantine_with_owner.filter(F.col("resolved_contact").isNotNull())
unresolved_df = quarantine_with_owner.filter(F.col("resolved_contact").isNull())

resolved_count   = resolved_df.count()
unresolved_count = unresolved_df.count()

# total_quarantine_rows counts (record x failing-check) pairs, not distinct
# records — gold's apply loop explodes each failing record's issues into one
# quarantine row per check, so a record failing 3 checks contributes 3 rows.
# The header needs a genuine distinct-opportunity count instead.
distinct_flagged_records = (
    quarantine_with_owner
    .select("opportunity_id")
    .where(F.col("opportunity_id").isNotNull())
    .distinct()
    .count()
)

print(f"resolved (has contact)   : {resolved_count}")
print(f"unresolved (no contact)  : {unresolved_count}")
print(f"distinct flagged records : {distinct_flagged_records}")
print(f"total opportunity records: {total_opportunity_records}")

# COMMAND ----------

# DBTITLE 1,Business-friendly labels
# Business recipients don't know what a check_name means, so it gets translated
# for display. Curated content is preferred; anything not yet curated falls
# back to an auto-humanized version rather than showing a raw identifier or
# breaking. CHECK_BUSINESS_REASON is intentionally incomplete — it only needs
# entries for checks that actually fail; it grows over time as new checks
# start producing findings that fall back to the auto-humanized label below.
#
# Phrasing convention: state the defect ("address is blank"), not
# the rule ("address should be populated") — the reader shouldn't have to
# mentally invert a requirement into an observation. Add stage context for
# checks whose relevance is stage-gated.

# Curated from DQX-Rule-Candidates-Gold.xlsx (docs/), cross-checked by hand against
# the checks currently producing violations — see prior session notes.
# Only covers checks known to be failing as of this writing; see the fallback below.
CHECK_BUSINESS_REASON = {
    "required_columns_not_null": "A required field (opportunity ID, name, client, stage, dates, construction value, or fee %) is missing.",
    "anticipated_award_date_future_unless_booked": "Anticipated award date is not in the future, and no future booking date is set either.",
    "construction_start_date_future": "Construction start date is not in the future.",
    "construction_completion_after_start": "Construction completion date is on or before the construction start date.",
    "anticipated_booking_date_future": "Anticipated booking date is not in the future.",
    "fy_percent_complete_cumulative_order": "A later fiscal year shows less percent complete than an earlier one — progress went backward.",
    "win_project_probability_not_zero": "Win or project probability is 0% while the pursuit is active (stage 2-5) — likely a placeholder, not a real assessment.",
    "non_negative_numeric_fields": "Construction value, revenue, fee, trade labor hours, or duration is negative.",
    "manual_pct_cumulative_percent_complete_range": "Cumulative percent complete is outside the valid 0-100% range.",
    "manual_rev_completion_after_start": "Construction completion date is on or before the construction start date.",
    "manual_rev_cumulative_percent_complete_range": "Cumulative percent complete is outside the valid 0-100% range.",
    "manual_rev_fytd_projected_fee_not_negative": "Fiscal-year-to-date projected fee is negative.",
    "rev_proj_mtd_fee_not_negative": "Month-to-date projected fee is negative.",
    "rev_comp_fy_fytd_projected_fee_not_negative": "Fiscal-year-to-date projected fee is negative.",
    "rev_comp_monthly_mtd_projected_fee_not_negative": "Month-to-date projected fee is negative.",
    "opp_history_iwin_probability_range": "Win probability captured in history is outside the valid 0-100% range.",
    "opp_history_iproject_probability_range": "Project probability captured in history is outside the valid 0-100% range.",
    "opp_history_estimated_selection_date_reasonable": "Estimated selection date is more than 10 years past when the record was loaded.",
    "opp_history_lead_money_3_not_negative": "Lead money (snapshot 3) is negative.",
    "gng_scd_composite_key_unique": "This opportunity has more than one Go/No-Go record with the same start date — duplicate history entries.",
    "gng_scd_type2_integrity": "This open (current) Go/No-Go record has an end date set.",
    "gng_scd_max_one_active_record": "More than one current Go/No-Go record exists for this opportunity at the same time.",
    "gng_scd_go_no_go_required_from_stage_4": "No Go/No-Go recommendation set, but this opportunity has reached stage 4 or beyond, where one is required.",
    "gng_scd_go_no_go_valid_value_by_stage": "Go/No-Go recommendation value doesn't match what's allowed at this stage (only Conditional/Discuss before stage 4).",
    "dim_location_address1_not_null_or_empty": "Address is blank.",
    "dim_location_city_not_null_or_empty": "City is blank.",
    "dim_location_state_not_null_or_empty": "State is blank.",
    "dim_location_country_not_null_or_empty": "Country is blank.",
    "dim_studio_studio_acronym_not_null_or_empty": "Studio acronym is blank.",
    "dim_market_market_in_master_data_list": "Market doesn't match an active, approved entry in the master data list.",
    "dim_office_office_name_in_master_data_list": "Office name doesn't match an active, approved entry in the master data list.",
    "dim_division_business_unit_in_master_data_list": "Business unit doesn't match an active, approved entry in the master data list.",
    "dim_gorec_recommendation_not_null_or_empty": "Go/No-Go recommendation is blank.",
    "dim_selfperform_discipline_in_master_data_list": "Self-perform discipline doesn't match an active, approved entry in the master data list.",
}


def humanize_check_name(check_name: str) -> str:
    text = (check_name or "").replace("_", " ").strip()
    return text[:1].upper() + text[1:] if text else text


def get_check_label(check_name: str) -> str:
    return CHECK_BUSINESS_REASON.get(check_name, humanize_check_name(check_name))


# Friendly table labels — covers all 23 gold tables so any table
# that starts producing findings already has a label, not just the ones
# failing today.
TABLE_LABELS = {
    "gold_opportunities_fact": "Opportunities",
    "gold_opportunity_fiscal_time_allocation": "Fiscal Time Allocation",
    "gold_manual_fact_percent_complete": "Manual Percent Complete",
    "gold_manual_revenue_projections": "Manual Revenue Projections",
    "gold_revenue_projection": "Revenue Projections",
    "gold_revenue_projection_comparison_fiscal_year": "Revenue Projections (Fiscal Year)",
    "gold_revenue_projection_comparison_monthly": "Revenue Projections (Monthly)",
    "gold_opportunity_history": "Opportunity History",
    "gold_gng_scd": "Go/No-Go History",
    "gold_dim_project_location": "Project Location",
    "gold_dim_studio": "Studio",
    "gold_dim_market": "Market",
    "gold_dim_office": "Office",
    "gold_dim_division": "Division",
    "gold_dim_go_recommendation": "Go/No-Go Recommendation",
    "gold_dim_deliverymethod": "Delivery Method",
    "gold_dim_contracttype": "Contract Type",
    "gold_dim_primarycategory": "Primary Category",
    "gold_dim_servicetype": "Service Type",
    "gold_dim_selfperform": "Self-Perform Discipline",
    "gold_dim_clienttype": "Client Type",
    "gold_dim_priority": "Priority",
    "gold_dim_custom_field": "Custom Field",
}


def get_table_label(table_name: str) -> str:
    return TABLE_LABELS.get(table_name, table_name)

# COMMAND ----------

# DBTITLE 1,Content build — helpers
class _RawHtml(str):
    """Marks a value as pre-built HTML — passed through _esc unescaped."""


def _esc(value) -> str:
    if isinstance(value, _RawHtml):
        return value
    return html_lib.escape("" if value is None else str(value))


def _table_html(headers: list, rows: list) -> str:
    thead = "".join(f"<th>{_esc(h)}</th>" for h in headers)
    tbody = "".join(
        "<tr>" + "".join(f"<td>{_esc(v)}</td>" for v in row) + "</tr>"
        for row in rows
    )
    return (
        "<table border='1' cellpadding='4' cellspacing='0'>"
        f"<thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"
    )


SEVERITY_LABELS = {"error": "Error", "warn": "Warning"}


def get_severity_label(severity: str) -> str:
    return SEVERITY_LABELS.get(severity, (severity or "").capitalize())


def _check_cell(check_name: str) -> _RawHtml:
    # Business reason is what's shown; the raw check_name only surfaces as a
    # hover tooltip, for anyone technical who needs to trace it back to the YAML.
    label = _esc(get_check_label(check_name))
    tooltip = _esc(check_name)
    return _RawHtml(f"<span title='{tooltip}'>{label}</span>")


def _table_cell(table_name: str) -> _RawHtml:
    label = _esc(get_table_label(table_name))
    tooltip = _esc(table_name)
    return _RawHtml(f"<span title='{tooltip}'>{label}</span>")


SECTION_ROW_LIMIT = 10
ID_SAMPLE_LIMIT = 10
DASHBOARD_URL = "https://your-workspace.azuredatabricks.net/sql/dashboardsv3/<dashboard-id>?o=<workspace-id>"

# "Immediate Priorities" scope - active pursuit stages (excludes
# stage 1-too-early and 8/9/10/11/12-closed/on-hold) plus a recency cutoff.
# Stage alone isn't enough: opportunities can sit in an active stage for
# months without being touched, so both conditions are required together.
ACTIVE_STAGE_MIN = 2
ACTIVE_STAGE_MAX = 7
RECENCY_DAYS = 90


def _id_sample_cell(ids: list) -> str:
    shown_ids = ids[:ID_SAMPLE_LIMIT]
    text = ", ".join(shown_ids)
    remaining = len(ids) - len(shown_ids)
    if remaining > 0:
        text += f" (+{remaining} more)"
    return text


def _pct_cell(rate) -> str:
    return f"{rate * 100:.1f}%" if rate is not None else "—"


def _more_note(total_count: int, shown_count: int) -> str:
    # Static cap, not an in-email expand/collapse — Outlook desktop strips JS
    # and doesn't support <details> as a real toggle, so this is the only way
    # to guarantee every recipient gets a short email regardless of mail
    # client. The footer's dashboard link is the pointer to the rest.
    remaining = total_count - shown_count
    if remaining <= 0:
        return ""
    return f"<p style='font-size:13px;color:#666;'>+ {remaining} more</p>"


def _severity_bar(error_count: int, warn_count: int) -> str:
    total = error_count + warn_count
    if total == 0:
        return "<p>No data quality checks are currently failing.</p>"
    error_pct = round(error_count / total * 100)
    warn_pct = 100 - error_pct
    # Plain HTML table with colored cells — no JS, no images — so this survives
    # an actual email client (Outlook) once send_mode=webhook is wired up, not
    # just the notebook's displayHTML preview.
    return (
        "<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
        "border='0' style='height:14px;border-radius:4px;overflow:hidden;margin:8px 0 4px;'>"
        f"<tr><td style='background-color:#c0392b;width:{error_pct}%;'></td>"
        f"<td style='background-color:#e1a100;width:{warn_pct}%;'></td></tr>"
        "</table>"
        "<p style='font-size:13px;margin:0 0 16px;'>"
        f"<span style='color:#c0392b;'>&#9632;</span> {error_count} error-level &nbsp;&nbsp;"
        f"<span style='color:#e1a100;'>&#9632;</span> {warn_count} warning-level</p>"
    )


def build_header(run_ts, distinct_flagged_records: int, total_opportunity_records: int, error_count: int, warn_count: int) -> str:
    total_violating_checks = error_count + warn_count
    flagged_pct = (
        f"{distinct_flagged_records / total_opportunity_records * 100:.1f}%"
        if total_opportunity_records > 0 else "—"
    )
    return (
        f"<h2>CRM DQ Weekly Digest — {run_ts.date()}</h2>"
        f"<p><b>{distinct_flagged_records} of {total_opportunity_records} opportunity records "
        f"({flagged_pct})</b> are currently flagged by <b>{total_violating_checks} data quality checks</b>.</p>"
        + _severity_bar(error_count, warn_count)
    )


def build_immediate_priorities(quarantine_with_owner, check_totals_df) -> str:
    agg = (
        quarantine_with_owner
        .filter(
            (F.col("severity") == "error")
            & F.col("stage_number").between(ACTIVE_STAGE_MIN, ACTIVE_STAGE_MAX)
            & (F.col("last_modified_date_time") >= F.date_sub(F.current_date(), RECENCY_DAYS))
        )
        .groupBy("resolved_contact", "table_name", "check_name")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .join(check_totals_df, on=["table_name", "check_name"], how="left")
        .withColumn(
            "pct",
            F.when(F.col("total_row_count") > 0, F.col("row_count") / F.col("total_row_count")),
        )
        .orderBy(F.col("row_count").desc())
        .collect()
    )
    shown = agg[:SECTION_ROW_LIMIT]
    rows = [
        (r["resolved_contact"] or "No owner — needs assignment", _table_cell(r["table_name"]), _check_cell(r["check_name"]),
         r["row_count"], _pct_cell(r["pct"]))
        for r in shown
    ]
    body = (
        "<h3 style='color:#c0392b;margin:0 0 4px;'>Immediate Priorities This Week</h3>"
        "<p style='font-size:13px;color:#666;margin:0 0 8px;'>Error-level findings on active pursuits "
        f"(stages {ACTIVE_STAGE_MIN}&ndash;{ACTIVE_STAGE_MAX}) modified in the last {RECENCY_DAYS} days</p>"
        + _table_html(["Contact", "Table", "What's wrong", "Records affected", "% of records"], rows)
        + _more_note(len(agg), len(shown))
    )
    # Table-based border, not a div - divs render inconsistently in Outlook
    # desktop, same reasoning as the severity bar above.
    return (
        "<table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='border:2px solid #c0392b;background-color:#fdf2f2;margin:0 0 16px;'>"
        f"<tr><td style='padding:12px 16px;'>{body}</td></tr></table>"
    )


def build_top_findings(finding_rows_df) -> str:
    # Grouped by table ("group findings by table, keep all 22")
    # rather than one flat top-10 list — a table-level cap would hide entire
    # tables with real findings behind a "+N more" note. Checks per table are
    # a small, bounded number in practice, so no per-table cap is needed.
    all_rows = (
        finding_rows_df
        .filter("fail_count > 0")
        .select("table_name", "check_name", "severity", "fail_count", "fail_rate")
        .collect()
    )

    table_totals = {}
    for r in all_rows:
        table_totals[r["table_name"]] = table_totals.get(r["table_name"], 0) + r["fail_count"]
    tables_by_volume = sorted(table_totals, key=lambda t: -table_totals[t])

    sections = []
    for table_name in tables_by_volume:
        table_rows = sorted(
            (r for r in all_rows if r["table_name"] == table_name),
            key=lambda r: -r["fail_count"],
        )
        rows = [
            (_check_cell(r["check_name"]), get_severity_label(r["severity"]),
             r["fail_count"], _pct_cell(r["fail_rate"]))
            for r in table_rows
        ]
        sections.append(
            f"<h4 title='{_esc(table_name)}'>{_esc(get_table_label(table_name))}</h4>"
            + _table_html(["What's wrong", "Severity", "Records affected", "% of records"], rows)
        )

    return "<h3>Top Findings by Check</h3>" + "".join(sections)


def build_findings_by_owner(resolved_df, check_totals_df) -> str:
    agg = (
        resolved_df
        .groupBy("resolved_contact", "table_name", "check_name")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sort_array(F.collect_list(F.col("opportunity_id").cast("long"))).alias("opportunity_ids"),
        )
        .join(check_totals_df, on=["table_name", "check_name"], how="left")
        .withColumn(
            "pct",
            F.when(F.col("total_row_count") > 0, F.col("row_count") / F.col("total_row_count")),
        )
        .orderBy(F.col("row_count").desc())
        .collect()
    )
    shown = agg[:SECTION_ROW_LIMIT]
    rows = [
        (r["resolved_contact"], _table_cell(r["table_name"]), _check_cell(r["check_name"]),
         _id_sample_cell([str(i) for i in r["opportunity_ids"]]), r["row_count"], _pct_cell(r["pct"]))
        for r in shown
    ]
    return (
        "<h3>Top Findings by Owner</h3>"
        + _table_html(["Contact", "Table", "What's wrong", "Opportunity IDs", "Records affected", "% of records"], rows)
        + _more_note(len(agg), len(shown))
    )


def build_findings_with_no_owner(unresolved_df, check_totals_df) -> str:
    agg = (
        unresolved_df
        .groupBy("table_name", "check_name")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .join(check_totals_df, on=["table_name", "check_name"], how="left")
        .withColumn(
            "pct",
            F.when(F.col("total_row_count") > 0, F.col("row_count") / F.col("total_row_count")),
        )
        .orderBy(F.col("row_count").desc())
        .collect()
    )
    shown = agg[:SECTION_ROW_LIMIT]
    rows = [
        (_table_cell(r["table_name"]), _check_cell(r["check_name"]), r["row_count"], _pct_cell(r["pct"]))
        for r in shown
    ]
    return (
        "<h3>Top Findings with No Owner</h3>"
        + _table_html(["Table", "What's wrong", "Records affected", "% of records"], rows)
        + _more_note(len(agg), len(shown))
    )


def build_footer() -> str:
    return (
        "<hr>"
        "<p>Full record-level detail is available in the DQX tables and "
        f"<a href='{DASHBOARD_URL}'>dashboard</a>.</p>"
    )


# COMMAND ----------

# DBTITLE 1,AI takeaways
# Per-recipient "AI-Generated Takeaways" section, generated via ai_query() against a
# Databricks-served model — no external API key, no data egress. Scoped today
# to FIXED_RECIPIENTS/TEST_RECIPIENTS (personalized by what each person owns),
# not yet to per-individual record owners — that's a later phase.
TAKEAWAY_MODEL = "databricks-claude-opus-4-8"
TAKEAWAY_TIMEOUT_SECONDS = 30
TAKEAWAY_MAX_TOKENS = 400
TAKEAWAY_OWNED_LIMIT = 5

# Run-level counters so a systematic failure (bad model name, missing
# ai_query entitlement, wrong call signature) is visible in RUN SUMMARY
# instead of silently omitting the section for every recipient while the
# run still reports success. takeaway_attempts only counts recipients who
# actually had owned findings (i.e., the model call was actually attempted).
takeaway_attempts = 0
takeaway_failures = []

TAKEAWAY_PROMPT = """You are summarizing data quality findings for one recipient of a weekly digest email.

Recipient: {recipient}

Findings owned by this recipient (JSON, one entry per table/check combination):
{owned_json}

Organization-wide top findings this week, for context only — do not tell the
recipient to fix these unless they also appear in their own owned findings above:
{org_json}

Write a short, prioritized "what to do" summary for this recipient.

Rules:
- Ground every sentence in the data above. Never invent a table, check, or number that isn't there.
- If the recipient has no owned findings, respond with exactly: NOTHING_NOTABLE
- Otherwise: respond as exactly 4 to 5 bullet points. Plain text only, no HTML and no markdown formatting.
- Each bullet on its own line, starting with a hyphen (-), one short sentence, under 25 words. No paragraph format.
- Sort strictly by severity first: cover every Error-severity finding before any Warning-severity finding,
  regardless of which has more records affected. Only use records-affected to order within the same severity.
- If there are more than 5 distinct findings, cover only the 5 highest-priority ones by the sort rule above
  and leave the rest out entirely — do not compress every finding into one bullet.
- Do not add a greeting, a sign-off, or any text outside the bullets themselves."""


def build_recipient_context(recipient_email: str, resolved_df, finding_rows_df) -> dict:
    # Case/whitespace-insensitive match - gold_opportunities_fact stores emails
    # mixed-case, but FIXED_RECIPIENTS/TEST_RECIPIENTS are typed lowercase. An
    # exact match here silently drops a recipient's owned findings to zero.
    normalized_recipient = recipient_email.strip().lower()
    # Same active/recency window as build_immediate_priorities (stage 2-7,
    # modified in the last RECENCY_DAYS) - without it, the AI can prioritize
    # stale or closed opportunities that Immediate Priorities deliberately
    # excludes, and the two sections end up contradicting each other.
    # Severity first (errors before warnings), volume only breaks ties within
    # the same severity - enforced here rather than left entirely to the
    # model's compliance with the prompt's sort/cap rules.
    owned_rows = (
        resolved_df
        .filter(
            (F.lower(F.trim(F.col("resolved_contact"))) == normalized_recipient)
            & F.col("stage_number").between(ACTIVE_STAGE_MIN, ACTIVE_STAGE_MAX)
            & (F.col("last_modified_date_time") >= F.date_sub(F.current_date(), RECENCY_DAYS))
        )
        .groupBy("table_name", "check_name", "severity")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .orderBy(F.when(F.col("severity") == "error", 0).otherwise(1), F.col("row_count").desc())
        .limit(TAKEAWAY_OWNED_LIMIT)
        .collect()
    )
    org_rows = (
        finding_rows_df
        .filter("fail_count > 0")
        .select("table_name", "check_name", "severity", "fail_count")
        .orderBy(F.col("fail_count").desc())
        .limit(SECTION_ROW_LIMIT)
        .collect()
    )
    return {
        "recipient": recipient_email,
        "owned": [
            {
                "table": get_table_label(r["table_name"]),
                "issue": get_check_label(r["check_name"]),
                "severity": get_severity_label(r["severity"]),
                "records_affected": r["row_count"],
            }
            for r in owned_rows
        ],
        "org_top": [
            {
                "table": get_table_label(r["table_name"]),
                "issue": get_check_label(r["check_name"]),
                "severity": get_severity_label(r["severity"]),
                "records_affected": r["fail_count"],
            }
            for r in org_rows
        ],
    }


def _format_takeaway_text(text: str) -> _RawHtml:
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    bullet_lines = [ln.lstrip("-").strip() for ln in lines if ln.startswith("-")]
    if bullet_lines and len(bullet_lines) == len(lines):
        items = "".join(f"<li>{_esc(b)}</li>" for b in bullet_lines)
        return _RawHtml(f"<ul style='margin:4px 0 0;padding-left:20px;'>{items}</ul>")
    return _RawHtml(f"<p>{_esc(text.strip())}</p>")


def build_takeaways(context: dict) -> str:
    # No owned findings for this recipient — nothing to say, so the section
    # is omitted entirely rather than showing an empty/placeholder block.
    if not context["owned"]:
        return ""

    prompt = TAKEAWAY_PROMPT.format(
        recipient=context["recipient"],
        owned_json=json.dumps(context["owned"]),
        org_json=json.dumps(context["org_top"]),
    )

    def _call_model():
        (
            spark.createDataFrame([(prompt,)], "prompt string")
            .createOrReplaceTempView("takeaway_prompt")
        )
        return spark.sql(
            f"""
            SELECT ai_query(
                '{TAKEAWAY_MODEL}',
                prompt,
                modelParameters => named_struct('max_tokens', {TAKEAWAY_MAX_TOKENS})
            ) AS takeaway
            FROM takeaway_prompt
            """
        ).collect()[0]["takeaway"]

    # ai_query() has no native request timeout, so the call runs on a worker
    # thread with a hard wall-clock timeout — a stuck or slow model call skips
    # this section rather than hanging the whole digest run. Deliberately not
    # using the executor as a context manager: `with` calls shutdown(wait=True)
    # on exit, which re-joins the still-running worker and blocks here anyway,
    # defeating the timeout. shutdown(wait=False) lets a hung call's thread
    # finish on its own in the background without blocking this call or the
    # next recipient's (fresh, independent) executor.
    global takeaway_attempts
    takeaway_attempts += 1
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        text = executor.submit(_call_model).result(timeout=TAKEAWAY_TIMEOUT_SECONDS)
    except Exception as error:
        takeaway_failures.append(context["recipient"])
        print(f"[takeaways] skipped for {context['recipient']}: {error}")
        return ""
    finally:
        executor.shutdown(wait=False)

    if not text or "NOTHING_NOTABLE" in text:
        return ""

    return (
        "<h3>AI-Generated Takeaways</h3>"
        f"<div style='font-size:14px;line-height:1.5;'>{_format_takeaway_text(text)}</div>"
    )


# COMMAND ----------

# DBTITLE 1,Assemble digest
violating_by_severity = (
    finding_rows_df
    .filter("fail_count > 0")
    .groupBy("severity")
    .agg(F.count(F.lit(1)).alias("cnt"))
    .collect()
)
severity_counts = {r["severity"]: r["cnt"] for r in violating_by_severity}
error_count = severity_counts.get("error", 0)
warn_count  = severity_counts.get("warn", 0)

# Per-check total_row_count, used to compute "% of records" in the owner and
# no-owner sections — same denominator DQX already uses for fail_rate in
# build_top_findings, joined back in here since quarantine rows don't carry it.
check_totals_df = (
    finding_rows_df
    .select("table_name", "check_name", "total_row_count")
    .dropDuplicates(["table_name", "check_name"])
)

# Shared across every recipient — org-wide, not personalized. Built once and
# reused per-recipient below so the per-recipient loop only re-does the one
# part that's actually personalized (the AI takeaways call).
shared_sections = (
    build_header(run_ts, distinct_flagged_records, total_opportunity_records, error_count, warn_count)
    + build_immediate_priorities(quarantine_with_owner, check_totals_df)
    + build_top_findings(finding_rows_df)
    + build_findings_by_owner(resolved_df, check_totals_df)
    + build_findings_with_no_owner(unresolved_df, check_totals_df)
)
footer_html = build_footer()

# COMMAND ----------

# DBTITLE 1,Delivery
# test_mode=True routes to TEST_RECIPIENTS (just the notebook owner) for
# validation runs. Only flip test_mode=False, after the content's been
# reviewed and approved, to route to the 5 named stakeholders in
# FIXED_RECIPIENTS — per ticket requirement #7 (test run before full send).
recipients = list(TEST_RECIPIENTS) if test_mode else list(FIXED_RECIPIENTS)


def _deliver_digest(html_content: str, recipients: list, send_mode: str):
    if send_mode == "webhook":
        webhook_url = dbutils.secrets.get(scope="data_pipeline_secrets", key="power_automate_digest_webhook_url")
        response = requests.post(webhook_url, json={
            "to": recipients,
            "subject": f"CRM DQ Weekly Digest — {run_ts.date()}",
            "body_html": html_content,
        }, timeout=30)
        response.raise_for_status()
        print(f"[WEBHOOK] Sent to: {recipients} (status {response.status_code})")
        return
    print(f"[RENDER-ONLY] Would send to: {recipients}")
    displayHTML(html_content)


# One email per recipient — each gets the same shared org-wide sections plus
# their own personalized "AI-Generated Takeaways" section. Today recipients is just
# FIXED_RECIPIENTS/TEST_RECIPIENTS (a handful of stakeholders, not the ~80
# individual record owners), so this is a handful of sends, not a fan-out.
#
# The whole per-recipient body — context build, AI call, and send — is
# guarded, not just the send: a transient Spark error building one recipient's
# context previously killed every remaining recipient's digest, same as an
# unguarded send would. One recipient's failure is logged and skipped so the
# rest of the run completes.
#
# This tracks failures within THIS run, not across runs — it does not make a
# re-run idempotent. A fresh run has no durable record of who already
# succeeded, so re-running the whole notebook would re-send to recipients who
# already got a copy. True idempotent retry needs a persisted per-run/
# per-recipient send record (the message-log table, still an open discussion
# point). For now, failed_recipients tells you who to retry by hand.
failed_recipients = []
for recipient in recipients:
    try:
        context = build_recipient_context(recipient, resolved_df, finding_rows_df)
        takeaways_html = build_takeaways(context)
        digest_html = shared_sections + takeaways_html + footer_html

        if verbose:
            print(f"digest_html length for {recipient}: {len(digest_html)} chars")

        _deliver_digest(digest_html, [recipient], send_mode)
    except Exception as error:
        failed_recipients.append(recipient)
        print(f"[delivery] FAILED for {recipient}: {error}")

if failed_recipients:
    print(f"[delivery] {len(failed_recipients)} of {len(recipients)} recipients failed: {failed_recipients}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("RUN SUMMARY")
print("=" * 60)
print(f"run_ts                  : {run_ts.isoformat()}")
print(f"environment             : {environment}")
print(f"test_mode               : {test_mode}")
print(f"send_mode               : {send_mode}")
print(f"recipients              : {recipients}")
print(f"total quarantine rows   : {total_quarantine_rows}")
print(f"distinct flagged records: {distinct_flagged_records}")
print(f"resolved (has contact)  : {resolved_count}")
print(f"unresolved (no contact) : {unresolved_count}")
print(f"error-level checks      : {error_count}")
print(f"warn-level checks       : {warn_count}")
print(f"takeaway calls attempted: {takeaway_attempts}")
print(f"takeaway calls failed   : {len(takeaway_failures)} {takeaway_failures}")
if takeaway_attempts > 0 and len(takeaway_failures) == takeaway_attempts:
    print(
        "[ALERT] Every AI takeaway call failed this run — likely a systematic "
        "issue (bad TAKEAWAY_MODEL, missing ai_query entitlement, or a wrong "
        "call signature), not per-recipient noise. Investigate before the next run."
    )
