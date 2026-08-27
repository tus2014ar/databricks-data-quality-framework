# DQX CRM Dashboard — Reference Export

Exported dashboard definition (Lakeview JSON) converted to a readable reference. The original `.json` file can be re-imported into Databricks Lakeview dashboards to fully recreate this dashboard, including layout and filters.

**Tabs:** 2  |  **Underlying queries:** 12

## Dashboard Tabs

- **Bronze Ingestion Health** — 5 widgets
- **Gold Data Quality** — 15 widgets

## Tab Details

### Bronze Ingestion Health

- **Widget `bronze_kpi_monitored`** (counter) — dataset(s): `bronze_kpis`
- **Widget `bronze_kpi_stale`** (counter) — dataset(s): `bronze_kpis`
- **Widget `bronze_kpi_freshness`** (counter) — dataset(s): `bronze_kpis`
- **Widget `bronze_kpi_last_checked`** (counter) — dataset(s): `bronze_kpis`
- **Widget `bronze_status_table`** (table) — dataset(s): `bronze_status`

### Gold Data Quality

- **Widget `kpi_tables_monitored`** (counter) — dataset(s): `dqx_kpis`
- **Widget `kpi_checks_run`** (counter) — dataset(s): `dqx_kpis`
- **Widget `kpi_pass_rate`** (counter) — dataset(s): `dqx_kpis`
- **Widget `kpi_flagged_records`** (counter) — dataset(s): `dqx_kpis`
- **Widget `kpi_last_checked`** (counter) — dataset(s): `dqx_kpis`
- **Widget `pie_severity`** (bar) — dataset(s): `dqx_severity`
- **Widget `bar_top10`** (bar) — dataset(s): `dqx_top10`
- **Widget `table_top15`** (table) — dataset(s): `dqx_top15_checks`
- **Widget `table_health`** (table) — dataset(s): `dqx_health`
- **Widget `table_creator_quality`** (table) — dataset(s): `dqx_creator_quality`
- **Widget `pie_attribution`** (pie) — dataset(s): `dqx_attribution`
- **Widget `table_all_flagged`** (table) — dataset(s): `dqx_all_flagged`
- **Widget `bar_issues_by_creator`** (bar) — dataset(s): `dqx_creator_bar`
- **Widget `filter_creator_email`** (filter-multi-select) — dataset(s): `dqx_creator_quality, dqx_creator_bar, dqx_all_flagged`
- **Widget `table_email_activity`** (table) — dataset(s): `dqx_email_activity`

## SQL Query Reference

All underlying datasets, keyed by internal dataset name (referenced above).

### `dqx_health` — DQX Health by Table

```sql
SELECT
  table_name,
  COUNT(*) AS total_checks,
  COUNT(
    CASE
      WHEN fail_count = 0 THEN 1
    END
  ) AS checks_passing,
  COUNT(
    CASE
      WHEN
        severity = 'error'
        AND fail_count > 0
      THEN
        1
    END
  ) AS critical_violations,
  COUNT(
    CASE
      WHEN
        severity = 'warn'
        AND fail_count > 0
      THEN
        1
    END
  ) AS warning_violations
FROM
  crm_data.opportunities.dqx_crm_gold
WHERE
  record_type = 'finding'
GROUP BY
  table_name
ORDER BY
  critical_violations DESC,
  warning_violations DESC
```

### `dqx_top10` — DQX Top 10 Tables by Flagged Records

```sql
SELECT
  table_name,
  SUM(fail_count) AS total_flagged
FROM
  crm_data.opportunities.dqx_crm_gold
WHERE
  record_type = 'finding'
GROUP BY
  table_name
ORDER BY
  total_flagged DESC
LIMIT 10
```

### `dqx_severity` — DQX Violations by Severity

```sql
SELECT
  CASE
    WHEN severity = 'error' THEN 'Critical'
    WHEN severity = 'warn' THEN 'Warning'
    ELSE severity
  END AS severity_label,
  COUNT(*) AS violation_count
FROM
  crm_data.opportunities.dqx_crm_gold
WHERE
  record_type = 'finding'
  AND fail_count > 0
GROUP BY
  severity_label
```

### `dqx_top15_checks` — DQX Top 15 Violated Checks

```sql
SELECT
  table_name,
  check_name,
  CASE
    WHEN severity = 'error' THEN 'Critical'
    WHEN severity = 'warn' THEN 'Warning'
    ELSE severity
  END AS severity_label,
  fail_count,
  ROUND(fail_rate * 100, 1) AS fail_rate_pct
FROM
  crm_data.opportunities.dqx_crm_gold
WHERE
  record_type = 'finding'
  AND fail_count > 0
ORDER BY
  fail_count DESC
LIMIT 15
```

### `dqx_kpis` — DQX KPIs

```sql
SELECT
  COUNT(DISTINCT
    CASE
      WHEN record_type = 'finding' THEN table_name
    END
  ) AS tables_monitored,
  COUNT(
    CASE
      WHEN record_type = 'finding' THEN 1
    END
  ) AS checks_run,
  ROUND(
    100.0
      * SUM(
        CASE
          WHEN
            record_type = 'finding'
            AND fail_count = 0
          THEN
            1
          ELSE 0
        END
      )
      / NULLIF(
        COUNT(
          CASE
            WHEN record_type = 'finding' THEN 1
          END
        ),
        0
      ),
    1
  ) AS pass_rate,
  COUNT(
    CASE
      WHEN record_type = 'quarantine' THEN 1
    END
  ) AS flagged_records,
  MAX(run_ts) AS last_checked
FROM
  crm_data.opportunities.dqx_crm_gold
```

### `bronze_status` — Bronze Ingestion Status

```sql
SELECT
  table_name,
  CASE
    WHEN fail_count = 1 THEN 'Stale'
    ELSE 'Fresh'
  END AS status,
  COALESCE(message, 'On time') AS details
FROM
  crm_data.opportunities.dqx_crm_bronze
WHERE
  record_type = 'finding'
ORDER BY
  fail_count DESC,
  table_name ASC
```

### `bronze_kpis` — Bronze Ingestion KPIs

```sql
SELECT
  COUNT(*) AS tables_monitored,
  SUM(fail_count) AS tables_stale,
  ROUND(
    100.0
      * SUM(
        CASE
          WHEN fail_count = 0 THEN 1
          ELSE 0
        END
      )
      / NULLIF(COUNT(*), 0),
    1
  ) AS freshness_health,
  MAX(run_ts) AS last_checked
FROM
  crm_data.opportunities.dqx_crm_bronze
WHERE
  record_type = 'finding'
```

### `dqx_creator_quality` — DQX Issue Rate by Creator

```sql
WITH flagged AS (
  SELECT DISTINCT
    get_json_object(q.row_data, '$.opportunity_id') AS opportunity_id,
    q.severity,
    f.created_by_email,
    f.last_modified_by_email,
    f.last_modified_date_time
  FROM
    crm_data.opportunities.dqx_crm_gold q
      JOIN crm_data.opportunities.gold_opportunities_fact f
        ON get_json_object(q.row_data, '$.opportunity_id') = CAST(f.opportunity_id AS STRING)
  WHERE
    q.record_type = 'quarantine'
    AND f.created_by_email IS NOT NULL
),
totals AS (
  SELECT
    created_by_email,
    COUNT(DISTINCT opportunity_id) AS total_opportunities
  FROM
    crm_data.opportunities.gold_opportunities_fact
  WHERE
    created_by_email IS NOT NULL
  GROUP BY
    created_by_email
),
flagged_summary AS (
  SELECT
    created_by_email,
    COUNT(DISTINCT opportunity_id) AS flagged_opportunities,
    COUNT(DISTINCT
      CASE
        WHEN severity = 'error' THEN opportunity_id
      END
    ) AS critical_opportunities,
    COUNT(DISTINCT
      CASE
        WHEN severity = 'warn' THEN opportunity_id
      END
    ) AS warning_opportunities
  FROM
    flagged
  GROUP BY
    created_by_email
),
most_recent AS (
  SELECT
    created_by_email,
    last_modified_date_time,
    last_modified_by_email,
    ROW_NUMBER() OVER (
        PARTITION BY created_by_email
        ORDER BY last_modified_date_time DESC, last_modified_by_email IS NULL ASC
      ) AS rn
  FROM
    flagged
)
SELECT
  t.created_by_email,
  t.total_opportunities,
  COALESCE(fs.flagged_opportunities, 0) AS flagged_opportunities,
  ROUND(
    100.0 * COALESCE(fs.flagged_opportunities, 0) / t.total_opportunities,
    1
  ) AS flagged_rate_pct,
  COALESCE(fs.critical_opportunities, 0) AS critical_opportunities,
  COALESCE(fs.warning_opportunities, 0) AS warning_opportunities,
  mr.last_modified_date_time AS most_recent_modification,
  COALESCE(mr.last_modified_by_email, 'Not Available') AS who_modified_it,
  DATEDIFF(CURRENT_DATE(), mr.last_modified_date_time) AS days_since_last_modification,
  CASE
    WHEN
      mr.last_modified_by_email IS NOT NULL
      AND mr.last_modified_by_email != t.created_by_email
    THEN
      'external'
    ELSE 'self'
  END AS modifier_flag
FROM
  totals t
    LEFT JOIN flagged_summary fs
      ON t.created_by_email = fs.created_by_email
    LEFT JOIN most_recent mr
      ON t.created_by_email = mr.created_by_email
      AND mr.rn = 1
ORDER BY
  flagged_rate_pct DESC,
  t.total_opportunities DESC
```

### `dqx_attribution` — DQX Attribution vs Orphaned

```sql
WITH base AS (
  SELECT
    CASE
      WHEN f.opportunity_id IS NOT NULL THEN 'Owner Resolved'
      ELSE 'Orphaned'
    END AS attribution_status,
    COUNT(*) AS quarantine_rows
  FROM
    crm_data.opportunities.dqx_crm_gold q
      LEFT JOIN crm_data.opportunities.gold_opportunities_fact f
        ON get_json_object(q.row_data, '$.opportunity_id') = CAST(f.opportunity_id AS STRING)
  WHERE
    q.record_type = 'quarantine'
  GROUP BY
    attribution_status
)
SELECT
  attribution_status,
  quarantine_rows,
  ROUND(100.0 * quarantine_rows / SUM(quarantine_rows) OVER (), 1) AS pct_share
FROM
  base
```

### `dqx_all_flagged` — DQX All Flagged Issues by Creator

```sql
SELECT
  f.created_by_email,
  f.last_modified_by_email,
  f.opportunity_name,
  q.table_name,
  q.check_name,
  CASE
    WHEN q.severity = 'error' THEN 'Critical'
    WHEN q.severity = 'warn' THEN 'Warning'
    ELSE q.severity
  END AS severity_label,
  get_json_object(q.row_data, '$.opportunity_id') AS opportunity_id,
  q.message
FROM
  crm_data.opportunities.dqx_crm_gold q
    JOIN crm_data.opportunities.gold_opportunities_fact f
      ON get_json_object(q.row_data, '$.opportunity_id') = CAST(f.opportunity_id AS STRING)
WHERE
  q.record_type = 'quarantine'
ORDER BY
  CAST(get_json_object(q.row_data, '$.opportunity_id') AS INT),
  severity_label
```

### `dqx_creator_bar` — DQX Creator Bar Data

```sql
WITH flagged AS (
  SELECT DISTINCT
    get_json_object(q.row_data, '$.opportunity_id') AS opportunity_id,
    q.severity,
    f.created_by_email,
    f.last_modified_by_email,
    f.last_modified_date_time
  FROM
    crm_data.opportunities.dqx_crm_gold q
      JOIN crm_data.opportunities.gold_opportunities_fact f
        ON get_json_object(q.row_data, '$.opportunity_id') = CAST(f.opportunity_id AS STRING)
  WHERE
    q.record_type = 'quarantine'
    AND f.created_by_email IS NOT NULL
),
totals AS (
  SELECT
    created_by_email,
    COUNT(DISTINCT opportunity_id) AS total_opportunities
  FROM
    crm_data.opportunities.gold_opportunities_fact
  WHERE
    created_by_email IS NOT NULL
  GROUP BY
    created_by_email
),
flagged_summary AS (
  SELECT
    created_by_email,
    COUNT(DISTINCT opportunity_id) AS flagged_opportunities,
    COUNT(DISTINCT
      CASE
        WHEN severity = 'error' THEN opportunity_id
      END
    ) AS critical_opportunities,
    COUNT(DISTINCT
      CASE
        WHEN severity = 'warn' THEN opportunity_id
      END
    ) AS warning_opportunities
  FROM
    flagged
  GROUP BY
    created_by_email
),
summary AS (
  SELECT
    t.created_by_email,
    INITCAP(REPLACE(SPLIT(t.created_by_email, '@')[0], '.', ' ')) AS creator_name,
    COALESCE(fs.critical_opportunities, 0) AS critical_opportunities,
    COALESCE(fs.warning_opportunities, 0) AS warning_opportunities
  FROM
    totals t
      LEFT JOIN flagged_summary fs
        ON t.created_by_email = fs.created_by_email
  WHERE
    COALESCE(fs.flagged_opportunities, 0) > 0
)
SELECT
  created_by_email,
  creator_name,
  'Critical' AS severity_label,
  critical_opportunities AS issue_count
FROM
  summary
WHERE
  critical_opportunities > 0
UNION ALL
SELECT
  created_by_email,
  creator_name,
  'Warning' AS severity_label,
  warning_opportunities AS issue_count
FROM
  summary
WHERE
  warning_opportunities > 0
ORDER BY
  creator_name
```

### `dqx_email_activity` — DQX Email Activity

```sql
WITH activity AS (
  SELECT
    created_by_email AS email,
    date_created AS activity_date
  FROM
    crm_data.opportunities.gold_opportunities_fact
  WHERE
    created_by_email IS NOT NULL
  UNION ALL
  SELECT
    last_modified_by_email AS email,
    CAST(last_modified_date_time AS DATE) AS activity_date
  FROM
    crm_data.opportunities.gold_opportunities_fact
  WHERE
    last_modified_by_email IS NOT NULL
)
SELECT
  email,
  MAX(activity_date) AS last_activity_date,
  DATEDIFF(CURRENT_DATE(), MAX(activity_date)) AS days_since_last_activity
FROM
  activity
GROUP BY
  email
ORDER BY
  days_since_last_activity DESC
```
