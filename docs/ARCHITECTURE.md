# DQX — CRM Opportunities Pipeline

Data quality monitoring for the CRM opportunities pipeline (`crm_data.opportunities`), built with `[databricks-labs-dqx](https://github.com/databrickslabs/dqx)` pinned at `0.15.0`. This README documents the implementation delivered as part of a larger internal epic, and follows the structure of an internal data-quality documentation standard.

## What DQX is, and why it's used here

DQX validates data that has already been written to a table — it runs **at-rest**, downstream of the ingestion/model job, reading each table, evaluating a set of checks, and writing structured findings for monitoring and drill-down. It never gates a write; it observes.

## Scope: two layers


| Layer      | What it checks                                                                                                 | Mechanism                                                                                                                         | Output                                                |
| ---------- | -------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| **Bronze** | Ingestion freshness — is each bronze table being refreshed on schedule?                                        | Custom monitor (no YAML, no `DQEngine`): `hours_since = run_ts − MAX(ingestion_timestamp)`, breach when `hours_since > threshold` | `finding` rows only, always `warn`                    |
| **Gold**   | Full rule-based validation — nulls, ranges, uniqueness, referential integrity, casing, cross-table consistency | YAML rule catalog (`dqx_crm_gold_checks.yml`) applied via `DQEngine.apply_checks_by_metadata`                               | `finding` and `quarantine` rows, mixed `error`/`warn` |


Bronze thresholds differ by load type: **25h** for the 15 full-overwrite tables (a tight threshold — `MAX(ingestion_timestamp)` tracks the last pipeline run directly), **96h** for the 2 incremental-merge tables (`bronze_opportunitiesview`, `bronze_opportunitystagehistoryview` — looser, since a quiet source shouldn't false-alarm a healthy pipeline).

Gold runs **211 checks** cross all **23 gold tables** in `crm_data.opportunities`.

## Findings and quarantine table schema

Both layers write to a shared 13-column custom DDL (`dqx_crm_bronze`, `dqx_crm_gold`).


| Column            | Type               | Record type    | Notes                                                                                                                                                                                                                                                                                              |
| ----------------- | ------------------ | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `run_id`          | STRING NOT NULL    | both           | UUID per run                                                                                                                                                                                                                                                                                       |
| `run_ts`          | TIMESTAMP NOT NULL | both           | Run start, UTC                                                                                                                                                                                                                                                                                     |
| `pipeline`        | STRING NOT NULL    | both           | `crm`                                                                                                                                                                                                                                                                                        |
| `layer`           | STRING NOT NULL    | both           | `gold` or `bronze`                                                                                                                                                                                                                                                                                 |
| `table_name`      | STRING NOT NULL    | both           | e.g. `gold_opportunities_fact`                                                                                                                                                                                                                                                                     |
| `check_name`      | STRING NOT NULL    | both           | Rule name (gold) / `ingestion_freshness` (bronze)                                                                                                                                                                                                                                                  |
| `severity`        | STRING NOT NULL    | both           | `error` or `warn`                                                                                                                                                                                                                                                                                  |
| `record_type`     | STRING NOT NULL    | both           | `finding` or `quarantine`                                                                                                                                                                                                                                                                          |
| `total_row_count` | BIGINT             | finding        | NULL on quarantine rows                                                                                                                                                                                                                                                                            |
| `fail_count`      | BIGINT             | finding        | NULL on quarantine rows                                                                                                                                                                                                                                                                            |
| `fail_rate`       | DOUBLE             | finding        | `fail_count / total_row_count`                                                                                                                                                                                                                                                                     |
| `message`         | STRING             | **quarantine** | Per-record failure message. Finding rows carry `NULL` here — a `message` is a per-record concept (some DQX built-ins embed the specific failing value), and a finding row aggregates many records, so there's no single message to roll up. Drill into that check's quarantine rows for the "why." |
| `row_data`        | STRING             | quarantine     | Failing row, serialized as JSON; NULL on finding rows                                                                                                                                                                                                                                              |


`finding` rows are written **unconditionally** — one row per check per table per run, including a clean pass (`fail_count = 0`). This is never conditional on failure.

### Write mode: current-state overwrite

Both findings tables use `mode("overwrite")` — each holds only the latest run's state, no history. This keeps the implementation simple; the tradeoff is that no trend-over-time query is possible against these tables today. Overwriting does **not** drop the tables' Unity Catalog grants, owner, or tags, since those are set once at table setup (gated by `initial_full_load`) and the securable itself is never dropped.

### No `PRIMARY KEY`

Internal Databricks job standards require all silver and gold tables to declare a `PRIMARY KEY`. This table declares none, deliberately: only 8 of the 13 columns are `NOT NULL`-eligible, and the best composite candidate (`run_id, table_name, check_name, record_type`) is unique for `finding` rows but **not** for `quarantine` rows — multiple failing records on the same check/table/run collapse to an identical key, distinguishable only by the nullable `row_data`. Declaring it anyway would document a false grain for quarantine rows.

## Environment separation — the `__PREFIX__` token

Every environment-specific catalog reference in the gold YAML is written with a `__PREFIX__` token:

```yaml
ref_table: __PREFIX__crm_data.opportunities.gold_opportunities_fact
# ... FROM __PREFIX__enterprise.master_data.gold_market ...
```

At load time, a generic recursive substitution replaces `__PREFIX__` with the runtime prefix (`dev_` in dev, empty string in prod), derived from the `environment` widget — never passed as its own job parameter. `__PREFIX__` is used instead of an f-string-style `{prefix}` because a leading `{` starts a YAML flow-mapping and breaks plain scalars. Because dev and prod findings land in separate catalogs, the findings table itself needs no `environment` column — the catalog prefix is the isolation.

## Check-to-table grouping: `table_name` tag, single source of truth

Every check in the YAML carries a `table_name` key, popped out of the check dict at runtime (after prefix substitution, before `validate_checks`) to build the table→checks grouping. The notebook asserts the derived table set matches an `EXPECTED_TABLES` constant, so a mistyped tag fails loudly instead of silently dropping a check.

## Cross-table check patterns

DQX evaluates one DataFrame at a time. Three patterns cover every cross-table need in this implementation:

1. **Referential integrity —** `foreign_key`**.** 14 checks across all 14 dimension tables, each with an environment-prefixed `ref_table`. DQX runs the anti-join internally; the checked table's DataFrame is passed directly.
2. **Value membership —** `sql_expression` **subquery.** 6 master-data checks, e.g. `... IN (SELECT term_name FROM __PREFIX__enterprise.master_data.gold_<type> WHERE master_data_type = '<Type>' AND is_active = 'Active')`. Several of these (delivery method, contract type, self-perform discipline) map raw CRM shorthand values to governed terms with a `CASE WHEN` before the membership test.
3. **Pre-join for a column needed in the check body —** `gold_gng_scd`**.** `gold_gng_scd` has no `stage_number` column, but its two stage-dependent Go/No-Go checks need one. `stage_number` is joined in from `gold_opportunities_fact` in memory before `apply_checks_by_metadata` — nothing is written, no new catalog object. The join itself is **not** filtered to `current_flag = TRUE`; instead, the `current_flag = TRUE` condition lives inside each of the two checks' own YAML `filter:` field. 

## Performance: single aggregation pass, distributed quarantine construction

Per-table fail counts are computed in **one** `.agg()` call per table (one `F.sum(F.when(...))` expression per check, keyed by check **name**, not list position — positional keying was considered and rejected as an auditability risk). Quarantine rows are built as a **distributed DataFrame** (`F.transform`/`F.concat`/`F.explode` over tagged errors and warnings), never `.collect()`-ed to the driver — serverless compute doesn't support `.cache()`/`.persist()`, so avoiding repeated recomputation and any driver-side collection of the full failing-row set were both required for this to scale safely.



## Reference implementation

- `[dqx_candidate_rule_generation.py](../notebooks/dqx_candidate_rule_generation.py)` — AI-assisted profiler that drafts rule candidates for SME review
- `[dqx_crm_gold_checks.yml](../notebooks/dqx_crm_gold_checks.yml)` — the gold rule catalog
- `[dqx_crm_gold.py](../notebooks/dqx_crm_gold.py)` — gold at-rest execution notebook
- `[dqx_crm_bronze.py](../notebooks/dqx_crm_bronze.py)` — bronze ingestion-freshness monitor

Candidate rules go through a review workbook (one sheet per gold table: Check Number, Column, Rule, Criticality, Business Reason, Status, Notes) before promotion into the YAML catalog above — omitted from this repo since it carries reviewer discussion, not implementation.



## Known follow-ups

- `has_self_perform` **range/type check** — still pending SME confirmation; currently just an `is_not_null` check in the YAML.
- `gold_opportunities_fact` **completeness gap** — confirmed during later dashboard work: the fact table was missing roughly 39% of its true record population, verified independently against two dimension tables that agreed with each other on the real count. The missing records were confirmed as real, bronze-layer-present records across six different dimension tables — this is a completeness bug in how `gold_opportunities_fact` is built, not bad or fabricated data. **This needs its own tracking ticket.** It's the reason a large share of gold quarantine rows can't currently resolve an owner via `created_by_email`/`last_modified_by_email`.
- Master Data Checks - 100% Failing.
- **Weekly digest notification** — in progress, currently held pending two external dependencies: the BU/BD leader static reference table (`crm_source.reference.static_bu_bd_leaders`, not yet uploaded) and confirmation of the Power Automate webhook for email delivery.

## Related
- Standard: internal data-quality documentation standard

