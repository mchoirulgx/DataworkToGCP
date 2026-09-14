# 04 · Case Study: House-Buying Analysis (end to end)

The sample pipeline to be migrated is here: https://www.alibabacloud.com/help/en/dataworks/user-guide/dataworks-for-application-development

> **Goal:** take a real, simple DataWorks pipeline and convert it to GCP,
> step by step, with every file involved. This mirrors Alibaba's own public
> tutorial — *"Develop and analyze data in DataWorks by building a house-buying
> group analysis from scratch"* — so you can reproduce the source on Alibaba
> Cloud if you want.

---

## 1. What we are migrating

### The business problem

A bank runs a marketing campaign for house-buying groups. Every day it loads
call-campaign data into a table, then asks: *"How many single people with a
mortgage are there, by education level?"* The answer feeds a Power BI report.

### The source pipeline (DataWorks / MaxCompute)

Two tables:

**`bank_data`** — raw business data, one row per contacted customer
(21 columns: `age`, `job`, `marital`, `education`, `default`, `housing`,
`loan`, `contact`, `month`, `day_of_week`, `duration`, `campaign`, `pdays`,
`previous`, `poutcome`, `emp_var_rate`, `cons_price_idx`, `cons_conf_idx`,
`euribor3m`, `nr_employed`, `y`). Loaded from an on-premises CSV via Data
Integration.

**`result_table`** — the analytics result (`education`, `num`).

One workflow **`house_buying_analysis`** with three nodes:

| Node | Type | What it does |
| --- | --- | --- |
| `workshop_start` | VIRTUAL (zero-load) | Start node; no code; clarifies the data flow |
| `ddl_result_table` | ODPS_SQL | `CREATE TABLE IF NOT EXISTS result_table (...)` |
| `insert_result_table` | ODPS_SQL | `INSERT OVERWRITE TABLE result_table SELECT ...` |

Dependencies: `workshop_start → ddl_result_table → insert_result_table`.
Schedule: **daily at 02:00** (`00 02 00 * * ?`), timezone `Asia/Jakarta`,
variable `${bizdate}`.

The SQL of `insert_result_table`:

```sql
INSERT OVERWRITE TABLE result_table
SELECT education
     , COUNT(marital) AS num
FROM bank_data
WHERE housing = 'yes'
  AND marital = 'single'
GROUP BY education;
```

> Full source tutorial: *DataWorks user guide → "Tutorial: Home buyer analysis"*
> — https://www.alibabacloud.com/help/en/dataworks/user-guide/dataworks-for-application-development

---

## 2. Step 1 — Extract (OpenAPI)

We never open the DataWorks UI. We run the extraction script with two clients:

```bash
cd code
uv run extract/extract_dataworks.py --name-filter house_buying
```

This issues (per workflow): `ListWorkflows` → `GetWorkflow` → `ListNodes` →
`GetNode` (all on the `2024-05-18` SDK), and `GetMetaTableColumn` /
`GetMetaTablePartition` (on the `2020-05-18` SDK) for each output table.

---

## 3. Step 2 — The extracted metadata (FlowSpec)

`GetNode` returns the node as **FlowSpec** — the same canonical schema Alibaba's
own Airflow→DataWorks tutorial produces. Here is the whole workflow expressed as
FlowSpec (see the real file at
[`code/extract/sample_flowspec_house_buying.json`](../code/extract/sample_flowspec_house_buying.json)):

```jsonc
{
  "kind": "CycleWorkflow",
  "version": "1.1.0",
  "spec": {
    "name": "house_buying_analysis",
    "workflows": [
      {
        "trigger": { "type": "Scheduler", "cron": "00 02 00 * * ?", "timezone": "Asia/Jakarta" },
        "variables": [ { "id": "v_bizdate", "name": "bizdate", "scope": "Workflow", "type": "System", "value": "${bizdate}" } ],
        "nodes": [
          { "id": "n_start",  "name": "workshop_start",
            "script": { "runtime": { "command": "VIRTUAL" } } },
          { "id": "n_ddl",    "name": "ddl_result_table",
            "script": { "runtime": { "command": "ODPS_SQL" },
                        "content": "CREATE TABLE IF NOT EXISTS result_table (...)" } },
          { "id": "n_insert", "name": "insert_result_table",
            "script": { "runtime": { "command": "ODPS_SQL" },
                        "content": "INSERT OVERWRITE TABLE result_table SELECT ..." } }
        ],
        "flow": [
          { "nodeId": "n_start",  "depends": [] },
          { "nodeId": "n_ddl",    "depends": [ { "nodeId": "n_start",  "type": "Normal" } ] },
          { "nodeId": "n_insert", "depends": [ { "nodeId": "n_ddl",    "type": "Normal" } ] }
        ]
      }
    ]
  }
}
```

**Read this carefully** — the `flow` section is what preserves execution order.
If you only looked at per-node inputs/outputs you would lose it.

---

## 4. Step 3 — Route every node

Run the classifier:

```bash
uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
```

| Node | `runtime.command` | GCP target | Why |
| --- | --- | --- | --- |
| `workshop_start` | `VIRTUAL` | **EmptyOperator** | No-op / flow control |
| `ddl_result_table` | `ODPS_SQL` | **Dataform `.sqlx`** | In-warehouse SQL |
| `insert_result_table` | `ODPS_SQL` | **Dataform `.sqlx`** | In-warehouse SQL |

**Key insight:** the two ODPS_SQL nodes collapse into **one** Dataform model.
`ddl_result_table` (CREATE TABLE) is absorbed by Dataform — the `.sqlx` model
*is* the DDL. `insert_result_table` (INSERT … SELECT … GROUP BY) becomes the
model's `SELECT` body.

---

## 5. Step 4 — Generate the GCP artifacts

### 5a. Dataform project

**`code/gcp/dataform/workflow_settings.yaml`**

```yaml
defaultProject: my-gcp-project
defaultLocation: asia-southeast2
defaultDataset: dwh
defaultAssertionDataset: dwh_assertions
```

> Placeholders like `my-gcp-project` are used throughout the code examples.
> In the repo, the DAG reads these from environment variables documented in
> `code/.env.example`; the Dataform files keep them literal (Dataform cannot
> read env vars), so `code/.env.example` is the single place to change them.

**`code/gcp/dataform/definitions/sources/bank_data.sqlx`** — a *declaration*:
tells Dataform `bank_data` already exists in BigQuery (it's loaded by a sync
pipeline), so other models can `ref()` it.

```sqlx
config {
  type: "declaration",
  tags: ["daily"],
  database: "my-gcp-project",
  schema: "dwh",
  name: "bank_data",
  description: "Raw house-buying campaign data (migrated from MaxCompute bank_data)."
}
```

**`code/gcp/dataform/definitions/marts/result_table.sqlx`** — the migration of
both ODPS_SQL nodes. Note how the ODPS SQL maps over:

```sqlx
config {
  type: "table",
  tags: ["daily"],
  description: "Education-level distribution of single people who have mortgages. Migrated from ddl_result_table + insert_result_table.",
  columns: {
    education: "Education level",
    num: "Number of persons"
  },
  assertions: {
    nonNull: ["education"],
    rowConditions: ["num > 0"]
  }
}

SELECT
  education,
  COUNT(marital) AS num
FROM ${ref("bank_data")}
WHERE housing = 'yes'
  AND marital = 'single'
GROUP BY education
```

**Translation notes (this is the high-effort 20%):**

| ODPS (source) | BigQuery/Dataform (target) | Comment |
| --- | --- | --- |
| `CREATE TABLE IF NOT EXISTS result_table` | `config { type: "table" }` | Dataform generates the DDL and `CREATE OR REPLACE`/`MERGE` for you |
| `INSERT OVERWRITE TABLE result_table` | the model `SELECT` | A "table" materialisation replaces the table each run |
| `FROM bank_data` | `FROM ${ref("bank_data")}` | `ref()` resolves the name **and** creates the dependency edge in Dataform's DAG |
| `WHERE housing = 'yes' AND marital = 'single'` | unchanged | String comparison is valid in both dialects |
| `COUNT(marital)` | `COUNT(marital)` | Direct |
| `${bizdate}` | Airflow macro (see below) | Business-date semantics differ — chapter 05 |

### 5b. Cloud Composer DAG

**`code/gcp/dags/house_buying_daily.py`** — the orchestration. Because Dataform
owns the intra-BigQuery dependency graph, the DAG only needs *three* tasks:
the parity start node, then compile → run.

```python
with DAG(
    dag_id="house_buying_daily",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["house-buying", "migrated-from-dataworks"],
) as dag:
    start = EmptyOperator(task_id="workshop_start")

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": "main"},
    )

    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {"included_tags": ["daily"]},
        },
    )

    start >> compile_dataform >> run_dataform
```

**Schedule mapping:** DataWorks `00 02 00 * * ?` (cron with seconds) →
Airflow `0 2 * * *` (no seconds field). Keep the **same timezone** as the
source (`Asia/Jakarta`).

**bizdate:** the workflow used `${bizdate}` (run date − 1). In Airflow, that's
`{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}` for a
`yyyymmdd` business date, or `{{ logical_date }}` if the pipeline never needs
the off-by-one (see chapter 05). Our case-study SQL doesn't reference `bizdate`,
so the DAG needs no macro — but flag it as a checklist item anyway.

---

## 6. Step 5 — Validate & reconcile

Before cut-over, prove parity:

| Check | Source (MaxCompute) | Target (BigQuery) | How |
| --- | --- | --- | --- |
| Per-table row counts | `SELECT COUNT(*) FROM result_table` | same query | must match |
| Per-table aggregates | `SELECT education, num FROM result_table ORDER BY education` | same | full compare (checksum) |
| Per-DAG behaviour | `workshop_start → ddl → insert`, 02:00 daily | `workshop_start → compile → run`, 02:00 daily | schedule + edges identical |
| Downstream consumption | Power BI reads `result_table` | point Power BI at `my-gcp-project.dwh.result_table` | swap connection after sign-off |

Run the generated pipeline **in parallel** with the DataWorks original until
they match, then cut over by domain wave.

---

## 7. The complete picture

```
On-prem CSV ──Data Integration──▶ MaxCompute.bank_data ──┐
                                                          │  workshop_start (VIRTUAL)
                                                          ▼
                                                    ddl_result_table (ODPS_SQL)
                                                          │
                                                          ▼
                                                    insert_result_table (ODPS_SQL)  ──▶ result_table ──▶ Power BI

  ================================= MIGRATION =================================

CSV/GCS ──Datastream/DTS/Dataflow──▶ BigQuery.dwh.bank_data ──┐
                                                               │  workshop_start (EmptyOperator)
                                                               ▼
                                                    Dataform: result_table.sqlx (ref bank_data)
                                                               │
                                                               ▼
                                                    BigQuery.dwh.result_table ──▶ Power BI

Daily 02:00 Asia/Jakarta  ·  Airflow: house_buying_daily (compile → run "daily" tag)
```

## 8. What we learned from this case

1. **Decomposition, not translation.** One UI workflow became: a sync pipeline
   (not shown, for the raw load) + one Dataform model + a 3-task Composer DAG.
2. **Dataform absorbs DDL.** The `CREATE TABLE` node disappears into the model.
3. **Don't duplicate the dependency graph.** Two SQL nodes → one Dataform tag,
   *not* two Airflow tasks with an edge between them.
4. **Schedule semantics matter.** Seconds-in-cron and timezone must be handled
   explicitly; `bizdate` is a landmine even when the current SQL ignores it.

---

**Next:** [05 · FlowSpec JSON reference](05-flowspec-json-reference.md)
