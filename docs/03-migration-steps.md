# 03 · Migration Steps (the Playbook)

> **Goal:** a single, repeatable, metadata-driven flow you can follow end to
> end — from raw DataWorks metadata to a reconciled GCP pipeline.

---

## The flow at a glance

```
DataWorks (source)
   │  (1) OpenAPI extraction  ── ListWorkflows → GetWorkflow → ListNodes → GetNode
   ▼
Structured metadata  ── (2) FlowSpec + DDL, checkpointed to MySQL
   │  (3) AI-assisted generation ── Gemini + routing matrix
   ▼
GCP artifacts  ── Composer DAGs · Dataform .sqlx · Python/Dataflow · Datastream
   │  (4) Human review
   ▼
Reconciliation  ── (5) per-table + per-DAG parity gates
   ▼
(6) Cut over by domain wave
```

---

## Step 0 · Preparation

1. **Credentials.** Create a RAM user with `dataworks:*` read access and an
   AccessKey. Google: a Composer service account with `roles/bigquery.dataEditor`
   and `roles/dataform.editor`.
2. **Workspace inventory.** Confirm the **DataWorks workspace ID**, the
   environment (`Prod`/`Dev`), and the region (e.g. `ap-southeast-5` Jakarta).
3. **Tooling.** Install `uv`, clone/copy this repository, then:

```bash
cd code
uv sync
cp .env.example .env      # fill in your credentials
```

---

## Step 1 · Extract metadata via the OpenAPI (≈10% of the effort)

Run the extraction script (two SDK clients, paginated, checkpointed):

```bash
uv run extract/extract_dataworks.py --name-filter house_buying   # try a small slice first
uv run extract/extract_dataworks.py                              # then the whole estate
```

What it does, top-down:

| # | Call | What you get |
| --- | --- | --- |
| 1 | `ListWorkflows` (paged) | all master pipelines |
| 2 | `GetWorkflow` | DAG-level config: schedule, trigger, variables, cross-DAG deps |
| 3 | `ListNodes` filtered by workflow (paged) | the nested tasks |
| 4 | `GetNode` | the FlowSpec: code, command type, inputs/outputs, `flow` dependency graph |
| 5 | `GetMetaTableColumn` / `GetMetaTablePartition` (`2020-05-18` client) | DDL for output tables |

**Resilience rules (bake these in):**

- **Static pacing.** `time.sleep` between `GetNode` calls to stay under the QPS
  limit (≈0.5 s).
- **Exponential backoff.** On `Throttling.User` / `Throttling.API`, back off
  `2s → 4s → 8s…` and retry the same ID.
- **Quota increase.** For a large estate, raise a support ticket to temporarily
  lift the `dataworks-public` QPS limit during the migration window.
- **Checkpoint & resume.** Append each extracted node to a MySQL (or CSV) store
  the moment it's processed — never hold thousands of scripts in RAM. On
  startup, read the store and skip already-extracted IDs so the run is
  idempotent and crash-safe (across pages, too).

---

## Step 2 · Structure the metadata

The extraction output — node code, command type, dependency graph, and DDL per
table — is exactly the context handed to the AI generator. Keep it:

```
data/extract/
├── node_<id>.json              # one FlowSpec per node
└── workflow_<id>.json

# resumable checkpoint -> MySQL (MYSQL_* env vars, see code/.env.example)
```

Sanity-check it with the routing classifier:

```bash
uv run classify/route_nodes.py --extract-dir ../data/extract --csv-out ../data/routing.csv
```

---

## Step 3 · Generate GCP artifacts (AI-assisted)

Feed the structured metadata to **Gemini** (Google's AI model) as the only AI
assistant for this step. Give the agent three things:

1. the extracted **FlowSpec**,
2. the **inverted typeMapping** / routing matrix (chapter 02),
3. the **scheduling & parameter rules** (chapter 05 — `bizdate`, self-dependency).

It emits, per workflow:

- one **Airflow DAG** (`.py`) — schedule, task dependencies, cross-cycle →
  `depends_on_past`, cross-workflow → `ExternalTaskSensor`;
- **Dataform `.sqlx`** for every `ODPS_SQL` node (ODPS→GoogleSQL already translated);
- **Python/Dataflow** for PyODPS.

**Guardrails.** First draft, not ground truth — treat generated DAGs, SQL and
Python as a starting point that must pass human review and reconciliation,
especially ODPS→BigQuery SQL. Keep production logic inside approved
data-governance boundaries; don't paste it into unmanaged tools.

---

## Step 4 · Human review

Engineers check each generated artifact against the source logic and the routing
rules:

- **SQL translation.** Expect ~60–80% clean auto-translation via the HiveQL
  proxy (see §Step 5). It breaks on ODPS-specific built-ins, UDFs, and semantic
  differences — budget a manual last-mile plus per-query validation.
- **Python routing.** Confirm every PyODPS node flagged `TRIAGE` is routed
  correctly (SQL push-down vs PythonOperator vs Dataflow).
- **Scheduling.** Verify every schedule, timezone, and variable translation.

---

## Step 5 · Translate SQL & semantics (the hard 20%)

### ODPS → BigQuery SQL

BigQuery Migration Service has **no native MaxCompute translator** (its dialects
are Teradata, Redshift, HiveQL, and similar). The practical route: translate
`ODPS_SQL` via the **HiveQL source dialect as a proxy** (MaxCompute SQL is
Hive-derived), then validate.

- Operator mapping is not automatic: you are building the reverse map
  (`ODPS_SQL → Dataform / BigQuery operators`).
- PyODPS that is really SQL should be **pushed down to BigQuery SQL**, not
  reimplemented in Python.

### Scheduling & parameters

A silent source of estate-wide bugs:

- **The `bizdate` landmine.** In DataWorks, `bizdate` is the business date —
  normally the run date minus one day. Airflow's `logical_date` /
  `data_interval` semantics differ. A naive copy produces off-by-one-day errors
  across every migrated pipeline. Translate deliberately (see chapter 05).
- **Self / cross-cycle dependency** → `depends_on_past=True` or a previous-run
  `ExternalTaskSensor`.
- **Resource groups** have no direct Composer equivalent — size the Composer
  worker pool separately; don't try to map them 1:1.

### Schema & data types

- **Type collapse.** MaxCompute `TINYINT/SMALLINT/INT/BIGINT` all collapse to
  BigQuery `INT64`; `DATETIME` (ms) vs `TIMESTAMP/DATETIME` (µs) is a deliberate
  choice; `DECIMAL` precision/scale, `BINARY→BYTES`, and `MAP/ARRAY/STRUCT` all
  need explicit rules (chapter 05).
- **Partition-model mismatch (the big one).** MaxCompute *string* partitions
  such as `pt/ds='20240101'` do **not** map onto BigQuery's
  `DATE/TIMESTAMP/INT` partitioning. Many become clustering keys or require
  partition redesign — this is design work, not a lookup.

---

## Step 6 · Validate, reconcile, cut over

Mandatory in a regulated environment, and the step that **proves** the
migration. The plan cannot end at "generate the code" — particularly when a
share of that code is AI-generated.

- **Per-table parity:** row counts, checksums, and aggregate comparisons between
  MaxCompute and BigQuery for **every** migrated table.
- **Per-DAG parity:** same schedule, same dependency graph, and same outputs as
  the source workflow before cut-over.
- **Gate each domain wave on reconciliation sign-off;** run generated pipelines
  **in parallel** with the DataWorks originals until they match.

---

## Recommended end-to-end checklist

- [ ] Extraction (`2024-05-18`): paged `ListWorkflows → GetWorkflow → paged ListNodes → GetNode`; persist FlowSpec + flow graph with checkpointing.
- [ ] DDL (`2020-05-18`): `GetMetaTableColumn / GetMetaTablePartition` for output tables.
- [ ] Classify each node via the routing matrix; **flag large PyODPS for human triage**.
- [ ] Generate (AI-assisted): Dataform `.sqlx`, Composer DAGs, Python/Dataflow; translate ODPS_SQL via BQMS (HiveQL) with manual last-mile.
- [ ] Review: engineers check generated artifacts against source logic and routing rules.
- [ ] Validate: per-table and per-DAG reconciliation; parallel-run; sign off; cut over by domain wave.

---

**Next:** [04 · Case study: house-buying analysis](04-case-study-house-buying.md)
