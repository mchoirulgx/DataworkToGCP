# 03 · Migration playbook

> **Last reviewed:** 2026-09-22

> **Goal:** a single, repeatable, metadata-driven flow you can follow end to
> end — from raw DataWorks metadata to a reconciled GCP pipeline.

---

## Contents

- [The canonical six steps](#the-canonical-six-steps)
- [The flow at a glance](#the-flow-at-a-glance)
- [Step 0 · Preparation (prerequisite)](#step-0--preparation-prerequisite)
- [Step 1 · Extract metadata via the OpenAPI](#step-1--extract-metadata-via-the-openapi)
- [Step 2 · Structure and review the metadata](#step-2--structure-and-review-the-metadata)
- [Step 3 · Generate GCP artifacts](#step-3--generate-gcp-artifacts)
- [Step 4 · Human review](#step-4--human-review)
- [Step 5 · Translate SQL & semantics](#step-5--translate-sql--semantics)
- [Step 6 · Validate, reconcile, cut over](#step-6--validate-reconcile-cut-over)
- [Recommended end-to-end checklist](#recommended-end-to-end-checklist)

---

## The canonical six steps

This document is the single owner of the migration steps. **The playbook is six
steps**, preceded by a one-off **Step 0 preparation** prerequisite that is not
counted as a migration step. Every other document refers to this list; the H2
headings below match it exactly.

| # | Step | Outcome |
| --- | --- | --- |
| 0 | Preparation *(prerequisite)* | credentials, workspace inventory, tooling installed |
| 1 | Extract metadata via the OpenAPI | FlowSpec + DDL on disk, checkpointed |
| 2 | Structure and review the metadata | structured tree + reviewed `migration_matrix.csv` |
| 3 | Generate GCP artifacts | DAGs, `.sqlx`, Python/Dataflow, plus `review/*.md` |
| 4 | Human review | every generated artifact and flagged item signed off |
| 5 | Translate SQL & semantics | ODPS→GoogleSQL, schedules, types resolved |
| 6 | Validate, reconcile, cut over | parity proven, wave cut over |

---

## The flow at a glance

```
DataWorks (source)
   │  (1) OpenAPI extraction  ── ListWorkflows → GetWorkflow → ListNodes → GetNode
   ▼
Structured metadata  ── (2) FlowSpec + DDL, checkpointed to MySQL
   │                       + migration_matrix.csv reviewed by a human
   │  (3) Rule-based generation ── routing matrix + CSV overrides
   ▼
GCP artifacts  ── Composer DAGs · Dataform .sqlx · Python/Dataflow · Datastream
   │  (4) Human review (incl. review/*.md)
   ▼
Translated SQL & semantics  ── (5) ODPS→GoogleSQL, bizdate, types, partitions
   ▼
Reconciliation  ── (6) per-table + per-DAG parity gates, then cut over by domain wave
```

---

## Step 0 · Preparation (prerequisite)

1. **Credentials.** Create a RAM user with `dataworks:*` read access and an
   AccessKey. Google: a Composer service account with `roles/bigquery.dataEditor`
   and `roles/dataform.editor`.
2. **Workspace inventory.** Confirm the **DataWorks workspace ID**, the
   environment (`Prod`/`Dev`), and the region (e.g. `ap-southeast-5` Jakarta).
3. **Tooling.** Install `uv`, clone/copy this repository, sync the environment
   and fill in `.env`. Don't repeat the commands here — setup is owned by
   [`code/README.md`](../code/README.md). Every command in this playbook is run
   from the `code/` directory, which is why data paths look like `../data/...`.

---

## Step 1 · Extract metadata via the OpenAPI

This is roughly **10% of the total effort**. Run the extraction script from
`code/` (two SDK clients, paginated, checkpointed):

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

## Step 2 · Structure and review the metadata

The extraction output — node code, command type, dependency graph, and DDL per
table — is exactly the input the generator consumes. Keep it:

```
data/extract/
├── node_<id>.json              # one FlowSpec per node
├── workflow_<id>.json
└── migration_matrix.csv        # auto-generated after extraction

# resumable checkpoint -> MySQL (MYSQL_* env vars, see code/.env.example)
```

Sanity-check it with the routing classifier (from `code/`):

```bash
uv run classify/route_nodes.py --extract-dir ../data/extract --csv-out ../data/routing.csv
```

### Review and override `migration_matrix.csv`

Extraction auto-generates **`migration_matrix.csv`** alongside the JSON. This is
the **human review / override layer**: one row per node, with the routing target
the rules picked, review notes, and a `skip` flag. Edit it before you generate —
the generator picks it up automatically and your decisions win over the defaults.
Column-by-column reference:
[reference/migration-matrix-csv.md](reference/migration-matrix-csv.md).

---

## Step 3 · Generate GCP artifacts

### Automated route (recommended)

The shipped toolkit does this step for you. From the `code/` directory:

```bash
uv run migrate/migrate.py all --flowspec ../data/extract/workflow_<id>.json
```

- The generator is **rule-based and deterministic** — same input, same output.
  No model call is involved in the conversion itself.
- `all` runs four gates in order: **generate → compile → run → verify**. Each
  gate stops the pipeline on failure. (Don't re-read the gate details here —
  [05 · Tools architecture](05-tools-architecture.md) owns that table.)
- Anything the rules can't convert is **flagged, not fatal**: `TRIAGE` and
  unconvertible items are written to `review/*.md` and the pipeline keeps going.
- **Gemini is an optional assist for those flagged items only.** It is not part
  of the pipeline, and nothing in the generated output depends on it.

Full CLI usage, flags and environment setup: [`code/README.md`](../code/README.md).

### What it emits, per workflow

- one **Airflow DAG** (`.py`) — schedule, task dependencies, cross-cycle →
  `depends_on_past`, cross-workflow → `ExternalTaskSensor`;
- **Dataform `.sqlx`** for every `ODPS_SQL` node (ODPS→GoogleSQL already translated);
- **Python/Dataflow** for PyODPS.

### Inputs the generator relies on

1. the extracted **FlowSpec**,
2. the **inverted typeMapping** / routing matrix
   ([02 · Architecture & routing](02-architecture-and-routing.md#3-the-routing-matrix-node-type--gcp-target)),
3. the **scheduling & parameter rules**
   ([reference/variables-and-macros.md](reference/variables-and-macros.md) —
   `bizdate`, self-dependency),
4. any human overrides from `migration_matrix.csv` (Step 2).

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

## Step 5 · Translate SQL & semantics

This is the hard 20%.

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

- **Dates, `bizdate`, cron and macros — warning:** a naive copy of `bizdate`,
  Quartz cron fields or date macros produces off-by-one-day errors across every
  migrated pipeline; translate them from
  [reference/variables-and-macros.md](reference/variables-and-macros.md), not by eye.
- **Self / cross-cycle dependency** → `depends_on_past=True` or a previous-run
  `ExternalTaskSensor`.
- **Resource groups** have no direct Composer equivalent — size the Composer
  worker pool separately; don't try to map them 1:1.

### Schema & data types

> **Warning.** MaxCompute types do not map 1:1 onto BigQuery — the integer family
> collapses, `DATETIME`/`TIMESTAMP` precision differs, and string partitions such
> as `pt/ds='20240101'` do **not** map onto BigQuery `DATE/TIMESTAMP/INT`
> partitioning. Treat partitioning as design work, not a lookup, and take every
> concrete rule from [reference/type-mapping.md](reference/type-mapping.md).

---

## Step 6 · Validate, reconcile, cut over

Mandatory in a regulated environment, and the step that **proves** the
migration. The plan cannot end at "generate the code" — particularly when that
code was machine-generated.

- **Per-table parity:** row counts, MD5 checksums, and per-column numeric
  aggregates compared between MaxCompute and BigQuery for **every** migrated
  table.
- **Per-DAG parity:** same schedule, same dependency graph, and same outputs as
  the source workflow before cut-over.
- **Gate each domain wave on reconciliation sign-off;** run generated pipelines
  **in parallel** with the DataWorks originals until they match.

The `verify` gate of the shipped toolkit automates these checks — see
[05 · Tools architecture](05-tools-architecture.md) for exactly what it compares.

---

## Recommended end-to-end checklist

- [ ] Extraction (`2024-05-18`): paged `ListWorkflows → GetWorkflow → paged ListNodes → GetNode`; persist FlowSpec + flow graph with checkpointing.
- [ ] DDL (`2020-05-18`): `GetMetaTableColumn / GetMetaTablePartition` for output tables.
- [ ] Review / override `migration_matrix.csv` (auto-generated after extraction) before generating anything.
- [ ] Classify each node via the routing matrix; **flag large PyODPS for human triage**.
- [ ] Generate with the shipped toolkit (`uv run migrate/migrate.py all …`, rule-based and deterministic): Dataform `.sqlx`, Composer DAGs, Python/Dataflow; translate ODPS_SQL via BQMS (HiveQL) with manual last-mile.
- [ ] Review: engineers check generated artifacts against source logic and routing rules, and clear every item in `review/*.md`.
- [ ] Validate: per-table and per-DAG reconciliation; parallel-run; sign off; cut over by domain wave.

---

[← Prev: 02 · Architecture & routing](02-architecture-and-routing.md) · [Index](README.md) · [Next: 04 · Case study →](04-case-study-house-buying.md)
