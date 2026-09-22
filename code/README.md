# Code · DataWorks → GCP migration toolkit

> **Last reviewed:** 2026-09-22

> [!IMPORTANT]
> **Every command in this file is run from the `code/` directory.** All data
> paths are relative to it (`../data/extract`, `../data/generated`, …), so
> `cd code` once and stay there.

Run everything with [`uv`](https://docs.astral.sh/uv/) (Python project manager).

```bash
cd code
uv sync                # installs the SDKs listed in pyproject.toml
cp .env.example .env   # then fill in your credentials (every variable is documented in .env.example)
```

## Contents

- [1. Extract — `extract/extract_dataworks.py`](#1-extract--extractextract_dataworkspy)
- [2. Review — `classify/flowspec_to_csv.py`](#2-review--classifyflowspec_to_csvpy)
- [3. Route — `classify/route_nodes.py`](#3-route--classifyroute_nodespy)
- [4. Target artifacts — `gcp/`](#4-target-artifacts--gcp)
- [5. Automatic migration — `migrate/`](#5-automatic-migration--migrate)
- [Notes](#notes)

## 1. Extract — `extract/extract_dataworks.py`

Pulls every workflow + node (FlowSpec) + table DDL from DataWorks via the
OpenAPI. Uses **two SDK clients** (2024-05-18 orchestration, 2020-05-18 metadata),
page-based pagination, static pacing + exponential backoff, and a **MySQL**
checkpoint so runs are resumable.

```bash
uv run extract/extract_dataworks.py --name-filter house_buying
uv run extract/extract_dataworks.py --project-id 12345 --env Prod
```

Output lands in `../data/extract/` (default): `node_<id>.json` per node plus a
`workflow_<id>.json` per workflow carrying the schedule trigger and the
dependency graph rebuilt from each node's `inputs.nodeOutputs`. The resumable
checkpoint lives in the MySQL database configured via the `MYSQL_*`
env vars (create it first, e.g. `CREATE DATABASE dataworks_extract CHARACTER SET
utf8mb4;`). Reruns are **resumable**: workflows/nodes/tables already in the
checkpoint are skipped (`INSERT IGNORE`, append-only) and their output files are
regenerated from MySQL, so a crashed run only extracts what is still missing and
never re-pays the OpenAPI cost for completed work. A pre-made, illustrative
FlowSpec for the case study lives at
`extract/sample_flowspec_house_buying.json` so you can try the next steps
without Alibaba credentials. A richer multi-node sample covering PYODPS,
DIDE_SHELL, branches, and multiple variables lives at
`extract/sample_flowspec_complex_ecommerce.json` (its review items and
expected gate failure are documented in the
[migration toolkit test report](../docs/testing/reports/2026-08-19-migration-toolkit.md)).

## 2. Review — `classify/flowspec_to_csv.py`

Turns the extracted FlowSpec JSON into a human-editable
**`migration_matrix.csv`** (one row per node: workflow, node id/name, command,
input/output tables, schedule, `gcp_target`, `review_notes`, `skip`).

This step **runs automatically after extraction** — in the normal flow you never
invoke it by hand. The JSON stays the structural source of truth (code, flow
graph, DDL); the CSV is the **human override layer**. Edit it to redirect a
routing decision (`gcp_target`), adjust a `schedule`, add `review_notes`, or set
`skip` to drop a node, and `migrate/migrate.py generate` picks the overrides up
— it auto-detects `migration_matrix.csv` next to the FlowSpec or in the extract
dir, or you can pass one explicitly with `--csv`.

Regenerate it manually:

```bash
uv run classify/flowspec_to_csv.py --flowspec extract/sample_flowspec_house_buying.json
uv run classify/flowspec_to_csv.py --extract-dir ../data/extract --csv-out ../data/extract/migration_matrix.csv
```

Without `--csv-out` the file is written next to the input, i.e.
`<input-dir>/migration_matrix.csv`. Full column-by-column semantics:
[migration matrix CSV reference](../docs/reference/migration-matrix-csv.md).

## 3. Route — `classify/route_nodes.py`

Applies the [routing matrix](../docs/02-architecture-and-routing.md) to each
extracted node and prints where it should live on GCP. PyODPS nodes are flagged
for human triage.

```bash
uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
uv run classify/route_nodes.py --extract-dir ../data/extract --csv-out ../data/routing.csv
```

Expected output for the case study:

```
SOURCE                                  NODE                   COMMAND   GCP TARGET
sample_flowspec_house_buying.json       workshop_start         VIRTUAL   EmptyOperator
sample_flowspec_house_buying.json       ddl_result_table       ODPS_SQL  Dataform .sqlx
sample_flowspec_house_buying.json       insert_result_table    ODPS_SQL  Dataform .sqlx
```

## 4. Target artifacts — `gcp/`

Hand-written equivalents that the AI generator (Gemini) would
produce and an engineer would review:

| File | Purpose |
| --- | --- |
| `gcp/dags/house_buying_daily.py` | Cloud Composer (Airflow) DAG: daily 02:00, compiles + runs the Dataform "daily" tag. Project/region/repo/branch/schedule read from env vars (see `.env.example`). |
| `gcp/dataform/workflow_settings.yaml` | Dataform defaults (project, dataset, location). |
| `gcp/dataform/dataform.json` | Legacy Dataform v2 config — **see the warning below before running the Dataform CLI.** |
| `gcp/dataform/definitions/sources/bank_data.sqlx` | Declaration for the already-loaded source table. |
| `gcp/dataform/definitions/marts/result_table.sqlx` | The migrated transformation (`INSERT OVERWRITE ... GROUP BY` → Dataform table). |

> [!WARNING]
> **`gcp/dataform/dataform.json` is a legacy Dataform v2 config and breaks
> Dataform v3.** Dataform v3 cannot read a project that contains *both*
> `dataform.json` and `workflow_settings.yaml` — `dataform compile` fails.
> This repo still ships both files, so you must **delete `dataform.json`**
> before running the Dataform CLI against `gcp/dataform/`. The recorded
> end-to-end test had to do exactly that to get `dataform compile` to pass.

## 5. Automatic migration — `migrate/`

Rule-based generator + gates that run the migration with **minimal human
review**. One command:

```bash
uv run migrate/migrate.py all --flowspec extract/sample_flowspec_house_buying.json
uv run migrate/migrate.py all --extract-dir ../data/extract   # real extractor output
uv run migrate/migrate.py all --extract-dir ../data/extract \
  --workflows extract/sample_workflows_selector.json          # migrate only some
```

With `--extract-dir`, `migrate/reconstruct.py` rebuilds one workflow-level
FlowSpec per workflow (dependencies from `inputs.nodeOutputs`, schedule from
`workflow_<id>.json` when present, else a fallback cron flagged for re-verify at
cutover) instead of requiring a hand-authored FlowSpec. Duplicate writers of the
same output table are auto-deduped (first writer deployed, duplicates flagged in
`review/*.md`).

**Selecting which workflows to migrate.** Without a selector, *every* workflow
in the extract dir is migrated. Add `--workflows <file.json>` to migrate only the
ones you list — entries match a workflow's `spec.name` or `spec.id` (exact).
Entries that match nothing are reported on stderr, and an empty selection stops
the run. Two accepted formats:

```json
// 1) array of names / ids
["house_buying_analysis", "463497880880954XXXX"]

// 2) object with a "workflows" key (names, ids, or {name, id} entries)
{ "workflows": [ "house_buying_analysis", { "name": "daily_sales_report" } ] }
```

`extract` is an **optional preparatory step**, not a gate: `migrate/migrate.py
extract` shells out to `extract/extract_dataworks.py` (needs Alibaba creds). You
can skip it entirely and pass `--flowspec`/`--extract-dir` instead.

The pipeline itself has **four gates** — `generate → compile → run → verify` —
and each gate **stops the pipeline on failure**:

| Gate | What it does |
| --- | --- |
| `generate` | `migrate/generator.py` + `migrate/translator.py`: routes each node, turns `ODPS_SQL`/SQL-ish `PYODPS` into Dataform `.sqlx` models + declarations, emits a Composer DAG, and writes a `review/*.md` report **only** for items that need a human (TRIAGE nodes, unresolved vars, ODPS built-ins that differ in BigQuery, DDL-only tables). |
| `compile` | `dataform compile` on the generated project — fails the run on SQL that isn't valid GoogleSQL. |
| `run` | `dataform run` — creates tables and runs assertions in BigQuery. |
| `verify` | **Four checks, not just row counts:** (1) row count per generated table, (2) MD5 checksum of the table contents, (3) per-column numeric aggregates, and (4) DAG parity — the generated DAG's schedule and declared outputs against the source workflow. Any mismatch (including a missing/empty table) fails the gate. |

TRIAGE items are **not** a gate failure: they are written to `review/*.md` for a
human to pick up and the pipeline keeps going.

Generated project lands in `GENERATED_OUTPUT_DIR` (`../data/generated`, git-ignored):
`definitions/{marts,sources}/*.sqlx`, `workflow_settings.yaml`,
`.df-credentials.json` (gcloud ADC), `review/*.md`, plus the Airflow DAGs.

**Airflow 2 and 3 DAGs are both always written.** The generator emits a
version-specific variant into *both* `version_2/dags/<workflow>.py` and
`version_3/dags/<workflow>.py` on every run. The plain `dags/<workflow>.py`
folder is just a mirror of one of them: `AIRFLOW_MAJOR_VERSION` (`2` or `3`, see
`.env.example`) only picks *which* variant `dags/` mirrors — it never suppresses
the other.

Requires: GCP project + BigQuery (from `.env`: `GCP_PROJECT_ID`, `GCP_REGION`,
`DATAFORM_DEFAULT_SCHEMA`, …), the Dataform CLI
(`npm i -g @dataform/cli`, binary `dataform`, set `DATAFORM_CLI` and
`DATAFORM_CORE_VERSION`), and raw source tables already loaded into BigQuery.

**Known limits (still validated by the gates, not by a human):** ODPS SQL that
uses non-trivial built-ins (`DATEADD`, `SPLIT_PART`, UDFs, `EXPLODE`, …) is
*flagged* in `review/*.md`, not silently rewritten; schedules are mechanically
stripped of seconds and must be re-verified at cutover (see
[03 · Migration playbook](../docs/03-migration-playbook.md), step 5); parity
with live MaxCompute requires source access.

## Notes

- **All configuration lives in `.env.example`** (credentials, MySQL checkpoint,
  GCP/Dataform target). Copy it to `.env` and adjust. The only exception is the
  Dataform config files, which are committed literally because Dataform cannot
  read env vars — keep them in sync with `.env.example`.
- **Do not commit `.env`.** It holds cloud credentials.
- The Airflow DAG is written for Airflow ≥ 2.4 (`schedule=`, `EmptyOperator`)
  and the current `apache-airflow-providers-google` (Dataform operators).
- See the [documentation index](../docs/README.md) at the repository root for
  the full migration playbook.

---

[Docs index](../docs/README.md) · [Tools architecture](../docs/05-tools-architecture.md)
