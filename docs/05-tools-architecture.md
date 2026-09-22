# 05 · Tools architecture

> **Last reviewed:** 2026-09-22

> **Goal:** understand how every piece of this toolkit fits together — what each
> file does, how data flows between them, and where to look when something
> breaks or you want to extend the system.

> [!IMPORTANT]
> This chapter is the **canonical owner** of the pipeline **gate semantics**
> (§3, §4) and of the **Known limits** list (§7). Other chapters link here
> instead of restating them.

## Contents

- [1. The big picture](#1-the-big-picture)
- [2. File map](#2-file-map)
- [3. Component details](#3-component-details)
  - [3.1 Extract](#31-extract--extractextract_dataworkspy)
  - [3.2 Reconstruct](#32-reconstruct--migratereconstructpy)
  - [3.3 Classify + CSV override](#33-classify--classifyroute_nodespy--classifyflowspec_to_csvpy)
  - [3.4 Generate](#34-generate--migrategeneratorpy--migratetranslatorpy)
  - [3.5 Compile](#35-compile--dataform-compile-external-cli)
  - [3.6 Run](#36-run--dataform-run-external-cli)
  - [3.7 Verify](#37-verify--migrateverifierpy)
  - [3.8 Orchestrator](#38-orchestrator--migratemigratepy)
- [4. Data flow between components](#4-data-flow-between-components)
- [5. Configuration](#5-configuration)
- [6. How to extend the system](#6-how-to-extend-the-system)
- [7. Known limits (validated by the gates)](#7-known-limits-validated-by-the-gates)

---

## 1. The big picture

The toolkit is a **pipeline of eight stages**, each handled by a dedicated
module. Some stages are standalone scripts; others are called as libraries by
the orchestrator.

```
Stage 1  EXTRACT      extract_dataworks.py   Alibaba API -> JSON + MySQL
Stage 2  RECONSTRUCT  reconstruct.py         per-node JSON -> FlowSpec
Stage 3  CLASSIFY     route_nodes.py         standalone planning tool
Stage 4  CSV OVERRIDE flowspec_to_csv.py     FlowSpec -> migration_matrix.csv
                                             (human edits, read back at stage 5)
Stage 5  GENERATE     generator.py           FlowSpec + CSV -> .sqlx + DAGs
                        translator.py          ODPS SQL -> GoogleSQL
Stage 6  COMPILE      dataform CLI           validates GoogleSQL
Stage 7  RUN          dataform CLI           deploys to BigQuery
Stage 8  VERIFY       verifier.py            row count + checksum + aggregates
                                             + DAG parity gate
```

The orchestrator `migrate.py` chains stages 5-6-7-8 in a single command
(`migrate.py all`), and **stops the pipeline on failure** at every gate.
Stages 1-4 can be run independently.

```
Alibaba DataWorks API
        |
   [1] extract_dataworks.py
        |
        v
  data/extract/              (git-ignored)
  node_*.json, workflow_*.json
        |
   [2] reconstruct.py  (or passed as --flowspec)
        |
        v
  Workflow-level FlowSpec dicts (in memory)
        |
   [3] route_nodes.py        -- optional routing preview
   [4] flowspec_to_csv.py    -- migration_matrix.csv (human-editable
        |                       override layer, read back by the generator)
        v
   [5] generator.py + translator.py
        |
        v
  data/generated/            (git-ignored)
  definitions/marts/*.sqlx   -- Dataform models
  definitions/sources/*.sqlx -- Dataform declarations
  version_2/dags/*.py        -- Airflow 2 DAG variants (always written)
  version_3/dags/*.py        -- Airflow 3 DAG variants (always written)
  dags/*.py                  -- mirror of the AIRFLOW_MAJOR_VERSION variant
  review/*.md                -- TRIAGE items needing human review
                                (they do NOT stop the pipeline)
  workflow_settings.yaml     -- Dataform project config + variables
  .df-credentials.json       -- gcloud ADC credentials
        |
   [6] dataform compile     -- gate: valid GoogleSQL?
   [7] dataform run         -- gate: deploys to BigQuery
   [8] verifier.py          -- gate: row count, checksum, aggregates, DAG parity
        |
        v
  BigQuery tables ready
```

---

## 2. File map

All runnable code lives under `code/`. Here is every source file and what it
does.

```
code/
  pyproject.toml                      uv project -- SDK dependencies
  .env.example                        all configuration (credentials, GCP, MySQL)

  extract/
    extract_dataworks.py              stage 1: OpenAPI extraction
    sample_flowspec_house_buying.json   3-node case study (for testing)
    sample_flowspec_complex_ecommerce.json  10-node complex sample
    sample_migration_matrix.csv        committed example migration_matrix.csv
    sample_workflows_selector.json     example --workflows selector

  classify/
    route_nodes.py                    stage 3: standalone routing table
    flowspec_to_csv.py                stage 4: FlowSpec -> migration_matrix.csv

  migrate/
    migrate.py                        orchestrator CLI (stages 5-8)
    generator.py                      stage 5: .sqlx + DAG generation
    translator.py                     stage 5: ODPS -> GoogleSQL translation
    reconstruct.py                    stage 2: per-node -> workflow FlowSpec
    verifier.py                       stage 8: BigQuery + DAG parity gate

  gcp/
    dags/house_buying_daily.py        hand-written reference DAG
    dataform/                         hand-written reference Dataform project

  tests/
    conftest.py                       shared pytest fixtures
    test_migrate.py                   the test suite (`uv run pytest`)

  README.md                           code-level usage guide
```

---

## 3. Component details

### 3.1 Extract — `extract/extract_dataworks.py`

**What it does:** Connects to Alibaba Cloud DataWorks via two SDK clients
(orchestration API `2024-05-18` + metadata API `2020-05-18`), pulls every
workflow and node, and writes the results as JSON files. Uses MySQL as a
crash-resumable checkpoint so reruns skip work already done.

**Why two API versions:** DataWorks splits orchestration (workflows, nodes,
code) and metadata (table DDL, partitions) across different SDK packages.
Calling the wrong one silently returns errors — this is the number one trap
when working with the DataWorks API.

**How the MySQL checkpoint works:**

1. On startup, load all already-extracted IDs from three tables: `workflows`,
   `nodes`, `tables`.
2. For each workflow, check if its ID is in the checkpoint. If yes, skip all
   API calls and regenerate output files from MySQL.
3. For each node within a workflow, check if its ID is in the checkpoint. If
   yes, skip `GetNode` API call.
4. After all nodes in a workflow are done, write the workflow row (only then
   is the workflow "complete").
5. All writes use `INSERT IGNORE` — existing rows are never overwritten.

This means a crash mid-extraction leaves the workflow unfinished. The next
run re-processes only the missing nodes.

**CLI arguments:**

| Argument | Default | Purpose |
| --- | --- | --- |
| `--project-id` | `DATAWORKS_PROJECT_ID` env | DataWorks workspace ID (required) |
| `--env` | `DATAWORKS_ENV` or `Prod` | `Prod` or `Dev` |
| `--name-filter` | `None` (all workflows) | Only extract workflows whose name contains this string |
| `--output-dir` | `EXTRACT_OUTPUT_DIR` or `../data/extract` | Where to write JSON files |
| `--delay` | `CALL_DELAY_SECONDS` or `0.5` | Seconds between API calls (rate-limit pacing) |

**Output files:** `node_<id>.json` (one per node) + `workflow_<id>.json` (one
per workflow) in the output directory. Full JSON schema:
[Reference · FlowSpec JSON](reference/flowspec-json.md).

**Intra-project dependency:** imports `migrate.reconstruct` for
`nodes_to_flowspec()` and `spec_nodes()`.

---

### 3.2 Reconstruct — `migrate/reconstruct.py`

**What it does:** Reads the extractor's per-node JSON output and rebuilds
them into workflow-level FlowSpec dicts that the generator can consume.

**Why it exists:** The extractor writes one file per node (because that is
what the API returns). But the generator needs a whole workflow at once —
with trigger, variables, and the dependency graph. `reconstruct.py` bridges
this gap.

**Two input paths:**

| Path | When | What it reads |
| --- | --- | --- |
| Primary | `workflow_*.json` files exist | Loads them directly (they already contain the rebuilt flow graph) |
| Fallback | Only `node_*.json` files | Groups by `workflow_id`, deduplicates, rebuilds flow from `inputs.nodeOutputs` |

**Key functions:**

| Function | What it does |
| --- | --- |
| `load_workflows(extract_dir)` | Entry point. Returns `[FlowSpec_dict, ...]`. |
| `nodes_to_flowspec(...)` | Assembles a complete workflow-level FlowSpec from node dicts, trigger, and variables. |
| `build_flow(nodes)` | Rebuilds the `flow[]` dependency graph from each node's `inputs.nodeOutputs` references. |
| `spec_nodes(spec)` | Normalises any FlowSpec shape into a flat list of node dicts. |

**Intra-project dependency:** standalone — no project imports.

---

### 3.3 Classify — `classify/route_nodes.py` + `classify/flowspec_to_csv.py`

This stage covers two scripts: the routing preview (`route_nodes.py`) and the
CSV override layer (`flowspec_to_csv.py`).

#### route_nodes.py — the routing preview

**What it does:** A standalone **planning tool** (not part of the automated
pipeline). Reads FlowSpec files, applies the routing matrix from
[02 · Architecture](02-architecture-and-routing.md)
to each node's `runtime.command`, and prints a human-readable table showing
where each node should live on GCP.

**When to use it:** Before running the generator, to preview which nodes will
be auto-generated, which need human review (`TRIAGE`), and which will be
routed to Dataflow or Dataproc.

```bash
uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
uv run classify/route_nodes.py --extract-dir ../data/extract --csv-out ../data/routing.csv
```

**Note:** The generator (`generator.py`) has its own independent routing
logic (`route_node()`) that mirrors this classifier but is tightly integrated
with the generation flow. They share the same routing matrix but are not
called from each other — the classifier is for planning, the generator is for
execution.

#### flowspec_to_csv.py — the CSV override layer

**What it does:** After extraction, this script turns the FlowSpec JSON into
**`migration_matrix.csv`** — one row per node, with the columns `workflow`,
`node_id`, `node_name`, `command`, `input_tables`, `output_tables`,
`schedule`, `gcp_target`, `review_notes`, `skip`.

**Why it exists:** the JSON stays the structural source of truth (code, flow
graph, DDL); the CSV is the **human-editable override layer**. A person edits
`gcp_target`, `schedule`, `skip`, and `review_notes`, and the generator reads
those overrides back when it runs — so the loop is
*extract → CSV → human review → generate*.

```bash
uv run classify/flowspec_to_csv.py --flowspec extract/sample_flowspec_house_buying.json
uv run classify/flowspec_to_csv.py --extract-dir ../data/extract --csv-out ../data/extract/migration_matrix.csv
```

Column-by-column semantics and precedence rules live in
[Reference · migration_matrix.csv](reference/migration-matrix-csv.md); a
committed example is
[`../code/extract/sample_migration_matrix.csv`](../code/extract/sample_migration_matrix.csv).

---

### 3.4 Generate — `migrate/generator.py` + `migrate/translator.py`

This is the core of the toolkit. Two modules work together:

#### generator.py — the generation orchestrator

**What it does:** Takes a workflow-level FlowSpec dict, routes every node,
translates SQL nodes into Dataform `.sqlx` models, generates Composer DAG
Python files, and writes a review report for anything it cannot auto-route.

**Key data types:**

| Type | What it represents |
| --- | --- |
| `Model` | One Dataform `.sqlx` model (table name, tag, SELECT SQL, columns, flags) |
| `Declaration` | A Dataform source declaration (an already-loaded table the model reads from) |
| `GeneratedWorkflow` | Aggregate output: all models, declarations, DAG code, review lines, schedule |

**The generation algorithm (step by step):**

1. **Collect variables** from the workflow's `variables[]` — separate system
   vars (e.g., `bizdate`) from custom vars (e.g., `region`).
2. **Pass 1 — find produced tables.** Scan all nodes; any node whose script
   contains `INSERT OVERWRITE TABLE <table>` is marked as producing that table.
3. **Pass 2 — route + translate.** For each node:
   - Call `route_node()` to determine the GCP target.
   - If target is "Dataform .sqlx": extract the SELECT body via `_dml_select()`,
     translate it via `translator.translate_query()`, and create a `Model`.
   - If target is "TRIAGE": add to the review report.
   - If target is "skip" (VIRTUAL, branch): ignore.
4. **Build declarations.** Any input table that no model produces becomes a
   `Declaration` (it must already exist in BigQuery).
5. **Deduplicate.** If two models write the same table, the first wins;
   duplicates are flagged in review.
6. **Write files.** `.sqlx` for each model and declaration, **two** Python DAG
   variants per workflow (`version_2/dags/` for Airflow 2 and `version_3/dags/`
   for Airflow 3, with plain `dags/` mirroring the one selected by
   `AIRFLOW_MAJOR_VERSION`), `workflow_settings.yaml`, `.df-credentials.json`,
   and `review/*.md`.

**Routing logic (the three dictionaries):**

| Dict | Node types | What happens |
| --- | --- | --- |
| `SAFE_COMMANDS` | `ODPS_SQL` | Auto-generate Dataform `.sqlx` — no human review needed |
| `SQLISH_COMMANDS` | `PYODPS`, `PYTHON` | Check if the script is really SQL (`_is_sqlish_pyodps()`). If yes, push down to Dataform. If no, route to PythonOperator. |
| `TRIAGE_COMMANDS` | `DIDE_SHELL`, others | Always flagged for human review: written to `review/*.md`. **TRIAGE does not stop the pipeline** — generation continues for every other node. |

**Variable resolution:** All workflow variables are written to
`workflow_settings.yaml` under `vars:`. When generating model SQL, resolved
variables become `${{dataform.projectConfig.vars["name"]}}`. System variables
get Airflow Jinja expressions in the DAG's `variables` block (e.g., `bizdate`
= scheduled date - 1 day).

#### translator.py — the SQL translation engine

**What it does:** Rule-based translator from MaxCompute (ODPS/Hive-derived)
SQL to GoogleSQL (Dataform). Flags ambiguous constructs for human review
rather than silently rewriting.

**What it rewrites automatically:**

- 14 ODPS functions to BigQuery equivalents (e.g., `NVL()` -> `IFNULL()`)
- Table references to `${ref("table_name")}` so Dataform resolves dependencies
- Resolved `${variables}` to `${{dataform.projectConfig.vars["name"]}}`
- Column types (e.g., `DECIMAL(10,2)` -> `NUMERIC`)

**What it flags (never rewrites):**

- Non-trivial ODPS built-ins whose semantics differ (`DATEADD`, `SPLIT_PART`,
  `EXPLODE`, UDFs, etc.) — 11 functions flagged
- BigQuery reserved words used as bare identifiers
- Unresolved `${variables}` not in the workflow's variable list

**Key functions:**

| Function | What it does |
| --- | --- |
| `translate_query(query, available_vars)` | Main entry. Returns `(google_sql, flags)`. |
| `translate_type(odps_type)` | Maps one ODPS column type to BigQuery. |
| `parse_create(content)` | Parses `CREATE TABLE` -> column metadata. |
| `parse_insert(content)` | Parses `INSERT OVERWRITE ... SELECT` -> table + query. |
| `build_model_sql(dml_content, available_vars)` | Convenience: parse + extract SELECT + translate. |

---

### 3.5 Compile — `dataform compile` (external CLI)

**What it does:** Validates that the generated `.sqlx` files are valid
GoogleSQL. The orchestrator runs this as a subprocess and fails the pipeline
on any error.

**Invocation:** `dataform compile` in the generated project directory.

**Why it matters:** This is the first gate that catches SQL translation errors
before they hit BigQuery. It catches syntax errors, missing `ref()` targets,
undefined variables, and type mismatches.

**What a failure looks like:**

```
definitions/marts/ods_raw_orders.sqlx: ReferenceError: region is not defined
[gate] compile FAILED
```

---

### 3.6 Run — `dataform run` (external CLI)

**What it does:** Executes the compiled Dataform project against BigQuery.
Creates tables (or replaces them) and runs any assertions.

**Invocation:** `dataform run` in the generated project directory.

**What a failure looks like:** The orchestrator checks both the exit code and
stdout for assertion failures. If either indicates a problem, the pipeline
stops.

---

### 3.7 Verify — `migrate/verifier.py`

**What it does:** The final gate. It does **four** things — not just a row
count — and fails the pipeline if any of them fails. Source:
[`../code/migrate/verifier.py`](../code/migrate/verifier.py).

| # | Check | How it is computed |
| --- | --- | --- |
| 1 | **Row count** | `SELECT COUNT(*) FROM dataset.table` per generated table; a missing or empty table fails the gate |
| 2 | **MD5 checksum** | `MD5(ARRAY_TO_STRING(ARRAY(SELECT TO_JSON_STRING(t) ...)))` — a deterministic fingerprint of all rows |
| 3 | **Per-column numeric aggregates** | `SUM` / `MIN` / `MAX` for every `INT64`, `FLOAT64`, `NUMERIC`, `BIGNUMERIC` column (columns discovered via `INFORMATION_SCHEMA.COLUMNS`) |
| 4 | **DAG parity** | Source FlowSpec vs generated artifacts: same schedule (compared on the five core cron fields, seconds ignored) and the same declared output tables; cyclic generated dependencies are reported |

**How it works:**

1. Discover generated table names by reading `.sqlx` files from
   `definitions/marts/`.
2. For each table, run the row-count, checksum, and aggregate queries via the
   `bq` CLI (checksum and aggregates are skipped when the table has 0 rows).
3. Compare the source workflow's schedule and outputs against the generated
   ones.
4. Return pass/fail status plus markdown summary tables (per-table and
   per-DAG).

**Key functions:**

| Function | What it does |
| --- | --- |
| `check_tables(project, dataset, tables)` | Per-table gate: row count + checksum + numeric aggregates. Returns `[TableCheck, ...]`. |
| `check_dag_parity(...)` | Per-DAG gate: schedule, node/model counts, output tables, dependency sanity. Returns a `DAGParityCheck`. |
| `summarize(checks)` | Returns `(passed: bool, markdown_table: str)` for the per-table gate. |
| `summarize_dag_parity(checks)` | Returns `(passed: bool, markdown_table: str)` for the per-DAG gate. |

**Intra-project dependency:** standalone — no project imports.

---

### 3.8 Orchestrator — `migrate/migrate.py`

**What it does:** The top-level CLI that chains the pipeline stages together.
Loads `.env` configuration and dispatches to the appropriate handler.

**Subcommands:**

| Subcommand | What it does |
| --- | --- |
| `extract` | Shells out to `extract_dataworks.py` |
| `generate` | Loads FlowSpecs (from `--flowspec` or `--extract-dir`), optionally filters by `--workflows`, calls `generator.generate_project()` |
| `compile` | Runs `dataform compile` on the generated project |
| `run` | Runs `dataform run` |
| `verify` | Discovers tables from `.sqlx` files, runs `verifier.check_tables()` |
| `all` | Chains: generate -> compile -> run -> verify (stops on failure) |

**Input sources (mutually exclusive):**

| Flag | What it provides |
| --- | --- |
| `--flowspec <file.json>` | A hand-authored or pre-built FlowSpec file |
| `--extract-dir <dir>` | Real extractor output; `reconstruct.load_workflows()` rebuilds FlowSpecs from per-node JSON files |

**Workflow selector (optional):** `--workflows <file.json>` filters which
workflows to migrate. Accepts a JSON array of names/ids or an object with a
`"workflows"` key. Entries that match nothing are warned; an empty selection
aborts.

---

## 4. Data flow between components

### 4.1 The automatic path (`migrate.py all`)

```
FlowSpec JSON (or extract dir)
        |
        v
  reconstruct.load_workflows()
        |  or load_flowspecs()
        v
  [workflow-level FlowSpec dicts]
        |
        v
  generator.generate_project()
        |-- route_node() per node        -> routing decision
        |-- translator.translate_query()  -> GoogleSQL + flags
        |-- build_models_and_decls()      -> Model + Declaration objects
        |-- _build_dag()                  -> Airflow 2 + Airflow 3 DAG strings
        |-- write .sqlx, DAGs (version_2/ + version_3/ + dags/),
        |          review, settings
        v
  [data/generated/ directory]
        |
        v
  dataform compile   -> validates SQL syntax
        |
        v
  dataform run       -> deploys to BigQuery
        |
        v
  verifier.py        -> row count + checksum + aggregates + DAG parity
        |
        v
  PASS / FAIL
```

### 4.2 The extract path (`extract_dataworks.py`)

```
Alibaba DataWorks API
   |-- ListWorkflows (2024-05-18)
   |-- GetWorkflow   (2024-05-18)
   |-- ListNodes     (2024-05-18)
   |-- GetNode       (2024-05-18)  -> FlowSpec per node
   |-- GetMetaTableColumn (2020-05-18) -> DDL per table
        |
        v
  MySQL checkpoint (INSERT IGNORE, append-only)
        |
        v
  node_<id>.json + workflow_<id>.json
        |
        v
  reconstruct.load_workflows()  -> feeds into generate
```

---

## 5. Configuration

All configuration lives in `.env` (copied from `.env.example`). Nothing else
needs to be edited for a standard migration.

> [!IMPORTANT]
> [`code/.env.example`](../code/.env.example) is the **authoritative list** of
> every supported variable, with comments and defaults. The table below is a
> short orientation subset, not the full set — do not treat it as complete.

| Variable | Default | Purpose |
| --- | --- | --- |
| `ALIBABA_ACCESS_KEY` / `ALIBABA_SECRET_KEY` / `ALIBABA_REGION` | — | Authenticate with the DataWorks API |
| `DATAWORKS_PROJECT_ID` / `DATAWORKS_ENV` | — / `Prod` | Which workspace and environment to extract |
| `EXTRACT_OUTPUT_DIR` | `../data/extract` | Where extracted `node_*.json` / `workflow_*.json` are written |
| `GENERATED_OUTPUT_DIR` | `../data/generated` | Where the generator writes the Dataform project, DAGs, and review reports |
| `GCP_PROJECT_ID` / `GCP_REGION` | `my-gcp-project` / `asia-southeast2` | Deployment target |
| `AIRFLOW_MAJOR_VERSION` | `3` | Which variant the plain `dags/` folder mirrors (`2` or `3`). **Both `version_2/dags/` and `version_3/dags/` are always generated.** |
| `DAG_SCHEDULE` | `0 2 * * *` | Schedule for the hand-written reference DAG (`gcp/dags/house_buying_daily.py`) |
| `DATAFORM_REPOSITORY_ID` / `DATAFORM_GIT_BRANCH` | `house-buying-analysis` / `main` | Dataform repository and branch the DAG compiles |
| `DATAFORM_CLI` / `DATAFORM_CORE_VERSION` | `dataform` / `3.0.64` | Dataform CLI binary and the core version it must match |
| `DATAFORM_DEFAULT_SCHEMA` / `DATAFORM_ASSERTION_SCHEMA` | `dwh` / `dwh_assertions` | BigQuery datasets for models and assertions |
| `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_DATABASE` | `127.0.0.1` / `3306` / … | Crash-resumable extraction checkpoint |
| `CALL_DELAY_SECONDS` | `0.5` | Pacing between DataWorks API calls |

The only exception is `workflow_settings.yaml`, which is generated by the
toolkit and committed in the reference `gcp/dataform/` directory because
Dataform cannot read env vars at compile time.

---

## 6. How to extend the system

### 6.1 Adding a new node type to auto-generation

1. Add the command name (e.g., `ODPS_SPARK`) to `SAFE_COMMANDS` in
   `generator.py` with a `(target, reason)` tuple.
2. If the SQL can be pushed down to BigQuery, add any needed rewrites to
   `translator.py` (`FUNCTION_MAP`, `TYPE_MAP`, or `FLAG_FUNCTIONS`).
3. Add a test to [`../code/tests/test_migrate.py`](../code/tests/test_migrate.py)
   and run `uv run pytest` from `code/`.

### 6.2 Adding a new ODPS function rewrite

1. Add the mapping to `FUNCTION_MAP` in `translator.py`.
2. If semantics differ (e.g., `DATEADD`), add to `FLAG_FUNCTIONS` instead
   so it gets flagged rather than silently rewritten.

### 6.3 Adding a new variable

1. Add the variable to the FlowSpec's `variables[]` array.
2. The generator automatically propagates it to `workflow_settings.yaml`
   and rewrites it in model SQL.
3. System variables get Airflow Jinja expressions in the DAG; custom
   variables get their default values.

---

## 7. Known limits (validated by the gates)

> [!IMPORTANT]
> This section — together with the gate semantics in §3 — is **canonical**.
> Other chapters link here; they must not restate these limits.

These are not bugs — they are deliberate design choices validated by the
pipeline gates, not by a human:

- **Non-trivial ODPS built-ins** (`DATEADD`, `SPLIT_PART`, `EXPLODE`, UDFs)
  are flagged in `review/*.md` rather than silently rewritten.
- **Schedules** are stripped of seconds and marked "RE-VERIFY" at cutover.
- **Branch nodes** (`branches[]`) are silently absorbed — DataWorks branching
  has no direct Airflow equivalent. Documented trade-off in
  [03 · Migration playbook](03-migration-playbook.md).
- **Heavy Python** (data-intensive PyODPS) is routed to TRIAGE — it needs
  human judgment to decide between PythonOperator and Dataflow.
- **Live MaxCompute parity** and **Composer DAG execution** are untested —
  they require source access and a running Airflow environment.

---

[← Prev: Tutorial · End-to-end on GCP](tutorials/end-to-end-on-gcp.md) · [Index](README.md) · [Next: Cutover checklist →](cutover-checklist.md)
