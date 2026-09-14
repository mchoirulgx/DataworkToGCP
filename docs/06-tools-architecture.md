# 06 · Tools Architecture

> **Goal:** understand how every piece of this toolkit fits together — what each
> file does, how data flows between them, and where to look when something
> breaks or you want to extend the system.

---

## 1. The big picture

The toolkit is a **pipeline of seven stages**, each handled by a dedicated
module. Some stages are standalone scripts; others are called as libraries by
the orchestrator.

```
Stage 1  EXTRACT      extract_dataworks.py   Alibaba API -> JSON + MySQL
Stage 2  RECONSTRUCT  reconstruct.py         per-node JSON -> FlowSpec
Stage 3  CLASSIFY     route_nodes.py         standalone planning tool
Stage 4  GENERATE     generator.py           FlowSpec -> .sqlx + DAG
                       translator.py          ODPS SQL -> GoogleSQL
Stage 5  COMPILE      dataform CLI           validates GoogleSQL
Stage 6  RUN          dataform CLI           deploys to BigQuery
Stage 7  VERIFY       verifier.py            row-count gate
```

The orchestrator `migrate.py` chains stages 4-5-6-7 in a single command
(`migrate.py all`), and **stops the pipeline on failure** at every gate.
Stages 1-3 can be run independently.

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
   [4] generator.py + translator.py
        |
        v
  data/generated/            (git-ignored)
  definitions/marts/*.sqlx   -- Dataform models
  definitions/sources/*.sqlx -- Dataform declarations
  dags/*.py                  -- Airflow DAGs
  review/*.md                -- items needing human review
  workflow_settings.yaml     -- Dataform project config + variables
  .df-credentials.json       -- gcloud ADC credentials
        |
   [5] dataform compile     -- gate: valid GoogleSQL?
   [6] dataform run         -- gate: deploys to BigQuery
   [7] verifier.py          -- gate: tables non-empty?
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
    sample_workflows_selector.json     example --workflows selector

  classify/
    route_nodes.py                    stage 3: standalone routing table

  migrate/
    migrate.py                        orchestrator CLI (stages 4-7)
    generator.py                      stage 4: .sqlx + DAG generation
    translator.py                     stage 4: ODPS -> GoogleSQL translation
    reconstruct.py                    stage 2: per-node -> workflow FlowSpec
    verifier.py                       stage 7: BigQuery row-count gate

  gcp/
    dags/house_buying_daily.py        hand-written reference DAG
    dataform/                         hand-written reference Dataform project

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
per workflow) in the output directory. See docs/05 for the full JSON schema.

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

### 3.3 Classify — `classify/route_nodes.py`

**What it does:** A standalone **planning tool** (not part of the automated
pipeline). Reads FlowSpec files, applies the routing matrix from chapter 02
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
6. **Write files.** `.sqlx` for each model and declaration, a Python DAG for
   each workflow, `workflow_settings.yaml`, `.df-credentials.json`, and
   `review/*.md`.

**Routing logic (the three dictionaries):**

| Dict | Node types | What happens |
| --- | --- | --- |
| `SAFE_COMMANDS` | `ODPS_SQL` | Auto-generate Dataform `.sqlx` — no human review needed |
| `SQLISH_COMMANDS` | `PYODPS`, `PYTHON` | Check if the script is really SQL (`_is_sqlish_pyodps()`). If yes, push down to Dataform. If no, route to PythonOperator. |
| `TRIAGE_COMMANDS` | `DIDE_SHELL`, others | Always flagged for human review |

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

**What it does:** The final gate. Queries BigQuery for the row count of every
table the generator produced, and fails the pipeline if any table is missing
or empty.

**How it works:**

1. Discover generated table names by reading `.sqlx` files from
   `definitions/marts/`.
2. For each table, run `SELECT COUNT(*) FROM dataset.table` via the `bq` CLI.
3. Return pass/fail status and a markdown summary table.

**Key functions:**

| Function | What it does |
| --- | --- |
| `check_tables(project, dataset, tables)` | Queries BigQuery for row counts. Returns `[TableCheck, ...]`. |
| `summarize(checks)` | Returns `(passed: bool, markdown_table: str)`. |

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
        |-- _build_dag()                  -> Airflow DAG Python string
        |-- write .sqlx, DAG, review, settings
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
  verifier.py        -> queries BigQuery for row counts
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

| Variable group | Key variables | Purpose |
| --- | --- | --- |
| Alibaba credentials | `ALIBABA_ACCESS_KEY`, `ALIBABA_SECRET_KEY`, `ALIBABA_REGION` | Authenticate with DataWorks API |
| DataWorks workspace | `DATAWORKS_PROJECT_ID`, `DATAWORKS_ENV` | Which workspace to extract from |
| GCP target | `GCP_PROJECT_ID`, `GCP_REGION` | Where to deploy |
| Dataform | `DATAFORM_DEFAULT_SCHEMA`, `DATAFORM_CORE_VERSION` | Dataform project settings |
| MySQL checkpoint | `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Crash-resumable extraction state |
| Pipeline tuning | `CALL_DELAY_SECONDS`, `GENERATED_OUTPUT_DIR` | Rate limiting, output location |

The only exception is `workflow_settings.yaml`, which is generated by the
toolkit and committed in the reference `gcp/dataform/` directory because
Dataform cannot read env vars at compile time.

---

## 6. How to extend the system

### Adding a new node type to auto-generation

1. Add the command name (e.g., `ODPS_SPARK`) to `SAFE_COMMANDS` in
   `generator.py` with a `(target, reason)` tuple.
2. If the SQL can be pushed down to BigQuery, add any needed rewrites to
   `translator.py` (`FUNCTION_MAP`, `TYPE_MAP`, or `FLAG_FUNCTIONS`).
3. Add a test case to `docs/unit-test-result.md`.

### Adding a new ODPS function rewrite

1. Add the mapping to `FUNCTION_MAP` in `translator.py`.
2. If semantics differ (e.g., `DATEADD`), add to `FLAG_FUNCTIONS` instead
   so it gets flagged rather than silently rewritten.

### Adding a new variable

1. Add the variable to the FlowSpec's `variables[]` array.
2. The generator automatically propagates it to `workflow_settings.yaml`
   and rewrites it in model SQL.
3. System variables get Airflow Jinja expressions in the DAG; custom
   variables get their default values.

---

## 7. Known limits (validated by the gates)

These are not bugs — they are deliberate design choices validated by the
pipeline gates, not by a human:

- **Non-trivial ODPS built-ins** (`DATEADD`, `SPLIT_PART`, `EXPLODE`, UDFs)
  are flagged in `review/*.md` rather than silently rewritten.
- **Schedules** are stripped of seconds and marked "RE-VERIFY" at cutover.
- **Branch nodes** (`branches[]`) are silently absorbed — DataWorks branching
  has no direct Airflow equivalent. Documented trade-off in docs/03.
- **Heavy Python** (data-intensive PyODPS) is routed to TRIAGE — it needs
  human judgment to decide between PythonOperator and Dataflow.
- **Live MaxCompute parity** and **Composer DAG execution** are untested —
  they require source access and a running Airflow environment.

---

**Next:** [07 · Appendices & resources](07-appendices-and-resources.md)
