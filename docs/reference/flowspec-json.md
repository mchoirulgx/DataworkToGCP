# Reference · FlowSpec JSON

> **Last reviewed:** 2026-09-22

> **Goal:** a complete, beginner-friendly reference for the FlowSpec JSON format —
> every property, its possible values, and what it means. Use this to read any
> `workflow_<id>.json` / `node_<id>.json` / `sample_flowspec_*.json` produced by
> this repo's extractor.
>
> **Scope note:** the case-study file (`code/extract/sample_flowspec_house_buying.json`)
> is a small 3-node example. The real format is richer — this chapter documents
> the **full property surface**, marking which values appear in the case study
> and which you may meet on other workflows. Properties the migration toolkit
> ignores are labelled *informational*.

## Contents

- [1. FlowSpec in one sentence](#1-flowspec-in-one-sentence)
- [2. JSON crash course](#2-json-crash-course-if-youve-never-read-json)
- [3. The file at a glance](#3-the-file-at-a-glance)
- [4. Top level](#4-top-level)
- [5. `spec` — the definition](#5-spec--the-definition)
- [6. `spec.workflows[]` — one workflow](#6-specworkflows--one-workflow)
- [7. `nodes[]` — the steps](#7-nodes--the-steps-array-of-objects-)
- [8. `flow[]` — execution order](#8-flow--execution-order-array-of-objects-)
- [9. Node types you will meet](#9-node-types-you-will-meet-scriptruntimecommand)
- [10. What `migrate.py` actually reads (the contract)](#10-what-migratepy-actually-reads-the-contract)
- [11. Worked example](#11-worked-example--read-the-case-study-end-to-end)

---

## 1. FlowSpec in one sentence

A **FlowSpec** is DataWorks' JSON description of one piece of pipeline metadata —
a workflow (a.k.a. cycle workflow / DAG) **or** a single node. The extractor pulls
one per node (`GetNode`) and assembles one per workflow; `migrate.py` turns it
into BigQuery + Airflow code.

Two shapes exist and both are accepted by the toolkit:

- **Workflow-level** — has `spec.workflows[].nodes[]` + a `flow[]` graph. This is
  what `migrate.py` consumes (`sample_flowspec_house_buying.json`,
  `workflow_<id>.json`).
- **Node-level** — a single node's `{id, name, script, inputs, outputs, ...}`.
  This is what the extractor stores per `node_<id>.json` and what
  `migrate/reconstruct.py` normalizes back into workflow-level shape.

---

## 2. JSON crash course (if you've never read JSON)

- `{ "a": 1 }` — an **object**: a bag of named values.
- `[ "x", "y" ]` — an **array**: an ordered list.
- `"key": value` — one **property**. Values can be strings `"text"`, numbers `1`,
  booleans `true`/`false`, objects `{}`, arrays `[]`, or `null`.
- Read it like a file tree: `spec.workflows[0].nodes[1].script.content` means
  "inside `spec`, inside the first `workflows` entry, inside the second `nodes`
  entry, inside `script`, the `content` property."

---

## 3. The file at a glance

```
FlowSpec
├── metadata            (who/what owns this document)
├── kind                (type of document)
├── version             (schema version)
└── spec                (the real definition)
    ├── name / id / type / owner
    └── workflows[]     (usually one entry)
        ├── id / name
        ├── script      (workflow-level metadata)
        ├── trigger     (the schedule)
        ├── strategy    (retry / timeout policy)
        ├── variables[] (parameters the code can read)
        ├── nodes[]     (the actual steps)
        └── flow[]      (which step waits for which)
```

---

## 4. Top level

### `metadata` — document identity (object, informational)

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `tenantId` | string | `"52425742456XXXX"` | Your Alibaba tenant (company) ID |
| `projectId` | string | `"307XXXX"` | The DataWorks project ID |
| `uuid` | string | `"463497880880954XXXX"` | Unique ID of this workflow / document |

### `kind` — document type (string)

| Value | Meaning |
| --- | --- |
| `"CycleWorkflow"` | A scheduled (cyclic) workflow — the common case |
| `"OfflineWorkflow"` / `"ManualWorkflow"` | Manual / ad-hoc workflow (no schedule). Both spellings are seen in the wild — see the [glossary](glossary.md) |
| `"Workflow"` / other | Other DataWorks document kinds |

### `version` — schema version (string)

`"1.1.0"` in the case study. The extractor normalizes whatever the API returns.

### `spec` — the definition (object) ⭐

Everything that matters lives under `spec` (see §5).

---

## 5. `spec` — the definition

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `name` | string | `"house_buying_analysis"` | Display name of the pipeline |
| `id` | string | `"463497880880954XXXX"` | Workflow ID (same as `metadata.uuid`) |
| `type` | string | `"CycleWorkflow"` | Same idea as `kind` |
| `owner` | string | `"110755000425XXXX"` | Alibaba account ID of the owner (informational) |
| `workflows` | array | `[ {...} ]` | ⭐ The workflow block(s). Usually exactly one |

> Some FlowSpecs put node-level content directly under `spec` (`spec.nodes[]`,
> `spec.trigger`, `spec.flow`) instead of wrapping it in `workflows[]`. The
> toolkit's `spec_nodes()` in `migrate/reconstruct.py` handles **all three**
> shapes automatically, so you don't need to care which one you have.

---

## 6. `spec.workflows[]` — one workflow

### `id` / `name` (strings)
Unique ID and display name of this workflow.

### `script` — workflow-level script (object, informational)

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `path` | string | `"Business/house_buying_analysis"` | Folder path in the DataWorks console |
| `runtime.command` | string | `"WORKFLOW"` | Marks this block as the workflow shell |

### `trigger` — the schedule (object) ⭐

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `type` | string | `"Scheduler"` | How it's started. `Scheduler` = on schedule |
| `cron` | string | `"00 02 00 * * ?"` | Quartz 6-field cron: **sec min hour day month weekday**. `00 02 00 * * ?` = **00:02 daily** (two minutes past midnight) |
| `startTime` | string | `"2025-01-01 00:00:00"` | Schedule valid from |
| `endTime` | string | `"9999-01-01 00:00:00"` | Schedule valid until (`9999` = never ends) |
| `timezone` | string | `"Asia/Jakarta"` | Timezone the cron is evaluated in |

> `migrate.py` drops the seconds field to produce the Airflow 5-field expression
> (`00 02 00 * * ?` → `02 00 * * *`, also 00:02 daily) and passes `timezone` to
> the DAG. Full rules: [Variables & macros §3 · Cron translation](variables-and-macros.md#3-cron-translation).

### `strategy` — run / retry policy (object)

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `timeout` | integer | `0` | Timeout in ms; `0` = no timeout |
| `instanceMode` | string | `"T+1"` | Runs against yesterday's data. Other values exist (e.g. `T`) |
| `rerunMode` | string | `"Allowed"` | Whether failed instances may be re-run (`Allowed` / `Denied` / …) |
| `rerunTimes` | integer | `3` | Auto-retry attempts |
| `rerunInterval` | integer | `180000` | Wait between retries, in ms |

### `variables[]` — parameters available to the code (array)

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `id` | string | `"v_bizdate"` | Internal variable ID |
| `name` | string | `"bizdate"` | Name you use in code, e.g. `${bizdate}` |
| `scope` | string | `"Workflow"` | Visibility. `Workflow` = whole workflow; `Node` = one node |
| `type` | string | `"System"` / `"Custom"` | `System` = built-in (DataWorks fills it); `Custom` = user-defined |
| `value` | string | `"${bizdate}"` | The value / reference |

> **The `bizdate` landmine.** DataWorks business date semantics differ from
> Airflow `logical_date` / `data_interval`. The toolkit flags `${vars}` it can't
> resolve instead of guessing — see [Variables & macros](variables-and-macros.md).

---

## 7. `nodes[]` — the steps (array of objects) ⭐

### Common properties (all node types)

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `id` | string | `"n_start"` | Short unique node ID; used by `flow[]` and `inputs.nodeOutputs` to wire steps |
| `name` | string | `"workshop_start"` | Display name |
| `recurrence` | string | `"Normal"` | `Normal` = runs each cycle; other values for special runs |
| `script` | object | `{...}` | The node's code + type (see §7.1) |
| `inputs` | object | `{...}` | What feeds this step (see §7.2) |
| `outputs` | object | `{...}` | What this step produces (see §7.2) |
| `branches` | array | `[{"condition": "...", "goto": {...}}]` | Branch nodes only — routing rules (may not appear) |
| `schedule` | object | — | Node-level timing overrides (may not appear) |
| `maxRetryTimes` / `retryInterval` | integer | — | Node-level retry overrides (may not appear) |
| `timeout` | integer | — | Node-level timeout in ms (may not appear) |
| `delay` | integer | — | Delay before the node runs (may not appear) |

### 7.1 `script` — code + engine (object) ⭐

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `id` | string | `"s_insert"` | Script ID |
| `path` | string | `"Business/house_buying_analysis/insert_result_table"` | Console folder path (informational) |
| `language` | string | `"odps-sql"`, `"python"`, `"shell"`, … | Code language |
| `runtime.command` | string | `"ODPS_SQL"`, `"VIRTUAL"`, … | ⭐ **Node type / engine — drives routing** |
| `runtime.commandTypeId` | integer | `10` | Internal DataWorks numeric type code (informational) |
| `content` | string | the SQL / Python / shell code | The actual code to translate |

### 7.2 `inputs` and `outputs` — dependencies and data (object)

| Property | Type | Meaning |
| --- | --- | --- |
| `inputs.tables[]` | array of `{guid, ...}` | Tables the node **reads** |
| `inputs.nodeOutputs[]` | array of `{data, artifactType}` | Which **nodes** must finish first (`data` = their node id) |
| `inputs.forecasts[]` | array | Predicted-completion hints (may appear; informational) |
| `inputs.localization` | object | Locale info (may appear; informational) |
| `outputs.tables[]` | array of `{guid, ...}` | Tables the node **writes** |
| `outputs.nodeOutputs[]` | array of `{data, artifactType}` | The node's own "done" handle later nodes wait on |
| `artifactType` | string | `"NodeOutput"` | Type of the artifact produced |

Each `tables[]` entry:

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `guid` | string | `"odps.house_buying.result_table"` | Full MaxCompute name: `engine.project.table` |
| `name` / `projectName` | string | — | May also appear; same table, split into parts |

---

## 8. `flow[]` — execution order (array of objects) ⭐

| Property | Type | Example | Meaning |
| --- | --- | --- | --- |
| `nodeId` | string | `"n_ddl"` | The node this entry describes |
| `depends[]` | array of `{nodeId, type}` | `[{"nodeId": "n_start", "type": "Normal"}]` | Nodes that must finish first |
| `depends[].type` | string | `"Normal"` | `Normal` = wait for completion; other values for cross-cycle waits |

Example from the case study — reads as *start → create table → fill table*:

```json
[
  { "nodeId": "n_start",  "depends": [] },
  { "nodeId": "n_ddl",    "depends": [ { "nodeId": "n_start", "type": "Normal" } ] },
  { "nodeId": "n_insert", "depends": [ { "nodeId": "n_ddl",   "type": "Normal" } ] }
]
```

> The extractor does **not** need `flow[]` from the API for `--extract-dir` runs:
> `migrate/reconstruct.py` rebuilds it from each node's `inputs.nodeOutputs`.

---

## 9. Node types you will meet (`script.runtime.command`)

> **Routing lives in [02 · Architecture & routing](../02-architecture-and-routing.md).**
> That chapter owns the canonical command → GCP target matrix. The table below
> only says what each `command` *is* and what the toolkit does with it.

| command | What it is / toolkit behaviour |
| --- | --- |
| `VIRTUAL` | No-op / flow-control marker (case study: `workshop_start`). No SQL emitted |
| `ODPS_SQL` | MaxCompute SQL (case study: both `ddl_*` and `insert_*`). Translated ODPS → GoogleSQL |
| `PYODPS` | MaxCompute Python. SQL-ish subset pushed down; complex code flagged **TRIAGE** |
| `DATAX` | Offline sync definition. Captured for review |
| `DIDE_SHELL` / `SHELL` | Shell script. Audited individually |
| `WORKFLOW_DAG` | Nested workflow. Recursively processed |

Each command type can add its own properties under `script` (e.g. `DATAX` carries
a JSON config in `content`; `PYODPS` carries Python with `odps` imports). The
generator treats unknown/unsupported commands conservatively: rather than
guessing, it flags them as **TRIAGE** items in `review/*.md` for human review.
TRIAGE items do **not** block the run — generation continues for every other
node.

---

## 10. What `migrate.py` actually reads (the contract)

The generator ignores informational fields and only consumes:

| Field | Used for |
| --- | --- |
| `spec.workflows[].trigger.cron` / `.timezone` | Composer DAG schedule |
| `spec.workflows[].variables[]` | Variable resolution + unresolved-var review flags |
| `spec.workflows[].nodes[].script.runtime.command` | Routing (which target) |
| `spec.workflows[].nodes[].script.content` | SQL / code translation |
| `spec.workflows[].nodes[].inputs` / `.outputs` | Table refs (`${ref(...)}` rewrites) + flow rebuild |
| `spec.workflows[].flow[]` | DAG task ordering (when present) |

The extractor's `workflow_<id>.json` is a **slimmed** FlowSpec: it keeps
`kind`, `version`, `metadata.uuid`, and `spec.{name,id,type,workflows[{id,name,
trigger,variables,nodes,flow}]}`, and drops `owner`, `strategy`, and
`script.path` — the pieces the generator never uses. Both the full sample file
and the slimmed output are valid inputs.

---

## 11. Worked example — read the case study end to end

1. **Envelope**: `metadata.tenantId/projectId/uuid` — this document is workflow
   `463497880880954XXXX` in project `307XXXX`. `kind` = `CycleWorkflow`, so it
   runs on a schedule.
2. **spec.name/id/type**: the pipeline is called `house_buying_analysis`.
3. **workflows[0].trigger**: `cron = "00 02 00 * * ?"` in `Asia/Jakarta` →
   every day at 00:02 Jakarta time (two minutes past midnight).
4. **workflows[0].variables**: `bizdate` (system var) is available to the code.
5. **workflows[0].nodes**:
   - `workshop_start` (`VIRTUAL`) — start marker.
   - `ddl_result_table` (`ODPS_SQL`) — `CREATE TABLE IF NOT EXISTS result_table`; reads nothing, writes `result_table`; waits for `n_start`.
   - `insert_result_table` (`ODPS_SQL`) — the `INSERT OVERWRITE … GROUP BY`
     query; reads `bank_data`, writes `result_table`; waits for `n_ddl`.
6. **workflows[0].flow**: `n_start → n_ddl → n_insert` (create the table, then
   fill it).

Both `ODPS_SQL` nodes collapse into **one** Dataform model (`result_table.sqlx`):
the `CREATE TABLE` node is absorbed as the model's schema, and the `INSERT
OVERWRITE` becomes the model `SELECT`.

---

[← Prev: DataWorks OpenAPI](dataworks-openapi.md) · [Index](../README.md) · [Next: Migration matrix CSV →](migration-matrix-csv.md)
