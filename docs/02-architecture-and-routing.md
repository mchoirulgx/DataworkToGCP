# 02 · Architecture & Routing Matrix

> **Last reviewed:** 2026-09-22

> **Goal:** understand the DataWorks object model, map it onto GCP services, and
> learn the rules for deciding *where each node goes*.

---

## Contents

- [1. The DataWorks object model (core structure)](#1-the-dataworks-object-model-core-structure)
- [2. Architecture shift (paradigm)](#2-architecture-shift-paradigm)
- [3. The routing matrix (Node-type → GCP target)](#3-the-routing-matrix-node-type--gcp-target)
- [4. Routing rule (encode in the classifier / AI generator, in priority order)](#4-routing-rule-encode-in-the-classifier--ai-generator-in-priority-order)
- [5. Inverting Alibaba's own tutorial (why the reverse is easier than it looks)](#5-inverting-alibabas-own-tutorial-why-the-reverse-is-easier-than-it-looks)

---

## 1. The DataWorks object model (core structure)

DataWorks logic is **nested**. You cannot extract scripts without preserving the
hierarchy, or you lose execution order.

```
Workflow (CycleWorkflow = scheduled / ManualWorkflow = manual)
 └── Nodes (the tasks inside the workflow)
      ├── script.content        → the raw code (ODPS_SQL, PyODPS, Shell…)
      ├── script.runtime.command → the node type / engine (drives routing)
      ├── inputs / outputs      → tables and artifacts the node touches
      └── flow / FlowDepend     → the dependency graph BETWEEN nodes
```

- **Workflow → Airflow DAG.** One workflow maps to one DAG. Extract via
  `GetWorkflow`: the master CRON, trigger type (scheduled vs manual), global
  parameters/variables (e.g. `bizdate=${yyyymmdd}`), and cross-workflow
  dependencies (→ `ExternalTaskSensor` in Airflow).
- **Node → operator or Dataform model.** Extract via `GetNode`, whose `spec`
  field *is* FlowSpec. Critically, the dependency graph lives in the
  **`flow` / FlowDepend section** — read it, not just per-node inputs/outputs.
- **Self / cross-cycle dependency** (`CrossCycleDependsOnOtherNode`): a node whose
  current cycle depends on the *previous* cycle's instance → `depends_on_past`
  or a previous-run sensor in Airflow. **Miss this and DAGs run out of order.**

### The FlowSpec shape (from aliyun/dataworks-spec, the official schema)

```jsonc
{
  "version": "1.1.0",
  "kind": "CycleWorkflow",            // or "ManualWorkflow", "Node"
  "metadata": { "tenantId": "...", "projectId": "...", "uuid": "..." },
  "spec": {
    "name": "house_buying_analysis",
    "trigger": { "type": "Scheduler", "cron": "00 02 00 * * ?", "timezone": "Asia/Jakarta" },
    "variables": [ { "id": "v_bizdate", "name": "bizdate", "scope": "Workflow",
                     "type": "System", "value": "${bizdate}" } ],
    "nodes": [
      {
        "id": "n_insert",
        "name": "insert_result_table",
        "script": {
          "runtime": { "command": "ODPS_SQL", "commandTypeId": 10 },
          "language": "odps-sql",
          "content": "INSERT OVERWRITE TABLE result_table ..."
        },
        "inputs":  { "tables": [ { "guid": "odps.house_buying.bank_data" } ] },
        "outputs": { "tables": [ { "guid": "odps.house_buying.result_table" } ] }
      }
    ],
    "flow": [
      { "nodeId": "n_insert", "depends": [ { "nodeId": "n_ddl", "type": "Normal" } ] }
    ]
  }
}
```

> Parse against Alibaba's published **FlowSpec model** (`github.com/aliyun/dataworks-spec`)
> rather than reverse-engineering JSON shapes by hand. Shapes can vary slightly
> between the `GetNode` (kind `Node`) and `GetWorkflowDefinition` (kind
> `CycleWorkflow`) responses — validate against the schema at runtime.

### The two API surfaces (the #1 trap)

> **Warning.** DataWorks exposes orchestration and table metadata through **two
> different API versions, shipped in two different SDK packages, on two
> different endpoints** — so you must instantiate **two clients**. Calling a
> metadata operation on the orchestration client (or vice versa) is the single
> most common setup mistake: depending on the SDK, it fails late, returns empty
> results, or silently yields no schema at all, so tables appear to have no
> columns rather than raising an obvious error. Wire up both clients before you
> extract anything, and route every Data Map / metadata call to the metadata
> client. The per-operation and per-field tables (which operations live on which
> version, and what each returns) are in
> [reference/dataworks-openapi.md](reference/dataworks-openapi.md).

---

## 2. Architecture shift (paradigm)

| | DataWorks (today) | GCP (target) |
| --- | --- | --- |
| Warehouse | MaxCompute | **BigQuery** |
| SQL transformation | Data Studio (visual SQL) | **Dataform** (`.sqlx`) — pipeline-as-code, native Git |
| Master orchestrator | Operation Center | **Cloud Composer** (Apache Airflow) |
| Dependency inside the warehouse | encoded as workflow edges | **Dataform `ref()`** |
| Cross-system dependencies | workflow inputs | Airflow `ExternalTaskSensor` |

**Key principle — let Dataform own the in-BigQuery DAG.** Dataform resolves
table-to-table dependencies through `ref()`. Do **not** recreate every
intra-BigQuery SQL dependency as an Airflow edge. This shapes what your
DAG-generation script (and the AI generator) should emit.

---

## 3. The routing matrix (Node-type → GCP target)

The mapping is **not** binary ("Operator or `.sqlx`"). Route each node by its
FlowSpec `runtime.command` and, for Python, by **data volume**. The guiding
principle for Python: **keep heavy data off the Airflow worker.**

| DataWorks node type | Nature | GCP target |
| --- | --- | --- |
| `ODPS_SQL` | In-warehouse SQL | **Dataform `.sqlx`** |
| `PyODPS` where logic is really SQL (DataFrame ops, SQL submission) | Expressible as set-based SQL | **Push down to BigQuery SQL in Dataform** — don't reimplement in Python |
| `PyODPS` / `PYTHON` — lightweight & procedural (control flow, glue, API calls, small param-driven transforms, low data volume) | Procedural, low data | **`PythonOperator`** in Composer / Airflow |
| `PyODPS` / `PYTHON` — data-heavy (large transforms, big joins/shuffles, high row volume) | Data-intensive | **Dataflow (Beam)**, triggered by Composer — not run on the worker |
| `ODPS_SPARK` — heavy Spark code only | Genuinely distributed Spark | **Dataproc (PySpark)** — never as a default |
| `DI` / Data Integration (sync) | Source→sink movement, not SQL | **BigQuery Data Transfer / Datastream / Dataflow** |
| `DIDE_SHELL` | Arbitrary scripts | **Composer (Bash/Python operator)** — audit each |
| `VIRTUAL` / control / branch / assignment | No-op / flow control | **Airflow `EmptyOperator` / branching operators** |
| MySQL / PostgreSQL | DB operations | Keep DB operator, or route to BigQuery via **Datastream/Dataflow** |
| *(anything else)* | — | **PythonOperator, flagged for human review** |

**This table is the canonical routing matrix.** It is maintained here and
nowhere else; every other document in this set links to
[`02-architecture-and-routing.md#3-the-routing-matrix-node-type--gcp-target`](02-architecture-and-routing.md#3-the-routing-matrix-node-type--gcp-target)
rather than restating it. Update this table, not a copy.

> **Guardrail: don't process heavy data on the Composer worker.** A
> `PythonOperator` runs inside the Airflow worker's memory, which is sized for
> orchestration, not data crunching. Reimplementing a large PyODPS transform as
> in-task pandas is a classic way to OOM the worker and destabilize the whole
> environment.

---

## 4. Routing rule (encode in the classifier / AI generator, in priority order)

1. **SQL-expressible?** → Dataform.
2. **Light procedural Python?** → `PythonOperator` in Composer.
3. **Data-heavy?** → Dataflow (triggered by Composer).
4. **Actually PySpark?** → Dataproc (only then).

The FlowSpec `runtime.command` gives the node type; the volume/shape decision
for the two Python branches usually needs **human review** — flag PyODPS nodes
above a size threshold for triage rather than auto-routing them.

The ready-to-run classifier lives at
[`code/classify/route_nodes.py`](../code/classify/route_nodes.py) and implements
exactly this rule, emitting `TRIAGE` for ambiguous Python nodes.

```bash
cd code && uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
```

---

## 5. Inverting Alibaba's own tutorial (why the reverse is easier than it looks)

Alibaba publishes an official tutorial for the *opposite* migration — Airflow →
DataWorks — using the **LHM / MigrationX** tool. It loads a DAG folder, converts
it to FlowSpec JSON with an operator `typeMapping`, and imports it. **Our
migration is the exact inverse.**

| | Forward (the tutorial) | Reverse (our case) |
| --- | --- | --- |
| Direction | Airflow → DataWorks | DataWorks → Airflow / GCP |
| Source read | MigrationX Airflow Reader (boots an Airflow runtime) | **DataWorks OpenAPI — `GetNode` returns FlowSpec directly** (no runtime needed) |
| Intermediate form | DataWorks Spec (FlowSpec) JSON | Same FlowSpec — the API already emits it |
| Mapping | Airflow operator → DataWorks node type | Inverted: DataWorks node type → GCP target |
| Writer | DataWorks import (native) | **Not shipped by Alibaba — filled by AI generation** |

**Why the reverse is actually easier to source:** MigrationX ships readers only
*into* DataWorks (airflow, dolphinscheduler, aliyunemr) — there is no
DataWorks-reader / Airflow-writer. But we don't need one: the DataWorks OpenAPI
already returns FlowSpec (`GetNode.spec`), the same canonical schema the
tutorial's parser produces. We inherit Alibaba's own intermediate
representation for free over HTTPS. The only missing piece is the
FlowSpec→Airflow writer — which is exactly the step we hand to an AI agent.

---

[← Prev: 01 · Concepts](01-concepts.md) · [Index](README.md) · [Next: 03 · Migration playbook →](03-migration-playbook.md)
