# DataWorks → GCP Pipeline Migration Guide

A playbook to migrate enterprise data pipelines from GUI based **Alibaba Cloud DataWorks** to code driven **Google Cloud analytics platform**:

| Concern | DataWorks (source) | Google Cloud (target) |
| --- | --- | --- |
| Orchestration / scheduling | Operation Center | **Cloud Composer** (managed Apache Airflow) |
| SQL transformation | Data Studio (visual SQL) | **Dataform** (`.sqlx`) |
| Warehouse / compute | MaxCompute | **BigQuery** |
| Data integration (sync) | Data Integration | **BigQuery Data Transfer / Datastream / Dataflow** |
| Governance / metadata | Data Map | **Knowledge Catalog** |

> This guide is a hands-on implementation of *"DataWorks → GCP Pipeline
> Migration Guide v3.1"* (working draft). It adds a complete worked
> example, runnable Python code, and a curated knowledge base.

How the pipeline works:
1. Assume you have existing data pipeline run at Alicloud's Dataworks. The aim is to do semi automatic migration rather than coding it by hand
2. Collect current pipeline metadata by reading Dataworks Open API
3. Save extracted metadata and flows inti a csv file. Human needs to verify this csv before continuing the process
4. Use small py script to read csv and pass it as context to Gemini. The LLM translate the pipeline into GCP data pipeline code.
5. The outcome is Airflow + Dataform/ SQL scripts/ Py code/ Dataflow; whichever is suitable.
6. Deploy the conversion result into GCP.
7. Test the new pipline in new home

---

## Migration pipeline at a glance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ALIBABA CLOUD DATAWORKS                                │
│              3,000 data pipelines · runs on MaxCompute                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       OPENAPI EXTRACTION                                   │
│  ┌─────────────────────────────┐  ┌──────────────────────────────────────┐ │
│  │  2024-05-18                 │  │  2020-05-18                          │ │
│  │  Workflows & Nodes          │  │  Table metadata (Data Map)           │ │
│  │  (FlowSpec)                 │  │  DDL · columns · types · partitions  │ │
│  └─────────────────────────────┘  └──────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     STRUCTURED METADATA                                    │
│          Schedules · dependency graph · code payload · DDL                 │
│                            (JSON / CSV)                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     AI-ASSISTED GENERATION                                 │
│              Gemini · metadata as context · agents plan → generate          │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐ │
│  │  Cloud Composer      │  │  Dataform .sqlx     │  │  Python / Dataflow  │ │
│  │  DAGs (orchestration)│  │  (BigQuery SQL)     │  │  (procedural/heavy) │ │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     VALIDATE & RECONCILE                                   │
│              per-table + per-DAG parity vs source                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DEPLOY TO GCP                                          │
│            BigQuery + Cloud Composer + Dataform                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1 — Source: Alibaba Cloud DataWorks

DataWorks is a GUI based data orchestration, transformation, and
governance. For this migration, we extracted metadata from dataworks pipelines using the OpenAPI.

The sample pipeline to be migrated is here: https://www.alibabacloud.com/help/en/dataworks/user-guide/dataworks-for-application-development

### Step 2 — OpenAPI Extraction

Two API clients are **mandatory** — they live in different SDK packages with
different endpoints. Using the wrong one silently returns errors.

| API version | What it pulls | SDK operations |
| --- | --- | --- |
| `2024-05-18` | Workflows, nodes, FlowSpec, dependencies | `ListWorkflows`, `GetWorkflow`, `ListNodes`, `GetNode` |
| `2020-05-18` | Table metadata (DDL, columns, partitions) | `GetMetaTableColumn`, `GetMetaTablePartition` |

**Run the extractor:**

```bash
cd code
uv sync                                          # install SDKs
uv run extract/extract_dataworks.py \
    --project-id YOUR_PROJECT_ID \
    --name-filter house_buying                   # optional: filter workflows
```

Output: `workflow_<id>.json` + `node_<id>.json` files in `data/extract/`.

### Step 3 — Structured Metadata (JSON / CSV)

The extractor produces JSON files. The toolkit also generates a
**`migration_matrix.csv`** — a human-editable override layer where you can
review and redirect routing decisions before code generation.

```bash
# Auto-generates CSV after extraction (no manual step needed)
# Or regenerate manually:
uv run python classify/flowspec_to_csv.py \
    --flowspec-dir data/extract \
    --output data/extract/migration_matrix.csv
```

Edit the CSV to override `gcp_target`, `schedule`, or add `review_notes`.
See [05b · CSV override reference](docs/05b-csv-override-reference.md).

### Step 4 — AI-Assisted Generation

The generator uses extracted metadata as context to produce GCP artifacts.
Three output types, routed by the [routing matrix](docs/02-architecture-and-routing.md):

| Output | What it is | When |
| --- | --- | --- |
| **Cloud Composer** | Airflow DAGs (`.py`) | Orchestration nodes |
| **Dataform .sqlx** | BigQuery SQL models | `ODPS_SQL`, SQL-ish `PYODPS` |
| **Python / Dataflow** | Beam pipelines | Heavy procedural `PYODPS` |

```bash
uv run migrate/migrate.py generate \
    --flowspec data/extract/sample_flowspec_house_buying.json
```

### Step 5 — Validate & Reconcile

Before deployment, every generated artifact is checked:

- **SQL compilation** — `dataform compile` catches invalid GoogleSQL
- **Row-count parity** — compares BigQuery output vs MaxCompute source
- **Per-DAG review** — schedules, dependencies, variable translation

```bash
uv run migrate/migrate.py compile    # SQL syntax check
uv run migrate/migrate.py run        # execute in BigQuery
uv run migrate/migrate.py verify     # row-count parity
```

### Step 6 — Deploy to GCP

Output lands in `data/generated/`:

```
definitions/
  marts/*.sqlx          ← Dataform models (your tables)
  sources/*.sqlx        ← source declarations
dags/<workflow>.py      ← Cloud Composer DAGs
workflow_settings.yaml  ← Dataform config
```

Deploy the Dataform project via Git, and the DAGs via Cloud Composer.

---

## Contents

- **[01 · Beginners guide](docs/01-beginners-guide.md)** — basic concepts, terminology glossary, and why this is a *decomposition*, not a 1:1 copy.
- **[02 · Architecture & routing matrix](docs/02-architecture-and-routing.md)** — how DataWorks concepts map to GCP services, and how to route every node type.
- **[03 · Migration steps](docs/03-migration-steps.md)** — the 6-step, metadata-driven playbook (extract → structure → generate → review → validate → cut over).
- **[04 · Case study: house-buying analysis](docs/04-case-study-house-buying.md)** — a real example from Alibaba's tutorial, converted end-to-end with code.
- **[05 · FlowSpec JSON reference](docs/05-flowspec-json-reference.md)** — every property of the extractor's FlowSpec output, with values and explanations.
- **[05b · CSV override reference](docs/05b-csv-override-reference.md)** — the human-editable layer for reviewing and redirecting routing decisions before code generation.
- **[06 · Tools architecture](docs/06-tools-architecture.md)** — how every module fits together, data flow, configuration, and how to extend the system.
- **[07 · Appendices & resources](docs/07-appendices-and-resources.md)** — variable mapping, MaxCompute→BigQuery type map, API cheat-sheet, and external resources.

## Repository layout

```
.
├── README.md                              # this file
├── docs/
│   ├── 01-beginners-guide.md
│   ├── 02-architecture-and-routing.md
│   ├── 03-migration-steps.md
│   ├── 04-case-study-house-buying.md
│   ├── 05-flowspec-json-reference.md
│   ├── 05b-csv-override-reference.md
│   ├── 06-tools-architecture.md
│   └── 07-appendices-and-resources.md
├── code/                                  # runnable Python + configs
│   ├── pyproject.toml                     # uv project (SDK dependencies)
│   ├── extract/
│   │   ├── extract_dataworks.py           # REST API extraction (2 clients)
│   │   └── sample_flowspec_house_buying.json
│   ├── classify/
│   │   └── route_nodes.py                 # routing-matrix classifier
│   ├── migrate/                           # automatic migration (generator + gates)
│   │   ├── migrate.py                     # orchestrator CLI (all/generate/compile/run/verify)
│   │   ├── generator.py                   # FlowSpec -> .sqlx + Composer DAG + review report
│   │   ├── translator.py                  # ODPS -> BigQuery SQL/type translation
│   │   └── verifier.py                    # BigQuery row-count verification gate
│   └── gcp/
│       ├── dags/house_buying_daily.py     # Cloud Composer DAG
│       └── dataform/                      # Dataform project (.sqlx)
└── data/                                  # extraction output + generated project (git-ignored)
```

## Quick start

```bash
cd code
uv sync                 # install the Alibaba Cloud SDKs
uv run extract/extract_dataworks.py --name-filter house_buying
uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json
```

The `uv` command manages the Python environment for you
([install uv](https://docs.astral.sh/uv/getting-started/installation/)), so you
never touch `pip` or `venv` by hand.

## Automatic migration (minimal human review)

`code/migrate/` turns the extracted FlowSpec into **GCP artifacts and runs them
in BigQuery** — no hand-written SQL. One command:

```bash
cd code
uv run migrate/migrate.py all --flowspec extract/sample_flowspec_house_buying.json
```

The gated pipeline is `generate → compile → run → verify`, and each gate
**stops on failure**:

| Gate | What happens |
| --- | --- |
| `generate` | Routes every node; `ODPS_SQL`/SQL-ish `PYODPS` become Dataform `.sqlx` models + source declarations; a Composer DAG is emitted per workflow. Anything needing a human (TRIAGE commands, unresolved `${vars}`, ODPS built-ins that differ in BigQuery, DDL-only tables) goes to `review/*.md` instead of blocking. |
| `compile` | `dataform compile` — fails on SQL that is not valid GoogleSQL. |
| `run` | `dataform run` — creates the tables and runs assertions in BigQuery. |
| `verify` | Row-counts every generated table in BigQuery — fails on missing/empty tables. |

Output lands in `GENERATED_OUTPUT_DIR` (`../data/generated`, git-ignored):
`definitions/{marts,sources}/*.sqlx`, `dags/<workflow>.py`, `workflow_settings.yaml`,
`.df-credentials.json` (gcloud ADC), and `review/*.md`.

**Prerequisites:** a GCP project with BigQuery enabled, gcloud ADC
(`gcloud auth application-default login`), raw source tables already loaded into
BigQuery (the Data Integration → DTS/Datastream/Dataflow sync is a separate
pipeline), the Dataform CLI (`npm i -g @dataform/cli`, binary `dataform`), and
the env vars in [`code/.env.example`](code/.env.example). For a real estate you
also need Alibaba credentials to extract metadata live — otherwise pass the
sample FlowSpec as shown above.

**Known limits (validated by the gates, not by a human):** non-trivial ODPS
built-ins (`DATEADD`, `SPLIT_PART`, UDFs, `EXPLODE`, …) are *flagged* in the
review report rather than silently rewritten; schedules are stripped of seconds
mechanically and marked "RE-VERIFY" at cutover (docs/03 step 5); parity with
live MaxCompute still requires source access. See
[`code/README.md`](code/README.md) for details.

## The 60-second summary

1. **DataWorks is one coupled UI.** It fuses orchestration, transformation,
   integration and governance into one drag-and-drop product.
2. **GCP deliberately splits those concerns** into code-first services. So the
   migration is a *decomposition*: each DataWorks node must be routed to the
   right GCP service (see the [routing matrix](docs/02-architecture-and-routing.md)).
3. **Extract metadata via the DataWorks REST API, not the UI.** Two API versions are
   needed: `2024-05-18` for workflows/nodes (FlowSpec), `2020-05-18` for table
   DDL. Scale to thousands of jobs with pagination + checkpointing.
4. **Generate GCP artifacts with AI assistance** (Gemini) using the extracted
   metadata as context, then **review and reconcile** — per-table and per-DAG —
   before cut-over.
5. **Watch the three classic traps:** ODPS→BigQuery SQL semantics, the
   `${bizdate}` off-by-one, and string partition columns that don't map to
   BigQuery partitioning.

## How to use this guide

- **New to the stack?** Read chapter 01 first, then follow chapter 04 as a hands-on walkthrough.
- **Engineer planning the migration?** Chapter 03 is the playbook; chapter 02 is the design reference.
- **Running the code?** See [`code/README.md`](code/README.md).

---

*License: this repository contains original documentation and code, plus a
rewrite of concepts from publicly available Alibaba Cloud / Google Cloud
documentation. Verify all APIs and product capabilities against the current
official docs at execution time (the API ecosystem moves fast).*
