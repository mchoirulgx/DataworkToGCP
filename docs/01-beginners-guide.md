# 01 · Beginners Guide

> **Goal:** understand, in plain English, what each piece of the stack is, why
> we are moving, and the vocabulary you need to follow the rest of this guide.

---

## 1. The players

Imagine you are building a daily report for a house-buying business. Raw data
arrives every night; someone must **(a)** copy it into the data warehouse,
**(b)** transform it with SQL, **(c)** run everything on a schedule, and
**(d)** keep track of what data exists. In the old world, one tool did all four
things. In the new world, four specialised tools share the work.

### Alibaba Cloud DataWorks (where we are today)

DataWorks is an **all-in-one data development platform** that runs on top of
**MaxCompute** (Alibaba's petabyte-scale data warehouse). Inside one visual,
drag-and-drop web UI you can:

- build a **pipeline** (a sequence of steps called *nodes* — SQL, Python, shell, sync…),
- schedule it (the *Operation Center*),
- copy data between systems (*Data Integration*),
- and manage metadata and governance (*Data Map*).

That is very convenient, but everything is coupled together and lives in the
UI — hard to version-control, hard to test, hard to scale to thousands of jobs.

### The Google Cloud targets (where we are going)

Google splits the same job across four purpose-built, **code-first** services:

| Service | What it is | What it does |
| --- | --- | --- |
| **Cloud Composer** | Google's managed **Apache Airflow** | *Orchestration only.* It schedules tasks and runs them in the right order (as Python files called DAGs), retries failures, and triggers other services. It does **not** transform data itself. |
| **Dataform** | Google's in-BigQuery ELT framework | *SQL transformations as code.* You write `.sqlx` files; Dataform works out the dependency order between tables automatically (`ref()`), and adds Git + testing. |
| **BigQuery** | Google's serverless data warehouse | *The compute + storage engine.* This replaces MaxCompute. |
| **Datastream / Data Transfer / Dataflow** | Data movement services | Copy data between systems. Replaces Data Integration. |

**Analogy.** Think of a restaurant kitchen. DataWorks is a single chef who
does everything in one pan. The GCP stack is a kitchen with stations: the
*chef* (Composer) only decides what gets cooked next, the *recipe book*
(Dataform) holds the recipes, the *oven* (BigQuery) does the actual cooking,
and the *delivery van* (Dataflow/Datastream) brings ingredients in.

---

## 2. Vocabulary (plain-English glossary)

| Term | Meaning | See also |
| --- | --- | --- |
| **Workflow** | A container for one business process — a set of steps with a schedule. In DataWorks it is called a *CycleWorkflow* (scheduled) or *ManualWorkflow* (manual). | §03 |
| **Node** | One step inside a workflow: an ODPS_SQL node, a Python node, a shell node, a sync node… | §03 |
| **FlowSpec** | Alibaba's open JSON "blueprint" that describes a workflow/node: code, engine, dependencies, trigger. The API returns it, so we never reverse-engineer it. | [aliyun/dataworks-spec](https://github.com/aliyun/dataworks-spec) |
| **DAG** | *Directed Acyclic Graph* — the standard way to describe task order (A before B before C, no loops). Airflow DAGs are Python files; Dataform builds a DAG out of `.sqlx` dependencies. | §04 |
| **ODPS** | The SQL dialect / engine of MaxCompute. | §05 |
| **bizdate** | A DataWorks variable = the *business date* of a run (usually run date − 1 day). Its semantics differ from Airflow's; mishandling causes off-by-one bugs. | §05 |
| **OpenAPI** | Alibaba's REST API + SDKs for talking to DataWorks programmatically (instead of clicking the UI). | §03 |
| **Reconciliation** | Proving the migrated data matches the original: same row counts, same values, same schedule. | §03 |

---

## 3. Why this is NOT a "swap product A for product B"

The single most important idea in this guide:

> **DataWorks fuses four concerns into one coupled, UI-bound product.
> GCP deliberately splits them into specialised, code-first services.
> The migration is a *decomposition*, and every DataWorks node has to be
> routed to the right GCP service.**

| Concern | DataWorks (one coupled UI) | GCP (decoupled, code-first) |
| --- | --- | --- |
| Orchestration / scheduling | Operation Center | Cloud Composer (Airflow) |
| SQL transformation | Data Studio (visual SQL) | Dataform (`.sqlx`) |
| Compute / warehouse | MaxCompute | BigQuery |
| Data integration (sync) | Data Integration | BigQuery Data Transfer / Datastream / Dataflow |
| Governance / metadata | Data Map | Dataplex |

**Consequences for the migration:**

- One DataWorks **workflow** → one Airflow **DAG**.
- A **node** does *not* map to one Airflow task. An `ODPS_SQL` node becomes a
  **Dataform model**; a `VIRTUAL` node becomes an **EmptyOperator**; a heavy
  Python node becomes a **Dataflow job**.
- Sometimes **several nodes collapse into one** (e.g. a `CREATE TABLE` node plus
  an `INSERT` node become a single Dataform table model), because Dataform owns
  the DDL.

### A key design principle: let Dataform own the in-BigQuery DAG

Dataform resolves table-to-table dependencies through `ref()`. **Do not**
recreate every intra-BigQuery SQL dependency as an Airflow edge — that
duplicates the dependency graph and fights the tool.

> Correct division: **Composer orchestrates across systems and triggers
> Dataform tags; Dataform manages ordering within BigQuery.**

If your whole workflow is SQL on BigQuery tables, the Airflow DAG can be as
small as *compile → run* (see the [case study](04-case-study-house-buying.md)).

---

## 4. The big picture approach

We never open thousands of pipelines in the UI and rebuild each by hand.
Instead we take a **metadata-driven** path:

1. **Extract** — use the DataWorks OpenAPI to programmatically pull the metadata
   that fully describes each pipeline: schedule, dependency graph, actual
   SQL/Python code, and table schemas.
2. **Generate (AI-assisted)** — feed that structured metadata to **Gemini**
   which drafts the GCP equivalents: Composer DAGs, Dataform `.sqlx`,
   Python/Dataflow jobs.
3. **Validate before trusting** — every generated artifact is reviewed and must
   pass **per-table** and **per-DAG** reconciliation before cut-over. The AI
   does the mechanical ~80%; engineers own the last mile and sign-off.

> **Guardrail.** Treat AI output as a *first draft*, not ground truth —
> especially ODPS→BigQuery SQL, where semantics differ. Never paste production
> business logic into unmanaged tools; keep extracted metadata inside approved
> governance boundaries.

---

## 5. Where the effort actually goes (set expectations honestly)

| Workstream | Why it's hard | Relative effort |
| --- | --- | --- |
| Metadata extraction | Mostly mechanical once pagination and the two API versions are handled. | **Low** |
| SQL translation (ODPS → BigQuery) | No native translator; HiveQL-proxy plus manual last-mile and validation. | **High** |
| Non-SQL node types (PyODPS / Shell / Data Integration) | They don't map to Dataform; each needs a target. | **High** |
| Scheduling & parameters | `bizdate` / self-dependency semantics differ from Airflow; off-by-one landmines. | **Medium–High** |
| Schema & partition mapping | Type collapse and partition-model mismatch need design, not lookup. | **Medium** |
| Validation & reconciliation | Mandatory in regulated environments; proves parity per table and per DAG. | **Medium** |

AI assistance compresses extraction and first-draft generation, but it does
**not** remove the High rows — those still need engineering judgment and
validation.

---

## 6. Prerequisites (what you need before you start)

- An **Alibaba Cloud** account with a **DataWorks workspace** and the
  **AccessKey** (`ALIBABA_ACCESS_KEY`, `ALIBABA_SECRET_KEY`) of a RAM user that
  has `dataworks:*` read permission (see [chapter 07](07-appendices-and-resources.md)).
- The **DataWorks workspace ID** (console → Workspace page).
- A **Google Cloud project** with **BigQuery**, **Dataform** and **Cloud Composer**
  enabled, and a service account for Composer.
- A machine with **Python ≥ 3.11** and **uv** installed (see [Quick start](../README.md#quick-start)).

---

**Next:** [02 · Architecture & routing matrix](02-architecture-and-routing.md)
