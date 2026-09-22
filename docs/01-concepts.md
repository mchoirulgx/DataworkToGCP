# 01 · Concepts

> **Last reviewed:** 2026-09-22

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

## 2. Vocabulary

Every term used in this guide — `FlowSpec`, `bizdate`, `ODPS`, `TRIAGE`, gate,
model, declaration, `.sqlx`, estate, domain wave — is defined in one place:

**→ [Reference · Glossary](reference/glossary.md)**

Keep it open in a second tab while you read the rest of this guide.

---

## 3. Why this is NOT a "swap product A for product B"

The single most important idea in this guide:

> **DataWorks fuses four concerns into one coupled, UI-bound product.
> GCP deliberately splits them into specialised, code-first services.
> The migration is a *decomposition*, and every DataWorks node has to be
> routed to the right GCP service.**

The concern-by-concern mapping is in
[02 · Architecture & routing](02-architecture-and-routing.md#2-architecture-shift-paradigm).

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
2. **Generate** — feed that structured metadata to the toolkit in this repo,
   which emits the GCP equivalents: Composer DAGs, Dataform `.sqlx`, and
   Python/Dataflow stubs.
3. **Validate before trusting** — every generated artifact must pass the
   automated gates and then **per-table** and **per-DAG** reconciliation before
   cut-over.

> [!IMPORTANT]
> **What is automated, and how.** The generator that ships in this repo is
> **rule-based and deterministic** — same input, same output, no model in the
> loop. It routes every node through the
> [routing matrix](02-architecture-and-routing.md), translates the SQL it can
> translate with confidence, and **flags everything else** into a
> `review/*.md` report instead of guessing.
>
> **Gemini is an optional assist, not part of the pipeline.** It is useful for
> the items the generator deliberately refuses to auto-convert — heavy PyODPS
> procedural code, unusual ODPS built-ins, shell nodes. Treat any model output
> as a *first draft*: ODPS→BigQuery semantics differ, and the gates will not
> catch a logically-wrong-but-valid query.
>
> Never paste production business logic into unmanaged tools; keep extracted
> metadata inside approved governance boundaries.

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

The toolkit compresses extraction and first-draft generation, but it does
**not** remove the High rows — those still need engineering judgment and
validation.

---

## 6. Prerequisites (what you need before you start)

- An **Alibaba Cloud** account with a **DataWorks workspace** and the
  **AccessKey** (`ALIBABA_ACCESS_KEY`, `ALIBABA_SECRET_KEY`) of a RAM user that
  has `dataworks:*` read permission (see
  [OpenAPI reference](reference/dataworks-openapi.md)).
- The **DataWorks workspace ID** (console → Workspace page).
- A **Google Cloud project** with **BigQuery**, **Dataform** and **Cloud Composer**
  enabled, and a service account for Composer.
- A machine with **Python ≥ 3.11** and **uv** installed (see
  [`code/README.md`](../code/README.md)).

---

[Index](README.md) · [Next: 02 · Architecture & routing →](02-architecture-and-routing.md)
