# Reference · Glossary

> **Last reviewed:** 2026-09-22

Plain-English definitions for every term used across this guide.

> [!IMPORTANT]
> This page is the **single source of truth** for terminology. Other documents
> link here rather than redefining terms inline.

---

## Source platform (DataWorks / MaxCompute)

| Term | Meaning |
| --- | --- |
| **DataWorks** | Alibaba Cloud's all-in-one, UI-driven data development platform. Fuses orchestration, SQL transformation, data integration and governance into one product. |
| **MaxCompute** | Alibaba's petabyte-scale data warehouse — the compute/storage engine underneath DataWorks. Replaced by BigQuery. |
| **ODPS** | The SQL dialect and engine of MaxCompute. `ODPS_SQL` is the DataWorks node type that runs it. |
| **PyODPS** | Python nodes running against MaxCompute. Some are thin SQL wrappers; some are heavy procedural code. The two route to different GCP targets. |
| **Workflow** | A container for one business process: a set of steps plus a schedule. DataWorks calls a scheduled one a *CycleWorkflow*; the manual/ad-hoc kind appears as `ManualWorkflow` in the console and `OfflineWorkflow` in some FlowSpec payloads. |
| **Node** | One step inside a workflow — an `ODPS_SQL` node, a `PYODPS` node, a `DIDE_SHELL` node, a `VIRTUAL` node, a sync node. |
| **FlowSpec** | Alibaba's open JSON blueprint describing a workflow or node: code, engine, dependencies, trigger. Returned directly by the API, so it never has to be reverse-engineered. See [FlowSpec reference](flowspec-json.md). |
| **OpenAPI** | Alibaba's REST API + SDKs for driving DataWorks programmatically instead of clicking the UI. See [OpenAPI reference](dataworks-openapi.md). |
| **bizdate** | A DataWorks variable holding the *business date* of a run, usually run date − 1 day. Its semantics differ from Airflow's. See [variables & macros](variables-and-macros.md). |

## Target platform (Google Cloud)

| Term | Meaning |
| --- | --- |
| **Cloud Composer** | Google's managed Apache Airflow. *Orchestration only* — schedules tasks, enforces order, retries, triggers other services. Does not transform data itself. |
| **Dataform** | Google's in-BigQuery ELT framework. SQL transformations as code. |
| **BigQuery** | Google's serverless data warehouse. Replaces MaxCompute. |
| **Dataplex** | Governance and metadata. Replaces Data Map. |
| **DAG** | *Directed Acyclic Graph* — the standard way to express task order with no loops. Airflow DAGs are Python files; Dataform derives its DAG from `ref()` calls. |
| **`.sqlx`** | A Dataform source file: SQL plus a small config block. Compiles to a BigQuery table, view or assertion. |
| **Model** | A `.sqlx` file that **produces** a table or view. Lives in `definitions/marts/`. |
| **Declaration** | A `.sqlx` file that merely **declares** an already-existing source table so `ref()` can point at it. Produces nothing. Lives in `definitions/sources/`. |
| **`ref()`** | The Dataform function that references another model or declaration. Dataform uses these calls to derive dependency order automatically. |
| **Assertion** | A Dataform data-quality check that fails the run when violated. |

## This toolkit

| Term | Meaning |
| --- | --- |
| **Routing** | Deciding which GCP service each DataWorks node becomes. Governed by the [routing matrix](../02-architecture-and-routing.md). |
| **Routing matrix** | The lookup table mapping node type → GCP target. |
| **TRIAGE** | A routing outcome meaning *no automatic target — a human must decide*. TRIAGE items are written to the generated `review/*.md` report. They do **not** stop the pipeline. |
| **Gate** | One stage of the migration pipeline that must pass before the next runs: `generate → compile → run → verify`. A failing gate stops the pipeline. |
| **Review report** | `review/*.md`, emitted by the generator. Lists only items that need human attention. |
| **Migration matrix** | `migration_matrix.csv` — the human-editable override layer between extraction and generation. See [migration matrix CSV](migration-matrix-csv.md). |
| **Reconciliation** | Proving migrated data matches the original: same row counts, same values, same schedule. |
| **Estate** | The complete set of pipelines being migrated (e.g. "a 3,000-pipeline estate"). |
| **Domain wave** | One batch of related workflows migrated and cut over together, rather than migrating everything at once. |
| **Cutover** | The moment a workflow's production schedule moves from DataWorks to Cloud Composer. |

---

[← Prev: Reference index](README.md) · [Index](../README.md) · [Next: Variables & macros →](variables-and-macros.md)
