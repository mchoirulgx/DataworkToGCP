# Reference · Migration matrix CSV

> **Last reviewed:** 2026-09-22

> **Goal:** a complete reference for the `migration_matrix.csv` file — the
> human-editable layer that sits between extraction (JSON) and generation (code).
> Use this to review, approve, or redirect every routing decision before any GCP
> artifacts are created.

> [!IMPORTANT]
> This page is the **single source of truth** for the CSV override layer. The
> duplicate appendix that used to restate these columns has been deleted; other
> documents link here rather than repeating the table.

> **All commands on this page run from the `code/` directory**, so data paths are
> written relative to it (`../data/...`).

---

## 1. Why a CSV?

The FlowSpec JSON is the *structural* source of truth — it describes what DataWorks
has. The CSV is the *decision* layer — it captures what you want GCP to become.

| Layer | Source of truth | Edited by | Consumed by |
| --- | --- | --- | --- |
| FlowSpec JSON | Extraction API | Automated | `route_nodes.py`, `generator.py` |
| `migration_matrix.csv` | Generated from JSON | Human | `generator.py` (overrides) |

Every row in the CSV corresponds to one node. Empty override columns mean "use
the JSON default." The generator merges CSV overrides *before* applying routing
logic, so your decisions take priority.

---

## 2. CSV columns

| Column | Purpose | Values |
| --- | --- | --- |
| `workflow` | Parent workflow name | String (informational) |
| `node_id` | Node identifier (matches JSON) | String (informational) |
| `node_name` | Human-readable node name | String (informational) |
| `command` | Original DataWorks `script.runtime.command` (e.g. `ODPS_SQL`) | String (informational) |
| `input_tables` | Comma-separated input table GUIDs | String (informational) |
| `output_tables` | Comma-separated output table GUIDs | String (informational) |
| `schedule` | Cron expression override | Airflow 5-field, e.g. `02 00 * * *`, or empty |
| `gcp_target` | Override GCP target | One of the routing target strings below, or empty |
| `review_notes` | Free-text notes for reviewers | String |
| `skip` | Skip this node entirely | `true` / `false` (default) |

### 2.1 `gcp_target` — the real vocabulary

There is exactly **one** vocabulary for this column: the human-readable target
strings produced by `classify()` in
[`code/classify/route_nodes.py`](../../code/classify/route_nodes.py). These are
the values written into the generated CSV, and the same values you should use
when overriding:

| `gcp_target` value | Emitted for |
| --- | --- |
| `Dataform .sqlx` | `ODPS_SQL`, and PyODPS/Python that is really SQL (pushed down) |
| `EmptyOperator` | `VIRTUAL` (no-op / flow control) |
| `PythonOperator` | Lightweight procedural Python |
| `Dataflow (Beam)` | Data-heavy Python (kept off the Composer worker) |
| `Dataproc (PySpark)` | `ODPS_SPARK` |
| `TRIAGE` | `DIDE_SHELL` and anything with no automatic mapping |
| `Branching / loop operators` | `DIDE_DO_WHILE`, `DIDE_FOR_EACH` |
| `BigQuery DTS / Datastream / Dataflow` | `DATA_INTEGRATION`, `DI` |
| `Datastream / Dataflow` | `MYSQL`, `POSTGRESQL` |
| `PythonOperator (review)` | Unknown command — human review |

> [!IMPORTANT]
> `VIRTUAL`, `DATAFORM` and `SKIP` are **not** `gcp_target` values. `VIRTUAL` is
> a `command` value (its target is `EmptyOperator`), and skipping is controlled
> by the separate `skip` column, not by `gcp_target`.

How the generator reads the column (`build_models_and_decls` in
[`code/migrate/generator.py`](../../code/migrate/generator.py)):

- The column is **free text** — a non-empty value is taken verbatim as the
  target, overriding the auto-routed one. There is no validation, so a typo
  silently becomes an unrecognised target.
- Only `Dataform .sqlx` causes a model to be generated.
- `TRIAGE` writes the node to the review report and generates nothing for it.
- Any other value is recorded in the review report as *not auto-generated*.

---

## 3. User workflow

```
1. Run extraction   →  JSON files written
2. Auto-generate CSV →  migration_matrix.csv (all nodes, defaults)
3. User reviews CSV  →  edits gcp_target, schedule, review_notes, skip
4. Run generator     →  CSV overrides applied before code generation
```

**Step 2 is automatic.** After extraction, `extract_dataworks.py` calls
`flowspec_to_csv.py` to produce `migration_matrix.csv` in the same output
directory. You can also regenerate it manually:

```bash
uv run classify/flowspec_to_csv.py \
    --extract-dir ../data/extract \
    --csv-out ../data/extract/migration_matrix.csv
```

**Step 3 is manual.** Open the CSV in any editor (Excel, Google Sheets, VS Code)
and modify:

- **`gcp_target`** — force a specific target (e.g. `TRIAGE` for nodes you want
  to review manually before mapping)
- **`schedule`** — override the workflow-level cron with a node-specific schedule
- **`review_notes`** — add context for reviewers ("this node has downstream
  dependencies we haven't captured yet")
- **`skip`** — set to `true` to exclude a node from generation entirely

**Step 4 is automatic.** The generator reads the CSV (if present) and applies
overrides early in the node loop — before routing decisions are made.

---

## 4. How overrides are applied

The generator merges CSV overrides in this order:

1. Load CSV from `--csv` path or auto-detect `migration_matrix.csv` in
   the flowspec/extract directory
2. For each node, look up the override row by `node_id`
3. Apply `gcp_target` override *before* calling `route_node()`
4. Apply `schedule` override *before* building the workflow cron
5. Append `review_notes` to the generated `review/*.md` entry
6. Skip node entirely if `skip == "true"`

This means CSV overrides take priority over both JSON defaults and routing-matrix
classifications.

---

## 5. Example CSV — house-buying case study

> **Canonical example:**
> [`code/extract/sample_migration_matrix.csv`](../../code/extract/sample_migration_matrix.csv)
> is committed in the repo. It is the output of running `flowspec_to_csv.py`
> against `sample_flowspec_house_buying.json`, and it is the file to copy when
> you want a known-good starting point.

Its contents, reproduced here:

```csv
workflow,node_id,node_name,command,input_tables,output_tables,schedule,gcp_target,review_notes,skip
house_buying_analysis,n_start,workshop_start,VIRTUAL,,,02 00 * * *,EmptyOperator,,
house_buying_analysis,n_ddl,ddl_result_table,ODPS_SQL,,result_table,02 00 * * *,Dataform .sqlx,,
house_buying_analysis,n_insert,insert_result_table,ODPS_SQL,bank_data,result_table,02 00 * * *,Dataform .sqlx,,
```

**What you see:** 3 nodes, all using the workflow-level schedule (`02 00 * * *`
— 00:02 daily), no overrides applied yet. The `gcp_target` column holds the
auto-routed targets: `EmptyOperator` for the `VIRTUAL` node and `Dataform .sqlx`
for the two `ODPS_SQL` nodes.

**After user review — example overrides:**

```csv
workflow,node_id,node_name,command,input_tables,output_tables,schedule,gcp_target,review_notes,skip
house_buying_analysis,n_start,workshop_start,VIRTUAL,,,02 00 * * *,EmptyOperator,,true
house_buying_analysis,n_ddl,ddl_result_table,ODPS_SQL,,result_table,03 00 * * *,TRIAGE,DDL node — verify BigQuery schema before generating,false
house_buying_analysis,n_insert,insert_result_table,ODPS_SQL,bank_data,result_table,,Dataform .sqlx,Inherits the workflow-level schedule,false
```

Every row has the same 10 fields as the header — keep it that way, or
`csv.DictReader` will misalign the columns.

**What changed:**

| Node | Change | Reason |
| --- | --- | --- |
| `n_start` | `skip=true` | Virtual start marker — no GCP artifact needed |
| `n_ddl` | `gcp_target=TRIAGE`, `schedule=03 00 * * *` | DDL needs manual schema review; moved one minute later (00:02 → 00:03) |
| `n_insert` | `schedule` cleared, `review_notes` added | Falls back to the workflow-level schedule; the note records why |

---

## 6. JSON ↔ CSV relationship

| JSON field | CSV column | Relationship |
| --- | --- | --- |
| `spec.workflows[].nodes[].name` | `node_name` | Matched for lookup |
| `spec.workflows[].nodes[].id` | `node_id` | Matched for lookup (primary) |
| `spec.workflows[].nodes[].script.runtime.command` | `command` | Informational copy |
| `spec.workflows[].trigger.cron` | `schedule` | Converted to the Airflow 5-field form; CSV overrides JSON |
| Routing matrix result | `gcp_target` | CSV overrides matrix |
| *(none)* | `review_notes` | CSV-only, appended to the `review/*.md` report |
| *(none)* | `skip` | CSV-only, excludes from generation |

---

## 7. When to use CSV overrides

The CSV layer is trivial for small examples (like the house-buying case study).
It becomes essential when migrating hundreds of nodes where:

- Some nodes need manual `TRIAGE` before final routing
- Downstream dependencies require schedule adjustments
- Certain nodes should be skipped (deprecated, test-only, etc.)
- You want to add review notes for downstream engineers

---

[← Prev: FlowSpec JSON](flowspec-json.md) · [Index](../README.md)
