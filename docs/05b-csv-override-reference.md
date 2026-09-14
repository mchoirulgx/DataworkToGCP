# 05b · CSV Override Reference

> **Goal:** a complete reference for the `migration_matrix.csv` file — the
> human-editable layer that sits between extraction (JSON) and generation (code).
> Use this to review, approve, or redirect every routing decision before any GCP
> artifacts are created.

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
| `command` | Original command / SQL text | String (informational) |
| `input_tables` | Comma-separated input table GUIDs | String (informational) |
| `output_tables` | Comma-separated output table GUIDs | String (informational) |
| `schedule` | Cron expression override | `02 00 * * *` or empty |
| `gcp_target` | Override GCP target type | `VIRTUAL`, `DATAFORM`, `TRIAGE`, `SKIP`, or empty |
| `review_notes` | Free-text notes for reviewers | String |
| `skip` | Skip this node entirely | `true` / `false` (default) |

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
uv run python code/classify/flowspec_to_csv.py \
    --flowspec-dir data/extract \
    --output data/extract/migration_matrix.csv
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
2. For each node, look up by `node_id` (or `node_name` if ID not found)
3. Apply `gcp_target` override *before* calling `route_node()`
4. Apply `schedule` override *before* building the workflow cron
5. Append `review_notes` to the generated `reviews.md` entry
6. Skip node entirely if `skip == "true"`

This means CSV overrides take priority over both JSON defaults and routing-matrix
classifications.

---

## 5. Example CSV — house-buying case study

After extraction, `flowspec_to_csv.py` produces this CSV from
`sample_flowspec_house_buying.json`:

```csv
workflow,node_id,node_name,command,input_tables,output_tables,schedule,gcp_target,review_notes,skip
house_buying_analysis,n_start,workshop_start,VIRTUAL,,,02 00 * * *,EmptyOperator,,
house_buying_analysis,n_ddl,ddl_result_table,ODPS_SQL,,result_table,02 00 * * *,Dataform .sqlx,,
house_buying_analysis,n_insert,insert_result_table,ODPS_SQL,bank_data,result_table,02 00 * * *,Dataform .sqlx,,
```

**What you see:** 3 nodes, all using the workflow-level schedule (`02 00 * * *`),
no overrides applied yet. The `gcp_target` column shows the routing-matrix
classification (EMPTY, Dataform).

**After user review — example overrides:**

```csv
workflow,node_id,node_name,command,input_tables,output_tables,schedule,gcp_target,review_notes,skip
house_buying_analysis,n_start,workshop_start,VIRTUAL,,,02 00 * * *,EmptyOperator,,true
house_buying_analysis,n_ddl,ddl_result_table,ODPS_SQL,,result_table,03 00 * * *,TRIAGE,DDL node — verify BigQuery schema before generating,false
house_buying_analysis,n_insert,insert_result_table,ODPS_SQL,bank_data,result_table,,,Dataform .sqlx,Run at 03:00 instead of 02:00,false
```

**What changed:**

| Node | Change | Reason |
| --- | --- | --- |
| `n_start` | `skip=true` | Virtual start marker — no GCP artifact needed |
| `n_ddl` | `gcp_target=TRIAGE`, `schedule=03 00 * * *` | DDL needs manual schema review; moved 1 hour later |
| `n_insert` | `schedule` cleared, `review_notes` added | Uses workflow-level schedule; notes flag the override |

---

## 6. JSON ↔ CSV relationship

| JSON field | CSV column | Relationship |
| --- | --- | --- |
| `spec.workflows[].nodes[].name` | `node_name` | Matched for lookup |
| `spec.workflows[].nodes[].id` | `node_id` | Matched for lookup (primary) |
| `spec.workflows[].nodes[].command` | `command` | Informational copy |
| `spec.workflows[].trigger.cron` | `schedule` | CSV overrides JSON |
| Routing matrix result | `gcp_target` | CSV overrides matrix |
| *(none)* | `review_notes` | CSV-only, appended to reviews.md |
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

**Next:** [06 · Tools architecture](06-tools-architecture.md)
