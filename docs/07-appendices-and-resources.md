# 07 · Appendices & Resources

> **Goal:** every reference table and external link you need to actually run
> this migration — variables, data types, API cheat-sheet, and a curated
> knowledge base.

---

## Appendix A · DataWorks variable → Airflow macro

> **Indicative mapping.** Validate against your actual DataWorks scheduling
> configuration and timezone before relying on it.

| DataWorks variable | Typical meaning | Airflow equivalent (indicative) |
| --- | --- | --- |
| `${bizdate}` | Business date = run date − 1 day (`yyyymmdd`) | `{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}` |
| `${yyyymmdd}` | Run/scheduled date (no dash) | `{{ ds_nodash }}` |
| `$[yyyymmdd]` | Brackets = supports date/time arithmetic | `macros.ds_add` / `data_interval` with offset |
| `${cyctime}` | Cycle (scheduled) time, incl. hour/min | `{{ logical_date }}` / `{{ data_interval_start }}` |
| Self / cross-cycle dependency | Depends on previous cycle's instance | `depends_on_past=True` or prior-run sensor |

> **The `bizdate` landmine.** DataWorks business date ≠ Airflow `logical_date` /
> `data_interval` semantics. A naive copy produces **off-by-one-day** errors
> across every migrated pipeline. Translate deliberately — never copy-paste.

---

## Appendix B · MaxCompute → BigQuery type map

| MaxCompute | BigQuery | Notes |
| --- | --- | --- |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | `INT64` | All integer widths collapse to INT64 |
| `FLOAT` / `DOUBLE` | `FLOAT64` | Single precision folds into FLOAT64 |
| `DECIMAL(p,s)` | `NUMERIC` / `BIGNUMERIC` | Choose by precision/scale; BIGNUMERIC for wide ranges |
| `STRING` / `VARCHAR` / `CHAR` | `STRING` | Watch source length limits |
| `DATETIME` | `DATETIME` or `TIMESTAMP` | Millisecond → microsecond; pick per timezone handling |
| `DATE` | `DATE` | Direct |
| `TIMESTAMP` | `TIMESTAMP` | Verify timezone semantics |
| `BOOLEAN` | `BOOL` | Direct |
| `BINARY` | `BYTES` | Direct |
| `MAP` / `ARRAY` / `STRUCT` | `ARRAY` / `STRUCT` | Remodel; BigQuery has no native MAP — use repeated STRUCT |
| Partition column (`pt`/`ds` string) | Partitioning / clustering | String partitions don't map to native partitioning — **redesign** |

**Two areas need real design (not lookup):**

1. **Type collapse & precision** — see the notes above.
2. **Partition-model mismatch** — MaxCompute *string* partitions such as
   `pt/ds='20240101'` do not map onto BigQuery's `DATE/TIMESTAMP/INT`
   partitioning. Many become **clustering keys** or require partition redesign.

---

## Appendix C · OpenAPI cheat-sheet

### Two clients (mandatory)

| Purpose | Package | Operations |
| --- | --- | --- |
| Orchestration | `alibabacloud-dataworks-public20240518` | `ListWorkflows`, `GetWorkflow`, `ListNodes`, `GetNode`, `ListWorkflowDefinitions`, `GetWorkflowDefinition` |
| Table metadata | `alibabacloud-dataworks-public20200518` | `GetMetaTableColumn`, `GetMetaTablePartition`, `GetMetaTableBasicInfo`, `ListTables` |

### Key request fields (SDK v8+/current)

| Operation | Request fields | Response access |
| --- | --- | --- |
| `ListWorkflows` | `project_id` (req), `env_type`, `name`, `page_number`, `page_size` | `resp.body.paging_info.workflows` |
| `GetWorkflow` | `env_type`, `id` | `resp.body.workflow` (trigger, tasks, parameters, dependencies) |
| `ListNodes` | `project_id` (req), `container_id` (workflow id), `page_number`, `page_size` | `resp.body.paging_info.nodes` |
| `GetNode` | `project_id`, `id` | `resp.body.node.spec` (**FlowSpec string** — parse JSON) |
| `GetMetaTableColumn` | `table_guid` (e.g. `odps.proj.table`), `page_num`, `page_size` | `resp.body.data.column_list` |
| `GetMetaTablePartition` | `table_guid`, `page_number`, `page_size` | `resp.body.data.data_entity_list` (MaxCompute/EMR only) |

> **Note:** `Id` was `Long` in SDKs < 8.0.0 and is `String` in ≥ 8.0.0 for the
> node APIs — cast defensively.

---

## Appendix D · Error codes & resilience

| Error | Meaning | Handling |
| --- | --- | --- |
| `Throttling.User` / `Throttling.API` | QPS limit reached | Static pacing + exponential backoff (`2s → 4s → 8s`), then retry same ID |
| `Invalid.Tenant.ConnectionNotExists` | Wrong tenant/connection | Check workspace/tenant config |
| Result too large (>10,000 rows / >10 MB) | Query-result display limit | Use Tunnel download / export instead |

For a large estate, raise a support ticket to **temporarily lift the
`dataworks-public` QPS limit** during the migration window.

---

## Appendix E · External resources (knowledge base)

### Alibaba Cloud (source platform)

- **Home-buying tutorial (the case study source):**
  https://www.alibabacloud.com/help/en/dataworks/user-guide/dataworks-for-application-development
- **Create tables & upload data (bank_data / result_table):**
  https://www.alibabacloud.com/help/en/dataworks/create-tables-and-upload-data
- **Create a workflow (nodes & dependencies):**
  https://www.alibabacloud.com/help/en/dataworks/create-a-workflow-1
- **FlowSpec official spec & MigrationX:** https://github.com/aliyun/dataworks-spec
- **OpenAPI portal (SDK download / debug):** https://api.alibabacloud.com/api-tools/sdk/dataworks-public
- **API `2024-05-18` overview:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/api-dataworks-public-2024-05-18-overview
- **API `2020-05-18` overview:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/api-dataworks-public-2020-05-18-overview
- **GetNode:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/api-dataworks-public-2024-05-18-getnode
- **GetWorkflow:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/api-dataworks-public-2024-05-18-getworkflow
- **GetMetaTableColumn:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/api-dataworks-public-2020-05-18-getmetatablecolumn
- **OpenAPI limits & billing:** https://www.alibabacloud.com/help/en/dataworks/developer-reference/use-dataworks-openapi
- **Python SDK packages:** https://pypi.org/project/alibabacloud-dataworks-public20240518/ · https://pypi.org/project/alibabacloud-dataworks-public20200518/

### Google Cloud (target platform)

- **Cloud Composer (managed Airflow):** https://cloud.google.com/composer
- **Airflow Google Dataform operators:**
  https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataform.html
- **`apache-airflow-providers-google` on PyPI:** https://pypi.org/project/apache-airflow-providers-google/
- **Dataform overview:** https://cloud.google.com/dataform
- **Dataform: create tables (`.sqlx` reference):** https://docs.cloud.google.com/dataform/docs/create-tables
- **Dataform quickstart (connect a Git repo, run a workflow):**
  https://docs.cloud.google.com/dataform/docs/quickstart-connect-git-repo
- **Transform SQL into SQLX (Google Cloud blog):**
  https://cloud.google.com/blog/products/data-analytics/transform-sql-into-sqlx-for-dataform
- **BigQuery create pipelines (Dataform-powered):** https://docs.cloud.google.com/bigquery/docs/create-pipelines
- **BigQuery Migration Service (HiveQL / Teradata / Redshift dialects):**
  https://cloud.google.com/bigquery/docs/migration-intro
- **Dataflow (Beam) for heavy Python:** https://cloud.google.com/dataflow
- **Datastream (CDC / replication):** https://cloud.google.com/datastream
- **BigQuery Data Transfer Service:** https://cloud.google.com/bigquery-transfer

### Tooling

- **uv (Python project manager):** https://docs.astral.sh/uv/
- **Python SDK usage example (Alibaba):**
  https://github.com/aliyun/alibabacloud-python-sdk/blob/master/docs/0-Usage-EN.md

---

## Appendix F · CSV override layer

> **Purpose:** a human-editable layer that sits between extraction (JSON) and
> generation (code). The CSV lets you review, approve, or redirect every routing
> decision before any GCP artifacts are created.

### F.1 Why a CSV?

The FlowSpec JSON is the *structural* source of truth — it describes what DataWorks
has. The CSV is the *decision* layer — it captures what you want GCP to become.

| Layer | Source of truth | Edited by | Consumed by |
| --- | --- | --- | --- |
| FlowSpec JSON | Extraction API | Automated | `route_nodes.py`, `generator.py` |
| `migration_matrix.csv` | Generated from JSON | Human | `generator.py` (overrides) |

Every row in the CSV corresponds to one node. Empty override columns mean "use
the JSON default." The generator merges CSV overrides *before* applying routing
logic, so your decisions take priority.

### F.2 CSV columns

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

### F.3 User workflow

```
1. Run extraction   →  JSON files written
2. Auto-generate CSV →  migration_matrix.csv (all nodes, defaults)
3. User reviews CSV  →  edits gcp_target, schedule, review_notes, skip
4. Run generator     →  CSV overrides applied before code generation
```

**Step 2 is automatic.** After extraction, `extract_datawrites.py` calls
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

### F.4 How overrides are applied

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

### F.5 Example CSV row

```csv
workflow,node_id,node_name,command,input_tables,output_tables,schedule,gcp_target,review_notes,skip
house_buying_analysis,n_ddl,ddl_result_table,"CREATE TABLE IF NOT EXISTS result_table...",,,TRIAGE,DDL node — verify BigQuery schema before generating,false
house_buying_analysis,n_insert,insert_result_table,"INSERT OVERWRITE...",bank_data,result_table,0 3 * * *,,Run at 03:00 instead of 02:00,false
```

### F.6 JSON ↔ CSV relationship

| JSON field | CSV column | Relationship |
| --- | --- | --- |
| `spec.workflows[].nodes[].name` | `node_name` | Matched for lookup |
| `spec.workflows[].nodes[].id` | `node_id` | Matched for lookup (primary) |
| `spec.workflows[].nodes[].command` | `command` | Informational copy |
| `spec.workflows[].trigger.cron` | `schedule` | CSV overrides JSON |
| Routing matrix result | `gcp_target` | CSV overrides matrix |
| *(none)* | `review_notes` | CSV-only, appended to reviews.md |
| *(none)* | `skip` | CSV-only, excludes from generation |

### F.7 Notes for the case study

For the house-buying example, the CSV is trivial — three nodes, all VIRTUAL or
Dataform, no overrides needed. The CSV layer becomes essential when migrating
hundreds of nodes where:

- Some nodes need manual `TRIAGE` before final routing
- Downstream dependencies require schedule adjustments
- Certain nodes should be skipped (deprecated, test-only, etc.)

---

## Appendix G · File map of this repository

| Path | What it is |
| --- | --- |
| `README.md` | Landing page + quick start |
| `docs/01-beginners-guide.md` | Concepts & glossary |
| `docs/02-architecture-and-routing.md` | Architecture & routing matrix |
| `docs/03-migration-steps.md` | The 6-step playbook |
| `docs/04-case-study-house-buying.md` | End-to-end worked example |
| `docs/05-flowspec-json-reference.md` | FlowSpec JSON property reference |
| `docs/06-tools-architecture.md` | Tools architecture & data flow |
| `code/extract/extract_dataworks.py` | OpenAPI extraction (2 clients, checkpointed) |
| `code/extract/sample_flowspec_house_buying.json` | Illustrative FlowSpec for the case study |
| `code/classify/route_nodes.py` | Routing-matrix classifier |
| `code/gcp/dags/house_buying_daily.py` | Cloud Composer DAG |
| `code/gcp/dataform/…` | Dataform project (`.sqlx` models) |
| `data/` | Extraction output (git-ignored) |

---

*Verify all APIs and product capabilities against the current official docs at
execution time — both platforms evolve quickly.*
