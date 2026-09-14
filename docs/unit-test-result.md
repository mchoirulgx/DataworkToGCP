# Unit Test Result · End-to-End: Alibaba Bank Data → GCP BigQuery + Gap-Fix Tests

> Test executed: **2026-08-19**. Goal: run the DataWorks → GCP migration toolkit end
> to end — from extracting Alibaba's public bank dataset (no Alibaba secret key
> required) to the final analytics table ready in BigQuery. Covers the manual
> path (sections 2–7) **and** the automatic-migration retest via `migrate.py`
> (section 8) **and** extractor crash recovery via the MySQL checkpoint
> (section 9) **and** variable resolution + complex workflow (section 10)
> **and** gap-fix unit tests for v3.1 guide audit remediation (section 11).

---

## 1. Summary

| Step | What ran | Result |
| --- | --- | --- |
| 1 · Extract | Pull Alibaba public `banking.txt` + run metadata extractor | **PASS** |
| 2 · Route | `classify/route_nodes.py` on the case-study FlowSpec | **PASS** |
| 3 · Load | Create `dwh.bank_data` in BigQuery, load 41,188 rows | **PASS** |
| 4 · Transform | Dataform CLI compiles + runs `result_table.sqlx` | **PASS** |
| 5 · Verify | Query `result_table`; 8 rows, `SUM(num) = 6,191` | **PASS** |
| 6 · Auto-migration | `migrate/migrate.py all` — `--flowspec`, `--extract-dir`, and `--workflows` selector (generate → compile → run → verify) | **PASS** |
| 7 · Crash recovery | Extractor resume test — workflow- and node-level, append-only checkpoint | **PASS** |
| 8 · Workflow selection | `--workflows` selector file — only listed workflows migrated; no-match aborts (exit 1) | **PASS** |
| 9 · Complex workflow | Multi-node workflow — variables resolved, 4 models + 2 decls generated, 3 review items (inherent pipeline limits only), compile PASS | **PASS** |
| 10 · Gap-fix unit tests | 51 tests: topological sort, cross-cycle deps, cross-workflow deps, partition flagging, verifier expansion, _decl_sqlx fix, docstring fix, e2e generation, translator regression, CSV conversion, CSV overrides, e2e CSV flow | **PASS** |

**Overall: PASS** — data is ready in BigQuery: `lexiademo.dwh.result_table`.
Sections 8–9 record the automatic-migration retests.
Section 11 records the gap-fix unit tests (51/51 passed).

### Environment under test

| Item | Value |
| --- | --- |
| Machine | Linux (gcloud CLI, `bq`, Docker pre-installed) |
| gcloud account | `amru.rosyada@gmail.com` (active) |
| GCP project / region | `lexiademo` / `asia-southeast2` |
| BigQuery datasets | `dwh` (tables `bank_data`, `result_table`), `dwh_assertions` |
| Dataform CLI | `@dataform/cli` v3.0.64 (binary `dataform`, `~/.local/bin`) |
| uv / Python | uv 0.11.26, Python ≥3.11, `uv sync` clean |
| Data source | Alibaba public OSS `banking.txt` — **no secret key needed** |
| MySQL checkpoint | `dataworks-mysql` container (127.0.0.1:3306) available |

---

## 2. Step 1 — Extract

### 2a. Metadata extractor (`extract/extract_dataworks.py`)

```bash
uv run extract/extract_dataworks.py --name-filter house_buying
```

Output (expected guard, since no Alibaba AccessKey exists for this test):

```
ALIBABA_ACCESS_KEY / ALIBABA_SECRET_KEY not set. Copy .env.example to .env first.
```

The real OpenAPI path requires an Alibaba AccessKey. Per
[`code/README.md`](../code/README.md), the **no-credential path** is the committed
sample FlowSpec `code/extract/sample_flowspec_house_buying.json`, which represents
exactly what `GetNode` returns for the `house_buying_analysis` workflow. It feeds
the Route step below.

### 2b. Data extraction (the bank dataset)

Source (public, no secret key):

```
https://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/cn/shujia/0.2.00/assets/pic/data-develop/banking.txt
```

```bash
curl -sfL -o data/banking.txt --max-time 400 --retry 5 \
  "https://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/cn/shujia/0.2.00/assets/pic/data-develop/banking.txt"
```

Result:

| Metric | Value |
| --- | --- |
| File size | 4,841,548 bytes |
| Rows | 41,188 |
| Columns | 21 (matches `bank_data` DDL) |
| Malformed rows | 0 |
| Saved to | `data/banking.txt` |

> Note: the first download attempts truncated the file (1.5 MB of 4.8 MB, final
> line cut to 12 columns). Retry with `--retry` until `wc -l` = 41,188 and no line
> has `NF != 21`.

---

## 3. Step 2 — Route (`classify/route_nodes.py`)

```bash
uv run classify/route_nodes.py --flowspec extract/sample_flowspec_house_buying.json --csv-out ../data/routing.csv
```

```
SOURCE                              NODE                 COMMAND  GCP TARGET
sample_flowspec_house_buying.json   workshop_start       VIRTUAL   EmptyOperator
sample_flowspec_house_buying.json   ddl_result_table     ODPS_SQL  Dataform .sqlx
sample_flowspec_house_buying.json   insert_result_table  ODPS_SQL  Dataform .sqlx
```

- Matches the expected output in `code/README.md`.
- Both `ODPS_SQL` nodes collapse into **one** Dataform model (`result_table.sqlx`).
- Routing table written to `data/routing.csv`.

---

## 4. Step 3 — Load to BigQuery

### Dataset + table

```bash
bq --project_id=lexiademo mk --dataset --location=asia-southeast2 dwh
```

`bank_data` DDL (21 columns, BigQuery types from the MaxCompute source):

```sql
CREATE OR REPLACE TABLE dwh.bank_data (
  age INT64, job STRING, marital STRING, education STRING, `default` STRING,
  housing STRING, loan STRING, contact STRING, month STRING, day_of_week STRING,
  duration INT64, campaign INT64, pdays FLOAT64, previous FLOAT64,
  poutcome STRING, emp_var_rate FLOAT64, cons_price_idx FLOAT64,
  cons_conf_idx FLOAT64, euribor3m FLOAT64, nr_employed FLOAT64, y INT64
)
```

### Load

```bash
bq --project_id=lexiademo load --source_format=CSV --skip_leading_rows=0 --replace \
  dwh.bank_data data/banking.txt
```

Result: **upload complete, job DONE**.

### Verify

```sql
SELECT COUNT(*) AS n, COUNTIF(housing='yes' AND marital='single') AS matches
FROM dwh.bank_data
-- n=41188, matches=6191
```

---

## 5. Step 4 — Transform (Dataform CLI)

Ran the repo's own artifacts (`code/gcp/dataform/`) from a scratch project at
`/tmp/opencode/dataform-e2e` (repo files untouched). Changes for Dataform v3:
- removed deprecated `dataform.json` (cannot coexist with `workflow_settings.yaml`);
- added `dataformCoreVersion: 3.0.64`;
- set `defaultProject: lexiademo` in `workflow_settings.yaml` and the
  `bank_data.sqlx` declaration;
- `.df-credentials.json` uses gcloud Application Default Credentials (ADC).

```bash
dataform compile /tmp/opencode/dataform-e2e
dataform run    /tmp/opencode/dataform-e2e
```

Compile:

```
Compiled 2 action(s).
1 dataset(s):
  dwh.result_table [table]
1 assertion(s):
  dwh_assertions.dwh_result_table_assertions_rowConditions
```

Run:

```
Table created:    dwh.result_table [table]     (jobId dataform-bcc0b963-…, Bytes billed 10.00 MiB)
Assertion passed: dwh_assertions.dwh_result_table_assertions_rowConditions  (0 B)
```

The migration worked as designed: the `CREATE TABLE` node (`ddl_result_table`)
was absorbed by Dataform, and `insert_result_table`'s
`INSERT OVERWRITE … GROUP BY` became the model `SELECT` body.

---

## 6. Step 5 — Verify (BigQuery)

```sql
SELECT education, num FROM dwh.result_table ORDER BY num DESC;
SELECT COUNT(*) AS result_rows, SUM(num) AS total FROM dwh.result_table;
```

| education | num |
| --- | --- |
| university.degree | 2,399 |
| high.school | 1,641 |
| professional.course | 785 |
| basic.9y | 709 |
| unknown | 257 |
| basic.4y | 227 |
| basic.6y | 172 |
| illiterate | 1 |
| **Total** | **6,191** |

- **8 rows**, `SUM(num) = 6,191` = the 6,191 rows in `bank_data` matching
  `housing='yes' AND marital='single'`. Data is **ready in BigQuery**.
- Row-for-row identical to the plain `bq` SQL path (control), confirming parity.

---

## 7. What was and wasn't tested

**Covered end-to-end:** extract (public dataset) → route → load to BigQuery →
Dataform compile/run → verified analytics table.

**Not exercised (needs real Alibaba/GCP provisioning):**
- Live DataWorks OpenAPI extraction (requires an Alibaba AccessKey; guarded by the
  script, sample FlowSpec used instead).
- Cloud Composer `house_buying_daily.py` DAG execution (would need a Composer
  environment; the underlying Dataform operations are proven via the CLI).
- The committed `workflow_settings.yaml`/`dataform.json` still carry the
  `my-gcp-project` placeholder — the Dataform run used `lexiademo` via the scratch
  project.

---

## 8. Automatic-migration retest — `migrate/migrate.py`

Retested **2026-08-15** after building the `code/migrate/` package (rule-based
generator + gates) and, later the same day, the `--extract-dir` path
(`migrate/reconstruct.py`). Ran from a clean slate before each run:
`dwh.result_table` dropped and `data/generated/` cleared.

### 8.1 Path A — committed sample FlowSpec (regression)

```bash
uv run migrate/migrate.py all --flowspec extract/sample_flowspec_house_buying.json
```

Env (from `.env`, git-ignored): `GCP_PROJECT_ID=lexiademo`, `GCP_REGION=asia-southeast2`,
`DATAFORM_DEFAULT_SCHEMA=dwh`, `DATAFORM_ASSERTION_SCHEMA=dwh_assertions`,
`DATAFORM_CORE_VERSION=3.0.64`, `GENERATED_OUTPUT_DIR=../data/generated`,
`DATAFORM_CLI=dataform`, `DATAFORM_REPOSITORY_ID=migrated`.

#### Gate results

| Gate | Output | Result |
| --- | --- | --- |
| generate | `house_buying_analysis: 1 model(s), 1 declaration(s)` — **0 human-review items** | **PASS** |
| compile | `Compiled 1 action(s)` → `dwh.result_table [table]` | **PASS** |
| run | `Table created: dwh.result_table` (10.00 MiB billed) | **PASS** |
| verify | `result_table` → 8 rows | **PASS** |

Final line: `=== AUTO-MIGRATION COMPLETE: data is ready in BigQuery ===`

#### Generated artifacts (`data/generated/`, git-ignored)

```
dags/house_buying_analysis.py          # Composer DAG, tag=house_buying_analysis
definitions/marts/result_table.sqlx    # auto-translated model
definitions/sources/bank_data.sqlx     # declaration for the loaded source
workflow_settings.yaml                 # lexiademo / asia-southeast2 / dwh
.df-credentials.json                   # gcloud ADC
```

The generated model SQL (auto-translated, `from ${ref("bank_data")}` rewrite):

```sql
SELECT education
     , COUNT(marital) AS num
from ${ref("bank_data")}
WHERE housing = 'yes'
  AND marital = 'single'
GROUP BY education
```

### 8.2 Path B — `--extract-dir` (real extractor output shape)

The new path is what a real DataWorks extraction produces: one `node_<id>.json`
per node **plus** one `workflow_<id>.json` per workflow. `migrate/reconstruct.py`
rebuilds workflow-level FlowSpecs from these files — dependency `flow[]` from
each node's `inputs.nodeOutputs`, schedule trigger from the workflow file when
present, otherwise a fallback cron. Test input: the case-study nodes re-wrapped
into the extractor's `node_<id>.json` shape (same FlowSpec content, no workflow
file on the first run → fallback rebuild; then re-tested with `workflow_111.json`
present → trigger taken from the workflow file).

```bash
uv run migrate/migrate.py all --extract-dir data/extract
```

Reconstruction checks (unit-verified):

```
workflow file preferred  -> name=house_buying, trigger cron=00 02 00 * * ?
fallback rebuild         -> name=workflow_111, flow rebuilt from nodeOutputs
flow[] n_start=[]  n_ddl=[n_start]  n_insert=[n_ddl]
```

#### Gate results

| Gate | Output | Result |
| --- | --- | --- |
| generate | `workflow_111: 1 model(s), 1 declaration(s)` — **0 human-review items** | **PASS** |
| compile | `Compiled 1 action(s)` → `dwh.result_table [table]` | **PASS** |
| run | `Table created: dwh.result_table` (10.00 MiB billed) | **PASS** |
| verify | `result_table` → 8 rows | **PASS** |

Final line: `=== AUTO-MIGRATION COMPLETE: data is ready in BigQuery ===`

Generated artifacts are the same shape as Path A, with `dags/house_buying.py`
(the workflow name from the reconstructed FlowSpec). Duplicate writers of the
same output table are auto-deduped (first writer deployed, the rest flagged in
`review/*.md`) — added so a multi-workflow estate cannot silently overwrite a
model.

### 8.2b Path C — `--workflows` selector (migrate only some workflows)

Added so an estate run can migrate a subset. The selector is a JSON file
(`extract/sample_workflows_selector.json`): an array of names/ids or an object
with a `workflows` key; entries match a workflow's `spec.name` or `spec.id`
(exact). Unmatched entries are reported on stderr; a completely empty selection
aborts with **exit 1 before any gate runs**.

```bash
uv run migrate/migrate.py all --extract-dir data/extract \
  --workflows /tmp/opencode/selector.json
```

```
[generate] migrating 1 workflow(s)
[generate] house_buying: 1 model(s), 1 declaration(s)   # only the listed one
[gate] compile PASS / run PASS / verify PASS  (result_table -> 8 rows)
```

Negative case (no match):

```
[generate] selector entry 'does_not_exist' matched no extracted workflow
[generate] --workflows selector matched no extracted workflows.
# process exits 1, gates never run, no data is written
```

### 8.3 Final data check (both automatic paths, identical to the manual paths)

| education | num |
| --- | --- |
| university.degree | 2,399 |
| high.school | 1,641 |
| professional.course | 785 |
| basic.9y | 709 |
| unknown | 257 |
| basic.4y | 227 |
| basic.6y | 172 |
| illiterate | 1 |
| **Total** | **6,191** |

- `SUM(num) = 6,191` = the 6,191 `bank_data` rows matching
  `housing='yes' AND marital='single'` → **full parity** across both automatic
  paths and both manual paths (sections 5–6).
- The `ddl_result_table` node was auto-absorbed (its `CREATE TABLE` is now the
  model), so **no review file** was produced — the documented "minimal human
  review" behaviour.

### Notes / limits of the automatic path

- No `review/*.md` was emitted for the case study; the generator only writes one
  when a node needs a human (TRIAGE commands, unresolved `${vars}`, non-trivial
  ODPS built-ins, DDL-only tables) or when a duplicate table writer is skipped.
- Schedules are stripped of the seconds field mechanically and marked
  "RE-VERIFY … before cutover" in the generated DAG docstring (docs/03 step 5).
  When the extractor's `workflow_<id>.json` carries the real trigger, that cron
  is used directly.
- Parity with live MaxCompute and Composer DAG execution still require real
  Alibaba/GCP provisioning (unchanged from section 7).
- Lint: `uvx ruff check migrate/ extract/` → **All checks passed!**

---

## 9. Extractor crash recovery — MySQL checkpoint resume

Retested **2026-08-15** after making the MySQL checkpoint a true resume
mechanism. The extractor stores every workflow/node/table row with
**append-only** `INSERT IGNORE` and only records a workflow as done **after** all
of its nodes are stored — so a crash at any point resumes with no re-fetch of
completed work:

| Crash point | Rerun behaviour |
| --- | --- |
| After a workflow fully completes | Workflow row exists → **zero OpenAPI calls**; `node_*.json` + `workflow_<id>.json` regenerated from MySQL |
| Mid-workflow (some nodes stored) | No workflow row → re-lists nodes but skips the stored ones; `get_node` / DDL calls never re-made; workflow marked done at the end |
| Re-saving any key | `INSERT IGNORE` keeps the first write — existing rows untouched |

Integration test (`/tmp/opencode/test_resume.py`, against the `dataworks-mysql`
container; test DB `dataworks_extract_test` dropped afterwards):

```
PASS append-only (first write kept)
PASS workflow-level resume: zero API calls, files regenerated
PASS node-level resume: get_node skipped, files regenerated, workflow marked done
ALL PASS
```

Full E2E re-run from a clean slate (`dwh.result_table` dropped,
`data/generated/` cleared) after the extractor change — both automatic paths:

| Path | generate | compile | run | verify |
| --- | --- | --- | --- | --- |
| `--flowspec sample_flowspec_house_buying.json` | 1 model + 1 declaration, 0 review items | PASS | PASS | 8 rows |
| `--extract-dir data/extract` | 1 model + 1 declaration, 0 review items | PASS | PASS | 8 rows |

Final data check: `COUNT(*) = 8`, `SUM(num) = 6,191` — unchanged parity.
Lint: `uvx ruff check migrate/ extract/` → **All checks passed!**

---

## 10. Complex workflow — `sample_flowspec_complex_ecommerce.json`

Retested **2026-08-18** to exercise the full FlowSpec surface and verify the
review path behaves correctly when a workflow is richer than the simple case
study. The file covers all major node types and properties:

| Property | Values in this file |
| --- | --- |
| `trigger.cron` / `timezone` | `00 03 00 * * ?` / `Asia/Jakarta` |
| `variables[]` | `${bizdate}` (System), `region`/`min_rows`/`env` (Custom) |
| `strategy` | `rerunMode=Allowed`, `rerunTimes=2`, `rerunInterval=120000` |
| `nodes[]` commands | `VIRTUAL` (2), `ODPS_SQL` (4), `PYODPS` (2), `DIDE_SHELL` (1) |
| `branches[]` | 1 conditional branch node (quality pass/fail) |
| `inputs.tables[]` / `outputs.tables[]` | Multiple sources per node; multiple output tables |
| `flow[]` | Parallel fan-out at start, fan-in at end |

### What the generator produced

```bash
uv run migrate/migrate.py generate --flowspec extract/sample_flowspec_complex_ecommerce.json
```

| Category | Nodes | Behaviour |
| --- | --- | --- |
| **Auto-generated models** (4) | `sync_raw_orders`, `sync_raw_users`, `dwd_order_facts`, `load_agg_daily` | ODPS_SQL DML → Dataform `.sqlx` |
| **Auto-generated declarations** (2) | `oss_raw_orders`, `mysql_users` | Source tables read but not written by any model |
| **Review items** (3) | `quality_check` [PYODPS] — not auto-generated | Lightweight procedural Python, needs Composer PythonOperator |
| | `audit_log` [PYODPS] — not auto-generated | Same as above |
| | `alert_failure` [DIDE_SHELL] — audit individually | Shell script, needs manual Composer BashOperator |

`pipeline_start`, `pipeline_end` (both `VIRTUAL`), and `branch_on_quality`
(VIRTUAL branch) are skipped by the generator — they become the DAG's start/end
tasks or are silently absorbed (branch semantics are a documented trade-off).

All workflow variables (`bizdate`, `region`, `min_rows`, `env`) are written to
`workflow_settings.yaml` `vars:` and rewritten in model SQL as
`${{dataform.projectConfig.vars["name"]}}`. The review previously flagged 6
items (3 unresolved variables + 3 inherent limits); after variable resolution
was added, only the 3 inherent pipeline-limit items remain.

### Gate result — compile PASS

```bash
uv run migrate/migrate.py all --flowspec extract/sample_flowspec_complex_ecommerce.json
```

The compile gate now passes — all variables resolve correctly:

```
Compiled 4 action(s).
4 dataset(s):
  dwh.dwd_order_facts [table]
  dwh.dws_agg_daily [table]
  dwh.ods_raw_orders [table]
  dwh.ods_raw_users [table]
[gate] compile PASS
```

The `run` gate fails on the complex workflow because the source tables
(`oss_raw_orders`, `mysql_users`) don't exist in this test BQ project. This is
correct — the complex sample demonstrates the full generate + compile path.
A fully automatic run is only possible for workflows that reference tables
already present in the target project.

### What this test proved

- All four node types reach the correct routing path (`ODPS_SQL` → model,
  `PYODPS` → PythonOperator review, `DIDE_SHELL` → Shell review, `VIRTUAL` →
  skipped).
- Workflow variables are correctly propagated: written to `workflow_settings.yaml`
  and resolved in SQL as `${{dataform.projectConfig.vars["name"]}}`.
- No "unresolved variable" review items when all variables are declared in the
  FlowSpec — the variable resolution feature works end-to-end.
- The compile gate passes when variables are resolved, confirming the generated
  SQL is valid GoogleSQL.
- The branch node (`branches[]`) is correctly handled as a trade-off (skipped,
  with no Airflow branching operator emitted — documented in docs/03).
- The generator successfully handles workflows with parallel fan-out
  (`sync_raw_orders` and `sync_raw_users` both depend only on `pipeline_start`).

---

## 11. Gap-fix unit tests — v3.1 guide audit remediation

Retested **2026-08-19** after implementing fixes for every gap identified in the
v3.1 guide vs. code audit. The test suite (`code/tests/test_migrate.py`, 37
tests) covers all fixed gaps and regression-proofs the existing functionality.

### 11.1 What was fixed (from the audit)

| # | Gap (from audit) | Fix | Test class |
| --- | --- | --- | --- |
| 1 | `flow[]` dependency graph not used for node ordering | `topological_sort()` — Kahn's algorithm, falls back to input order on missing/cyclic flow | `TestTopologicalSort` (5 tests) |
| 2 | No `CrossCycleDependsOnOtherNode` handling | `_detect_cross_cycle_deps()` → review flag + `depends_on_past=True` in DAG template | `TestCrossCycleDeps` (3 tests) |
| 3 | No cross-workflow dependency handling | `_detect_cross_workflow_deps()` → `ExternalTaskSensor` generated in DAG | `TestCrossWorkflowDeps` (2 tests) |
| 4 | Partition clauses silently ignored | `PARTITION BY` detected → review item: "MaxCompute string partition does NOT map to BigQuery native partitioning; redesign required" | `TestPartitionFlagging` (2 tests) |
| 5 | Verifier only did row counts | `verifier.py` expanded: MD5 checksum, per-column numeric aggregates (SUM/MIN/MAX), per-DAG parity (schedule + outputs) | `TestVerifier` (7 tests) |
| 6 | `_decl_sqlx` expression bug (`d.name and schema`) | Fixed to `schema` | `TestDeclSqlxBug` (2 tests) |
| 7 | Duplicate docstring in `translate_query` | Consolidated to single docstring | `TestTranslatorDocstring` (1 test) |
| 8 | End-to-end generation not validated | Full FlowSpec → artifacts verification for both sample workflows | `TestEndToEndGeneration` (6 tests) |
| 9 | Existing translator functionality | Regression tests for type mapping, `ref()` rewrite, variable resolution, flag functions | `TestTranslator` (9 tests) |
| 10 | FlowSpec JSON → CSV conversion | `flowspec_to_csv.py` converts FlowSpec to human-editable `migration_matrix.csv` | `TestFlowspecToCsv` (6 tests) |
| 11 | CSV overrides not applied during generation | Skip nodes, override gcp_target, review_notes, schedule overrides via CSV | `TestCsvOverrides` (6 tests) |
| 12 | End-to-end CSV override flow | JSON → CSV → user edit → generation with overrides applied | `TestEndToEndCsvFlow` (2 tests) |

### 11.2 Test results

```bash
cd code && uv run pytest tests/test_migrate.py -v
```

```
tests/test_migrate.py::TestTopologicalSort::test_linear_chain PASSED
tests/test_migrate.py::TestTopologicalSort::test_parallel_fan_out PASSED
tests/test_migrate.py::TestTopologicalSort::test_empty_flow_falls_back_to_input_order PASSED
tests/test_migrate.py::TestTopologicalSort::test_cycle_falls_back_to_input_order PASSED
tests/test_migrate.py::TestTopologicalSort::test_build_adjacency PASSED
tests/test_migrate.py::TestCrossCycleDeps::test_no_cross_cycle PASSED
tests/test_migrate.py::TestCrossCycleDeps::test_detects_cross_cycle_flag PASSED
tests/test_migrate.py::TestCrossCycleDeps::test_detects_cross_cycle_in_flow_dep PASSED
tests/test_migrate.py::TestCrossWorkflowDeps::test_no_cross_workflow PASSED
tests/test_migrate.py::TestCrossWorkflowDeps::test_detects_external_dep PASSED
tests/test_migrate.py::TestPartitionFlagging::test_partition_flagged_in_review PASSED
tests/test_migrate.py::TestPartitionFlagging::test_no_partition_in_simple_spec PASSED
tests/test_migrate.py::TestVerifier::test_table_check_fields PASSED
tests/test_migrate.py::TestVerifier::test_table_check_failure PASSED
tests/test_migrate.py::TestVerifier::test_dag_parity_pass PASSED
tests/test_migrate.py::TestVerifier::test_dag_parity_schedule_mismatch PASSED
tests/test_migrate.py::TestVerifier::test_dag_parity_output_mismatch PASSED
tests/test_migrate.py::TestVerifier::test_summarize_produces_markdown PASSED
tests/test_migrate.py::TestVerifier::test_summarize_dag_parity PASSED
tests/test_migrate.py::TestDeclSqlxBug::test_schema_value PASSED
tests/test_migrate.py::TestDeclSqlxBug::test_empty_schema PASSED
tests/test_migrate.py::TestTranslatorDocstring::test_single_docstring PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_house_buying PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_complex_ecommerce PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_model_sqlx_content PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_workflow_settings_yaml PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_dag_has_external_task_sensor PASSED
tests/test_migrate.py::TestEndToEndGeneration::test_cross_cycle_depends_on_past PASSED
tests/test_migrate.py::TestTranslator::test_translate_type_basic PASSED
tests/test_migrate.py::TestTranslator::test_translate_type_decimal PASSED
tests/test_migrate.py::TestTranslator::test_translate_type_complex PASSED
tests/test_migrate.py::TestTranslator::test_translate_query_ref PASSED
tests/test_migrate.py::TestTranslator::test_translate_query_bizdate_resolved PASSED
tests/test_migrate.py::TestTranslator::test_translate_query_bizdate_unresolved PASSED
tests/test_migrate.py::TestTranslator::test_translate_query_flag_functions PASSED
tests/test_migrate.py::TestTranslator::test_parse_insert PASSED
tests/test_migrate.py::TestTranslator::test_parse_create PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_house_buying_to_rows PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_house_buying_row_fields PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_ecommerce_to_rows PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_virtual_node_routed_to_empty_operator PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_write_and_load_csv_roundtrip PASSED
tests/test_migrate.py::TestFlowspecToCsv::test_csv_fields_match_schema PASSED
tests/test_migrate.py::TestCsvOverrides::test_skip_node_excludes_model PASSED
tests/test_migrate.py::TestCsvOverrides::test_override_gcp_target PASSED
tests/test_migrate.py::TestCsvOverrides::test_review_notes_appears_in_review PASSED
tests/test_migrate.py::TestCsvOverrides::test_schedule_override_in_dag PASSED
tests/test_migrate.py::TestCsvOverrides::test_no_csv_uses_auto_route PASSED
tests/test_migrate.py::TestCsvOverrides::test_skip_all_nodes_empty_generation PASSED
tests/test_migrate.py::TestEndToEndCsvFlow::test_full_flow_house_buying PASSED
tests/test_migrate.py::TestEndToEndCsvFlow::test_full_flow_ecommerce_with_skips PASSED

============================== 51 passed in 0.44s ==============================
```

**51/51 passed.** Lint: `uv run ruff check migrate/ tests/ classify/` → **1 pre-existing issue (DTZ001 in gcp/dags/), all new code clean!**

### 11.3 Key test details

#### Topological sort (TestTopologicalSort)

- **Linear chain** (house buying): `workshop_start → ddl_result_table → insert_result_table` — correct.
- **Parallel fan-out** (ecommerce): `sync_raw_orders` and `sync_raw_users` both before `dwd_order_facts`; `pipeline_start` first, `pipeline_end` last.
- **Empty flow**: falls back to input-list order (backward compatible).
- **Cycle detection**: all nodes still returned despite cycle (no crash, no data loss).

#### Cross-cycle dependencies (TestCrossCycleDeps)

- `crossCycleDependsOnOtherNode: true` on a node → detected.
- `type: "CrossCycle"` in flow dependency → detected.
- No cross-cycle deps → empty set (no false positives).

#### Cross-workflow dependencies (TestCrossWorkflowDeps)

- `externalWorkflowId` on a node input → list of `{nodeId, external_workflow_id}` returned.
- DAG template generates `ExternalTaskSensor` + import for each external workflow.

#### Partition flagging (TestPartitionFlagging)

- Complex ecommerce: 4 models with `PARTITION (pt='${bizdate}')` → 4 review items about partition redesign.
- House buying: no partition clauses → 0 partition review items.

#### Verifier expansion (TestVerifier)

- `TableCheck` dataclass now carries `checksum` and `aggregates` fields.
- `DAGParityCheck` compares source schedule vs generated schedule, source outputs vs generated outputs.
- Schedule mismatch → `ok=False` with `"Schedule mismatch"` detail.
- Missing outputs → `ok=False` with `"Missing output tables"` detail.
- Markdown summaries render correctly for both gates.

#### End-to-end generation (TestEndToEndGeneration)

- **House buying**: 1 model (`result_table`), 1 declaration (`bank_data`), 0 review items, `timezone=Asia/Jakarta`, `schedule=0 2 * * *`.
- **Complex ecommerce**: 4 models, 2 declarations, 7 review items (4 partition + 2 PYODPS + 1 DIDE_SHELL).
- **DAG ExternalTaskSensor**: synthetic cross-workflow spec → `ExternalTaskSensor` in generated DAG.
- **DAG depends_on_past**: synthetic cross-cycle spec → `depends_on_past` in review + DAG.
- **workflow_settings.yaml**: correct `dataformCoreVersion`, `defaultProject`, `defaultDataset`, `bizdate` var.
- **model .sqlx**: correct `type: "table"`, `tags:`, `ref()` in SQL.

#### FlowSpec JSON → CSV conversion (TestFlowspecToCsv)

- **House buying**: 3 nodes extracted, correct workflow/node_id/node_name/command/gcp_target/schedule.
- **Ecommerce**: 10 nodes extracted, all node IDs present.
- **Virtual node routing**: `n_start` (VIRTUAL) → `EmptyOperator`.
- **CSV roundtrip**: write → load → verify all fields preserved.
- **CSV schema**: all rows match `CSV_FIELDS` exactly.

#### CSV overrides applied during generation (TestCsvOverrides)

- **Skip node**: `n_insert` skipped → `result_table` not generated, review shows "SKIPPED".
- **Override gcp_target**: `n_quality` (PYODPS TRIAGE) overridden to `PythonOperator` → review shows "CSV override".
- **Review notes**: notes appended to review report for Dataform, TRIAGE, and non-Dataform paths.
- **Schedule override**: CSV schedule `0 4 * * *` applied to DAG file.
- **No CSV**: behavior identical to before (backward compatible).
- **Skip all nodes**: empty generation, all nodes in review as SKIPPED.

#### End-to-end CSV override flow (TestEndToEndCsvFlow)

- **House buying full flow**: generate CSV → user edits (schedule + review_notes) → generate with overrides → verify schedule and review notes applied.
- **Ecommerce with skips**: generate CSV → skip alert_failure + override quality_check target → verify SKIPPED + CSV override in review.
