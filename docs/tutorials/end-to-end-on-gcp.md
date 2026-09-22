# Tutorial · End-to-end migration on GCP

> **Last reviewed:** 2026-09-22

This walks you through migrating a DataWorks workflow to Google Cloud (BigQuery +
Dataform) using the tool **exactly as you would from a terminal** — one command at
a time. Copy-paste each command, watch the output, read the short explanation.

**What we end up with:** a Dataform project + Composer DAG generated from the
DataWorks metadata, the migrated query running on BigQuery, and a final table
`dwh.result_table` with **8 rows, SUM = 6,191** (the correct, expected answer).

## Placeholders used in this guide

```bash
export REPO_ROOT=~/DataworkToGCP      # wherever you cloned this repo
export GCP_PROJECT_ID=my-gcp-project  # your GCP project
export GCP_REGION=asia-southeast2     # your region
export WORK_DIR=/tmp/dataworks-work   # scratch dir for downloads/extracts
```

Every command below assumes these variables are exported in your shell.

## Contents

- [Step 0 — What you need before starting](#step-0--what-you-need-before-starting)
- [Step 1 — Prepare the configuration (`.env`)](#step-1--prepare-the-configuration-env)
- [Step 2 — Install the toolkit's dependencies](#step-2--install-the-toolkits-dependencies)
- [Step 3 — Download the bank data sample (from Alibaba's docs)](#step-3--download-the-bank-data-sample-from-alibabas-docs)
- [Step 4 — Extract & classify the DataWorks metadata](#step-4--extract--classify-the-dataworks-metadata)
- [Step 5 — Load the raw data into BigQuery](#step-5--load-the-raw-data-into-bigquery)
- [Step 6 — Run the migration (the main event)](#step-6--run-the-migration-the-main-event)
- [Step 7 — Last verification: check the final table](#step-7--last-verification-check-the-final-table)
- [Step 8 — DAGs for Airflow 2 and Airflow 3 (pick your version)](#step-8--dags-for-airflow-2-and-airflow-3-pick-your-version)
- [Step 9 — What you now have (and next steps)](#step-9--what-you-now-have-and-next-steps)

Everything below was executed successfully on GCP project `$GCP_PROJECT_ID`
(region `asia-southeast2`). Files live in `$REPO_ROOT`.

> **Re-test:** the full sequence (Steps 1–7) was re-run end-to-end on
> **2026-09-10** after adding the routing guardrail (heavy PYODPS/Python →
> Dataflow) and the `$[yyyymmdd±N]` bracket-offset translator fixes (62/62
> tests), and again on **2026-09-11** after adding Airflow 2.x / 3.x DAG
> variants in separate version folders (69/69 tests). All outputs below were
> captured fresh from the latest run — everything still matches: `routing.csv`
> unchanged, all gates PASS (compile / run / verify), and the final table is
> **8 rows, SUM = 6,191**.

---

## Step 0 — What you need before starting

| Thing | Value used here |
| --- | --- |
| A GCP project | `$GCP_PROJECT_ID` |
| A GCP region | `asia-southeast2` |
| Dataform CLI | `3.0.64` (`dataform --version`) |
| `bq` CLI | part of the Google Cloud SDK |
| Python + `uv` | for running the toolkit |

---

## Step 1 — Prepare the configuration (`.env`)

### Why

Every tool in this toolkit reads your settings (GCP project, region,
schema names, where to write output) from one file: `.env`. It's a copy of a
template — you fill in your own values. It is never committed to git because it
can hold secrets.

### Commands

```bash
cd $REPO_ROOT/code
cp .env.example .env
```

In this test we only changed the values that matter for running on GCP:

```bash
sed -i \
  -e "s|^GCP_PROJECT_ID=.*|GCP_PROJECT_ID=$GCP_PROJECT_ID|" \
  -e "s|^GCP_REGION=.*|GCP_REGION=$GCP_REGION|" \
  -e "s|^DATAFORM_REPOSITORY_ID=.*|DATAFORM_REPOSITORY_ID=migrated|" \
  -e "s|^DATAFORM_DEFAULT_SCHEMA=.*|DATAFORM_DEFAULT_SCHEMA=dwh|" \
  -e "s|^DATAFORM_ASSERTION_SCHEMA=.*|DATAFORM_ASSERTION_SCHEMA=dwh_assertions|" \
  -e "s|^GENERATED_OUTPUT_DIR=.*|GENERATED_OUTPUT_DIR=../data/generated|" \
  .env
```

> **Note:** the replacement values contain `/` characters, so these expressions
> use `|` as the `s` delimiter instead of `/`. With `s/.../GENERATED_OUTPUT_DIR=../data/generated/`
> the unescaped slashes would terminate the expression and `sed` would fail with
> `unknown option to 's'`. If you prefer, just open `.env` in an editor and set
> the six values by hand.

### Output — your `.env` now contains

```ini
GCP_PROJECT_ID=$GCP_PROJECT_ID
GCP_REGION=asia-southeast2
DATAFORM_REPOSITORY_ID=migrated
DATAFORM_GIT_BRANCH=main
DAG_SCHEDULE=0 2 * * *
GENERATED_OUTPUT_DIR=../data/generated
DATAFORM_CLI=dataform
DATAFORM_CORE_VERSION=3.0.64
DATAFORM_DEFAULT_SCHEMA=dwh
DATAFORM_ASSERTION_SCHEMA=dwh_assertions
```

(Alibaba AccessKey / DataWorks / MySQL entries stay as placeholders — the sample
path used in this guide reads the workflow from a local file, so it doesn't need
live DataWorks credentials.)

### Explain to a beginner

Think of `.env` as the tool's "settings panel". The
tools read it automatically every time they run.

---

## Step 2 — Install the toolkit's dependencies

### Why

The toolkit is a Python project. `uv sync` downloads all the packages it
needs into a private environment, so a later `uv run ...` works everywhere.

### Command

```bash
cd $REPO_ROOT/code
uv sync
```

### Output

```
Resolved 41 packages
Checked 38 packages
```

### Beginner note

You only run this once. After that, every command below is
prefixed with `uv run`, which just means "run with the installed packages".

---

## Step 3 — Download the bank data sample (from Alibaba's docs)

### Why

The example workflow counts bank-marketing records. Data migration moves
*pipeline code*, not raw data — so the source table has to exist in BigQuery first.
The data set used by the guide is the Alibaba *banking* sample: 41,188 rows.

### Primary download (Alibaba OSS docs)

```bash
cd $REPO_ROOT/data
curl -o banking.txt \
  "https://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/cn/shujia/0.2.00/assets/pic/data-develop/banking.txt"
```

### Output when it works

```
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
...
```

### Fallback if the OSS link is slow/unreachable

(As in this test.) The same 41,188 records come from the UCI repository,
formatted the same way (`;` separator, no header, `y` → `0/1`):

```bash
cd $WORK_DIR/bank-additional        # extracted from UCI bank-additional.zip
python3 - <<'PY'
import csv
import os
rows = list(csv.reader(open('bank-additional-full.csv'), delimiter=';'))
out = []
for r in rows[1:]:
    r = [c.strip('"') for c in r]
    r[-1] = '1' if r[-1] == 'yes' else '0'
    out.append(';'.join(r))
open(os.environ['REPO_ROOT'] + '/data/banking.txt', 'w').write('\n'.join(out) + '\n')
print('written', len(out), 'rows')
PY
```

### Output

```
written 41188 rows
```

### Check the file

```bash
cd $REPO_ROOT
wc -l data/banking.txt
head -3 data/banking.txt
```

### Output

```
41188 data/banking.txt
56;housemaid;married;basic.4y;no;no;no;telephone;may;mon;261;1;999;0;nonexistent;1.1;93.994;-36.4;4.857;5191;0
57;services;married;high.school;unknown;no;no;telephone;may;mon;149;1;999;0;nonexistent;1.1;93.994;-36.4;4.857;5191;0
37;services;married;high.school;no;yes;no;telephone;may;mon;226;1;999;0;nonexistent;1.1;93.994;-36.4;4.857;5191;0
```

### Beginner note

A real project wouldn't download data — a sync pipeline
(Datastream / Data Transfer Service) would push DataWorks tables into BigQuery.
This download just gives us the same sample data the guide's example uses.

---

## Step 4 — Extract & classify the DataWorks metadata

### Why

"Extracting metadata" means reading the source workflow definition and
deciding, per node, what it becomes in GCP:

- a `VIRTUAL`/shell node → an Airflow `EmptyOperator` (flow control only),
- an `ODPS_SQL` node → a Dataform `.sqlx` model (the SQL runs in BigQuery).

The tool writes a routing table (`data/routing.csv`) showing those decisions.

(When you have a real DataWorks workspace, this step fetches metadata through the
DataWorks API; here it reads the bundled sample workflow file
`extract/sample_flowspec_house_buying.json`.)

### Command

```bash
cd $REPO_ROOT/code
uv run classify/route_nodes.py \
  --flowspec extract/sample_flowspec_house_buying.json \
  --csv-out ../data/routing.csv
```

### Output

(`data/routing.csv`)

```csv
source_file,node_id,node_name,command,gcp_target,reason
sample_flowspec_house_buying.json,n_start,workshop_start,VIRTUAL,EmptyOperator,No-op / flow control
sample_flowspec_house_buying.json,n_ddl,ddl_result_table,ODPS_SQL,Dataform .sqlx,In-warehouse SQL -> BigQuery via Dataform
sample_flowspec_house_buying.json,n_insert,insert_result_table,ODPS_SQL,Dataform .sqlx,In-warehouse SQL -> BigQuery via Dataform
```

### Beginner note

This is the "brain" of the migration — the tool reads what
DataWorks does and writes a map of what to build on GCP. You can read that map in
`routing.csv` and check it makes sense before anything runs.

---

## Step 5 — Load the raw data into BigQuery

### Why

The migrated SQL queries a table called `bank_data`. We create it in
BigQuery and fill it with the 41,188 rows from Step 3. This is what the sync
pipeline would normally do automatically.

### Command — create the table

```bash
bq --project_id=$GCP_PROJECT_ID query --use_legacy_sql=false \
"CREATE OR REPLACE TABLE dwh.bank_data (
  age INTEGER, job STRING, marital STRING, education STRING,
  \`default\` STRING, housing STRING, loan STRING, contact STRING,
  \`month\` STRING, day_of_week STRING, duration INTEGER,
  campaign INTEGER, pdays FLOAT64, previous FLOAT64, poutcome STRING,
  emp_var_rate FLOAT64, cons_price_idx FLOAT64, cons_conf_idx FLOAT64,
  euribor3m FLOAT64, nr_employed FLOAT64, y INTEGER )"
```

### Output

```
Replaced $GCP_PROJECT_ID.dwh.bank_data
```

> **Beginner note:** BigQuery doesn't allow dots in column names, so the
> `emp.var.rate`, `cons.price.idx`, `cons.conf.idx`, `nr.employed` columns from
> the file become `emp_var_rate`, `cons_price_idx`, `cons_conf_idx`, `nr_employed`.
> The migrated SQL only uses `education`, `marital`, `housing` — so this rename
> doesn't affect the result. `\`` is how you escape the backticks of `default` /
> `month` inside a double-quoted shell string.

### Command — load the rows

```bash
cd $REPO_ROOT
bq --project_id=$GCP_PROJECT_ID load \
  --source_format=CSV \
  --field_delimiter=';' \
  --skip_leading_rows=0 \
  --replace \
  dwh.bank_data data/banking.txt
```

### Output

```
Upload complete.
Waiting on bqjob_r... ... ... DONE.
```

### Quick sanity check — how many rows match the migration's filter?

```bash
bq --project_id=$GCP_PROJECT_ID query --use_legacy_sql=false \
"SELECT COUNT(*) AS n FROM dwh.bank_data
 WHERE housing = 'yes' AND marital = 'single'"
```

### Output

```
+-------+
|   n   |
+-------+
| 6191  |
+-------+
```

### Beginner note

`;` separator → `--field_delimiter=';'`; no header line →
`--skip_leading_rows=0`. The `6,191` is our "target answer" — if the final
migrated table also sums to 6,191, we know the whole migration is correct.

---

## Step 6 — Run the migration (the main event)

### Why

This one command does everything automatically, in 4 stages called
*gates*:

1. **generate** — create the Dataform project + Composer DAG from the routing map (Step 4).
2. **compile** — check the generated SQL compiles (Dataform CLI).
3. **run** — actually create/populate the table in BigQuery.
4. **verify** — compare the output table and DAG to what the source should produce.

The pipeline **stops and reports FAIL** if any gate fails.

### Command

```bash
cd $REPO_ROOT/code
uv run migrate/migrate.py all --flowspec extract/sample_flowspec_house_buying.json
```

### Output

```
[generate] migrating 1 workflow(s)
[generate] house_buying_analysis: 1 model(s), 1 declaration(s)
[generate] no human-review items -- fully automatic path
[generate] project written to $REPO_ROOT/data/generated
Compiling...

Compiled 1 action(s).
1 dataset(s):
  dwh.result_table [table]
[gate] compile PASS
Compiling...

Compiled successfully.

Running...

Table created:  dwh.result_table [table]
 	 jobId: dataform-5cd7068e-7256-495f-bc68-86cd76311aaa,
 	 Bytes billed: 10.00 MiB
[gate] run PASS
=== Per-Table Verification ===
| Table | Rows | Checksum | Aggregates | Status |
| --- | --- | --- | --- | --- |
| `result_table` | 8 | FrlxSpgJnJyo... | 3 columns | PASS |

=== Per-DAG Parity Verification ===
| Workflow | Schedule | Nodes | Models | Outputs | Status |
| --- | --- | --- | --- | --- | --- |
| `house_buying_analysis` | 02 00 * * * | 2 | 1 | 1 | PASS (all checks passed) |
[gate] verify PASS

=== AUTO-MIGRATION COMPLETE: data is ready in BigQuery ===
```

### Beginner note

All three gates printed **PASS** — the generated code compiled,
ran, and matched expectations. The files it created are now in
`data/generated/`:

```
data/generated/dags/house_buying_analysis.py        # the Composer DAG (active version)
data/generated/version_2/dags/house_buying_analysis.py  # Airflow 2.x.x variant
data/generated/version_3/dags/house_buying_analysis.py  # Airflow 3.x.x variant
data/generated/definitions/marts/result_table.sqlx  # the migrated SQL model
data/generated/definitions/sources/bank_data.sqlx   # source table declaration
data/generated/workflow_settings.yaml               # project settings
```

(The generator now writes **one DAG per Airflow major version** — see Step 8 for
why, and how to pick which one you deploy. `jobId`/`Checksum` are per-run values
from this test run — the checksum is recomputed every run by the verify gate,
which is why it differs from older captures; the **rows/status** are the real
signal.)

---

## Step 7 — Last verification: check the final table

### Why

`migrate.py all` already verified itself, but this is the independent,
"show me the numbers" proof — the actual data that landed in BigQuery.

### Command

```bash
bq --project_id=$GCP_PROJECT_ID query --use_legacy_sql=false \
"SELECT education, num FROM dwh.result_table ORDER BY num DESC"

bq --project_id=$GCP_PROJECT_ID query --use_legacy_sql=false \
"SELECT COUNT(*) AS row_count, SUM(num) AS total FROM dwh.result_table"
```

### Output

```
+---------------------+------+
|      education      | num  |
+---------------------+------+
| university.degree   | 2399 |
| high.school         | 1641 |
| professional.course |  785 |
| basic.9y            |  709 |
| unknown             |  257 |
| basic.4y            |  227 |
| basic.6y            |  172 |
| illiterate          |    1 |
+---------------------+------+

+-----------+-------+
| row_count | total |
+-----------+-------+
|         8 |  6191 |
+-----------+-------+
```

### Beginner note

Compare `SUM = 6,191` with the `6,191` we found in Step 5.
They match. The migration is correct end to end:

- the SQL counted exactly the records it should (`housing = 'yes' AND marital =
  'single'`),
- grouped into the 8 education levels,
- and produced the same result the guide expects.

---

## Step 8 — DAGs for Airflow 2 and Airflow 3 (pick your version)

### Why

The generated Composer DAG targets a specific Airflow major version —
import paths and the `DAG()` constructor changed between Airflow 2.x and 3.x.
To let the same workflow run on either, the generator emits **one DAG per major
version** into its own folder, plus a copy in `dags/` that mirrors whichever
version you declared. Both are valid, importable Python files — Airflow does the
rest when it loads them.

### Folder layout

(After `migrate.py all`.)

```
data/generated/
├── dags/house_buying_analysis.py            # ACTIVE version (mirror of version_3/)
├── version_2/dags/house_buying_analysis.py  # Airflow 2.x.x variant
└── version_3/dags/house_buying_analysis.py  # Airflow 3.x.x variant
```

### What differs between the two DAGs

(Everything else is identical.)

| | `version_2/` (Airflow 2.x.x) | `version_3/` (Airflow 3.x.x) |
| --- | --- | --- |
| DAG import | `from airflow import DAG` | `from airflow.sdk import DAG` |
| Empty / Python operators | `airflow.operators.empty` / `.python` | `airflow.providers.standard.operators.*` |
| Schedule | `schedule_interval="02 00 * * *"` | `schedule=CronTriggerTimetable("02 00 * * *", timezone="Asia/Jakarta")` |
| Timezone | pendulum-aware `start_date` | timezone pinned in the cron timetable |
| `cyctime` variable | `{{ logical_date... }}` | `{{ data_interval_start... }}` (`logical_date` is gone in 3) |

### Choosing the active version

`.env` has one switch:

```bash
cd $REPO_ROOT/code
grep AIRFLOW_MAJOR_VERSION .env   # =3 (default) if you didn't set it
sed -i 's/^AIRFLOW_MAJOR_VERSION=.*/AIRFLOW_MAJOR_VERSION=2/' .env  # or =3
```

`AIRFLOW_MAJOR_VERSION=2` makes `dags/` mirror `version_2/`; `=3` (the default)
makes it mirror `version_3/`. Both `version_2/` and `version_3/` are **always**
written, so you can also just copy the folder you need into your sync directory.

### Two Airflow-3 runtime errors this fixed

(Both showed up when actually running the DAG on Airflow 3.)

1. `TypeError: __init__() got an unexpected keyword argument 'timezone'` — Airflow 3
   removed the `timezone=` kwarg on `DAG()`. The generator now carries the timezone
   in a pendulum-aware `start_date` (Airflow 2) and additionally pins it in a
   `CronTriggerTimetable` (Airflow 3) instead.
2. `ValueError: Unknown field for WorkflowInvocation: variables` — Dataform
   variables are **compile-time only**: there is no `variables` field on a
   workflow invocation. The DAG now passes them into the *compile* task as a flat
   dict under `code_compilation_config.vars` (e.g. `bizdate`), and the *run* task
   only references the compilation result.

### Beginner note

On Composer, sync the folder that matches your environment's
Airflow image — `version_2/dags/` for Composer on Airflow 2, `version_3/dags/`
for Airflow 3 — or keep the `dags/` alias if you only ever run the one version.

---

## Step 9 — What you now have (and next steps)

### Done

1. Settings ready (`.env`).
2. Sample data downloaded (41,188 rows).
3. Metadata extracted & classified (`routing.csv`).
4. Source data loaded into BigQuery (`dwh.bank_data`).
5. Pipeline generated, compiled, ran, verified — `migrate.py all` → all gates PASS.
6. Final table verified: 8 rows, SUM = 6,191. ✔

### Deploying to production

`Steps 1–7` built a one-off demo; these three steps
turn it into a pipeline that re-runs daily and refreshes `dwh.result_table` on
its own:

#### 1. Push `data/generated` into your Dataform repository

`data/generated` *is* a Dataform project — it contains `definitions/`
(`result_table.sqlx`, `bank_data.sqlx`) and `workflow_settings.yaml`. To let
Dataform manage it instead of the CLI:

1. `git init` the folder, commit it, and push to the code host your team uses
   (GitHub / GitLab / Cloud Source Repositories).
2. In the **Dataform** console, create a *repository* pointing at that git repo,
   then a **development workspace** and a **release config**. Dataform now
   compiles `definitions/` the same way the `migrate.py` "compile" gate did
   locally — but from the repo, on a schedule.
3. Keep `DATAFORM_REPOSITORY_ID` (and `DATAFORM_GIT_BRANCH`) in `.env` pointing
   at that repository — the generated DAG reads both at runtime.

#### 2. Sync the DAG folder into Cloud Composer

Composer is Airflow as a managed service: it runs whatever DAG files sit in its
`dags/` folder. Put the generated DAG there:

1. Note the **Airflow image** of your Composer environment — that decides which
   file to deploy:
   - environments on **Airflow 3** (e.g. Composer 4.x images) →
     copy `version_3/dags/house_buying_analysis.py` (uses `airflow.sdk` +
     `CronTriggerTimetable`),
   - environments on **Airflow 2** (older images) →
     copy `version_2/dags/house_buying_analysis.py` (`schedule_interval=`).
   Don't assume "the latest image" — check the image label. (This is exactly why
   the generator emits both folders — Step 8.)
2. Copy the file into the environment's `dags/` folder via the Composer bucket,
   or configure the environment's **git-sync** to pull `version_2/dags` (or
   `version_3/dags`) straight from the repo.
3. Because the DAG was generated with schedule `02 00 * * *` (`Asia/Jakarta`),
   Airflow now triggers it **every day at 00:02** — minute `02`, hour `00`.
   (The DataWorks source cron `00 02 00 * * ?` is Quartz `s m h`, so it also
   means 00:02, not 02:00. See
   [variables & macros](../reference/variables-and-macros.md#3-cron-translation).)
   Each run asks Dataform to recompile the repo and re-run the
   `house_buying_analysis` tag, refreshing `dwh.result_table` with the newest
   data.

#### 3. Replace the sample download with a real DataWorks sync pipeline

So far the source data was loaded once by hand (`bq load banking.txt`) to mimic
what DataWorks keeps in MaxCompute. In production the source table refills
itself:

- **Datastream** (change data capture) or **Cloud Data Transfer Service** push
  new DataWorks / MySQL rows into BigQuery continuously, or
- a scheduled **Dataflow / DTS batch job** refreshes `dwh.bank_data` on a cron.

Either way `bank_data` stays current, so the daily DAG always computes from the
latest source data.

### Beginner note

Do them in order — 1 → 2 → 3. Steps 1 and 2 provide the
*schedule and compute*, step 3 provides the *data*. Until step 3 is in place,
the daily DAG just recomputes `result_table` from whatever is currently in
`bank_data`.

---

[← Prev: 04 · Case study](../04-case-study-house-buying.md) · [Index](../README.md) · [Next: 05 · Tools architecture →](../05-tools-architecture.md)