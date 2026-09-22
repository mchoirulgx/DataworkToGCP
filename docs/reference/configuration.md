# Reference · Configuration

> **Last reviewed:** 2026-09-22

Every environment variable the toolkit reads.

> [!IMPORTANT]
> [`code/.env.example`](../../code/.env.example) is the **authoritative list**.
> This page explains what each variable does and which module reads it. If the
> two disagree, `.env.example` wins — and that is a bug worth filing.

Copy it and fill it in:

```bash
cd code
cp .env.example .env
```

`.env` is git-ignored. Never commit it.

---

## Alibaba Cloud credentials

| Variable | Read by | Purpose |
| --- | --- | --- |
| `ALIBABA_ACCESS_KEY` | `extract/extract_dataworks.py` | RAM user AccessKey ID. **Required** for live extraction. |
| `ALIBABA_SECRET_KEY` | `extract/extract_dataworks.py` | RAM user AccessKey secret. **Required** for live extraction. |
| `ALIBABA_REGION` | `extract/extract_dataworks.py` | Region endpoint, e.g. `ap-southeast-5`. Default `ap-southeast-5`. |

Not needed if you work from a committed sample FlowSpec instead of extracting live.

## DataWorks workspace

| Variable | Read by | Purpose |
| --- | --- | --- |
| `DATAWORKS_PROJECT_ID` | `extract/extract_dataworks.py` | Numeric workspace ID from the DataWorks console. |
| `DATAWORKS_ENV` | `extract/extract_dataworks.py` | `Prod` or `Dev`. Not validated — a typo silently extracts nothing. |

## Extraction tuning

| Variable | Read by | Purpose |
| --- | --- | --- |
| `CALL_DELAY_SECONDS` | `extract/extract_dataworks.py` | Pause between `GetNode` calls to stay under the QPS limit. Default `0.5`. |
| `EXTRACT_OUTPUT_DIR` | `extract/extract_dataworks.py` | Where extracted JSON is written. Default `../data/extract`. |

## MySQL checkpoint store

Makes extraction resumable: already-extracted items are skipped on rerun.

| Variable | Read by | Purpose |
| --- | --- | --- |
| `MYSQL_HOST` / `MYSQL_PORT` | `extract/extract_dataworks.py` | Checkpoint DB location. |
| `MYSQL_USER` / `MYSQL_PASSWORD` | `extract/extract_dataworks.py` | Credentials. Generate a password; do not reuse the placeholder. |
| `MYSQL_DATABASE` | `extract/extract_dataworks.py` | Database name. Create it first. |

## GCP / Dataform target

| Variable | Read by | Purpose |
| --- | --- | --- |
| `GCP_PROJECT_ID` | `migrate/migrate.py`, sample DAG | **The only mandatory GCP variable.** |
| `GCP_REGION` | `migrate/migrate.py`, sample DAG | Default `asia-southeast2`. |
| `DATAFORM_REPOSITORY_ID` | `migrate/migrate.py`, sample DAG | Dataform repo name. Code default is `migrated`; `.env.example` ships `house-buying-analysis`. |
| `DATAFORM_GIT_BRANCH` | sample DAG | Branch the Dataform repo tracks. Default `main`. |
| `DAG_SCHEDULE` | **sample DAG only** | Schedule for the hand-written `gcp/dags/house_buying_daily.py`. **Generated** DAGs take their schedule from the FlowSpec trigger or the CSV override, not from here. |

## Automatic migration

| Variable | Read by | Purpose |
| --- | --- | --- |
| `GENERATED_OUTPUT_DIR` | `migrate/migrate.py` | Where the generated project is written. Default `../data/generated`. |
| `AIRFLOW_MAJOR_VERSION` | `migrate/generator.py` | `2` or `3`. Variants are **always** emitted into both `version_2/dags/` and `version_3/dags/`; this only selects which one the plain `dags/` folder mirrors. |
| `DATAFORM_CLI` | `migrate/migrate.py` | Dataform CLI binary name. Default `dataform`. |
| `DATAFORM_CORE_VERSION` | `migrate/migrate.py` | Must match your installed CLI (`dataform --version`). |
| `DATAFORM_DEFAULT_SCHEMA` | `migrate/migrate.py` | Target BigQuery dataset. Default `dwh`. |
| `DATAFORM_ASSERTION_SCHEMA` | `migrate/migrate.py` | Dataset for assertion results. Default `dwh_assertions`. |

---

## Two things that will bite you

> [!WARNING]
> **1. Almost nothing is validated.** Only `GCP_PROJECT_ID` is mandatory.
> Everything else silently falls back to a default — so a missing or misspelt
> `.env` produces a *successful-looking* run against the **wrong dataset**.
> Check `GCP_PROJECT_ID`, `GCP_REGION` and `DATAFORM_DEFAULT_SCHEMA` before
> every real run.

> [!WARNING]
> **2. Dataform config cannot read these variables.** Dataform's own files —
> `workflow_settings.yaml`, `dataform.json`, and the `.sqlx` sources under
> `code/gcp/dataform/` — are committed literally. Values such as
> `my-gcp-project`, `asia-southeast2`, `dwh` and `dwh_assertions` are
> **duplicated** there and must be kept in sync with `.env` **by hand**.

---

## Known gap

`DATETIME_MAPS_TO` is read by `migrate/translator.py` to choose between
`DATETIME` and `TIMESTAMP` for MaxCompute `DATETIME` columns. It is **not**
listed in `.env.example`, and it is read at module import time — before
`load_dotenv()` runs — so **setting it in `.env` currently has no effect**.
It is documented here for completeness only; it is not yet usable.

---

[← Prev: Glossary](glossary.md) · [Index](../README.md) · [Next: Variables & macros →](variables-and-macros.md)
