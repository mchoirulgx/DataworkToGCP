# Resources

> **Last reviewed:** 2026-09-22

Curated external documentation for both platforms.

> [!NOTE]
> Both platforms evolve quickly. Verify APIs and product capabilities against
> the current official docs at execution time.

---

## Alibaba Cloud (source platform)

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

## Google Cloud (target platform)

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

## Tooling

- **uv (Python project manager):** https://docs.astral.sh/uv/
- **Python SDK usage example (Alibaba):**
  https://github.com/aliyun/alibabacloud-python-sdk/blob/master/docs/0-Usage-EN.md

---

[← Prev: Cutover checklist](cutover-checklist.md) · [Index](README.md)
