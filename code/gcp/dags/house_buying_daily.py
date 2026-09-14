"""Migrated Airflow DAG: DataWorks workflow "house_buying_analysis" -> Cloud Composer.

Source (DataWorks / MaxCompute)                       Target (GCP)
  workshop_start      (VIRTUAL zero-load node)   ->   EmptyOperator (parity only)
  ddl_result_table    (ODPS_SQL: CREATE TABLE)  ->   Dataform .sqlx (Dataform owns the DDL)
  insert_result_table (ODPS_SQL: INSERT ...)    ->   Dataform .sqlx (same model, "daily" tag)
  schedule 00 02 * * ? Asia/Jakarta             ->   schedule="0 2 * * *"
  variable ${bizdate}                           ->   Airflow macros (see docs/06 appendices)

Why one Dataform invocation and not two SQL tasks?
    The two ODPS_SQL nodes become ONE Dataform model (result_table.sqlx).
    Dataform resolves table-to-table dependencies via ref() inside BigQuery,
    so we must NOT recreate intra-warehouse edges as Airflow tasks. Composer
    orchestrates across systems and triggers Dataform tags; Dataform orders
    the SQL within BigQuery.

Deploy:
    Put this file in your Composer environment's dags/ folder (or a GCS bucket).
    Requires the google provider and a google_cloud_default connection.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator

# Configuration comes from environment variables (documented in code/.env.example)
# with sensible defaults for the case study. In Cloud Composer, set these on the
# environment; the code is the same either way.
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-gcp-project")
REGION = os.getenv("GCP_REGION", "asia-southeast2")            # e.g. Jakarta
REPOSITORY_ID = os.getenv("DATAFORM_REPOSITORY_ID", "house-buying-analysis")
GIT_BRANCH = os.getenv("DATAFORM_GIT_BRANCH", "main")           # Dataform repo branch
DAG_SCHEDULE = os.getenv("DAG_SCHEDULE", "0 2 * * *")

with DAG(
    dag_id="house_buying_daily",
    schedule=DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["house-buying", "migrated-from-dataworks"],
    description="Daily house-buying group analysis (migrated from DataWorks)",
) as dag:
    # VIRTUAL start node from DataWorks, kept for DAG parity and clarity.
    start = EmptyOperator(task_id="workshop_start")

    # Step 1: compile the Dataform repository (SQLX -> BigQuery SQL DAG).
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_BRANCH},
    )

    # Step 2: execute only the models tagged "daily" (result_table and its
    # upstream bank_data declaration). This replaces both ddl_result_table
    # and insert_result_table from the source workflow.
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {"included_tags": ["daily"]},
        },
    )

    start >> compile_dataform >> run_dataform
