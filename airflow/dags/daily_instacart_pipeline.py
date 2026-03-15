"""
daily_instacart_pipeline.py
Runs the full dbt pipeline daily at 6 AM UTC.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR     = "/opt/airflow/dbt_project"
DBT_TARGET  = "prod"

default_args = {
    "owner":           "analytics_engineering",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id           = "daily_instacart_pipeline",
    default_args     = default_args,
    schedule_interval= "0 6 * * *",
    catchup          = False,
    tags             = ["instacart", "dbt", "production"],
) as dag:

    run_staging = BashOperator(
        task_id      = "run_staging",
        bash_command = f"cd {DBT_DIR} && dbt run --select staging --target {DBT_TARGET}",
    )

    test_staging = BashOperator(
        task_id      = "test_staging",
        bash_command = f"cd {DBT_DIR} && dbt test --select staging --target {DBT_TARGET}",
    )

    run_intermediate = BashOperator(
        task_id      = "run_intermediate",
        bash_command = f"cd {DBT_DIR} && dbt run --select intermediate --target {DBT_TARGET}",
    )

    run_marts = BashOperator(
        task_id      = "run_marts",
        bash_command = f"cd {DBT_DIR} && dbt run --select marts --target {DBT_TARGET}",
    )

    test_marts = BashOperator(
        task_id      = "test_marts",
        bash_command = f"cd {DBT_DIR} && dbt test --select marts --target {DBT_TARGET}",
    )

    generate_docs = BashOperator(
        task_id      = "generate_docs",
        bash_command = f"cd {DBT_DIR} && dbt docs generate --target {DBT_TARGET}",
    )

    (
        run_staging
        >> test_staging
        >> run_intermediate
        >> run_marts
        >> test_marts
        >> generate_docs
    )
