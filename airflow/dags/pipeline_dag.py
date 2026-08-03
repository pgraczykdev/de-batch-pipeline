from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='de_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
    default_args=default_args
) as dag:
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='cd /opt/airflow && python -m pipeline.extract'
    )

    load_duckdb_task = BashOperator(
        task_id='load_duckdb_task',
        bash_command='cd /opt/airflow && python -m pipeline.load'
    )

    load_snowflake_task = BashOperator(
        task_id='load_snowflake_task',
        bash_command='cd /opt/airflow && python -m pipeline.load_snowflake'
    )

    dbt_duckdb_task = BashOperator(
        task_id='dbt_duckdb_task',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --exclude staging.oracle'
    )

    dbt_snowflake_task = BashOperator(
        task_id='dbt_snowflake_task',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --target snowflake --exclude staging.oracle'
    )

    extract_task >> [load_duckdb_task, load_snowflake_task]
    load_duckdb_task >> dbt_duckdb_task
    load_snowflake_task >> dbt_snowflake_task