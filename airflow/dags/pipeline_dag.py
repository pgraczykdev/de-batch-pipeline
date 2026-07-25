from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='de_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='cd /opt/airflow && python -m pipeline.extract'
    )

    load_task = BashOperator(
        task_id='load_task',
        bash_command='cd /opt/airflow && python -m pipeline.load'
    )

    dbt_task = BashOperator(
        task_id='dbt_task',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --exclude staging.oracle'
    )

    extract_task >> load_task >> dbt_task