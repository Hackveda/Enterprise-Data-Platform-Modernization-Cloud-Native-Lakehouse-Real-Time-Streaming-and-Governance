from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="enterprise_platform_daily",
    start_date=datetime(2026,1,1), schedule="@daily", catchup=False,
    tags=["lakehouse","governance"]
) as dag:
    extract = BashOperator(task_id="extract_postgres", bash_command="cd /workspace && python batch/extract_postgres.py")
    dbt_run = BashOperator(task_id="dbt_run", bash_command="cd /workspace && dbt --project-dir warehouse/dbt --profiles-dir warehouse/dbt run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command="cd /workspace && dbt --project-dir warehouse/dbt --profiles-dir warehouse/dbt test")
    reconcile = BashOperator(task_id="reconcile", bash_command="cd /workspace && pytest -q tests/test_data_quality.py")
    extract >> dbt_run >> dbt_test >> reconcile
