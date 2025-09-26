from datetime import datetime, timedelta
# import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

# log = logging.getLogger(
#     "airflow.task",
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )


default_args = {
    'owner': 'akwanybabu',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 14),
}

with DAG(
    'dbt_dag',
    default_args=default_args,
    description='A simple dbt DAG',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    
    def print_hello():
        print("Hello World!")

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="cd /usr/local/airflow/dags/sap_pipeline && \
            dbt run"
    )

    # dbt_test = BashOperator(
    #     task_id='dbt_test',
    #     bash_command="dbt test --project-dir sap_pipeline"

    # )

    start >> dbt_run >> end
    # >> dbt_test >> end