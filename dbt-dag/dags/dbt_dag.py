from datetime import datetime, timedelta
from airflow import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from sap_etl import sapbyd

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
    
    def call_sap_etl():
        sapbyd.main()

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    sap_etl = BashOperator(
        task_id='sap_etl',
        bash_command="cd /usr/local/airflow/dags/sap_etl && \
            python sapbyd.py",
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command= "python -m dbt run --project-dir /usr/local/airflow/dags/sap_pipeline"
        # "cd /usr/local/airflow/dags/sap_pipeline && \
        #     dbt run"
    )

    # dbt_test = BashOperator(
    #     task_id='dbt_test',
    #     bash_command="dbt test --project-dir sap_pipeline"

    # )

    start >> sap_etl >> dbt_run >> end
