from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from package.druid_ingest_func import ingest_spec_druid

with DAG("druid_ingest_dag",
         schedule_interval = "0 2 * * *",
         catchup = False,
         start_date = datetime(2023,2,17)) as dag:
     
    T1 = BashOperator(
            task_id = "start_task",
            bash_command = "echo start task"
    )

    T2 = PythonOperator(
            task_id = 'Ingest_spec_to_Druid',
            templates_dict = {'run_id': '{{ run_id }}'},
            provide_context = True,
            python_callable = ingest_spec_druid
    )

    T1 >> T2



