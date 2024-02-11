from datetime import datetime, timezone, timedelta

from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from package.recon_alert import CassandraDruidRecord

record = CassandraDruidRecord()
with DAG("Recon_Alert_Cassandra_Druid",
         schedule_interval = "@hourly",
         catchup = False,
         start_date = datetime(2024,2,5)) as dag:
    
    start_task = BashOperator(
        task_id = "start_task",
        bash_command = "echo start task"
    )

    cassandra_druid_alert = PythonOperator(
        task_id='alerting_cassandra_druid',
        templates_dict = {'run_id': '{{ run_id }}'},
        python_callable = record.recorder,
        provide_context = True
    )

    start_task >> cassandra_druid_alert