import os
import boto3
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from package.sap_intergration import DruidToSFTP

## date format for druid query
druids = DruidToSFTP()

with DAG("Match_Data_To_SAP",
         schedule_interval = "0 2 * * *",
         catchup = False,
         start_date = datetime(2023,2,15)) as dag:
        
    start_task = BashOperator(task_id = "start_task", bash_command="echo start")

    T1 = PythonOperator(
        task_id = "Upload_Match_Data_To_SAP",
        templates_dict = {'run_id':'{{run_id}}'},
        python_callable=druids.druid_to_sftp,
        provide_context = True
    )

    start_task >> T1