import os
import boto3
from airflow.utils.dates import days_ago

import paramiko
from airflow import models
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.hooks.base import BaseHook
from package.sap_intergration import PostgresToSftp

sender = PostgresToSftp()

date = "{{ds_nodash}}"
with DAG("Send_Data_Refference_To_SAP",
         schedule_interval = None,
         start_date = days_ago(1)) as dag:

    start_task = BashOperator(
        task_id = "start_task",
        bash_command = "echo start_task"
    )

    task = PythonOperator(
        task_id = 'test',
        python_callable = sender.postgres_sftp,
        op_kwargs = {
            'date':date
        }
    )

    start_task >> task