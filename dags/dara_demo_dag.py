import os
from urllib import request
import requests
import pandas as pd
import json

from io import StringIO
import boto3
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from package.copy_dara_s3 import copy_to_s3
from package import dara_to_s3_func

dara_to_s3 = dara_to_s3_func
writers = dara_to_s3.WriteToPostgres()

## date format
now = datetime.now() + timedelta(hours=7)
year = now.strftime('%Y')
month = now.strftime('%m')
day = now.strftime('%d')
start_date = now.strftime('%Y%m%d')
times = now.strftime('%H%M%S')

# s3 path
s3_path_channel = Variable.get("CHANNEL_S3_PATH")

path = "UAT_data/Channel/Data_DARA/"
configs = Variable.get("DARA_PATH_DEMO")

files = json.loads(configs)
get_elissa = files["elissa"]
get_byu = files["byu"]
get_channel_type = files["channel_type"]

elissa_files = os.path.splitext(get_elissa)[0]
byu_files = os.path.splitext(get_byu)[0]

path_elissa = path + get_elissa
path_byu = path + get_byu

## fulpath
elissa_s3_path = s3_path_channel + f"{year}/{month}/{day}/{elissa_files}_{times}.csv"
byu_s3_path = s3_path_channel + f"{year}/{month}/{day}/{byu_files}_{times}.csv"
## time
date= '{{ds_nodash}}'

with DAG("demo_dara_s3",
         schedule_interval = None,
         catchup = False,
         start_date = datetime(2022,10,20)) as dag:
     
     T1 = BashOperator(task_id = "start_task", bash_command="echo start")


     if get_channel_type == "ELISSA":
               
          T2 = PythonOperator(
                    task_id = 'Elissa_To_S3',
                    templates_dict = {'run_id': '{{ run_id }}'},
                    python_callable = copy_to_s3,
                    provide_context = True,
                    op_kwargs = {
                    "path":elissa_s3_path,
                    "date":date,
                    "source_data":path_elissa
                    }
                    )

          T3 = PythonOperator(
               task_id = "write_log_elissa_to_Postgres",
               python_callable = writers.write_to_postgres,
               op_kwargs = {
                    "channels_codes":"",
                    "partners_codes":"",
                    "task":"Elissa_To_S3"
               }
          )

          T6 = BashOperator(task_id = "Task_Done", bash_command="echo Done")

          T1 >> T2 >> T3 >> T6
     elif get_channel_type == "BYU":
               
          T4 = PythonOperator(
               task_id = "ByU_To_S3",
               templates_dict = {'run_id': '{{ run_id }}'},
               python_callable = copy_to_s3,
               provide_context = True,
               op_kwargs = {
                    "path":byu_s3_path,
                    "date":date,
                    "source_data":path_byu
               }
          )

          T5 = PythonOperator(
               task_id = "write_log_ByU_To_Postgres",
               python_callable = writers.write_to_postgres,
               op_kwargs = {
                    "channels_codes":"BYU000",
                    "partners_codes":"",
                    "task":"ByU_To_S3"
               }
          )

          T6 = BashOperator(task_id = "Task_Done", bash_command="echo Done")

          T1 >> T4 >> T5 >> T6
     else:
          T2 = PythonOperator(
                    task_id = 'Elissa_To_S3',
                    templates_dict = {'run_id': '{{ run_id }}'},
                    python_callable = copy_to_s3,
                    provide_context = True,
                    op_kwargs = {
                    "path":elissa_s3_path,
                    "date":date,
                    "source_data":path_elissa
                    }
                    )

          T3 = PythonOperator(
               task_id = "write_log_elissa_to_Postgres",
               python_callable = writers.write_to_postgres,
               op_kwargs = {
                    "channels_codes":"",
                    "partners_codes":"",
                    "task":"Elissa_To_S3"
               }
          )

          T4 = PythonOperator(
               task_id = "ByU_To_S3",
               templates_dict = {'run_id': '{{ run_id }}'},
               python_callable = copy_to_s3,
               provide_context = True,
               op_kwargs = {
                    "path":byu_s3_path,
                    "date":date,
                    "source_data":path_byu
               }
          )

          T5 = PythonOperator(
               task_id = "write_log_ByU_To_Postgres",
               python_callable = writers.write_to_postgres,
               op_kwargs = {
                    "channels_codes":"BYU000",
                    "partners_codes":"",
                    "task":"ByU_To_S3"
               }
          )

          T6 = BashOperator(task_id = "Task_Done", bash_command="echo Done")
          T1 >> T2 >> T3 >> T6
          T1 >> T4 >> T5 >> T6