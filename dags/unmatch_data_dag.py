from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from package.unmatch_data_druid import unmatch_data

with DAG("send_unmatch_data",
         schedule_interval = "0 2 * * *",
         catchup = False,
         start_date = datetime(2023,3,8)) as dag:
    
    start_task = BashOperator(
        task_id = "start_task",
        bash_command = "echo start_task"
    )

    send_unmatch_data = PythonOperator(
        task_id = "Send_Email_Unmatch_Notification",
        templates_dict = {'run_id': '{{ run_id }}'},
        python_callable = unmatch_data,
        provide_context = True
    )

    start_task >> send_unmatch_data

