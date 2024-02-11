from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import datetime
from package.bi_integration import PartnerIntergration


with DAG("bi_integration_sftp",
         schedule_interval = '0 8 * * *', ## Jam 8 Pagi UTC Time,
         catchup = False,
         start_date = datetime(2022,10,20)) as dag:
     
    T1 = BashOperator(task_id = "start_task", bash_command="echo start")
    
    T2 = PythonOperator(
        task_id='check_s3_sftp_new',
        templates_dict = {'run_id': '{{ run_id }}'},
        provide_context = True,
        python_callable=PartnerIntergration().s3_to_sftp_new
        )
    T1 >> T2