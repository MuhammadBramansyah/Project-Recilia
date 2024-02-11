from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import datetime
from package.job_rerun_failed_timeout import RerunFailedTimeOut




with DAG(
    'Ops_Update_Status_F',
    schedule_interval = '0 8 * * *', ## Jam 8 Pagi UTC Time,
    catchup = False,
    start_date = datetime(2022,10,20)) as dag:

    T1 = BashOperator(task_id = "start_task", bash_command="echo start")

    T2 = PythonOperator(
        task_id='update_status_f',
        templates_dict = {'run_id': '{{ run_id }}'},
        provide_context = True,
        python_callable=RerunFailedTimeOut().update_status_failed,
        dag=dag
    )

    T1 >> T2

