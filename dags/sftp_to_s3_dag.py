from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import logging 
logging.basicConfig(level=logging.INFO)

from package.sftp_to_s3_func import SftpToS3, EmailReminderToIFS

## sftp path
ebs_path = Variable.get("EBS_SFTP_PATH")
mandiri_sftp_path = f"{ebs_path}Mandiri/"
bca_sftp_path = f"{ebs_path}BCA/"
bni_sftp_path = f"{ebs_path}BNI/"

reminder = EmailReminderToIFS()
sftp_to_s3 = SftpToS3(
    mandiri_sftp=mandiri_sftp_path,
    bni_sftp= bni_sftp_path,
    bca_sftp= bca_sftp_path
)

def run_t4(**kwargs):
    execution_date = kwargs['execution_date']
    end_of_task = execution_date.replace(hour=11, minute=00, second=00, microsecond=0)
    logging.info(f"end datetimes of the last task in this days is = {end_of_task}")
    if execution_date == end_of_task:
        reminder.email_reminder_ifs()
    else:
        logging.info(f"Skip task T4. execution_date = {end_of_task}")

with DAG("EBS_file_to_S3",
         schedule_interval = "0 */6 * * * ",
         start_date = datetime(2022,10,13)) as dag:
    
    start_task = BashOperator(task_id = "Start_task",bash_command = "echo start_task")

    T1 = PythonOperator(
        task_id = "Upload_EBS_Mandiri",
        python_callable=sftp_to_s3.ebs_mandiri
    )

    T2 = PythonOperator(
        task_id = "Upload_EBS_BNI",
        python_callable=sftp_to_s3.ebs_bni
    )

    T3 = PythonOperator(
        task_id = "Upload_EBS_BCA",
        python_callable=sftp_to_s3.ebs_bca
    )

    T4 = PythonOperator(
        task_id = "Sending_email_reminder",
        templates_dict = {'run_id': '{{ run_id }}'},
        provide_context = True,
        python_callable=run_t4
    )

    T5 = BashOperator(task_id = "End_task",bash_command = "echo end_task")

    start_task >> [T1, T2, T3] >> T4 >> T5

    