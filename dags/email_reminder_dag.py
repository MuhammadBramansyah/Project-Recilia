import json 
import logging
logging.basicConfig(level=logging.INFO)
from datetime import datetime, timezone, timedelta

from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from package.email_reminder import email_reminder

conf = Variable.get("EMAIL_REMINDER_CONFIG")
reminder_conf = json.loads(conf)
get_reminder_type = reminder_conf["REMINDER_TYPE"]

def convert_utc_to_wib(utc_time):
    wib_timezone = timezone(timedelta(hours=7)) 
    return utc_time.astimezone(wib_timezone)

def decide_branch(**context):
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')
    execution_date_wib = convert_utc_to_wib(context['execution_date'])

    if is_manual:
        if get_reminder_type == "WEEKLY":
            return "Weekly"
        elif get_reminder_type == "MONTHLY":
            return "Monthly"
        else:
            return ['Weekly', 'Monthly']
    
    if execution_date_wib == 1 and execution_date_wib == 0:
        logging.info('weekly and monthly running in paralel')
        logging.info(f"weekly date = {execution_date_wib.weekday()}") 
        logging.info(f"monthly date = {execution_date_wib.day}")
        return ['Weekly', 'Monthly']
    
    if execution_date_wib == 0:
        logging.info('this is weekly process')
        logging.info(f"weekly date = {execution_date_wib.weekday()}") 
        logging.info(f"monthly date = {execution_date_wib.day}")
        return 'Weekly'
    
    if execution_date_wib == 1: 
        logging.info('this is monthly process')
        logging.info(f"weekly date = {execution_date_wib.weekday()}") 
        logging.info(f"monthly date = {execution_date_wib.day}")
        return 'Monthly'

with DAG("Sending_email_reminder_to_partner_and_BO",
         schedule_interval = "0 4 * * *",
         catchup = False,
         start_date = datetime(2023,10,26)) as dag:
    
    start_task = BashOperator(
            task_id = "start_task",
            bash_command = "echo start task"
    )
    ## weekly session
    if get_reminder_type == "WEEKLY":
        branch_task = BranchPythonOperator(
            templates_dict = {'run_id': '{{ run_id }}'},
            task_id='branch_task',python_callable=decide_branch,
            provide_context = True
        )

        weekly = PythonOperator(
        task_id='Weekly',python_callable=lambda: logging.info("Executing weekly task.")
        )
        
        weekly_reminders = PythonOperator(
            task_id='weekly_reminder',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = email_reminder,
            provide_context = True,
            op_kwargs = {
                        "is_weekly":True})

        end_task = DummyOperator(task_id='end_task',trigger_rule=TriggerRule.ONE_SUCCESS ,dag=dag)
        start_task >> branch_task >> weekly >> weekly_reminders >> end_task

    ## monthly session
    elif get_reminder_type == "MONTHLY":
        branch_task = BranchPythonOperator(
            templates_dict = {'run_id': '{{ run_id }}'},
            task_id='branch_task',python_callable=decide_branch,
            provide_context = True
        )

        monthly = PythonOperator(
        task_id='Monthly',python_callable=lambda: logging.info("Executing monthly task.")
        ) 

        monthly_reminders = PythonOperator(
            task_id='monthly_reminders',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = email_reminder,
            provide_context = True,
            op_kwargs = {
                        "is_monthly":True})
        
        end_task = DummyOperator(task_id='end_task',trigger_rule=TriggerRule.ONE_SUCCESS ,dag=dag)
        start_task >> branch_task >> monthly >> monthly_reminders >> end_task

    ## default
    else:
        branch_task = BranchPythonOperator(
            templates_dict = {'run_id': '{{ run_id }}'},
            task_id='branch_task',python_callable=decide_branch,
            provide_context = True
        )
    
        weekly = PythonOperator(
            task_id='Weekly',python_callable=lambda: print("Executing weekly task.")
        )

        monthly = PythonOperator(
            task_id='Monthly',python_callable=lambda: print("Executing monthly task.")
        )    

        weekly_reminders = PythonOperator(
            task_id='weekly_reminder',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = email_reminder,
            provide_context = True,
            op_kwargs = {
                        "is_weekly":True})
        
        monthly_reminders = PythonOperator(
            task_id='monthly_reminders',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = email_reminder,
            provide_context = True,
            op_kwargs = {
                        "is_monthly":True})   

        end_task = DummyOperator(task_id='end_task',trigger_rule=TriggerRule.ONE_SUCCESS ,dag=dag)
        start_task >> branch_task  >> weekly >>  weekly_reminders >> end_task
        start_task >> branch_task  >> monthly >> monthly_reminders >> end_task