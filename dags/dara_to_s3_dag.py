import os
import json 

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.models import Variable 
from pyhocon import ConfigFactory
from datetime import datetime

#import custom python function
from package import dara_to_s3_func

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
readfile = f'{CURRENT_FOLDER}/configFile/config.conf'
conf = ConfigFactory.parse_file(readfile)

## get config 
elisa_api = conf.get("dummy_api.ELISA_API")
byu_api = conf.get("dummy_api.BYU_API")

config= Variable.get("DARA_CONFIG")
dara_config = json.loads(config)
get_channel_type = dara_config["channel_type"]

dara_to_s3 = dara_to_s3_func
elisa = dara_to_s3.DaraToS3(api_url=elisa_api)
byu = dara_to_s3.DaraToS3(api_url=byu_api)

writers= dara_to_s3.WriteToPostgres()

# dates
date = "{{ macros.ds_add(ds, -2).replace('-','') }}"

with DAG("DARA_to_S3",
         schedule_interval = "0 */6 * * * ", 
         catchup = False,
         start_date = datetime(2022,10,13)) as dag:

    T1 = BashOperator(task_id = "start_task", bash_command="echo start")

    if get_channel_type == 'elisa':
        T2 = PythonOperator(
            task_id = 'generate_new_elisa_token',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = elisa.generate_token,
            provide_context = True,
            op_kwargs = {
                "is_elisa":True
            }
            )
        
        T3 = PythonOperator(
            task_id = 'Elisa_To_S3',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = elisa.fetch_data,
            provide_context = True,
            op_kwargs = {
                "date":date,
                "is_elisa":True
            }
            )

        T4 = PythonOperator(
            task_id = "write_log_elisa_to_Postgres",
            python_callable = writers.write_to_postgres,
            op_kwargs = {
                "channels_codes":"",
                "partners_codes":"",
                "task":"Elisa_To_S3"
            }
        )

        T8 = EmptyOperator(task_id='Task_done',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        T1 >> T2 >> T3 >> T4 >> T8

    elif get_channel_type =='BYU':

        T5 = PythonOperator(
                task_id = 'generate_new_byu_token',
                templates_dict = {'run_id': '{{ run_id }}'},
                python_callable = byu.generate_token,
                provide_context = True,
                op_kwargs = {
                "is_byu":True
                }
                )
        
        T6 = PythonOperator(
                task_id = 'Byu_To_S3',
                templates_dict = {'run_id': '{{ run_id }}'},
                python_callable = byu.fetch_data,
                provide_context = True,
                op_kwargs = {
                "date":date,
                "is_byu":True
                }
                )

        T7 = PythonOperator(
            task_id = "write_log_Byu_to_Postgres",
            python_callable = writers.write_to_postgres,
            op_kwargs = {
                "channels_codes":"BYU000",
                "partners_codes":"",
                "task":"Byu_To_S3"
            }
        )

        T8 = EmptyOperator(task_id='Done',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        T1 >> T5 >> T6 >> T7 >> T8

    else:
        T2 = PythonOperator(
            task_id = 'generate_new_elisa_token',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = elisa.generate_token,
            provide_context = True,
            op_kwargs = {
                "is_elisa":True
            }
            )
        
        T3 = PythonOperator(
            task_id = 'Elisa_To_S3',
            templates_dict = {'run_id': '{{ run_id }}'},
            python_callable = elisa.fetch_data,
            provide_context = True,
            op_kwargs = {
                "date":date,
                "is_elisa":True
            }
            )

        T4 = PythonOperator(
            task_id = "write_log_elisa_to_Postgres",
            python_callable = writers.write_to_postgres,
            op_kwargs = {
                "channels_codes":"",
                "partners_codes":"",
                "task":"Elisa_To_S3"
            }
        )
        
        T5 = PythonOperator(
                task_id = 'generate_new_byu_token',
                templates_dict = {'run_id': '{{ run_id }}'},
                python_callable = byu.generate_token,
                provide_context = True,
                op_kwargs = {
                "is_byu":True
                }
                )
        
        T6 = PythonOperator(
                task_id = 'Byu_To_S3',
                templates_dict = {'run_id': '{{ run_id }}'},
                python_callable = byu.fetch_data,
                provide_context = True,
                op_kwargs = {
                "date":date,
                "is_byu":True
                }
                )

        T7 = PythonOperator(
            task_id = "write_log_Byu_to_Postgres",
            python_callable = writers.write_to_postgres,
            op_kwargs = {
                "channels_codes":"BYU000",
                "partners_codes":"",
                "task":"Byu_To_S3"
            }
        )

        T8 = EmptyOperator(task_id='Task_done',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        T1 >> T2 >> T3 >> T4 >> T5 >> T8



    