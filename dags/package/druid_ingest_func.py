import json
import requests
from datetime import datetime,timedelta

from airflow.hooks.base import BaseHook
from airflow.models import Variable

connection = BaseHook.get_connection("airflow_druid_connection")
druid_host = connection.host
# druid_port = connection.port

# druid_host = "http://206.189.80.202"
# druid_port = "30463"

druid_api = "druid/indexer/v1/task"
druid_spec = "dags/json/rp_match_daily_newest_2.json"

def ingest_spec_druid(**context):
    ## dates 
    now = datetime.now() 
    now_yesterday = now - timedelta(days = 1)
    start_dates = now_yesterday.strftime('%Y-%m-%dT00:00:00')
    end_dates = now.strftime('%Y-%m-%dT00:00:00')

    druid_url = f"{druid_host}/{druid_api}"
    headersList = {
        "Content-Type": "application/json"
    }
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')
    if is_manual:
     try:
         start_dates = Variable.get("START_DATE_INGEST")
         end_dates = Variable.get("END_DATE_INGEST")
     except:
        raise Exception("Date Not Found, Please insert variable")

     else:
        start_dates = start_dates
        end_dates = end_dates

    print(f"using this date = {start_dates} & {end_dates}")

    with open(druid_spec, "rb") as spec:
        json_spec = json.load(spec)
    
    json_spec["spec"]["ioConfig"]["inputSource"]["interval"] = f"{start_dates}/{end_dates}"
    payload = json.dumps(json_spec)
    response = requests.post(druid_url, data=payload, headers=headersList)

    status = response.status_code
    if status == 200:
        print("Igeset Succeded")
    else:
        print(f"Ingest failed with status {response.status_code}")
    
    return status