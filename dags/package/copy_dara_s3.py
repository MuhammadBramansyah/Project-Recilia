import boto3 
from botocore.config import Config
from datetime import datetime, timedelta
import json

from airflow.models import Variable
from airflow.hooks.base import BaseHook

now = datetime.now() + timedelta(hours=7)
year = now.strftime('%Y')
month = now.strftime('%m')
day = now.strftime('%d')
start_date = now.strftime('%Y%m%d')
times = now.strftime('%H%M%S')

## get aws config from airflow connection
connection = BaseHook.get_connection("airflow_s3_connection")
extra = connection.get_extra()
extra_convt = json.loads(extra)

aws_access_key_id = extra_convt["aws_access_key_id"]
aws_access_secret_key = extra_convt["aws_secret_access_key"]
aws_end_point = extra_convt["host"]
aws_region = extra_convt["region"]

## s3 bucket
bucket_name = Variable.get("S3_BUCKET_NAME")

## source data
# source_data = "UAT_data/Channel/Data_DARA/data_dara_newest.csv"

def copy_to_s3(path,date,source_data):

    path = path
    dates =date
    
    my_config = Config(
    region_name = aws_region,
    retries = {
        'max_attempts':10,
        'mode': 'standard'
    }
        )
    s3 = boto3.client(
                        's3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_access_secret_key,
                        endpoint_url = aws_end_point,
                        verify = False,
                        config = my_config
        )

    copy_source = {
        "Bucket":bucket_name,
        "Key":source_data
    }

    ## copy file to destination folder
    s3.copy(copy_source,Key=path,Bucket=bucket_name)

    return path