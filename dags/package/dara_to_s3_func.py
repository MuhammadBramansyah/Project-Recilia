import os 
import requests
import pandas as pd
import json
from io import StringIO
from datetime import datetime, timedelta
from pyhocon import ConfigFactory

import logging 
logging.basicConfig(level=logging.INFO)

from pyhocon import ConfigFactory
import numpy as np

from airflow.models import Variable
from airflow.hooks.base import BaseHook

from package.connection import Connections

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()
read_file = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(read_file)

class WriteToPostgres:
    def __init__(self):
        self.now = datetime.now()
        pg_conn = BaseHook.get_connection("airflow_postgres_connection")
        self.host = pg_conn.host
        self.user = pg_conn.login
        self.password = pg_conn.password
        self.database = pg_conn.schema
        self.port = pg_conn.port

        s3_conn = BaseHook.get_connection("airflow_s3_connection")
        extra  = s3_conn.get_extra()
        extra_convt = json.loads(extra)
        self.aws_acces_key = extra_convt['aws_access_key_id']
        self.aws_secret_key = extra_convt['aws_secret_access_key']
        self.aws_endpoint = extra_convt['host']
        self.aws_region = extra_convt['region']
        self.bucket_name = Variable.get("S3_BUCKET_NAME")

        self.connections = Connections(
            postgres_host=self.host,
            postgres_username=self.user,
            postgres_password= self.password,
            postgres_database=self.database,
            postgres_port= self.port,
            s3_acces_key=self.aws_acces_key,
            s3_secret_key=self.aws_secret_key,
            s3_endpoint= self.aws_endpoint,
            s3_region=self.aws_region
        )
    
    def query_channel_id(self,channel_codes):
        return f"""
                select id
                from channels c 
                where code = '{channel_codes}';
            """

    def query_partner_id(self,partner_codes):
        return f"""
                select id
                from partners c 
                where code = '{partner_codes}';
            """

    def get_channel_id(self,c_codes,curr):
        query = self.query_channel_id(c_codes)
        channel_name = self.query_executer(query, curr)
        return channel_name[0]

    def get_partner_id(self,p_codes,curr):
        query = self.query_partner_id(p_codes)
        partner_name = self.query_executer(query, curr)
        if partner_name:
            return partner_name[0]
        else:
            return None

    def query_executer(self, query, curr):
        sql = query
        curr.execute(sql)
        result = curr.fetchall()
        result = [x[0] for x in result]
        return result

    def path_parser(self,c_code,p_code,path):
        conn = self.connections.postgres_connection()
        curr = conn.cursor() 
        
        file_type = path.split(".")[-1]
        file_name = path.split("/")[-1]
        url = f"S3://{self.bucket_name}/{path}"
        source = path.split("/")[-5] 

        if c_code != "":
            channel_id = self.get_channel_id(c_code, curr)
            partner_id = self.get_partner_id(p_code,curr)
        else:
            channel_id = None
            partner_id = None
        return file_name,url,file_type,channel_id,partner_id,source 

    def write_to_postgres(self, channels_codes,partners_codes,task,**kwargs):
        s3 = self.connections.s3_connection()
        conn = self.connections.postgres_connection()
        curr = conn.cursor() 

        ti = kwargs['ti']
        s3_path = ti.xcom_pull(task_ids = task)
        
        file_name,url,file_type,channel_id,partner_id,source = self.path_parser(channels_codes,partners_codes,s3_path)

        result = s3.list_objects_v2(
            Bucket = self.bucket_name,
            Prefix = s3_path
        )
        if 'Contents' in result:
            data = result["Contents"]
            datetimes = data[0]["LastModified"] 
            qdate = datetimes.strftime("%Y-%m-%d %H:%M:%S")
            query_date = qdate
            created_at = self.now.strftime("%Y-%m-%d %H:%M:%S")
            upload_date = self.now.strftime("%Y-%m-%d")
            sql_query = """
                insert into transaction_logs(created_at,file_name, url, file_type,upload_date, channel_id, partner_id,status,source,upload_to_s3,s3_object)
                        values (%s,%s,%s,%s,%s,%s,%s,'R',%s,%s,%s) 
            """
            record_to_insert = (created_at,file_name,url,file_type,upload_date, channel_id,partner_id,source,query_date,url)
        curr.execute(sql_query,record_to_insert)
        conn.commit()
        conn.close()
        return logging.info("Log Has Been Writted Into Postgres")
            
class DaraToS3:
    def __init__(self, api_url):
        self.api_url = api_url
        s3_connection = BaseHook.get_connection("airflow_s3_connection")
        extra = s3_connection.get_extra()
        extra_convt = json.loads(extra)
        self.aws_access_key = extra_convt["aws_access_key_id"]
        self.aws_secret_key = extra_convt["aws_secret_access_key"]
        self.aws_endpoint = extra_convt["host"]
        self.aws_region = extra_convt["region"]
        self.bucket_name = Variable.get("S3_BUCKET_NAME")

        self.connections = Connections(
            s3_acces_key= self.aws_access_key,
            s3_secret_key=self.aws_secret_key,
            s3_endpoint=self.aws_endpoint,
            s3_region=self.aws_region
        )
        self.s3_path = Variable.get("CHANNEL_S3_PATH")
        
    def upload_to_s3(self, path,df):
        csv_buf = StringIO()
        df.replace(r'^\s*$', np.nan, regex=True)
        df.to_csv(csv_buf, index= False, sep="|", quotechar="'") 
        csv_buf.seek(0)

        ## s3_connection
        s3 = self.connections.s3_connection()
        s3_path = path
        s3.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(), Key=s3_path)

        return s3_path
    
    def get_token(self,var_name,url,ref_token):
        token = Variable.get(var_name)
        url = url
        headers = {"Authorization": f"Bearer {token}", "refresh-token": ref_token}

        response = requests.get(url,headers=headers)
        data = response.json()
        token = data['access_token']

        Variable.set(key = var_name, value = token)
        return token
    
    def generate_token(self,is_elisa=False,is_byu=False):
        if is_elisa:
            return self.get_token(
                'TOKEN_ELISA',
                conf.get("REFRESH_TOKEN.ELISA_API"),
                conf.get("REFRESH_TOKEN.REFRESH_TOKEN_ELISA")
            )
        elif is_byu:
            return self.get_token(
                'TOKEN_BYU',
                conf.get("REFRESH_TOKEN.BYU_API"),
                conf.get("REFRESH_TOKEN.REFRESH_TOKEN_BYU")
            )

    def pull_data(self,var_name,start_date,end_date):
        token = Variable.get(var_name)
        headers_get_data = {
                "Authorization":f"Bearer {token}"
            }
        url = self.api_url + f"start_period={start_date}&end_period={end_date}"
        response_api = requests.get(url,headers=headers_get_data).content
        return pd.read_csv(StringIO(response_api.decode("utf-8")), sep="|", skiprows=1)

    def get_data(self,start_date,end_date,is_elisa=False,is_byu=False):
        start_date = start_date
        end_date = end_date
        
        if is_elisa:
            return self.pull_data('TOKEN_ELISA',start_date,end_date)
        elif is_byu:
            return self.pull_data('TOKEN_BYU',start_date,end_date)
        
    def fetch_data(self, date,is_elisa=False,is_byu=False,**context):
        now = datetime.now() 
        now_mins_2 = now - timedelta(days=2)
        year = now_mins_2.strftime('%Y')
        month = now_mins_2.strftime('%m')
        day = now_mins_2.strftime('%d')
        times = now_mins_2.strftime('%H%M%S')

        start_date = date
        end_date = date
        run_id = context['templates_dict']['run_id']
        is_manual = run_id.startswith('manual__')
        if is_manual:
            try:
                config = Variable.get("DARA_CONFIG")
                dara_config = json.loads(config)

                start_date = dara_config['start_date']
                end_date = dara_config['end_date']
                year = now.strftime('%Y')
                month = now.strftime('%m')
                day = now.strftime('%d')
                times = now.strftime('%H%M%S')
            except:
                raise Exception("Date Not Found, Please insert variable")
        else:
            start_date = start_date
            end_date = end_date
        logging.info(f"used this start_date = {start_date} and end_date = {end_date} ")
        
        if is_elisa:
            df = self.get_data(start_date,end_date,is_elisa=True)
        elif is_byu:
            df = self.get_data(start_date, end_date, is_byu= True)

        s3_path = f"{self.s3_path}{year}/{month}/{day}/RECILIA_DARA_{start_date}_{times}.csv"
        return self.upload_to_s3(s3_path,df) ## send data to s3

