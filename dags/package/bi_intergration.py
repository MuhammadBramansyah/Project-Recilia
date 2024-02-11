import logging
import pandas as pd
import json
from package.data_frame_factory_func import DataFrameFactory
from package.connection import Connections
from datetime import datetime,timedelta

logging.basicConfig(level=logging.INFO)

from airflow.models import Variable
from airflow.hooks.base import BaseHook

dataframe_factory = DataFrameFactory()

class PartnerIntergration:
    def __init__(self):
        pg_conn = BaseHook.get_connection("airflow_postgres_connection")
        self.host = pg_conn.host
        self.user = pg_conn.login
        self.password = pg_conn.password
        self.database = pg_conn.schema
        self.port = pg_conn.port

        sftp_connection = BaseHook.get_connection("airflow_sftp_server_connection")
        self.sftp_username = sftp_connection.login
        self.sftp_password = sftp_connection.password
        self.sftp_host = sftp_connection.host
        self.sftp_port = sftp_connection.port

        s3_conn = BaseHook.get_connection("airflow_s3_connection")
        extra  = s3_conn.get_extra()
        extra_convt = json.loads(extra)
        self.aws_acces_key = extra_convt['aws_access_key_id']
        self.aws_secret_key = extra_convt['aws_secret_access_key']
        self.aws_endpoint = extra_convt['host']
        self.aws_region = extra_convt['region']

        self.sftp_path = Variable.get("SFTP_BI_PATH")
        self.s3_bucket_name = Variable.get("S3_BUCKET_NAME")

        self.connections = Connections(
            sftp_host=self.sftp_host,
            sftp_port=self.sftp_port,
            sftp_username=self.sftp_username,
            sftp_password=self.sftp_password,
            s3_acces_key=self.aws_acces_key,
            s3_secret_key=self.aws_secret_key,
            s3_endpoint=self.aws_endpoint,
            s3_region=self.aws_region,
            postgres_database=self.database,
            postgres_host=self.host,
            postgres_port=self.port,
            postgres_username=self.user,
            postgres_password=self.password
        )

    def s3_to_sftp_new(self,**context):
        sftp = self.connections.sftp_connection()
        s3 = self.connections.s3_connection()
        
        now = datetime.now()
        pref_date = now-timedelta(days=1)
        upload_date = pref_date.strftime('%Y-%m-%d')
        
        run_id = context['templates_dict']['run_id']
        is_manual = run_id.startswith('manual__')
        if is_manual:
            try:
                upload_date = Variable.get("SFTP_BI_DATE")

            except:
                raise Exception("Date Not Found, Please insert variable")
        else:
            upload_date = upload_date

        logging.info(f"Start With Periode : {upload_date}.")

        df_raw_files = dataframe_factory.get_files_s3(upload_date)

        if df_raw_files.empty:
            return logging.info(f"No files found in transaction_logs periode {upload_date}. Exiting.")
        
        for index, row in df_raw_files.iterrows():
            path = row['s3_object'].replace('S3://finterlabs-storage/', '')
            file_name = path.split('/')[-1]
            base_name, extension = file_name.rsplit('.', 1)
            file_names = f"{base_name.rsplit('_', 1)[0]}.{extension}"
            
            logging.info(f"processing this file = {file_names}")
            
            file_object = s3.get_object(Bucket=self.s3_bucket_name, Key=path)
            file_content = file_object["Body"].read()
            sizebyte = file_object['ContentLength']
            file_row = row['total_rows']
            file_name = row['file_name']
            file_name_without_extension = file_name.split('.')[0]

            with sftp.file(self.sftp_path + file_names, 'wb') as f:
               f.write(file_content)
            logging.info(f"Done. File Partner successfully sended to {self.sftp_path}{path.split('/')[-1]}")

            data_control = pd.DataFrame({'FILENAME': [file_name], 'FILESIZE': [sizebyte], 'RECORDCOUNT': [file_row + 1]})
            csv_buffers = data_control.to_csv(index=False, sep="|")
    
            with sftp.file(f"{self.sftp_path}{file_name_without_extension}.ctl", 'w') as remote_file:
               remote_file.write(csv_buffers)

            logging.info(f"Done. File Control successfully sended to {self.sftp_path}control_{file_name_without_extension}")

        return logging.info("All files processed and sent.")
