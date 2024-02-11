import os 
import json 
import pandas as pd 
import json
from datetime import datetime, timedelta

from pyhocon import ConfigFactory
from airflow.models import Variable
from airflow.hooks.base import BaseHook

import logging 
logging.basicConfig(level=logging.INFO)

from package.connection import Connections
from package.data_frame_factory_func import DataFrameFactory
from model.druid_query import get_ebs
from package.send_email_func import send_email_reminder_to_ifs

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

    def path_parser(self, path, sftp):
        file_type = "txt" if not path.endswith(".txt") else path.split(".")[-1]
        file_name = path.split("/")[-1]
        url = f"S3://{self.bucket_name}/{path}"
        source = path.split("/")[-6]
        sftp_path = sftp
        return file_name, url, file_type, source, sftp_path

    def write_to_postgres(self, path_s3, path_sftp, is_reject=False):
        s3 = self.connections.s3_connection()
        conn = self.connections.postgres_connection()
        curr = conn.cursor()

        s3_path = path_s3
        file_name, url, file_type, source, sftp_path = self.path_parser(s3_path, path_sftp)

        result = s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_path)
        data = result.get("Contents", [])
        datetimes = data[0]["LastModified"] if data else datetime.now()
        query_date = datetimes.strftime("%Y-%m-%d %H:%M:%S")
        created_at = self.now.strftime("%Y-%m-%d %H:%M:%S")
        upload_date = self.now.strftime("%Y-%m-%d")

        status = 'C' if is_reject else 'R'
        description = 'Duplicate file name' if is_reject else None

        sql_query = """
            insert into transaction_logs(created_at, file_name, url, file_type, upload_date, 
                status, source, upload_to_s3, sftp_path, s3_object, description)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """

        record_to_insert = (created_at, file_name, url, file_type, upload_date, status, source, query_date, sftp_path, url, description)
        curr.execute(sql_query, record_to_insert)
        conn.commit()
        conn.close()
        return logging.info("Log Has Been Written Into Postgres")
        

class SftpToS3:
    def __init__(self, mandiri_sftp, bni_sftp, bca_sftp):
        self.dataframe_factory = DataFrameFactory()
        ## sftp config
        sftp_connection = BaseHook.get_connection("airflow_sftp_server_connection")
        self.sftp_username = sftp_connection.login
        self.sftp_password = sftp_connection.password
        self.sftp_host = sftp_connection.host
        self.sftp_port = sftp_connection.port

        ## s3 config connection
        s3_connection = BaseHook.get_connection("airflow_s3_connection")
        extra = s3_connection.get_extra()
        extra_convt = json.loads(extra)
        self.aws_access_key = extra_convt["aws_access_key_id"]
        self.aws_secret_key = extra_convt["aws_secret_access_key"]
        self.aws_endpoint = extra_convt["host"]
        self.aws_region = extra_convt["region"]
        self.bucket_name = Variable.get("S3_BUCKET_NAME")

        ## s3 path
        self.s3_path = Variable.get("EBS_S3_PATH")

        self.mandiri_sftp = mandiri_sftp
        self.bni_sftp = bni_sftp
        self.bca_sftp = bca_sftp
        
        self.connections = Connections(
            sftp_host=self.sftp_host,
            sftp_username=self.sftp_username,
            sftp_password=self.sftp_password,
            sftp_port=self.sftp_port,
            s3_acces_key=self.aws_access_key,
            s3_secret_key=self.aws_secret_key,
            s3_endpoint=self.aws_endpoint,
            s3_region=self.aws_region
        )

    @staticmethod
    def file_name_parser(file_name, is_bni=False, is_bca=False, is_mandiri=False):
        if is_bca:
            date_part = file_name.split('-')[1].split('.')[1][:8]
        elif is_bni:
            year_month_day = file_name.split('-')[1]
            date_part = year_month_day[0:4] + year_month_day[4:6] + year_month_day[6:8]
        elif is_mandiri:
            start_index = file_name.find('MT940') + 5
            date_part = file_name[start_index:start_index + 8]
        return date_part
    
    def upload_to_s3(self, remote_path, s3_path_prefix, is_bca=False, is_bni=False, is_mandiri=False):
        writer = WriteToPostgres()
        sftp = self.connections.sftp_connection()
        s3 = self.connections.s3_connection()

        now = datetime.now()
        year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')

        remote_files = sftp.listdir(remote_path)
        if remote_files:
            for file_name in remote_files:
                sftp_path = f"{remote_path}{file_name}"
                s3_path = f"{s3_path_prefix}/{year}/{month}/{day}/{file_name}"

                with sftp.open(sftp_path, "r") as f:
                    f.prefetch()
                    s3.put_object(Body=f, Bucket=self.bucket_name, Key=s3_path)

                logging.info(f"Processing This File = {file_name}")

                if is_bni or is_bca or is_mandiri:
                    file_extracted = self.file_name_parser(file_name, is_bni=is_bni, is_bca=is_bca, is_mandiri=is_mandiri)
                    df_ebs = self.dataframe_factory.ebs_checker(file_extracted, remote_path)

                    if df_ebs.empty:
                        writer.write_to_postgres(s3_path, sftp_path)
                    else:
                        logging.warning("File already exists in Recilia")
                        logging.warning(f"Set Status To 'C' For This File = {file_name}")
                        writer.write_to_postgres(s3_path, sftp_path, is_reject=True)

                sftp.remove(sftp_path)
        else:
            return logging.info("Data EBS Not Found")
        return logging.info("Processing Done. All EBS Data Has Been Sent To S3 Bucket")

    def ebs_mandiri(self):
        s3_path_prefix = f"{self.s3_path}Mandiri"
        remote_path = self.mandiri_sftp
        return self.upload_to_s3(remote_path, s3_path_prefix, is_mandiri=True)

    def ebs_bca(self):
        s3_path_prefix = f"{self.s3_path}BCA"
        remote_path = self.bca_sftp
        return self.upload_to_s3(remote_path, s3_path_prefix, is_bca=True)

    def ebs_bni(self):
        s3_path_prefix = f"{self.s3_path}BNI"
        remote_path = self.bni_sftp
        return self.upload_to_s3(remote_path, s3_path_prefix, is_bni=True)

class EmailReminderToIFS:
    def __init__(self):
        self.dataframe_factory = DataFrameFactory()

    @staticmethod
    def list_email_altert():
        configs = Variable.get("IFS_EMAIL")
        mails = json.loads(configs)
        return mails["RECEIVER"]["EMAIL"]
    
    def email_reminder_ifs(self):
        now = datetime.now()
        q_date = now - timedelta(days=1)
        logging.info(f"Fetchin Data From This Date = {q_date}")

        query_ebs = get_ebs(q_date)
        df_ebs = self.dataframe_factory.fetch_data_druid(query_ebs)
        if df_ebs.empty:
            df_ebs = pd.DataFrame(columns= ['__time', 'account_number', 'issued_bank'])
        else:
            df_ebs = df_ebs

        df_ebs = self.dataframe_factory.renaming_bank_value(df_ebs)
        df_ebs["__time"] = pd.to_datetime(df_ebs["__time"])
        df_ebs["__time"] = df_ebs["__time"].dt.strftime("%d/%m/%Y")

        df_ebs = df_ebs.rename(columns = conf.get("ebs_reminder_config.cols_target"))
        cols = ['Bank Name', 'Tanggal EBS']
        df_ebs = df_ebs[cols]

        list_bank_name_df = df_ebs['Bank Name'].unique().tolist()
        list_main = ['Mandiri','BNI','BCA']
        if list_collect := list(set(list_main) - set(list_bank_name_df)):
            self._extracted_from_email_reminder_ifs_28(list_collect, q_date)
        else:
            logging.info("All data EBS has been uploaded to recillia")
        return logging.info("Process Done")

    # TODO Rename this here and in `email_reminder_ifs`
    def _extracted_from_email_reminder_ifs_28(self, list_collect, q_date):
        df_main = self.dataframe_factory.dataframe_creator(list_collect)
        df_main['Tanggal EBS'] = q_date
        df_main["Tanggal EBS"] = pd.to_datetime(df_main["Tanggal EBS"])
        df_main["Tanggal EBS"] = df_main["Tanggal EBS"].dt.strftime("%d/%m/%Y")

        receiver = self.list_email_altert()
        send_email_reminder_to_ifs(receiver, "Email Reminder Daily", df_main)