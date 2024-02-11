import logging
import pandas as pd
from package.data_frame_factory_func import DataFrameFactory
from package.connection import Connections
from model.druid_query import find_in_rp_merge
from datetime import datetime,timedelta

logging.basicConfig(level=logging.INFO)

from airflow.models import Variable
from airflow.hooks.base import BaseHook

dataframe_factory = DataFrameFactory()

class RerunFailedTimeOut:
    def __init__(self):
        pg_conn = BaseHook.get_connection("airflow_postgres_connection")
        self.host = pg_conn.host
        self.user = pg_conn.login
        self.password = pg_conn.password
        self.database = pg_conn.schema
        self.port = pg_conn.port

        self.connections = Connections(
            postgres_database=self.database,
            postgres_host=self.host,
            postgres_port=self.port,
            postgres_username=self.user,
            postgres_password=self.password
        )
    
    def update_status_failed(self,**context):
        now = datetime.now()
        retry_date = now.strftime('%Y-%m-%d')
        
        run_id = context['templates_dict']['run_id']
        is_manual = run_id.startswith('manual__')
        if is_manual:
            try:
                retry_date = Variable.get("RETRY_STATUS_DATE")

            except:
                raise Exception("Date Not Found, Please insert variable")
        else:
            retry_date = retry_date

        logging.info(f"Start With Created At : {retry_date}.")


        status_before = dataframe_factory.get_status_f_for_update(retry_date)
        num_rows = len(status_before)
        match_count = 0
        not_match_count = 0
        update_success_count = 0
        update_fail_count = 0

        if num_rows > 0:
            for index, row in status_before.iterrows():
                transaction_id = row['id']
                partners_code = row['partners_code']
                channels_code = row['channels_code']
                logging.info(f"ID: {row['id']}, partners_code : {row['partners_code']}, channels_code : {row['channels_code']}, Status: {row['status']}, File Name: {row['file_name']}")
                query_id_match = find_in_rp_merge(transaction_id,partners_code,channels_code)
                df_match = dataframe_factory.ceheck_status_druid_for_status_f(query_id_match)
                if len(df_match) > 0:
                    match_count += 1
                    logging.info(f"Data {transaction_id} Match in Druid")
                else:
                    not_match_count += 1
                    logging.info(f"Data {transaction_id} Not Match in Druid Status will Update to R")
                    update_status = dataframe_factory.update_status_f_for_update(transaction_id)
                    if update_status:
                        update_success_count += 1
                        logging.info("Update Success!!!")
                    else:
                        update_fail_count += 1
                        logging.info("Update Failed!!!")
                    
        else:
            logging.info("Tidak ada data yang akan di update")
        
        logging.info(f"Summary: Matches in Druid: {match_count}, Not Matches in Druid: {not_match_count}, Update Success: {update_success_count}, Update Failed: {update_fail_count}")

