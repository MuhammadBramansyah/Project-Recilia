import os 
from cassandra.query import SimpleStatement
import pandas as pd
import math
import json
import requests
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from pyhocon import ConfigFactory

from package.connection import Connections
from model.query_postgres_sftp import get_email,query_reminder,query_s3_object,query_check_before_update,query_update_satatus,get_ebs
from model.druid_query import channel_partner_code_druid

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()
read_file = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(read_file)

class DataFrameFactory:
    def __init__(self):
        pg_conn = BaseHook.get_connection("airflow_postgres_connection")
        self.connections = Connections(
            postgres_username=pg_conn.login,
            postgres_password=pg_conn.password,
            postgres_database=pg_conn.schema,
            postgres_host=pg_conn.host,
            postgres_port=pg_conn.port
        )
    
    @staticmethod
    def remove_nan(lst):
        list_new = []
        for x in lst:
            try:
                if math.isnan(x):
                    list_new = list_new
            except:
                list_new.append(x)
        return list_new

    @staticmethod
    def remove_empty_string(lst):
        return [x for x in lst if x != '']
    
    @staticmethod
    def dataframe_format(x):
        return x if isinstance(x, str) else "{:,.2f}".format(x).replace(",", ".")
    
    def get_emails(self,channels_code, BO = False,PARTNER = False):
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query_2 = get_email()

        if BO:
            cust_query = f""" 
                        select channel_email,partner_code from channel_partner
                        where channel_code = '{channels_code}' and is_bo = true
            """
        elif PARTNER:
            cust_query = f""" 
                        select partner_email,partner_code from channel_partner
                        where channel_code = '{channels_code}' and is_partner = true
            """
        query_2 += cust_query
        curr.execute(query_2)
        data = curr.fetchall()
        return pd.DataFrame(data,columns=[col[0] for col in curr.description])

    def list_channel_partner(self,df_unmatch):
        list_channel_code_x = self.remove_nan(df_unmatch.channel_code_x.unique())
        list_partner_code_x = self.remove_nan(df_unmatch.partner_code_x.unique())

        list_channel_code_y = self.remove_nan(df_unmatch.channel_code_y.unique())
        list_partner_code_y = self.remove_nan(df_unmatch.partner_code_y.unique())

        list_channel_code = set().union(list_channel_code_x,list_channel_code_y)
        list_partner_code = set().union(list_partner_code_x,list_partner_code_y)

        list_channel_codes = self.remove_empty_string(list_channel_code)
        list_partner_codes = self.remove_empty_string(list_partner_code)

        return list_channel_codes,list_partner_codes

    def summary_table(self,channel_code, df_match, df_unmatch):
        # Match section
        df_match = df_match[(df_match['channel_code_x'] == channel_code) | (df_match['channel_code_y'] == channel_code)]
        df_total_amount_match = df_match.groupby(['partner_code_y'])['transaction_amount_x'].sum()
        df_total_transaction_match = df_match.groupby(['partner_code_y'])['transaction_id'].count()

        # Unmatch section
        df_unmatch_channel = df_unmatch[df_unmatch['channel_code_x'] == channel_code]
        df_unmatch_partner = df_unmatch[df_unmatch['channel_code_y'] == channel_code]
        df_total_transaction_unmatch_channel = df_unmatch_channel.groupby(['partner_code_x'])['transaction_id'].count()
        df_total_transaction_unmatch_partner = df_unmatch_partner.groupby(['partner_code_y'])['transaction_id_r'].count()
        df_total_amount_unmatch_channel = df_unmatch_channel.groupby(['partner_code_x'])['transaction_amount_x'].sum()
        df_total_amount_unmatch_partner = df_unmatch_partner.groupby(['partner_code_y'])['transaction_amount_y'].sum()

        # Percentage section
        df_total_match_channel = df_match[df_match['channel_code_x'] == channel_code].groupby(['partner_code_x'])['transaction_id'].count()
        df_total_match_unmatch_channel = df_total_transaction_unmatch_channel + df_total_match_channel
        df_presentase_channel = (df_total_transaction_unmatch_channel / df_total_match_unmatch_channel) * 100
        df_total_match_partner = df_match[df_match['channel_code_y'] == channel_code].groupby(['partner_code_y'])['transaction_id_r'].count()
        df_total_match_unmatch_partner = df_total_transaction_unmatch_partner + df_total_match_partner
        df_presentase_partner = (df_total_transaction_unmatch_partner / df_total_match_unmatch_partner) * 100

        # Join all aggregations
        match_tables = pd.concat([df_total_transaction_match, df_total_amount_match], axis=1)
        total_trans_unmatch = pd.concat([df_total_transaction_unmatch_channel, df_total_transaction_unmatch_partner], axis=1)
        total_amount_unmatch = pd.concat([df_total_amount_unmatch_channel, df_total_amount_unmatch_partner], axis=1)
        presentase_channel_partner = pd.concat([df_presentase_channel, df_presentase_partner], axis=1)

        # Results
        result_table = pd.concat([match_tables, total_trans_unmatch, total_amount_unmatch, presentase_channel_partner], axis=1)
        df = result_table.reset_index()
        df.columns = conf.get("cols_.COLUMNS.BODY_COLS.SUMMARY_COLS")
        df = df.dropna(subset=conf.get("cols_.COLUMNS.BODY_COLS.DROP_COLS"), how='all')

        return df 

    def list_user_email(self): ## get list_email_bo_partner
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query = get_email()

        cust_query = """ 
            select channel_email,partner_email,channel_code,partner_code, is_bo,is_partner from channel_partner
        """
        query += cust_query
        curr.execute(query)
        data = curr.fetchall()
        return pd.DataFrame(data,columns=[col[0] for col in curr.description])

    def transaction_logs(self,start_date): ## trx logs
        conn = self.connections.postgres_connection()
        curr = conn.cursor()

        query_2 = query_reminder(start_date)
        curr.execute(query_2)
        data = curr.fetchall()

        return pd.DataFrame(data,columns=[col[0] for col in curr.description])

    def fetch_data_druid(self,query):
        ## druid url 
        connection = BaseHook.get_connection("airflow_druid_connection")
        druid_host = connection.host
        druid_api = "druid/v2/sql"
        url = f"{druid_host}/{druid_api}"

        payload = json.dumps({"query": query})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        return pd.read_json(response.text)

    def dataframe_checker(self,df_match,df_unmatch):
        if df_match.empty:
            df_match = pd.DataFrame(columns=conf.get("cols_.COLUMNS.BODY_COLS.DEFAULT_COLS"))
        else:
            df_match = df_match
        
        df_unmatch = df_unmatch.loc[(~df_unmatch['transaction_id_r'].isin(df_match['transaction_id_r'])) &
                                    (~df_unmatch['transaction_id'].isin(df_match['transaction_id']))]
        
        for column in ['day_month_year_x', 'day_month_year_y']:
            df_unmatch[column] = pd.to_datetime(df_unmatch[column]).dt.strftime('%Y/%m/%d')
        return df_match,df_unmatch
    
    def dataframe_body_converter(self,df,is_it = False, is_partner = False, is_bo = False):
        if is_it:
            columns_to_format = conf.get("cols_.COLUMNS.TRANS_COLS.IT_TRANS_COLS")
        elif is_partner:
            columns_to_format = conf.get("cols_.COLUMNS.TRANS_COLS.PARTNER_TRANS_COLS")
        else:
            columns_to_format = conf.get("cols_.COLUMNS.TRANS_COLS.BO_TRANS_COLS")

        for cols in columns_to_format:
            df[cols] = df[cols].apply(self.dataframe_format)
        return df  

    def get_files_s3(self,upload_date): ## put to Dataframe Factory
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query = query_s3_object(upload_date)

        curr.execute(query)
        data = curr.fetchall()
        return pd.DataFrame(data,columns=[col[0] for col in curr.description])
    
    def get_status_f_for_update(self,retry_date):
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query = query_check_before_update(retry_date)

        curr.execute(query)
        data = curr.fetchall()
        return pd.DataFrame(data,columns=[col[0] for col in curr.description])
    
    def update_status_f_for_update(self,transaction_id):
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query = query_update_satatus(transaction_id)

        curr.execute(query)
        conn.commit()
        return True
    
    def ceheck_status_druid_for_status_f(self,query):
        ## druid url 
        connection = BaseHook.get_connection("airflow_druid_connection")
        druid_host = connection.host
        druid_api = "druid/v2/sql"
        url = f"{druid_host}/{druid_api}"

        payload = json.dumps({"query": query})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        return pd.read_json(response.text)    
    
    def df_target_reminder(self,start_date, end_date):
        df_combined = pd.DataFrame()
        date_range_list = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]
       
        for date in date_range_list:
            ## druid section
            query_exact = channel_partner_code_druid(date)
            df_exact = self.fetch_data_druid(query_exact)

            if df_exact.empty:
                df_exact = pd.DataFrame(columns=["__time", "channel_code", "partner_code"])
            else:
                df_exact = df_exact

            df_exact['concat_id'] = df_exact['channel_code'] + df_exact['partner_code']
            df_exact['__time'] = pd.to_datetime(df_exact['__time']).dt.strftime('%Y/%m/%d')

            ## postgres section
            df_partner_bo = self.list_user_email()
            df_partner_bo['concat_id'] = df_partner_bo['channel_code'] + df_partner_bo['partner_code']

            list_none_id = [
                elemen
                for elemen in df_partner_bo["concat_id"].unique().tolist()
                if elemen not in df_exact["concat_id"].unique().tolist()
            ]

            df_target = df_partner_bo[df_partner_bo['concat_id'].isin(list_none_id)]
            df_target['date'] = date
            df_combined = pd.concat([df_combined, df_target])
        return df_combined
    
    def renaming_bank_value(self,df):
        for bank_name in df["issued_bank"].values:
            df["issued_bank"] = df["issued_bank"].replace(
            [bank_name], [conf.get(f"ebs_reminder_config.bank_name.{bank_name}")])    
        return df
    
    def dataframe_creator(self,list):
        data = {'Bank Name': list}
        df = pd.DataFrame(data)
        return df
    
    def ebs_checker(self,file_name,ebs_path):
        conn = self.connections.postgres_connection()
        curr = conn.cursor()
        query = get_ebs()

        cust_query = f""" 
                    select * from ebs_table
                    where file_name like '%{file_name}%' and sftp_path like '{ebs_path}%'
        """
        query += cust_query
        curr.execute(query)
        data = curr.fetchall()

        return pd.DataFrame(data,columns=[col[0] for col in curr.description])