import os
import pandas as pd 
import json 
from datetime import datetime, timedelta
from pyhocon import ConfigFactory
import warnings
import logging
logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")

from package.connection import Connections
from package.data_frame_factory_func import DataFrameFactory
from package.send_email_func import send_email_recon_alert
from model.cassandra_query import cql_agg
from model.druid_query import druid_agg

from airflow.hooks.base import BaseHook
from airflow.models import Variable

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()

readfile = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(readfile)

class CassandraDruidRecord():
    def __init__(self):
        self.dataframe_factory = DataFrameFactory()
        cq_conn = BaseHook.get_connection("airflow_cassandra_connection")
        self.connections = Connections(
            cassandra_host=cq_conn.host,
            cassandra_username=cq_conn.login,
            cassandra_password = cq_conn.password,
            cassandra_keyspace=cq_conn.schema,
            cassandra_port=cq_conn.port
        )

    @staticmethod
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    
    @staticmethod
    def convert_columns_str_to_int(df, columns):
        for column in columns:
            df[column] = df[column].fillna(0).astype(int)
        return df

    @staticmethod
    def list_email_altert():
        configs = Variable.get("ALERTING_EMAIL")
        mails = json.loads(configs)
        return mails["RECEIVER"]["EMAIL"]  

    @staticmethod
    def drop_zero_value(df):
        for cols in df.columns[-2:].tolist():
            df = df[df[cols] !=0]
        return df  
    
    def recorder(self,is_manual=False,**context):
        date_now = datetime.now()
        start = date_now - timedelta(days=5)
        start_date = start.strftime('%Y-%m-%d')
        end_date = date_now.strftime('%Y-%m-%d')
        
        run_id = context['templates_dict']['run_id']
        is_manual = run_id.startswith('manual__')
        if is_manual:
            try:
                config = Variable.get("DATE_RECON_ALERT")
                alert_config = json.loads(config)
                start_date = alert_config["RANGE_DATE"]["START_DATE"]
                end_date = alert_config["RANGE_DATE"]["END_DATE"]
            except:
                raise Exception("Date Not Found, Please insert variable")
        else:   
            start_date =start_date 
            end_date = end_date 
        
        logging.info(f"Range of date  : {start_date} ~ {end_date}")
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        date_range_list = [(start_dt + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_dt - start_dt).days + 1)]

        session = self.connections.cassandra_connection()
        session.row_factory = self.pandas_factory
        session.default_fetch_size = None

        df_cassandra = pd.DataFrame()
        df_druid = pd.DataFrame()
        df_merged = pd.DataFrame()

        for date in date_range_list:
            logging.info(f"querying date : {date}")
            for c_code in conf.get("channels_map.channels"):
                for p_code in conf.get(f"channels_map.{c_code}"):
                    query = cql_agg(date,c_code, p_code)
                    rslt = session.execute(query, timeout=None)
                    df_cass = rslt._current_rows

                    druid_query = druid_agg(date, c_code, p_code)
                    df_dr = self.dataframe_factory.fetch_data_druid(druid_query)

                    if df_dr.empty:
                        df_dr = pd.DataFrame(columns=[ "__time","channel_code", "partner_code","druid_counts","druid_amount"])
                    else:
                        df_dr = df_dr
                    df_cassandra = df_cassandra.append(df_cass)
                    df_dr['__time'] = pd.to_datetime(df_dr['__time']).dt.strftime('%Y-%m-%d')
                    df_druid = df_druid.append(df_dr)
        
        df_merged = pd.merge(df_druid, df_cassandra, left_on=['__time', 'channel_code', 'partner_code'], right_on=['day_month_year', 'channel_code', 'partner_code'], how='outer')
        if df_merged.empty:
            logging.info("All data completed sync to Druid")
        else:
            cols_numeric = [
                            'cassandra_counts','cassandra_amount','druid_counts','druid_amount'
                            ]
            df_merged = self.convert_columns_str_to_int(df_merged,cols_numeric)
            df_merged['different_rows'] = df_merged['cassandra_counts'] - df_merged['druid_counts']
            df_merged['different_amount'] = df_merged['cassandra_amount'] - df_merged['druid_amount']
            df_merged = df_merged.drop(['day_month_year'], axis=1)
            df_merged = df_merged.round()
            df_merged = self.drop_zero_value(df_merged)
            df_merged.rename(columns={'__time':"druid_date",'day_month_year':'cassandra_date'}, inplace= True)
            receiver_mails = self.list_email_altert()
            send_email_recon_alert(receiver_mails, df_merged)
            return logging.info("Report has been sended.")

                    


