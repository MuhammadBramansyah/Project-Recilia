import json
import pandas as pd
import requests
import os
from datetime import datetime, timedelta
from pyhocon import ConfigFactory
import logging 
logging.basicConfig(level=logging.INFO)

from package.connection import Connections
from model.druid_query import main_query,get_partner_processed
from model.query_postgres_sftp import list_channel_partner

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()
read_file = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(read_file)

class SFTPUploader:
    def __init__(self, path):
        connection = BaseHook.get_connection("airflow_sftp_connection_IFS")
        self.path = path
        self.username = connection.login
        self.passwords = connection.password
        self.host = connection.host
        self.port = connection.port
        self.connection = Connections(
            sftp_host= self.host,
            sftp_username= self.username,
            sftp_password=self.passwords,
            sftp_port=self.port
        )

    def upload_file(self, df):
        sftp_client = self.connection.sftp_connection()
        with sftp_client.open(self.path, "w") as f:
            f.write(df.to_csv(index=False))
        return logging.info("data uploaded")

class DruidToSFTP:
    def __init__(self):
        druid_connection = BaseHook.get_connection("airflow_druid_connection")
        self.druid_host = druid_connection.host
        self.druid_api = "druid/v2/sql"
        self.path_match = Variable.get("MATCH_DATA")

    def get_data(self, query_function):
        url =f"{self.druid_host}/{self.druid_api}"
        headers = {'Content-Type': 'application/json'}
        query = query_function
        payload = json.dumps({"query": query})
        response = requests.request("POST",url, headers=headers, data=payload )
        return pd.read_json(response.text, dtype=str, convert_axes=False)
    
    @staticmethod
    def get_adjustment(row, zdate_series):
        date_1 = row['ZDATE']
        date_2 = zdate_series.get(row.name)  

        if date_2 is None:
            return None
        elif date_1 != date_2:
            parsed_date = datetime.strptime(date_1, "%d%m%Y")
            formatted_date = parsed_date.strftime("%Y-%m-%d")
            return f"Adjustment ({formatted_date})"
        else:
            return None

    @staticmethod
    def update_zmerchant(df):
        payment_mapping = conf.get("sap_config.merchant_config")
        df['ZMERCHANT'] = df.apply(lambda row: payment_mapping.get(f"{row['ZMERCHANT']}_{row['payment_method']}", row['ZMERCHANT']), axis=1)
        return df
    
    @staticmethod
    def list_cols_num_obj(df):
        df_list_cols_num = df.select_dtypes(include=['float64','int64'])
        df_list_cols_obj = df.select_dtypes(include='object')
        return df_list_cols_num, df_list_cols_obj
        
    def df_factory(self,df, df_ext):
        df_ext['partner_processed'] = pd.to_datetime(df_ext['partner_processed']).dt.strftime('%d%m%Y')

        df["ZDATE"] = pd.to_datetime(df["ZDATE"])
        df["ZDATE"] = df["ZDATE"].dt.strftime("%d%m%Y")

        ## section convert merchant from payment_method
        df = self.update_zmerchant(df)

        ## drop rows and columns
        df = df.drop(df[df['ZMERCHANT'] == 'FINNET'].index)
        df = df.drop(['payment_method','total_sales_amount'], axis=1)
        
        ## adjustment
        df['ZKETERANGAN'] = df.apply(self.get_adjustment, axis=1, zdate_series=df_ext['partner_processed'])

        ## converting column from string to numeric
        cols_object = ['ZDATE','ZBRANDAPPS','ZMERCHANT','payment_method','ZVENDPROD', 'ZVENDDELIV', 'ZREKBANK','ZKETERANGAN']
        cols_compare = df.columns.tolist()
        cols_numeric = list(set(cols_compare) - set(cols_object)) 
        df[cols_numeric] = df[cols_numeric].apply(pd.to_numeric, errors = 'coerce')

        ## list column for each data type and aggregate the data
        list_cols_num, list_cols_obj = self.list_cols_num_obj(df)
        df = df.groupby(list_cols_obj.columns.tolist(), as_index=False)[list_cols_num.columns.tolist()].sum()

        for c_code in df.ZBRANDAPPS.unique():
            df["ZBRANDAPPS"] = df["ZBRANDAPPS"].replace([c_code], [conf.get(f"channel_code.channel_name.{c_code}")])
        for p_code in df.ZMERCHANT.unique():
            df["ZMERCHANT"] = df["ZMERCHANT"].replace([p_code], [conf.get(f"partner_code.partner_name.{p_code}")])
        
        ## main df
        cols_target = conf.get("sap_config.target_cols")
        df = df[cols_target]

        ## round section
        df[cols_numeric] = df[cols_numeric].round()
        return df

    def druid_to_sftp(self,**context):
        now = datetime.now()
        sec = now.strftime('%H%M%S')
        yeseterday = now -timedelta(days=2)
        start_date = yeseterday.strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")

        run_id = context['templates_dict']['run_id']
        is_manual = run_id.startswith('manual__')
        if is_manual:
            try:
                config = Variable.get("START_DATE_MATCH")
                match_config = json.loads(config)

                start_date = match_config['start_date']
                end_date = match_config['end_date']
            except:
                raise Exception("Date Not Found, Please insert variable")
            else:
                start_date = start_date
                end_date = end_date

        logging.info(f'using this date for query = {start_date} & {end_date}')

        query = main_query(start_date,end_date)
        ext_query = get_partner_processed(start_date)

        df_main = self.get_data(query)
        df_ext = self.get_data(ext_query)
        
        if not df_main.empty:
            df = self.df_factory(df_main,df_ext)
            path = self.path_match+ f'ZMatch_{start_date}_{end_date}_{sec}.csv'

            uploader = SFTPUploader(path)
            uploader.upload_file(df)
            return logging.info(f"Data Uploaded To {path}")
        else:
            return logging.info("Data Not Found, Please Check The Data In Druid")

class PostgresToSftp:
    def __init__(self):
        pg_conn = BaseHook.get_connection("airflow_postgres_connection")
        self.postgres_host = pg_conn.host
        self.postgres_user = pg_conn.login
        self.postgres_password = pg_conn.password
        self.postgres_database = pg_conn.schema
        self.postgres_port = pg_conn.port

        self.channel_path = Variable.get("MATCH_DATA_REF_CHANNEL")
        self.partner_path = Variable.get("MATCH_DATA_REF_PARTNER")

        self.connection = Connections(
            postgres_host=self.postgres_host,
            postgres_username=self.postgres_user,
            postgres_password=self.postgres_password,
            postgres_database=self.postgres_database,
            postgres_port= self.postgres_port
        )
            
    def postgres_sftp(self, date):
        conn = self.connection.postgres_connection()
        curr = conn.cursor()

        tables = {
            'channel': {
                'query': list_channel_partner()[0][1],
                'columns': ['ZBRANDAPSS', 'ZBRANDNUM', 'Product Description'],
                'path': self.channel_path + f'ZBrandApps_{date}'
            },
            'partner': {
                'query': list_channel_partner()[1][1],
                'columns': ['ZMERCHANT', 'ZMERCHANTNUM', 'ZMERCHANTCODE'],
                'path': self.partner_path + f'ZMerchant_{date}'
            }
        }

        for table in tables.values():
            curr.execute(table['query'])
            data = curr.fetchall()
            df = pd.DataFrame(data, columns=[col[0] for col in curr.description])
            df.columns = table['columns']

            uploader = SFTPUploader(table['path'])
            uploader.upload_file(df)