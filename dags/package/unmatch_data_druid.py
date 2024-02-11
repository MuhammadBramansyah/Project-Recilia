import os
import pandas as pd
import requests
import json 
from datetime import datetime, timedelta
from pyhocon import ConfigFactory

import logging 
logging.basicConfig(level=logging.INFO)
import warnings
warnings.filterwarnings("ignore")
from airflow.models import Variable

from model.druid_query import match_query, unmatch_query
from package.data_frame_factory_func import DataFrameFactory
from package.send_email_func import send_email
from package.read_json import it_email

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()
read_file = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(read_file)

now = datetime.now()
yesterday = now - timedelta(days=2)
dataframe_factory = DataFrameFactory()

def unmatch_data(**context):  
    start_date = yesterday.strftime('%Y-%m-%d')
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')

    if is_manual:
        try:
            start_date =Variable.get("START_DATE_DRUID")
        except:
            raise Exception("date failed")
        else:
            start_date = start_date 
    logging.info(f"using this date {start_date}")

    query_match = match_query(start_date)
    query_unmatch = unmatch_query(start_date)

    df_match = dataframe_factory.fetch_data_druid(query_match)
    df_unmatch = dataframe_factory.fetch_data_druid(query_unmatch)

    if not df_unmatch.empty:
        df_match, df_unmatch = dataframe_factory.dataframe_checker(df_match,df_unmatch)
        list_channel_codes, list_partner_codes = dataframe_factory.list_channel_partner(df_unmatch)
        for c_code in list_channel_codes:
            df_raw = dataframe_factory.summary_table(c_code,df_match,df_unmatch)

            ## Attachtment and sending Email Mechanism
            for reciver_type in conf.get("recipiants.RECIPIANTS_TYPE"):
                if reciver_type == 'PARTNER':
                    df = df_raw[df_raw["Total Transaction Unmatch Tsel"].notnull()]
                    list_partner_codes = df_raw[df_raw["Total Transaction Unmatch Tsel"].notnull()]["Payment Channel"].unique().tolist()
                elif reciver_type == 'IT':
                    df = df_raw[df_raw["Total Transaction Unmatch Partner"].notnull()]
                    list_partner_codes = df_raw[df_raw["Total Transaction Unmatch Partner"].notnull()]["Payment Channel"].unique().tolist()
                else:
                    df = df_raw
                    list_partner_codes = df_raw["Payment Channel"].unique().tolist()
            
                if df.empty:
                    continue

                dataframe_collection = {}
                for p_code in list_partner_codes:
                    df_unmatch_test = df_unmatch.loc[(df_unmatch.channel_code_x == c_code) & (df_unmatch.partner_code_x == p_code) | (df_unmatch.channel_code_y == c_code) & (df_unmatch.partner_code_y == p_code)]

                    if ((df_unmatch_test['channel_code_x'] == c_code) &(df_unmatch_test['partner_code_x'] == p_code) | (df_unmatch_test['channel_code_y'] == c_code)& (df_unmatch_test['partner_code_y'] == p_code)).any() == True:
                        df_results = df_unmatch_test

                        ## attachmetn logic
                        if reciver_type == "IT":    
                            df_results = df_results[df_results['transaction_id_r'] != '']
                        elif reciver_type == "PARTNER":
                            df_results = df_results[df_results['transaction_id'] != '']
                        
                        if c_code == "MYGRAP":
                            mygrap_report = ["trilogy","nutech","mcash"]
                            df_temp = df_results
                            for col_report in mygrap_report:
                                df_results = df_temp
                                cols_channel = conf.get(f"cols_.{c_code}.{p_code}.{col_report}.cols_channel")
                                cols_partner = conf.get(f"cols_.{c_code}.{p_code}.{col_report}.cols_partner")
                                df_results = df_results[cols_channel]
                                df_results.columns = cols_partner
                                dataframe_collection[f"{p_code}_{col_report}"] = df_results
                        else:   
                            cols_channel = conf.get(f"cols_.{c_code}.{p_code}.cols_channel")
                            cols_partner = conf.get(f"cols_.{c_code}.{p_code}.cols_partner")
                            df_results = df_results[cols_channel]
                            df_results.columns = cols_partner
                            dataframe_collection[p_code] = df_results

                    df["Payment Channel"] = df["Payment Channel"].replace([p_code], [conf.get(f"partner_code.partner_name.{p_code}")])
                
                ## logic for sending email mechanism
                if reciver_type == "IT":
                    cols = conf.get("cols_.COLUMNS.BODY_COLS.IT_COLS")
                    df_custom = df[cols]
                    receiver = it_email(c_code)
                    send_email(dataframe_collection,receiver,f"Recillia - Notifikasi Hasil Rekonsiliasi {conf.get(f'channel_code.channel_name.{c_code}')}",df_custom,conf.get(f'channel_code.channel_name.{c_code}'),start_date,is_it=True)
                elif reciver_type == "PARTNER":
                    cols = conf.get("cols_.COLUMNS.BODY_COLS.PARTNER_COLS")
                    df_email_p_code = dataframe_factory.get_emails(c_code,PARTNER=True)
                    df_email_p_code = df_email_p_code[df_email_p_code["partner_code"].isin(list(dataframe_collection.keys()))]

                    for email in df_email_p_code["partner_email"].unique().tolist():
                        df_custom = df_raw[cols]
                        list_p_code = df_email_p_code[df_email_p_code["partner_email"] == email]["partner_code"].values.tolist() ## list p_code for each partner from postgres db
                        dict_filter = dict(filter(lambda item:item[0] in list_p_code, dataframe_collection.items()))
                        df_custom = df_custom[df_custom["Payment Channel"].isin(list_p_code)] # filtering list p_code email db

                        for p_cd in df_custom["Payment Channel"].values: ## rename
                            df_custom["Payment Channel"] = df_custom["Payment Channel"].replace([p_cd], [conf.get(f"partner_code.partner_name.{p_cd}")])
                        send_email(dict_filter,email,f"Recillia - Notifikasi Hasil Rekonsiliasi {conf.get(f'channel_code.channel_name.{c_code}')}",df_custom,conf.get(f'channel_code.channel_name.{c_code}'),start_date,is_partner=True)
                else:
                    df_custom = df
                    df_email_p_code = dataframe_factory.get_emails(c_code,BO=True)

                    for email in df_email_p_code["channel_email"].unique().tolist():
                        send_email(dataframe_collection,email,f"Recillia - Notifikasi Hasil Rekonsiliasi {conf.get(f'channel_code.channel_name.{c_code}')}",df_custom,conf.get(f'channel_code.channel_name.{c_code}'),start_date,is_bo=True)
    
    else:   
        logging.info("No Data Unmatch")

    return logging.info("Process Done")
    