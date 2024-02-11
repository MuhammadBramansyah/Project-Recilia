from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from pyhocon import ConfigFactory
import logging 
logging.basicConfig(level=logging.INFO)
import os 
import json
from airflow.models import Variable

from package.data_frame_factory_func import DataFrameFactory
from package.send_email_func import send_email_reminder

import warnings
warnings.filterwarnings("ignore")

## path 
CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()

readfile = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(readfile)

now = datetime.now()
dataframe_factory = DataFrameFactory()

def email_reminder(is_weekly = False, is_monthly=False,**context):
    date_now = datetime.now()
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')

    if is_weekly:
        start = date_now - timedelta(days=7)
        start_date = start.strftime('%Y-%m-%d')
        end_date = date_now.strftime('%Y-%m-%d')

        if is_manual:
            try:
                config = Variable.get("EMAIL_REMINDER_CONFIG")
                reminder_config = json.loads(config)
                start_date = reminder_config["WEEKLY"]["START_DATE"]
                end_date = reminder_config["WEEKLY"]["END_DATE"]
            except:
                raise Exception("Date Not Found, Please insert variable")
        else:
            start_date = start_date
            end_date = end_date

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    elif is_monthly:
        start = date_now - relativedelta(months = 1)
        start_date = datetime(start.year, start.month, 1).strftime('%Y-%m-%d')
        end_date = (datetime(start.year + (start.month // 12), (start.month % 12) + 1, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        if is_manual:
            try:
                config = Variable.get("EMAIL_REMINDER_CONFIG")
                reminder_config = json.loads(config)
                start_date = reminder_config["MONTHLY"]["START_DATE"]
                end_date = reminder_config["MONTHLY"]["END_DATE"]
            except:
                raise Exception("Date Not Found, Please insert variable")
        else:
            start_date = start_date
            end_date = end_date

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    logging.info(f"Using start_date = {start_dt} & end_date = {end_dt}")
    logging.info("---------------------------------------------------------------------------")

    df_target = dataframe_factory.df_target_reminder(start_dt, end_dt)
    if not df_target.empty:
        for receiver_type in ["BO","PARTNER"]:
            if receiver_type == "PARTNER":
                df_partner = df_target.loc[(df_target.is_partner == True)]
                cols = ["partner_email","channel_code","partner_code","date"]
                df_partner = df_partner[cols]

                for c_code in df_partner['channel_code'].unique().tolist():
                    df_channel = df_partner.loc[(df_partner.channel_code == c_code)]

                    for email in df_channel["partner_email"].unique().tolist():
                        df_filtered_by_email = df_channel[df_channel["partner_email"] == email]
                        list_p_code = df_filtered_by_email["partner_code"].values.tolist()
                        df_body = df_filtered_by_email[['partner_code','date']].copy()
                        df_body = df_body[df_body["partner_code"].isin(list_p_code)]
                        df_body = df_body.drop_duplicates(subset=['partner_code','date'])

                        df_body['partner_code'] = df_body['partner_code'].map(conf.get("partner_code.partner_name"))
                        df_body.rename(columns = {'partner_code':'Nama Partner'}, inplace = True)
                        
                        logging.info(f"Sending Email To PARTNER {conf.get(f'channel_code.channel_name.{c_code}')}")
                        send_email_reminder(email,f"Recillia - Email Reminder {conf.get(f'channel_code.channel_name.{c_code}')}",df_body,conf.get(f'channel_code.channel_name.{c_code}'))
                        logging.info("---------------------------------------------------------------------------")
            else:
                df_bo = df_target.loc[(df_target.is_bo == True)]
                cols = ["channel_email","channel_code","partner_code","date"]
                df_bo = df_bo[cols]
        
                for c_code in df_bo['channel_code'].unique().tolist():
                    df_channel = df_bo.loc[(df_bo.channel_code == c_code)]
                    cols_target = ['partner_code','date']

                    for email in df_channel['channel_email'].unique().tolist():
                        df_body = df_channel[cols_target]
                        df_body = df_body.drop_duplicates(subset=['partner_code','date'])
                        df_body['partner_code'] = df_body['partner_code'].map(conf.get("partner_code.partner_name"))
                        df_body.rename(columns = {'partner_code':'Nama Partner'}, inplace = True)
                       
                        logging.info(f"Sending Email To BO {conf.get(f'channel_code.channel_name.{c_code}')}")
                        send_email_reminder(email,f"Recillia - Email Reminder {conf.get(f'channel_code.channel_name.{c_code}')}",df_body,conf.get(f'channel_code.channel_name.{c_code}'))
                        logging.info("---------------------------------------------------------------------------")
    else:
        logging.info("Data Not Found, all Partner has been upload data to Recilia")
    return logging.info("-------------------------------Process Done---------------------------------------")