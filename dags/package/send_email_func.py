import smtplib
import zipfile
import os
import logging 

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from io import BytesIO
from datetime import datetime
from pyhocon import ConfigFactory
from package.data_frame_factory_func import DataFrameFactory

logging.basicConfig(level=logging.INFO)
from airflow.hooks.base import BaseHook

## config
CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.join(CURRENT_FOLDER, ".."))
current_dir = os.getcwd()

readfile = f"{current_dir}/configFile/config.conf"
conf = ConfigFactory.parse_file(readfile)

connection = BaseHook.get_connection("airflow_email_connection")
dataframe_factory = DataFrameFactory()
def send_email(dataframe_collection,send_to, subject,df,channel,dates,is_it = False, is_partner= False,is_bo = False):
    username = connection.login
    password = connection.password
    host = connection.host
    port = connection.port

    ## date 
    date_obj = datetime.strptime(dates, '%Y-%m-%d')
    date_body = date_obj.strftime('%Y/%m/%d')
    date_attachtment = date_obj.strftime('%Y%m%d')

    ## sender
    send_from = username
    password = password

    if is_it:
        df = dataframe_factory.dataframe_body_converter(df,is_it=True)
        receiver = send_to
        logging.info(f"Sending email to IT {channel} = {receiver}")
    elif is_partner:
        df = dataframe_factory.dataframe_body_converter(df,is_partner=True)
        receiver = [send_to]
        logging.info(f"Sending email to Partner {channel} = {receiver}")
    else:
        df = dataframe_factory.dataframe_body_converter(df,is_bo=True)
        receiver = [send_to]
        logging.info(f"Sending email to BO {channel} ={receiver} ")

    html = df.to_html()
    image_paths = f"{current_dir}/logo/logo-telkomsel-baru.png"   

    counter = 1
    for recip in receiver:
        #body email
        message = f"""\
        <!DOCTYPE html>
        <html>
            <body>    
                <p>
                    Dear Rekan-Rekan {channel},
                </p><br> 
                <p>
                    Berikut kami sampaikan hasil proses rekonsiliasi yang telah dilakukan pada {date_body} sebagai berikut:
                </p> 
                {html}
                <br>
                <p>    
                    Terlampir detail hasil rekonsiliasi.    
                </p>
                <p>    
                    Mohon untuk dilakukan pemeriksaan lebih lanjut kepada Partner yang bersangkutan.    
                </p>
                <p>    
                    Terima kasih atas bantuan dan kerjasamanya.    
                </p><br>
                <img src="cid:image1"  width="500" height="333" ><br>
            </body>
        </html>
        """ 

        multipart = MIMEMultipart()
        multipart["From"] = send_from
        multipart["To"] = recip
        multipart["Subject"] = subject

        with open(image_paths, 'rb') as fp:
            msgImage = MIMEImage(fp.read())

        msgImage.add_header('Content-ID', f'<image{counter}>')
        multipart.attach(msgImage)

        ## attachment files
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for keys in dataframe_collection.keys():
                df_joins = dataframe_collection[keys]
                csv_string = df_joins.to_csv(index= False)
                zip_file.writestr(f"{channel}_{conf.get(f'partner_code.partner_name.{keys}')}_{date_attachtment}.csv", csv_string)

        zip_buffer.seek(0)
        attachment = MIMEApplication(zip_buffer.read())
        attachment["Content-Disposition"] = f"attachment; filename=\"{channel}_{date_attachtment}.zip\""
        multipart.attach(attachment)
        multipart.attach(MIMEText(message, "html"))

        server = smtplib.SMTP(host, port,timeout=360.0)
        server.starttls()
        server.login(multipart["From"], password)
        server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
        server.quit()

    logging.info(f'email sended to {channel}')
    return logging.info("------------------------------------------------------------------------------------------------------------")

def send_email_reminder(send_to, subject, df,channel): ## email reminder
    username = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    
    html = df.to_html(index=False)

    send_from = username
    password = password

    reciver = [send_to]

    mails = reciver
    print(f"email {channel} = ",mails)
    image_paths = f"{current_dir}/logo/logo-telkomsel-baru.png" 

    counter = 1
    for recip in mails:
        #body email
        message = f"""\
        <!DOCTYPE html>
        <html>
            <body>    
                <p>
                    Dear Rekan-Rekan {channel},
                </p><br> 
                <p>
                    Dimohon untuk upload data partner pada Landing Server Recilia:
                </p> 
                {html}
                <br>    
                <p>    
                    Terima kasih atas bantuan dan kerjasamanya.   
                </p><br>
                <img src="cid:image1"  width="500" height="333" ><br>
            </body>
        </html>
        """

        multipart = MIMEMultipart()
        multipart["From"] = send_from
        multipart["To"] = recip
        multipart["Subject"] = subject

        with open(image_paths, 'rb') as fp:
            msgImage = MIMEImage(fp.read())
        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', f'<image{counter}>')
        multipart.attach(msgImage)

        multipart.attach(MIMEText(message, "html"))

        server = smtplib.SMTP(host, port,timeout=360.0)
        server.starttls()
        server.login(multipart["From"], password)
        server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
        server.quit()

        return print(f'email sended to {channel}')
    
def send_email_reminder_to_ifs(send_to, subject, df): 
    username = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    reciver = send_to

    image_paths = f"{current_dir}/logo/logo-telkomsel-baru.png"
    html = df.to_html(index=False)
    counter = 1
    for recip in reciver:
        logging.info(f"Sending Email To Email IFS =  {recip}")
        #body email
        message = f"""\
        <!DOCTYPE html>
        <html>
            <body>    
                <p>
                    Dear Rekan-rekan IFS,
                </p><br> 
                <p>
                    Dimohon untuk mengirimkan data EBS ke STP Recilia:
                </p> 
                {html}
                <br>    
                <p>    
                    Terima kasih atas bantuan dan kerjasamanya.   
                </p><br>
                <img src="cid:image1"  width="1000" height="500" ><br>
            </body>
        </html>
        """

        multipart = MIMEMultipart()
        multipart["From"] = username
        multipart["To"] = recip
        multipart["Subject"] = subject

        with open(image_paths, 'rb') as fp:
            msgImage = MIMEImage(fp.read())
        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', f'<image{counter}>')
        multipart.attach(msgImage)

        multipart.attach(MIMEText(message, "html"))

        server = smtplib.SMTP(host, port,timeout=360.0)
        server.starttls()
        server.login(multipart["From"], password)
        server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
        
        logging.info(f"Email Sended to {recip}")
        logging.info("------------------------------------------------------------------------------------------------------------")
    server.quit()
    return logging.info("Done Sending Email TO IFS")

def send_email_recon_alert(send_to, df): 
    html = df.to_html(index=False)
    
    username = connection.login
    password = connection.password
    host = connection.host
    port = connection.port

    reciver = send_to
    mails = reciver
    image_paths = f"{current_dir}/logo/logo-telkomsel-baru.png" 

    counter = 1
    for recip in mails:
        logging.info(f"sending to {recip}")
        #body email
        message = f"""\
        <!DOCTYPE html>
        <html>
            <body>    
                <p>
                    Selamat Pagi,
                </p><br>
                <p>
                    Dimohon untuk mengecek total rows berikut:
                </p> 
                {html}
                <br>    
                <p>    
                    Terima kasih atas bantuan dan kerjasamanya.   
                </p><br>
                <img src="cid:image1"  width="1000" height="333" ><br>
            </body>
        </html>
        """

        multipart = MIMEMultipart()
        multipart["From"] = username
        multipart["To"] = recip
        multipart["Subject"] = "Recon Alerting Data From Cassandra To Druid"

        with open(image_paths, 'rb') as fp:
            msgImage = MIMEImage(fp.read())
        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', f'<image{counter}>')
        multipart.attach(msgImage)

        multipart.attach(MIMEText(message, "html"))

        server = smtplib.SMTP(host, port,timeout=360.0)
        server.starttls()
        server.login(multipart["From"], password)
        server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
    server.quit()

    return logging.info(f'email sended to {send_to}')