import json 
from airflow.models import Variable

def it_email(c_codes):
    configs = Variable.get("IT_EMAIL")
    mails = json.loads(configs)
    return mails["channel"][c_codes]["IT"]["email"]