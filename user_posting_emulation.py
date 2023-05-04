import requests
from time import sleep
#import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import datetime


#random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_pre_ping=True)
        return engine


new_connector = AWSDBConnector()
url = "https://mhmruluzt9.execute-api.us-east-1.amazonaws.com/prod/topics/"
headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

def datetime_converter(date_time):
    if isinstance(date_time, datetime.datetime):
        return date_time.__str__()

def run_infinite_post_data_loop():
    while True:
        #sleep(random.randrange(0, 2))
        #random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data")
            pin_selected_row = connection.execute(pin_string)
            pin_list,geo_list, user_list = [], [], []
            topic = "12f7a43505b1.pin"
            invoke_url = url+topic
            print('starts topic pin')
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_list.append({"value":pin_result})
            payload = json.dumps({"records": pin_list})
            response = requests.request("POST", invoke_url, headers=headers, data=payload)
            print(response.status_code)

        
            print('starts topic geo')
            geo_string = text(f"SELECT * FROM geolocation_data")
            geo_selected_row = connection.execute(geo_string)
            
            topic = "12f7a43505b1.geo"
            invoke_url = url+topic
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_list.append({"value":geo_result})
            payload = json.dumps({"records": geo_list}, default = datetime_converter)
            response = requests.request("POST", invoke_url, headers=headers, data=payload)
            print(response.status_code)
            
           
            print('starts topic user')
            user_string = text(f"SELECT * FROM user_data")
            user_selected_row = connection.execute(user_string)
            
            topic = "12f7a43505b1.user"
            invoke_url = url+topic
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_list.append({"value":user_result})
            payload = json.dumps({"records": user_list}, default = datetime_converter)
            response = requests.request("POST", invoke_url, headers=headers, data=payload)
            print(response.status_code)
            print('Completed!')
            break 


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


