import requests
import json
import sqlalchemy
from sqlalchemy import text
import datetime

class AWSDBConnector:

    def __init__(self):
        self.HOST     = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER     = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT     = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_pre_ping=True)
        return engine

def datetime_converter(date_time):
    if isinstance(date_time, datetime.datetime):
        return date_time.__str__()

def stream_to_kinesis():
    print("Table names: \npinterest_data\ngeolocation_data\nuser_data")
    table_name = input("Enter the table name from the above list ")
    if not (table_name == 'pinterest_data' or table_name == 'geolocation_data' or table_name == 'user_data'):
        raise NameError("The names are not matching. Please enter the table name as per the names provided in the list above")

    engine = new_connector.create_db_connector()
    with engine.connect() as connection:

        if table_name == 'pinterest_data':
            stream_name = "streaming-12f7a43505b1-pin"
        elif table_name == 'geolocation_data':
            stream_name = "streaming-12f7a43505b1-geo"
        else:
            stream_name = "streaming-12f7a43505b1-user"

        table_string = text(f"SELECT * FROM {table_name}")
        selected_row = connection.execute(table_string)
        invoke_url   = f"https://mhmruluzt9.execute-api.us-east-1.amazonaws.com/prod/streams/{stream_name}/record"
        headers      = {'Content-Type': 'application/json'}

        for row in selected_row:
            table_result = dict(row._mapping)
            payload      = json.dumps({"StreamName": f"{stream_name}","Data": table_result,"PartitionKey": "test"}, default = datetime_converter)
            response     = requests.request("PUT", invoke_url, headers=headers, data=payload)

new_connector = AWSDBConnector()
if __name__ == "__main__":
    stream_to_kinesis()
    print('Working')