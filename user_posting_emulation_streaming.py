import datetime
import json
import requests
import random
import sqlalchemy
from time import sleep
import yaml

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.creds = {}
    
    def read_db_creds(self):
        """
        Returns the database credentials from the yaml file

        """
        with open('db_creds.yaml') as yaml_file:
            self.creds = yaml.safe_load(yaml_file)
        return self.creds
        
    def create_db_connector(self):
        """
        Using the database credentials, creates a database engine to connect to the database
        """
        self.creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.creds['USER']}:{self.creds['PASSWORD']}@{self.creds['HOST']}:{self.creds['PORT']}/{self.creds['DATABASE']}?charset=utf8mb4")
        return engine

def datetime_converter(date_time):
    """
    Converts datetime to string

    Paramenter: 
    date_time : datetime
    """
    if isinstance(date_time, datetime.datetime):
        return date_time.__str__()

def run_infinite_post_data_loop():
    headers = {'Content-Type': 'application/json'}
    engine = new_connector.create_db_connector()
    with engine.connect() as connection:

        while True:
            """
            Creates a connection to the database to emulate row data that is generated randomly into Kinesis Streams
            """
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

            with engine.connect() as connection:
                tablenames_streams = {'pinterest_data':"streaming-12f7a43505b1-pin", 'geolocation_data':"streaming-12f7a43505b1-geo", 'user_data':"streaming-12f7a43505b1-user"}
                for table_name, stream_name in tablenames_streams.items():
                    sql_string = sqlalchemy.text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
                    selected_row = connection.execute(sql_string)
                    invoke_url   = f"https://mhmruluzt9.execute-api.us-east-1.amazonaws.com/prod/streams/{stream_name}/record"
                    for row in selected_row:
                        result = dict(row._mapping)
                        payload = json.dumps({"StreamName": f"{stream_name}","Data": result,"PartitionKey": "test"}, default=datetime_converter)
                        response = requests.request("PUT", invoke_url, headers=headers, data=payload)


new_connector = AWSDBConnector()
if __name__ == "__main__":
    run_infinite_post_data_loop()
