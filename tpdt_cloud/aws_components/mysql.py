import pandas as pd
from sqlalchemy import create_engine
import config
import json


class Connect(object):
    def __init__(self, env='prod', db='tpdt_db02'):
        secret_location = config.file_path()['secret']
        with open(secret_location) as file:
            credentials = json.load(file)

        host = credentials['secrets_manager'][env][db]['host']
        port = credentials['secrets_manager'][env][db]['port']
        user = credentials['secrets_manager'][env][db]['username']
        pw = credentials['secrets_manager'][env][db]['password']
        dbname = 'bi_dimension'

        self.connect = create_engine(f'mysql+pymysql://{user}:{pw}@{host}:{port}/{dbname}')

    def read(self, query, return_format='df'):
        df = pd.read_sql_query(query, con=self.connect)

        if return_format == 'df':
            result = df
        elif return_format == 'json':
            job_conf = df.to_json(orient="records")
            result = json.loads(job_conf)
        else:
            result = None
        return result

    def execute(self, query):
        connection = self.connect
        connection.execute(query)
        print(f'run query: {query}')

