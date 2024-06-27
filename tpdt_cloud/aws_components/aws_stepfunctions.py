import boto3
import json
import config
import datetime


class Client(object):
    def __init__(self, key_name, region='ap-east-1'):
        secret_location = config.file_path()['secret']
        with open(secret_location) as file:
            credentials = json.load(file)

        # get credentials and config
        access_key = credentials['access_key'][key_name]['access_key']
        secret_key = credentials['access_key'][key_name]['secret_key']
        self.env = credentials['access_key'][key_name]['env']
        self.account_id = credentials['access_key'][key_name]['account_id']

        self.client = boto3.client('stepfunctions',
                                   region_name=region,
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)

    def run(self, task):
        sf = self.client
        now = datetime.datetime.now()
        now_string = now.strftime("%Y%m%d%H%M%S")
        task_name = f'{task}_{now_string}_manual'
        input_root = "{\"task\": \"task_name\"}"
        input_string = input_root.replace('task_name', task)

        response = sf.start_execution(
            stateMachineArn=f'arn:aws:states:ap-east-1:{self.account_id}:stateMachine:tpdt-auto-report',
            name=task_name,
            input=input_string
        )

        print(response)