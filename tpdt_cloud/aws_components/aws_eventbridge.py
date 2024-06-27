import boto3
import json
import config


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

        self.client = boto3.client('events',
                                   region_name=region,
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)

        # config execution role arn
        if self.env == 'prod':
            self.role = 'tpdt-lambda'
        else:
            self.role = f'tpdt-lambda-{self.env}'

    def put_rule(self, event_name, schedule_expression):
        event = self.client

        response = event.put_rule(
            Name=f'tpdt_{event_name}',
            ScheduleExpression=schedule_expression,
            State='ENABLED',
            RoleArn=f'arn:aws:iam::{self.account_id}:role/{self.role}',
            Tags=[{
                'Key': 'application',
                'Value': 'tpdt'
                },
                {
                'Key': 'environemnt',
                'Value': self.env
                }
            ]
        )
        return response

    def put_targets(self, event_name, input_json):
        arn = f'arn:aws:states:ap-east-1:{self.account_id}:stateMachine:tpdt-auto-report'
        event = self.client

        response = event.put_targets(
            Rule=f'tpdt_{event_name}',
            Targets=[
                {
                    'Id': 'tpdt-auto-report',
                    'Arn': arn,
                    'RoleArn': f'arn:aws:iam::{self.account_id}:role/{self.role}',
                    'Input': input_json
                }])
        return response

    def list_all_events(self):
        event = self.client
        response = event.list_rules(NamePrefix='tpdt')
        return response

    def delete_rule(self, rule_name):
        event = self.client
        response = event.delete_rule(Name=rule_name)
        return response

    def disable_rule(self, rule_name):
        event = self.client
        response = event.disable_rule(Name=rule_name)
        return response

    def remove_target(self, rule_name):
        event = self.client
        response = event.remove_targets(Rule=rule_name,
                                        Ids=['tpdt-auto-report'])
        return response