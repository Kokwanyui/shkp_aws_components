import boto3
import config
import json


class Client(object):
    def __init__(self, key_name, region='ap-east-1'):
        secret_location = config.file_path()['secret']
        with open(secret_location) as file:
            credentials = json.load(file)

        # get credentials and config
        access_key = credentials['access_key'][key_name]['access_key']
        secret_key = credentials['access_key'][key_name]['secret_key']

        # set sns client
        self.client = boto3.client('sns', region_name=region,
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)
        self.env = credentials['access_key'][key_name]['env']
        self.account_id = credentials['access_key'][key_name]['account_id']

    def create_topic(self, topic_name):
        client = self.client

        response = client.create_topic(
            Name=f'tpdt_{topic_name}',
            Attributes={'DisplayName': topic_name}
        )
        print(f'Creating sns topic: {response}')

    def create_subscription(self, topic_name, endpoint):
        client = self.client

        subscription = client.subscribe(
            TopicArn=f'arn:aws:sns:ap-east-1:{self.account_id}:tpdt_{topic_name}',
            Protocol='email',
            Endpoint=endpoint,
            ReturnSubscriptionArn=True)['SubscriptionArn']
        return subscription
