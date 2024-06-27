import boto3
import json
from glob import glob
import os
import config
import requests


class Client(object):
    def __init__(self, key_name, region="ap-east-1"):
        secret_location = config.file_path()['secret']
        with open(secret_location) as file:
            credentials = json.load(file)

        # get credentials and config
        access_key = credentials['access_key'][key_name]['access_key']
        secret_key = credentials['access_key'][key_name]['secret_key']
        self.env = credentials['access_key'][key_name]['env']
        self.account_id = credentials['access_key'][key_name]['account_id']

        if self.env == 'prod':
            self.artifacts = 'tpdt-artifacts'
            self.role_arn = f'arn:aws:iam::{self.account_id}:role/tpdt-lambda'
        else:
            self.artifacts = f'tpdt-artifacts-{self.env}'
            self.role_arn = f'arn:aws:iam::{self.account_id}:role/tpdt-lambda-{self.env}'

        # set lambda client
        self.client = boto3.client('lambda', region_name=region,
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)

    def create_layer_version(self, library_name):
        """Create a new layer/ layer version, make sure zip file already in S3"""

        client = self.client
        layer_name = f'tpdt_{library_name}'
        print('Start Creating Layer:', layer_name)

        response = client.publish_layer_version(
            LayerName=layer_name,
            Content={
                'S3Bucket': self.artifacts,
                'S3Key': f'Lambda/Layers/{library_name}.zip'
            },
            CompatibleRuntimes=['python3.6', 'python3.7', 'python3.8', 'python3.9']
        )
        print('Create Layer response:', response)

    def batch_create_layers(self, location='s3'):
        """Create layers with all libraries in path: {artifacts_buclet}/Lambda/Layers"""

        # to change to S3
        # find all libraries
        lambda_layer_location = config.file_path()['lambda_layer']
        libraries = glob(lambda_layer_location, recursive=True)

        # create layer by calling create_layer_version
        for library in libraries:
            library_folder = os.path.split(library)[-1]
            library_name = library_folder.split('.')[0]

            # create layer
            response = create_layer_version(library_name)
            return response

    def create_lambda_function(self, function_name):
        """Create lambda function, make sure zip file already in S3"""
        client = self.client
        print('Start Creating Lmabda Function:', function_name)

        response = client.create_function(
            FunctionName=function_name,
            Handler='lambda_function.lambda_handler',
            Runtime='python3.9',
            Role=self.role_arn,
            Code={'S3Bucket': self.artifacts,
                  'S3Key': f'Lambda/Functions/{function_name}.zip'})
        print('Create Lambda Function Response:', response)

    def invoke_function(self, function_name, payload):
        client = self.client

        response = client.invoke(
            FunctionName=function_name,
            Payload=payload)
        return response

    def download_layer(self, public_layer_arn, dist_filepath):
        client = self.client

        # get layer location
        response = client.get_layer_version_by_arn(Arn=public_layer_arn)
        layer_location = response['Content']['Location']
        print('public layer location:', layer_location)

        # download layer
        req = requests.get(layer_location)
        with open(dist_filepath, 'wb') as output_file:
            output_file.write(req.content)
        print('layer downloaded to:', dist_filepath)

        return dist_filepath