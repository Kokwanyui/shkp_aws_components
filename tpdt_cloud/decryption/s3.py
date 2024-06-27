import boto3
import json
import pandas as pd
import io


class Client(object):
    def __init__(self, region='ap-east-1'):
        # set s3 client or resource
        self.client = boto3.client('s3', region_name=region)
        self.resource = boto3.resource('s3', region_name=region)

    def clean_with_prefix(self, bucket, prefix):
        s3 = self.resource
        s3_bucket = s3.Bucket(bucket)

        for obj in s3_bucket.objects.filter(Prefix=prefix):
            s3.Object(bucket, obj.key).delete()
            print(obj.key, ' deleted')

    def download_csv(self, bucket, object, destination, sep=','):
        s3 = self.client
        s3_bucket = bucket

        if object.startswith('s3://'):
            prefix=f's3://{bucket}/'
            key=object.replace(prefix, '')
        else:
            key=object

        s3_object = s3.get_object(Bucket=s3_bucket, Key=key)
        df = pd.read_csv(io.BytesIO(s3_object['Body'].read()))
        df.to_csv(destination, index=False, sep=sep)
        print(f'csv downloaded: {destination}')

    def read_csv(self, bucket, object):
        s3 = self.client
        s3_bucket = bucket

        if object.startswith('s3://'):
            prefix = f's3://{bucket}/'
            key = object.replace(prefix, '')
        else:
            key = object

        s3_object = s3.get_object(Bucket=s3_bucket, Key=key)
        df = pd.read_csv(io.BytesIO(s3_object['Body'].read()))
        return df

    def upload(self, file, bucket, object_path):
        s3 = self.client

        try:
            s3.upload_file(file, bucket, object_path)
            print(f'Object uploaded to: S3://{bucket}/{object_path}')
        except:
            print(object_path+' failed')
            pass

    def list_objects_with_prefix(self, bucket, prefix):
        s3 = self.client

        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']
        return objects

    def transfer_from_other_env(self, target_object, destination_bucket, destination_path):
        s3 = self.client
        source = target_object['Body']

        s3.upload_fileobj(source, destination_bucket, destination_path)
        print(f'{source} uploaded to {destination_bucket}/{destination_path}')

    def get_object(self, bucket, key):
        """how to get object key"""
        # result = list_objects_with_prefix(bucket, prefix)
        # key = result['Key']
        # or pass object name directly

        s3 = self.client
        result = s3.get_object(Bucket=bucket, Key=key)
        return result