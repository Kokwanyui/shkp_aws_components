import sys
import os
import aws_lambda
import aws_s3
import config

# config
# public layer source: https://github.com/keithrozario/Klayers
""" Must Entry """
runner = 'uat_runner'
public_layer_arn = 'arn:aws:lambda:ap-east-1:770693421928:layer:Klayers-p39-pysftp:5'
layer_name = 'pysftp'
""" Must Entry """

local_lambda_layer = config.file_path()['local_lambda_layer']
dist_layer_location = os.path.join(local_lambda_layer, f'{layer_name}.zip')


def download_layer():
    """download public layer via dev env, only dev env able to connect to klayer account"""
    awslambda = aws_lambda.Client('dev_runner')
    saved_path = awslambda.download_layer(public_layer_arn, dist_layer_location)
    return saved_path


def download_layer_and_deploy(git_runner):
    """Create lambda layer in env"""
    # connect client
    awslambda = aws_lambda.Client(git_runner)
    s3 = aws_s3.Client(git_runner)
    s3_object_path = f'Lambda/Layers/{layer_name}.zip'

    # s3 artifact bucket
    if git_runner == 'prod_runner':
        artifacts_bucket = 'tpdt-artifacts'
    else:
        env = git_runner.split('_')[0]
        artifacts_bucket = f'tpdt-artifacts-{env}'

    # Core Actions
    saved_path = download_layer()
    s3.upload(saved_path, artifacts_bucket, s3_object_path)
    awslambda.create_layer_version(layer_name)


if __name__ == '__main__':
    download_layer_and_deploy(runner)
