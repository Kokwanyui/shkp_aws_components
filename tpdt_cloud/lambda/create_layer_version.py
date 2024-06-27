import sys
import aws_lambda

"""Must Input"""
python_library_name = 'google_cloud_storage'
runner = 'prod_runner'
"""Must Input"""

awslambda = aws_lambda.Client(runner)
response = awslambda.create_layer_version(python_library_name)
print(response)

