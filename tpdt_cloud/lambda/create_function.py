import sys
import aws_lambda
import json

""" Must Entry """
function_name = 'tpdt_auto_report_data_checking_sns'
runner = 'prod_runner'
""" Must Entry """

# Config
awslambda = aws_lambda.Client(runner)


# Invoke Lamnbda function
response = awslambda.create_lambda_function(function_name)

# Result
print(response)
