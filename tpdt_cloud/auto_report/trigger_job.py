import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\untitled\venv\tpdt_cloud\aws_components')

import aws_stepfunctions

"""Must Entry"""
env = 'prod'
task = 'payment_checking_success_weekly'
runner = 'prod_runner'
"""Must Entry"""

sfn = aws_stepfunctions.Client(runner, 'ap-east-1')
sfn.run(task)