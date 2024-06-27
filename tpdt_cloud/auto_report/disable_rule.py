import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\untitled\venv\tpdt_cloud\aws_components')
import aws_eventbridge

rule_name = 'tpdt_payment_checking_success_weekly'
runner = 'prod_runner'

eventbridge = aws_eventbridge.Client(runner, 'ap-east-1')
response = eventbridge.disable_rule(rule_name)
print(response)