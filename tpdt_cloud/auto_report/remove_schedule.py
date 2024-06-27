import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\untitled\venv\tpdt_cloud\aws_components')

import aws_eventbridge
import mysql

"""Must Entry"""
env = 'prod'
event_name = 'tpdt_payment_checking_success_monthly'
runner = 'prod_runner'
"""Must Entry"""

db = mysql.Connect('prod', 'tpdt_db02')


def remove_target(event):
    eventbridge = aws_eventbridge.Client(runner, 'ap-east-1')
    eventbridge.remove_target(event)


def remove_schedule(event):
    eventbridge = aws_eventbridge.Client(runner, 'ap-east-1')

    eventbridge.delete_rule(event)
    update_query = f'''update automation_eventbridge_schedule
                       set event_status = "I", event_arn = "" 
                       where event_name = "{event}"'''
    db.execute(update_query)


if __name__ == "__main__":
    remove_target(event_name)
    remove_schedule(event_name)

