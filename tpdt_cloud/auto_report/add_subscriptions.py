import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\untitled\venv\tpdt_cloud\aws_components')

import aws_sns
import mysql

"""Must Entry"""
env = 'prod'
topic_name = 'payment_checking_success_weekly'
endpoints = ['alvinchu@shkp.com', 'edwardpcchan@shkp.com', 'jamesleung@shkp.com', 'janetcywong@shkp.com']
runner = 'prod_runner'
"""Must Entry"""

sns = aws_sns.Client(runner, 'ap-east-1')
db = mysql.Connect('prod', 'tpdt_db02')


def add_subscription():
    for endpoint in endpoints:
        # create subscription
        arn = sns.create_subscription(topic_name, endpoint)
        print(f'endpoint: {endpoint} added to topic: {topic_name}')

        # update automation_sns_subscription status and arn
        update_query = f'''update automation_sns_subscription
                               set subscription_status = "A", endpoint_arn = "{arn}" 
                               where topic = "{topic_name}" 
                               and endpoint = "{endpoint}"'''
        db.execute(update_query)


if __name__ == "__main__":
    add_subscription()
