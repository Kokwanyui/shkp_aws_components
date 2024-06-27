import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\untitled\venv\tpdt_cloud\aws_components')

import pandas as pd
from sqlalchemy import create_engine
import json
import aws_eventbridge
import mysql
import aws_sns

"""Must Entry"""
env = 'prod'
task = 'payment_checking_success_monthly'
runner = 'prod_runner'
"""Must Entry"""

db = mysql.Connect('prod', 'tpdt_db02')


def get_task_config(task_name):
    query = f'Select * from bi_dimension.automation_job_config where task = "{task_name}"'
    job_conf = db.read(query, 'json')
    conf_json = job_conf[0]
    return conf_json


def find_event_config(id):
    query = f'Select * from bi_dimension.automation_eventbridge_schedule where jobconfig_id = {id}'
    event_json = db.read(query, 'json')
    return event_json


def find_subscription_config(id):
    query = f'Select * from bi_dimension.automation_sns_subscription where jobconfig_id = {id}'
    subscription_json = db.read(query, 'json')
    return subscription_json


def create_single_rule(event_json):
    eventbridge = aws_eventbridge.Client(runner, 'ap-east-1')

    # create eventbridge rule
    event_name = event_json['event_name']
    cron = event_json['schedule_expression']
    print(f'event_name: {event_name}, schedule_expression: {cron}')

    creation_response = eventbridge.put_rule(event_name, cron)
    print('response of creating rule:', creation_response)

    # set which lambda function/ stepfunctions state machice to trigger
    input_json = job_config['input_json']

    trigger_response = eventbridge.put_targets(event_name, input_json)
    print('response of setting trigger function:', trigger_response)

    # Update evenbridge_schedule if success
    event_arn = creation_response['RuleArn']
    update_query = f'''update automation_eventbridge_schedule
                       set event_status = "A", event_arn = "{event_arn}" 
                       where event_name = "{event_name}" 
                       and schedule_expression = "{cron}"'''
    db.execute(update_query)


def create_rules(id):
    rules_config = find_event_config(id)
    numbers_of_rule = len(rules_config)

    for x in range(numbers_of_rule):
        rule_config = rules_config[x]
        create_single_rule(rule_config)


def create_a_topic(topic_name):
    sns = aws_sns.Client(runner, 'ap-east-1')

    # create sns topic
    sns.create_topic(topic_name)


def add_subscription(subscription_json):
    sns = aws_sns.Client(runner, 'ap-east-1')
    topic_name = subscription_json['topic']
    endpoint = subscription_json['endpoint']

    # create subscription
    arn = sns.create_subscription(topic_name, endpoint)
    print(f'endpoint: {endpoint} added to topic: {topic_name}')

    # update automation_sns_subscription status and arn
    update_query = f'''update automation_sns_subscription
                       set subscription_status = "A", endpoint_arn = "{arn}" 
                       where topic = "{topic_name}" 
                       and endpoint = "{endpoint}"'''
    db.execute(update_query)


def add_subscriptions(id):
    subscriptions = find_subscription_config(id)
    numbers_of_subscriptions = len(subscriptions)

    for x in range(numbers_of_subscriptions):
        subscription = subscriptions[x]
        add_subscription(subscription)


def update_task_status(id):
    update_query = f'''update automation_job_config
                set task_status = 'A'
                where id = {id}'''

    db.execute(update_query)


if __name__ == "__main__":
    job_config = get_task_config(task)
    task_id = job_config['id']
    sns_topic = job_config['sns_topic']

    # create eventbridge rules
    create_rules(task_id)

    # create sns topic
    create_a_topic(sns_topic)

    # add subscription
    add_subscriptions(task_id)

    # update task status once all resources created
    update_task_status(task_id)
