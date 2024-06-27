import sys
import aws_lambda
import json

""" Must Entry """
function_name = 'tpdt-carpark-gsheets'
payload = {
  "gsheet_url": "https://docs.google.com/spreadsheets/d/1fTZE9UliSq8G4SfX1VSQsrArlQNybc3SGzpsXn6IEDk/edit#gid=0",
  "columns_renaming": {
    "Sent Date": "Sent_Date",
    "Reply Date": "Reply_Date",
    "Member ID": "Member_ID",
    "Confirmed by which Mall": "Confirmed_by_which_Mall",
    "Shop": "Shop",
    "normal spender / abnormal spender / parallel trader / shop staff": "spender_type",
    "Remarks": "Remarks"
  },
  "worksheet": "sheet1",
  "bucket_to_save": "tpdt-adhoc",
  "path_name": "abnormal_spender_confirmed_by_mall/abnormal_spender_confirmed_by_mall.csv"
}
runner = 'prod_runner'
""" Must Entry """

# Config
awslambda = aws_lambda.Client(runner)
payload_json = json.dumps(payload)

# Invoke Lamnbda function
response = awslambda.invoke_function(function_name, payload_json)

# Result
print(response)
