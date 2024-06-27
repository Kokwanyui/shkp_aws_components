import json
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import pymysql
import numpy as np


extract_date_start = '2024-06-26 00:00:00'
extract_date_end = '2024-06-26 23:59:59'

# step 0: Credentail Config
with open(r'C:\Users\steveko\PycharmProjects\pythonProject\venv\credential\secret.json') as json_file:
    credential = json.load(json_file)
db01 = credential['secrets_manager']['prod']['tpdt_db01']
username = db01['username']
password =  db01['password']
engine =  db01['engine']
host =  db01['host']
port = db01['port']
dbname = db01['dbname']
engine_01 = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}')

# step 1: get delta spending transaction
sql_spending_transaction_record = f"""select spending_transaction_id, member_id, approval_date,platform 
from shkpmalls_vip.spending_transaction where status ='A' 
and approval_date between '{extract_date_start}' and '{extract_date_end}'
"""
spending_transaction_detla = pd.read_sql_query(sql_spending_transaction_record, engine_01)

# step 2: get program member transaction from delta spending transaction
df = pd.read_csv(r"C:\Users\steveko\Downloads\20240618_VIP_program_target_control_list_final.csv")
member = df['member_id']
program_member_spending_transaction_detla = pd.merge(spending_transaction_detla, member, on='member_id', how='inner')[spending_transaction_detla.columns]
detla_spending_transaction_id = program_member_spending_transaction_detla['spending_transaction_id'].astype(str).tolist()

# step 3:  get receipt detail of step 2 result
spending_transaction_id_string = "('" + "', '".join(detla_spending_transaction_id) + "')"
receipt_detail = f"""select receipt_id,invoice_no, amount, shop_name_lang1, receipt_date, created_date, spending_transaction_id, mall_id
from shkpmalls_vip.receipt where status ='A' 
and spending_transaction_id in {spending_transaction_id_string}
"""
receipt_detail_detla = pd.read_sql_query(receipt_detail, engine_01)

# step 4: merge data
columns = ['receipt_id', 'spending_transaction_id', 'member_id', 'amount', 'shop_name_lang1', 'approval_date', 'receipt_date', 'created_date', 'invoice_no', 'platform', 'mall_id']
merged_df = pd.merge(receipt_detail_detla, spending_transaction_detla, on='spending_transaction_id', how='left')[columns]

# step 4: filter our r receipt from counter
merged_df['invoice_no'] = merged_df['invoice_no'].str.lower()
merged_df['valid_receipt'] = np.where((merged_df['platform'] == 'counter') & (merged_df['invoice_no'].str.endswith('r')), 1, 0)
#merged_df.to_csv(r"C:\Users\steveko\Downloads\testing_result.csv")

# step 5: final adjustments
# step 5.0: get property_id
property_id_mapping = pd.read_csv(r"C:\Users\steveko\Downloads\tpdt_dim_tp_mall_mapping (1).csv")
columns2 = columns.append('property_id')
merged_df = pd.merge(merged_df, property_id_mapping, on='mall_id', how='inner')

# step 5.1: change column format and add columns
merged_df['approval_date'] = merged_df['approval_date'].dt.date
merged_df['receipt_date'] = merged_df['receipt_date'].dt.date
merged_df['created_date'] = merged_df['created_date'].dt.date
merged_df['abnormal_receipt'] = 0
merged_df['affluent_program_action'] = 'count as spending'
merged_df['affluent_program_action_date'] = extract_date_start[:10]

# step 5.2: select relevant receipts, rename and select columns
merged_df = merged_df[merged_df['valid_receipt'] == 0]
columns_to_rename = {'amount': 'spending_amount', 'shop_name_lang1': 'shop_name', 'receipt_date': 'receipt_transaction_date', 'created_date': 'receipt_upload_date'}
selected_columns = ['receipt_id','spending_transaction_id','member_id','spending_amount','property_id','shop_name','approval_date','receipt_transaction_date','receipt_upload_date','abnormal_receipt','affluent_program_action','affluent_program_action_date']
final_result = merged_df.rename(columns=columns_to_rename)
final_result = final_result[selected_columns]

# step 6: df to csv.gz
final_result.to_csv(r"C:\Users\steveko\Downloads\daily_transaction_26.csv", index=False)




