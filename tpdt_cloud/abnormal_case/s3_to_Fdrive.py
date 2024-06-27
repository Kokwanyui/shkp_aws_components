import sys
sys.path.insert(0, r'C:\Users\steveko\PycharmProjects\pythonProject\venv\tpdt_cloud\aws_components')
import aws_s3
import os
import pandas as pd
import time
from datetime import date

# Config
runner = 'prod_runner'
s3 = aws_s3.Client('prod_runner')

today = date.today().strftime("%Y%m%d")
distination_folder = r'C:\Users\steveko\Desktop\Projects\\abnormal_case\{}\staging'
final_folder = r'C:\Users\steveko\Desktop\Projects\\abnormal_case\{}'
distination_folder = distination_folder.replace('{}', today)
final_folder = final_folder.replace('{}', today)

# create relative folders
os.mkdir(final_folder)
os.mkdir(distination_folder)
os.mkdir(f'{final_folder}\All')

# create Malls excel
folders = ['abnormal_log/to_onedrive/abnormal_ccc_invoice/malls/',
           'abnormal_log/to_onedrive/abnormal_ocr_invoice/malls/',
           'abnormal_log/to_onedrive/abnormal_spend_member/malls/']

for folder in folders:
    objects = s3.list_objects_with_prefix('tpdt-automation', folder)
    for s3_object in objects:
        s3_path = s3_object['Key']
        file_name = s3_path.split('/')[-1]
        mall = file_name.split('_')[0]

        distination_path = f'{distination_folder}\{mall}\{file_name}'
        final_path = distination_path.replace('.csv', '.xlsx').replace('\staging', '')

        try:
            os.mkdir(f'{distination_folder}\{mall}')
            os.mkdir(f'{final_folder}\{mall}')
        except:
            pass
        s3.download_csv('tpdt-automation', s3_path, distination_path, sep=',')
        time.sleep(5)
        df = pd.read_csv(distination_path)
        df.to_excel(final_path, header=True, index=False)
        print(f'Dowloaded to {final_path}')

# Create Raw excel
raws = ['abnormal_log/to_onedrive/abnormal_ccc_invoice/raw/',
        'abnormal_log/to_onedrive/abnormal_ocr_invoice/raw/',
        'abnormal_log/to_onedrive/abnormal_spend_member/raw/']

for raw in raws:
    objects = s3.list_objects_with_prefix('tpdt-automation', raw)
    for s3_object in objects:
        s3_path = s3_object['Key']
        file_name = s3_path.split('/')[-1]
        file_type = file_name.split('.')[-1]
        final_filename = raw.split('/')[-3]

        if file_type == 'csv':
            distination_path_all = f'{distination_folder}\{final_filename}.csv'
            final_all_path = distination_path_all.replace('.csv', '.xlsx').replace('staging', 'all')

            s3.download_csv('tpdt-automation', s3_path, distination_path_all, sep=',')
            time.sleep(5)
            df = pd.read_csv(distination_path_all)

            if final_filename == 'abnormal_ccc_invoice' or final_filename == 'abnormal_ocr_invoice':
                df = df.sort_values(by='receipt_amount', ascending=False)
            else:
                pass

            df.to_excel(final_all_path, header=True, index=False)
            print(f'Dowloaded to {final_all_path}')
        else:
            pass




