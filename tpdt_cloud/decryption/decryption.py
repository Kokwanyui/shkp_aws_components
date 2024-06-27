import s3

"""Must Input"""
file_path = r''
phone_or_email = 'phone' # input 'phone' or 'email'
"""Must Input"""

# Config
aws_s3 = s3.Client()


# upload to s3 and encrypt
aws_s3.upload(file_path, 'tpdt-automation', 'encryption/find_memberid.csv')

