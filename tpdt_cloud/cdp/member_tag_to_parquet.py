import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
import io

def csv_to_gzip_parquet(input_csv_file, output_parquet_file):
    # Read the CSV file using pandas
    df = pd.read_csv(input_csv_file)

    # Compress the dataframe to a GZIP file in memory
    compressed_data = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_data, mode='w') as f:
        f.write(df.to_csv(index=False).encode())

    # Reset the buffer position and read the compressed data into a pandas dataframe
    compressed_data.seek(0)
    compressed_df = pd.read_csv(compressed_data, compression='gzip')
    compressed_df['tag_remark_id'] = df['tag_remark_id'].astype(int)
    compressed_df['created_date'] = pd.to_datetime(df['created_date'])
    schema = pa.schema([
        ('member_id', pa.string()),
        ('tag_category', pa.string()),
        ('tag', pa.string()),
        ('source', pa.string()),
        ('created_date', pa.date32()),
        ('tag_remark_id', pa.int32())
    ])

    # Convert the pandas dataframe to a pyarrow.Table
    table = pa.Table.from_pandas(compressed_df, schema=schema)

    # Write the table to a GZIP-compressed Parquet file
    pq.write_table(table, output_parquet_file, compression='gzip')

# Example usage
input_csv_file = r"C:\Users\steveko\Downloads\member_apps.csv"
output_parquet_file = r"C:\Users\steveko\Downloads\member_apps.parquet.gz"
csv_to_gzip_parquet(input_csv_file, output_parquet_file)