import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# update the variables below
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/crafty-ring-411313-a6f16bb225b2.json'
project_id = 'crafty-ring-411313'
bucket_name = 'mage-zoomcamp-amalia'
object_key = 'green_taxi_data.parquet'
table_name = 'green_taxi_data'
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    # creating a new date column from the existing datetime column
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
