import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/amaliatemneanu/Documents/Projects/data-engineering-zoomcamp-assignments/2.1_mage/crafty-ring-411313-a6f16bb225b2.json"
BUCKET = 'yc-tl-data'


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    aggregated_df = pd.DataFrame()  # Initialize an empty DataFrame for aggregation

    for i in range(12):
        month = f"{i+1:02d}"  # Formats the month properly
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"{init_url}{service}/{file_name}"

        # Download and save the file locally
        r = requests.get(request_url)
        with open(file_name, 'wb') as f:
            f.write(r.content)
        print(f"Downloaded local file: {file_name}")

        # Load the file into a DataFrame and append to the aggregated DataFrame
        df = pd.read_csv(file_name, compression='gzip')
        aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)

        # Delete the downloaded file to save space
        os.remove(file_name)
        print(f"Deleted local file: {file_name}")

    # Save the aggregated DataFrame to a parquet file
    agg_file_name = f"{service}_tripdata_{year}_aggregated.parquet"
    aggregated_df.to_parquet(agg_file_name, engine='pyarrow')
    print(f"Saved aggregated data to local file: {agg_file_name}")

    # Upload the aggregated file to GCS
    upload_to_gcs(BUCKET, f"{service}/{agg_file_name}", agg_file_name)

    # Delete the local aggregated file
    os.remove(agg_file_name)
    print(f"Deleted local aggregated file: {agg_file_name}")


web_to_gcs('2019', 'fhv')
