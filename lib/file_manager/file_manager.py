import os
import json
from google.cloud import storage
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import tcx_extract as tcx

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
ACTIVITY_FILES_PATH = "./activity_files"
ORIGINAL_FILES_PATH = f"{ACTIVITY_FILES_PATH}/original"
CONVERTED_FILES_PATH = f"{ACTIVITY_FILES_PATH}/converted"


def upload_to_cloud(source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def save_activity_raw_file(activity_id, activity_raw_data):
    file_path = f"{ORIGINAL_FILES_PATH}/{str(activity_id)}.tcx"
    with open(file_path, "wb") as activity_file:
        activity_file.write(activity_raw_data)
    return file_path

def to_unix(timestamp):
    unix_time_start = pd.Timestamp("1970-01-01").tz_localize("UTC")
    increment = pd.Timedelta("1s")
    timestamp = pd.to_datetime(timestamp, errors='coerce')
    return (timestamp - unix_time_start) // increment

def convert_activity_file(
    activity_id, original_file_path, format="csv"
):
    time_points = tcx.extract(original_file_path, "Time")
    watts = tcx.extract(original_file_path, "ns3:Watts")
    heart_rate = tcx.extract(original_file_path, "Value")

    data_df = pd.DataFrame({
        'time': time_points,
        'heart_rate': heart_rate,
        'watts': watts
    })
    
    data_df['time'] = to_unix(data_df['time']).fillna(0).astype('int64')
    data_df['heart_rate'] = pd.to_numeric(data_df['heart_rate'], errors='coerce').fillna(0).astype('uint16')
    data_df['watts'] = pd.to_numeric(data_df['watts'], errors='coerce').fillna(0).astype('uint16')

    data_dict = data_df.to_dict(orient='list')

    activity_data = {
        "activity_id": activity_id,
        "start_time": data_df.iloc[0]['time'],
        "data": data_dict
    }
    df = pd.DataFrame([activity_data])
    df = df.astype(
        {
            "activity_id": "int64", 
            "start_time": "int64", 
            "data": "string"
        }
    )
    if format == "csv":
        converted_file_path = f"{CONVERTED_FILES_PATH}/{activity_id}.csv"
        df.to_csv(converted_file_path, index=False)
    if format == "parquet":
        converted_file_path = f"{CONVERTED_FILES_PATH}/{activity_id}.parquet"
        df.to_parquet(converted_file_path, index=False)
    return converted_file_path
