import os
import json
from google.cloud import storage
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

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

def convert_activity_file(
    activity_id, original_file_path, selectors, format="csv"
):
    with open(f"{original_file_path}", "rb") as file:
        soup = BeautifulSoup(file, features="lxml-xml")
        time = np.array(soup.find_all(selectors["time"])).flatten()
        row_count = len(time)
        if row_count == 0:
            return False
        time = np.array([pd.Timestamp(x) for x in time.tolist()])
        unix_time_start = pd.Timestamp("1970-01-01").tz_localize("UTC")
        increment = pd.Timedelta("1s")
        time = np.array([(x - unix_time_start) // increment for x in time])
        watts = np.array(soup.find_all(selectors["watts"])).flatten()
        if len(watts) < row_count:
            watts = np.repeat(0, row_count)
        watts = np.array([int(x) for x in watts.tolist()])
        heart_rate = np.array(soup.select(selectors["heart_rate"])).flatten()
        if len(heart_rate) < row_count:
            heart_rate = np.repeat(0, row_count)
        heart_rate = np.array([int(x) for x in heart_rate.tolist()])
        activity_data = {
            "activity_id": activity_id,
            "start_time": time[0],
            "data": {
                "time": time.tolist(),
                "watts": watts.tolist(),
                "heart_rate": heart_rate.tolist(),
            },
        }
        df = pd.DataFrame([activity_data])
        df = df.astype(
            {"activity_id": "int64", "data": "string"}
        )
        if format == "csv":
            converted_file_path = f"{CONVERTED_FILES_PATH}/{activity_id}.csv"
            df.to_csv(converted_file_path, index=False)
        if format == "parquet":
            converted_file_path = f"{CONVERTED_FILES_PATH}/{activity_id}.parquet"
            df.to_parquet(converted_file_path, index=False)
        return converted_file_path
