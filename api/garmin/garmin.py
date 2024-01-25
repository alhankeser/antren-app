import os
import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import garth
from garth.exc import GarthHTTPError
from dotenv import load_dotenv
load_dotenv()

EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")
TOKEN = "./.token"
ACTIVITY_FILES_PATH = './activity_files'
ORIGINAL_FILES_PATH = f'{ACTIVITY_FILES_PATH}/original'
CONVERTED_FILES_PATH = f'{ACTIVITY_FILES_PATH}/converted'

config = {
    'garmin': {
        'domain': 'garmin.com',
        'selectors': {
            'heart_rate': 'HeartRateBpm > Value',
            'watts': 'ns3:Watts',
            'time': 'Time'
        }
    }
}

def authenticate():
    api_client = garth.Client(domain=config['garmin']['domain'])
    try:
        api_client.load(TOKEN)
    except GarthHTTPError:
        api_client.login(EMAIL,PASSWORD)
        api_client.dump(TOKEN)
    return api_client

def get_activities_between_dates(api_client, start_date, end_date):
    """
    Source: https://github.com/cyberjunky/python-garminconnect/
    """
    activities = []
    activity_type = 'cycling'
    start = 0
    limit = 20
    url = "/activitylist-service/activities/search/activities"
    params = {
        "startDate": str(start_date),
        "endDate": str(end_date),
        "start": str(start),
        "limit": str(limit),
        "activityType": str(activity_type)
    }
    while True:
        params["start"] = str(start)
        act = api_client.connectapi(url, params=params)
        if act:
            activities.extend(act)
            start = start + limit
        else:
            break

    return activities

def get_all_activities(api_client):
    today = datetime.date.today()
    start_date = datetime.date(2014, 1, 1)
    return get_activities_between_dates(api_client, 
                                        start_date=start_date.isoformat(), 
                                        end_date=today.isoformat())

def get_last_day_of_activities(api_client):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    return get_activities_between_dates(api_client,
                                        start_date=yesterday.isoformat(), 
                                        end_date=today.isoformat())

def get_activity_tcx_data(api_client, id):
    return api_client.download(f'/download-service/export/tcx/activity/{id}')

def get_activity_file(api_client, activity_id):
    activity_data = get_activity_tcx_data(api_client, activity_id)
    original_file_path = f"{ORIGINAL_FILES_PATH}/{str(activity_id)}.tcx"
    with open(original_file_path, "wb") as fb:
        fb.write(activity_data)
    return original_file_path

def convert_activity_file(activity_id, activity_name, original_file_path, format='csv'):
    with open(f'{original_file_path}', "rb") as file:
        soup = BeautifulSoup(file, features="lxml-xml")
        time = np.array(soup.find_all(config['garmin']['selectors']['time'])).flatten()
        seconds = len(time)
        if seconds == 0:
            return False
        user_ids = np.repeat(0, seconds)
        activity_ids = np.repeat(activity_id, seconds)
        activity_names = np.repeat(activity_name, seconds)
        watts = np.array(soup.find_all(config['garmin']['selectors']['watts'])).flatten()
        if len(watts) < seconds:
            watts = np.repeat(0, seconds)
        heart_rate = np.array(soup.select(config['garmin']['selectors']['heart_rate'])).flatten()
        if len(heart_rate) < seconds:
            heart_rate = np.repeat(0, seconds)
        df = pd.DataFrame(
            {
                'user_id': user_ids,
                'activity_id': activity_ids,
                'activity_name': activity_names,
                'time': time,
                'watts': watts,
                'heart_rate': heart_rate
            }
        )
        if format == 'csv':
            converted_file_path = f'{CONVERTED_FILES_PATH}/{activity_id}.csv'
            df.to_csv(converted_file_path, index=False)
        if format == 'parquet':
            converted_file_path = f'{CONVERTED_FILES_PATH}/{activity_id}.parquet'
            df.to_parquet(converted_file_path, index=False)
        return converted_file_path