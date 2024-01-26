import os
import datetime
import garth
from garth.exc import GarthHTTPError
from dotenv import load_dotenv
load_dotenv()

EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")
TOKEN = "./.token"

def authenticate(domain):
    api_client = garth.Client(domain=domain)
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

def get_activity_raw_data(api_client, activity_id, format):
    if format == 'tcx':
        activity_raw_data = get_activity_tcx_data(api_client, activity_id)
    return activity_raw_data