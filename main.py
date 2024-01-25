import os
import logging
import google.cloud.logging

from api.garmin.garmin import authenticate, get_last_day_of_activities, get_activity_file, convert_activity_file

logging.basicConfig(level=logging.INFO)
if os.environ.get("ENV", "dev") == "prod":
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

def main():
    logging.info("Authenticating API")
    api_client = authenticate()
    activities = get_last_day_of_activities(api_client)
    logging.info(f"Got {len(activities)} activities")
    for activity_data in activities:
        activity_id = activity_data["activityId"]
        activity_name = activity_data["activityName"]
        try:
            original_file_path = get_activity_file(api_client, activity_id)
        except:
            logging.warning(f'failed to get original: {activity_name} ({activity_id})' )
            continue
        try:
            convert_activity_file(activity_id, activity_name, original_file_path, format='parquet')
        except:
            logging.warning(f'failed to convert: {activity_name} ({activity_id})' )
            continue
        try:
            os.remove(original_file_path)
        except:
            logging.warning(f'failed to delete: {activity_name} ({activity_id})')
            continue
        logging.info(f'successfully saved: {activity_name} ({activity_id})' )

if __name__ == '__main__':
    main()