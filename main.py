import os
import logging
import time
from lib.api.garmin import garmin
from lib.file_manager import file_manager
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)


def main():
    logging.info("Authenticating API")
    api_client = garmin.authenticate("garmin.com")
    activities = garmin.get_last_day_of_activities(api_client)
    logging.info(f"Got {len(activities)} activities")
    for activity_data in activities:
        activity_id = activity_data["activityId"]
        activity_name = activity_data["activityName"]
        activity_raw_data = garmin.get_activity_raw_data(
            api_client, activity_id, format="tcx"
        )
        raw_file_path = file_manager.save_activity_raw_file(
            activity_id, activity_raw_data
        )
        converted_file_path = file_manager.convert_activity_file(
            activity_id, raw_file_path, format="parquet"
        )
        os.remove(raw_file_path)
        file_manager.upload_to_cloud(
            converted_file_path, converted_file_path.split("/")[-1]
        )
        os.remove(converted_file_path)
        logging.info(f"Successfully saved: {activity_name} ({activity_id})")

if __name__ == "__main__":
    main()
