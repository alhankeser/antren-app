import os
import logging
from api.garmin import garmin
from file_manager import file_manager
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
        try:
            activity_raw_data = garmin.get_activity_raw_data(
                api_client, activity_id, format="tcx"
            )
            raw_file_path = file_manager.save_activity_raw_file(
                activity_id, activity_raw_data
            )
        except:
            logging.warning(f"Failed to get original: {activity_name} ({activity_id})")
            continue
        try:
            selectors = {
                "heart_rate": "HeartRateBpm > Value",
                "watts": "ns3:Watts",
                "time": "Time",
            }
            converted_file_path = file_manager.convert_activity_file(
                activity_id, activity_name, raw_file_path, selectors, format="parquet"
            )
        except:
            print(raw_file_path)
            logging.warning(f"Failed to convert: {activity_name} ({activity_id})")
            continue
        try:
            os.remove(raw_file_path)
        except:
            logging.warning(f"Failed to delete: {activity_name} ({activity_id})")
            continue
        try:
            file_manager.upload_to_cloud(
                converted_file_path, converted_file_path.split("/")[-1]
            )
        except:
            logging.warning(
                f"Failed to upload to bucket: {activity_name} ({activity_id})"
            )
            pass
        try:
            os.remove(converted_file_path)
        except:
            logging.warning(f"Failed to delete: {activity_name} ({activity_id})")
            continue
        logging.info(f"Successfully saved: {activity_name} ({activity_id})")

if __name__ == "__main__":
    main()
