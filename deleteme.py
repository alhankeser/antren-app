import os
import pandas as pd

df = pd.DataFrame()
folder_path = '../workout_files/converted'

for parquet_file in os.listdir(folder_path):
    if '.parquet' in parquet_file:
        file_path = f'{folder_path}/{parquet_file}'
        print(file_path)
        activity = pd.read_parquet(file_path)
        activity = activity.astype({'activity_id': 'int32',
                                    'activity_name': 'string',
                                    'time': 'datetime64[s, UTC]',
                                    'watts': 'int8', 
                                    'heart_rate': 'int8',
                                    })
        activity.to_parquet(file_path, index=False)
        # activity['activity_type'] = 0
        # df = pd.concat([df, activity])

# df.to_parquet('all_activities.parquet', index=False)

# activity_1 = pd.read_parquet('../workout_files/converted/5984041649.parquet')
# activity_2 = pd.read_parquet('../workout_files/converted/1420582415.parquet')

