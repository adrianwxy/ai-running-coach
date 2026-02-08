import pandas as pd

col_from_raw =[
    'activityId', 'startTimeLocal', 'distance', 'duration', 'startLatitude', 'startLongitude',
    'calories', 'bmrCalories', 'averageHR', 
    'sportTypeId',  # sportTypeId = 1 as running
    'avgPower', 'elevationGain', 'averageRunningCadenceInStepsPerMinute'
]

insert_col =[
    'garmin_activity_id', 'date', 'time', 'distance_km', 'latitude', 'longitude',
    'duration_seconds', 'active_cal', 'elevation_gain_meter', 'avg_power_w',
    'avg_cadence_spm', 'avg_heart_rate_bpm', 'run_of_the_day'
]

def transform_garmin_main_log(df_raw: pd.DataFrame) -> pd.DataFrame:

    missing_cols = set(col_from_raw) - set(df_raw.columns)
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    df = df_raw[col_from_raw].copy()

    df = df.query("sportTypeId == 1")

    df['active_cal'] = df['calories'] - df['bmrCalories']
    df['distance_km'] = (df['distance'] / 1000).round(2)
    df['duration_seconds'] = df['duration'].round()
    df['avg_cadence_spm'] = df['averageRunningCadenceInStepsPerMinute'].round()

    df['date'] = df['startTimeLocal'].str[:10]
    df['time'] = df['startTimeLocal'].str[-8:]   


    df = df.rename(columns={
        'activityId': 'garmin_activity_id',
        'avgPower': 'avg_power_w',
        'averageHR': 'avg_heart_rate_bpm',
        'elevationGain': 'elevation_gain_meter',
        'startLatitude': 'latitude',
        'startLongitude': 'longitude'
    })

    df['run_of_the_day'] = (
        df.sort_values('startTimeLocal')
        .groupby('date')
        .cumcount() + 1
    )

    print("Transform Garmin main_log is done.")
    return df[insert_col]

