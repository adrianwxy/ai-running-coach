import pandas as pd
import numpy as np

col_from_raw =[
    'startTimeGMT', 'lapIndex', 'distance', 'duration',
    'elevationGain', 'elevationLoss', 'averageHR',
    'maxHR', 'averageRunCadence', 'averagePower', 'garmin_activity_id'
]

insert_col =[
    'split_index', 'duration_seconds',
    'ascent_meter', 'descent_meter', 'avg_heart_rate_bpm',
    'max_heart_rate_bpm', 'avg_cadence_spm', 'avg_power_w', 'distance_km',
    'date', 'is_partial_split','run_of_the_day'
]

def transform_garmin_split_log(
        df_main_log: pd.DataFrame, 
        df_raw: pd.DataFrame) -> pd.DataFrame:

    if df_raw.empty:
        return pd.DataFrame(columns=insert_col)

    missing_cols = set(col_from_raw) - set(df_raw.columns)
    
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")
    
    df = df_raw[col_from_raw].copy()
    df = df.query("distance >= 100")

    df['distance_km'] = (df['distance'] / 1000).round(2)
    df['duration'] = df['duration'].round()
    df['averageRunCadence'] = df['averageRunCadence'].round()
    df['date'] = df['startTimeGMT'].str[:10]
    df['is_partial_split'] = np.where(df['distance_km'] < 1.0, 1, 0)

    df = df.rename(columns={
        'lapIndex': 'split_index',
        'duration': 'duration_seconds',
        'averageRunCadence': 'avg_cadence_spm',
        'averagePower': 'avg_power_w',
        'averageHR': 'avg_heart_rate_bpm',
        'maxHR': 'max_heart_rate_bpm',
        'elevationGain': 'ascent_meter',
        'elevationLoss': 'descent_meter',
    })

    df_final = df.merge(
        df_main_log[['garmin_activity_id','run_of_the_day']],
        on = 'garmin_activity_id',
        how = 'inner')
    
    print("Transform Garmin split_log is done.")
    return df_final[insert_col]

        










