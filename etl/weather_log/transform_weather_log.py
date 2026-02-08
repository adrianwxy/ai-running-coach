import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

weather_codes_dict = {
    0: 'Clear sky',
    1: 'Mainly clear',
    2: 'Partly cloudy',
    3: 'Overcast',
    45: 'Fog',
    48: 'Depositing rime fog',
    51: 'Light drizzle',
    53: 'Moderate drizzle',
    55: 'Dense drizzle',
    61: 'Slight rain',
    63: 'Moderate rain',
    65: 'Heavy rain',
    71: 'Slight snow',
    73: 'Moderate snow',
    75: 'Heavy snow',
    77: 'Snow grains',
    80: 'Slight rain showers',
    81: 'Moderate rain showers',
    82: 'Violent rain showers',
    85: 'Slight snow showers',
    86: 'Heavy snow showers',
    95: 'Thunderstorm',
    96: 'Thunderstorm with slight hail',
    99: 'Thunderstorm with heavy hail'
}

col =[
    "date","time","latitude","longitude",
    "temp_celsius","humidity_pct","wind_speed_kph",
    "weather_condition"
]

def transform_weather_log(
        df_raw: pd.DataFrame,
        df_main_log: pd.DataFrame) -> pd.DataFrame:

    if df_raw.empty:
        return pd.DataFrame()
    
    df_raw = df_raw[col]
    df_raw["weather_condition"] = df_raw["weather_condition"].map(weather_codes_dict).fillna("Unknown")
    
    
    df_main_log = df_main_log[['date', 'time', 'latitude', 'longitude']].copy()
    df_main_log['time']= pd.to_datetime(
        df_main_log["time"], 
        format="%H:%M:%S",
        errors='coerce')

    df_main_log['time'] = df_main_log['time'].dt.round('h')
    df_main_log['time'] = df_main_log['time'].astype(str)
    df_main_log['time'] = df_main_log['time'].str[-8:] 

    df_final = df_raw.merge(
        df_main_log[['date','time','latitude', 'longitude']],
        on = ['date','time','latitude', 'longitude'],
        how ='inner'
    )

    df_final['time'] = df_final['time'].str[:2]
    print("Transform weather_log is done.")
    return df_final[col]
