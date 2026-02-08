import pandas as pd
import numpy as np
import requests

url = "https://archive-api.open-meteo.com/v1/archive"

def extract_weather_log(
        start_date, 
        end_date, 
        df_main_log: pd.DataFrame) -> pd.DataFrame:
    
    gps = df_main_log[['latitude', 'longitude']].values.tolist()
    df_raw = []

    for lat, lon in gps:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date, 
            "end_date": end_date, 
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "weathercode"
            ],
            "timezone": "auto"            
        }   

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        hourly = data["hourly"]

        if not hourly:
            continue

        df_weather = pd.DataFrame({
            "datetime": pd.to_datetime(hourly["time"]),
            "temp_celsius": hourly["temperature_2m"],
            "humidity_pct": hourly["relative_humidity_2m"],
            "wind_speed_kph": hourly["wind_speed_10m"],
            "weather_condition": hourly["weathercode"],
        })

        df_weather["latitude"] = lat
        df_weather["longitude"] = lon
        df_weather["date"] = df_weather["datetime"].dt.date.astype(str)
        df_weather["time"] = df_weather["datetime"].dt.time.astype(str)

        df_raw.append(df_weather)
    
    if not df_raw:
        return pd.DataFrame()

    print("Extract weather_log is done.")
    return pd.concat(df_raw, ignore_index=True)
