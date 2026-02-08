import pandas as pd

def extract_garmin_main_log(client, start_date, end_date) -> pd.DataFrame:

    activities = client.get_activities_by_date(start_date, end_date) 

    if not activities:
        return pd.DataFrame()
    
    df_raw = pd.DataFrame(activities)

    print("Extract Garmin main_log is done.")
    return df_raw