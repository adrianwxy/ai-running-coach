import pandas as pd

def extract_garmin_split_log(client, df_main_log: pd.DataFrame) -> pd.DataFrame:

    id_list = df_main_log['garmin_activity_id'].values.tolist()
    df_raw = []

    for id in id_list:
        
        activities_split = client.get_activity_splits(id) 

        split = activities_split['lapDTOs']

        if not split:
            continue
        
        df_split = pd.DataFrame(split)

        df_split['garmin_activity_id'] = id
        df_raw.append(df_split)

    if not df_raw:
        return pd.DataFrame()
    
    print("Extract Garmin split_log is done.")
    return pd.concat(df_raw, ignore_index=True)




