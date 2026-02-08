from utils.utils import connect_garmin_api, logging_setup
from pipeline_main_log import run_etl_main_log
from pipeline_split_log import run_etl_split_log
from pipeline_weather_log import run_etl_weather_log
import sys
from garminconnect import(
    Garmin, 
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError
)
import logging

logging_setup()
# logger = logging.getLogger("pipeline_master")
logger = logging.getLogger(__name__)

def run_master_pipeline():
    # print("--- Starting Master Pipeline ---")
    logger.info("Starting Master Pipeline")



    ###### 1. SINGLE LOGIN
    # print("Logging into Garmin...")
    logger.info("Logging into Garmin API")
    try: 
        client = connect_garmin_api()
        client.login()
        logger.info("Garmin login successful")

    # except GarminConnectAuthenticationError:
    #     raise RuntimeError("Garmin authentication failed")
    # except GarminConnectConnectionError:
    #     raise RuntimeError("Garmin connection error")
    # except GarminConnectTooManyRequestsError:
    #     raise RuntimeError("Garmin rate limit exceeded")
    except GarminConnectAuthenticationError:
        logger.exception("Garmin authentication failed")
        raise
    except GarminConnectConnectionError:
        logger.exception("Garmin connection error")
        raise
    except GarminConnectTooManyRequestsError:
        logger.exception("Garmin rate limit exceeded")
        raise



    ###### 2. Run Main Log ETL (Pass the client)
    # print("\n--- Running main_log ETL ---")
    logger.info("Running main_log ETL")
    df_main_log = run_etl_main_log(client=client)
    
    if df_main_log is None or df_main_log.empty:
        logger.warning("main_log ETL returned empty dataframe")
        return

    logger.info("main_log ETL completed | rows=%d", len(df_main_log))
    
    
    
    ###### 3. Run Split Log ETL (Pass the client AND the data from step 2)
    # print("\n--- Running split_log ETL ---")
    logger.info("Running split_log ETL")
    # run_etl_split_log(client=client, df_main_log=df_main_log)
    run_etl_split_log(client=client, df_main_log=df_main_log)
    logger.info("split_log ETL completed")
    
    
    
    ###### 4. Run Split Log ETL (Pass the client AND the data from step 2)
    # print("\n--- Running weather_log ETL ---")
    logger.info("Running weather_log ETL")
    # run_etl_weather_log()
    run_etl_weather_log(df_main_log=df_main_log)
    logger.info("weather_log ETL completed")

    logger.info("Master Pipeline Finished Successfully")

    # print("\n--- Master Pipeline Finished ---")

if __name__ == "__main__":
    run_master_pipeline()