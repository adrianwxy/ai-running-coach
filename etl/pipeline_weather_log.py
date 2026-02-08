from weather_log.extract_weather_log import extract_weather_log
from weather_log.transform_weather_log import transform_weather_log
from weather_log.load_weather_log import load_weather_log
from utils.utils import (connect_db, get_start_date, get_end_date, logging_setup)
from pipeline_main_log import run_etl_main_log
import requests
import logging

logging_setup()
logger = logging.getLogger(__name__)


def run_etl_weather_log(df_main_log=None):
    """
    Orchestrates the full ETL pipeline for table weather_log.
    """ 
    logger.info("Starting weather_log Pipeline")

    #1 Getting date ranges and database connections and import transformed main_log data

    conn, cursor = connect_db()

    if df_main_log is None or df_main_log.empty:
        logger.warning("Main log ETL returned no data. Skipping weather log ETL.")
        return
    
    logger.info("Main log ETL returned data. Continue weather log ETL.")

    #2 Extract for weather_log

    logger.info("Extract for weather_log")

    start_date = df_main_log['date'].min()
    end_date = df_main_log['date'].max()

    df_raw_weather_log = extract_weather_log(
        start_date = start_date,
        end_date = end_date,
        df_main_log = df_main_log)

    if df_raw_weather_log.empty:
        logger.warning("No new weather data found. Exit ETL.")
        return
    
    logger.info("Extract for weather_log completed | rows=%d", len(df_raw_weather_log))

    #3 Transform for weather_log

    logger.info("Transform for weather_log")

    df_transformed_weather_log = transform_weather_log(
         df_raw = df_raw_weather_log,
         df_main_log = df_main_log)

    if df_transformed_weather_log.empty:
         logger.warning("No transformed weather data. Exit ETL")
         return
    
    logger.info("Transform for weather_log completed | rows=%d", len(df_transformed_weather_log))

    #4 Load for weather_log

    logger.info("Load for weather_log")

    try: 
        load_weather_log(conn, cursor, df_transformed_weather_log)
        logger.info("Load for Weather_log completed")
    finally:
        conn.close()

    return df_transformed_weather_log   

