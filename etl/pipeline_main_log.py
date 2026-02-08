
from garminconnect import(
    Garmin, 
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError
)
from main_log.extract_main_log import extract_garmin_main_log
from main_log.transform_main_log import transform_garmin_main_log
from main_log.load_main_log import load_main_log
from utils.utils import (connect_db, get_start_date, get_end_date, connect_garmin_api, logging_setup)
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
import logging

logging_setup()
logger = logging.getLogger(__name__)


def run_etl_main_log(client=None):
    """
    Orchestrates the full ETL pipeline for table main_log.
    """
    logger.info("Starting main_log Pipeline")
    
    #1 Getting date ranges and database connections

    conn, cursor = connect_db()

    start_date = get_start_date()
    end_date = get_end_date() 

    if start_date is None:
        logger.warning("No start date. Exit ETL")
        return
    
    logger.info(f"Running main_log ETL from {start_date} to {end_date}")
    

    #2 Extract for main_log
    logger.info("Extract for main_log")

    df_raw_main_log = extract_garmin_main_log(
        client=client,
        start_date=start_date,
        end_date=end_date
    )

    if df_raw_main_log.empty:
        logger.warning("No new activities found. Exit ETL")
        return

    logger.info("Extract for main_log completed | rows=%d", len(df_raw_main_log))


    #3 Transform for main_log
    logger.info("Transform for main_log")

    df_transformed_main_log = transform_garmin_main_log(df_raw_main_log)

    if df_transformed_main_log.empty:
        logger.warning("No transformed activities data. Exit ETL")
        return
    
    logger.info("Transform for main_log completed | rows=%d", len(df_transformed_main_log))


    #4 Load for main_log
    logger.info("Load for main_log")
    
    try: 
        load_main_log(conn, cursor, df_transformed_main_log)
        logger.info("Load for main_log completed")
    finally:
        conn.close()

    return df_transformed_main_log

