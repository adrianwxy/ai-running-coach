
from garminconnect import(
    Garmin, 
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError
)
from split_log.extract_split_log import extract_garmin_split_log
from split_log.transform_split_log import transform_garmin_split_log
from split_log.load_split_log import load_split_log
from utils.utils import (connect_db, connect_garmin_api, logging_setup)
import os
import sys
import logging


logging_setup()
logger = logging.getLogger(__name__)


def run_etl_split_log(client=None, df_main_log=None):
    """
    Orchestrates the full ETL pipeline for table split_log.
    """
    logger.info("Starting split_log Pipeline")


    #1 Connect to database connection and import transformed main_log data

    conn, cursor = connect_db()


    if df_main_log is None or df_main_log.empty:
        logger.warning("Main log ETL returned no data. Skipping Split log ETL.")
        return
    
    logger.info("Main log ETL returned data. Continue Split log ETL.")

    #2 Extract for split_log

    logger.info("Extract for split_log")

    df_raw_split_log = extract_garmin_split_log(
        client = client,
        df_main_log = df_main_log)

    if df_raw_split_log.empty:
        logger.warning("No new split data found. Exit ETL.")
        return

    logger.info("Extract for split_log completed | rows=%d", len(df_raw_split_log))

    
    #3 Transform for split_log

    logger.info("Transform for split_log")

    df_transformed_split_log = transform_garmin_split_log(
         df_main_log = df_main_log,
         df_raw = df_raw_split_log)

    if df_transformed_split_log.empty:
         logger.warning("No transformed split data. Exit ETL")
         return
    
    logger.info("Transform for split_log completed | rows=%d", len(df_transformed_split_log))

    #4 Load for split_log
    logger.info("Load for split_log")

    try: 
        load_split_log(conn, cursor, df_transformed_split_log)
        logger.info("Load for split_log completed")
    finally:
        conn.close()

    return df_transformed_split_log    
