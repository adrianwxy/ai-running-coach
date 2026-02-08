import sqlite3
from datetime import datetime, timedelta, date
import os
from pathlib import Path
from dotenv import load_dotenv
from garminconnect import Garmin
import logging
import sys

def connect_db():
    """
    Connects to the SQLite database and returns a connection object.
    The connection is not automatically closed.
    """
    BASE_DIR = Path(__file__).resolve().parent
    database_path = BASE_DIR.parent / "running_db.db"
    
    conn = sqlite3.connect(database_path) 
    conn.execute("PRAGMA foreign_keys = ON;") 
    cursor = conn.cursor() 

    return conn, cursor


def get_start_date():
    """
    connect to the sqlite database and get the latest date from main_log
    """

    conn, cursor = connect_db()

    try:
        query = f"SELECT DATE(MAX(date), '+1 day') FROM main_log"
        cursor.execute(query)
        result = cursor.fetchone()[0]

        return result
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None

    finally:
        conn.close()

def get_end_date(end_date = None):
    """
    Returns the end date for ETL operations, with ISO format.
    If the end_date is not provided, the current date will be applied
    """

    if end_date is None:
        end_date = date.today().isoformat()
    
    return end_date


def connect_garmin_api():
    """
    Connect to garmin API with credentials.
    """

    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")

    if not email or not password:
        raise RuntimeError("Missing Garmin credentials in .env file")

    client = Garmin(email, password)

    return client


def logging_setup():
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)