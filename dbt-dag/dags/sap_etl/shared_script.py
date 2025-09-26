from datetime import datetime
import logging
import pandas as pd
import sys
from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
from sqlalchemy import create_engine

load_dotenv(    
    find_dotenv(),
    override=True,
)

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_table = os.getenv("DB_TABLE")

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

def convert_sap_byd_date(date_str: str):
    """
    Convert SAP ByD date string to standard YYYY-MM-DD format.
    Example input: '/Date(1609459200000)/'
    """
    if date_str is not None and isinstance(date_str, str):
        date_str = str(date_str).strip('/Date()')
        try:
            timestamp_ms = int(date_str)
            return pd.Timestamp(timestamp_ms).date()
        except ValueError as e:
            log.error(f"Error converting date: {e}")
            return None


def load_to_postgres():
    try:
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')      
        log.info("Successfully connected to PostgreSQL")   
        return engine      
    except Exception as e:
        log.error(f"Error connecting to PostgreSQL: {e}")
