import pandas as pd
import requests
import os
from dotenv import load_dotenv, find_dotenv
import logging
import sys
from datetime import datetime
from shared_script import convert_sap_byd_date, load_to_postgres
import mapping

load_dotenv(
    find_dotenv(),
    override=True,
)

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


username, password = os.getenv("SAP_BYD_USERNAME"), os.getenv("SAP_BYD_PASSWORD")
sap_base_url = os.getenv("SAP_API_ENDPOINT")

top = int(os.getenv("TOP", 100000))
offset = int(os.getenv("OFFSET", 0))


class SAPByDesign:
    def __init__(self, base_url, username, password, top, offset):
        self.top = top
        self.offset = offset
        self.base_url = base_url
        self.auth = (username, password)

    def fetch_data(self, params=None):
        while True:
            url = self.base_url.format(top=self.top, offset=self.offset)
            log.info(f"Fetching data from URL: {url}")
            response = requests.get(url, auth=self.auth, params=params)
            try:
                if response.status_code == 200:
                    data = response.json().get('d').get('results', [])
                    if len(data) == 0:
                        log.info("No more data to fetch.")
                        break
                    df = pd.DataFrame(data)
                    return df
                self.offset += self.top
                log.info(f"Fetched {len(data)} records. Next offset: {self.offset}")
            except Exception as e:
                log.error(f"Error parsing JSON response: {e}")
                break

    def transform_data(self, df):
        """
        Transform the fetched data.
        """
        log.info("Starting data transformation...")
        if df.empty:
            log.warning("DataFrame is empty. No data to transform.")
            return df
        # Drop unwanted columns and rename columns based on mapping 
        df.drop(columns=[col for col in mapping.column_mapping.values()], inplace=True, errors='ignore')

        # Rename columns based on mapping
        df.rename(
            columns= {key: items for key, items in mapping.column_mapping.items() if items is not None}, 
            inplace=True
        )

        # transformation: Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # Convert SAP ByD date strings to standard date format
        date_columns = ['create_date', 'change_date']
        for col in date_columns:
            df[col] = df[col].apply(convert_sap_byd_date)
        
        # process product_id to ensure it's a string and strip any whitespace
        df['product_id'] = df['product_id'].astype(str).str.strip()

        # DWH load date 
        df['dwh_load_date'] = datetime.now()
        
        log.info("Data transformation complete.")

        # Save transformed data to CSV
        df.to_csv('CSV/sap_byd_data.csv', index=False)
        
        log.info("Data saved to testing.csv")
        return df
    
    def to_db(self, df, table_name):
        engine = load_to_postgres()
        try:
            with engine.connect() as con:
                df.to_sql(table_name, con=con, schema = os.getenv("DB_SCHEMA"), if_exists='replace', index=False)
                log.info(f"Data successfully loaded into table {table_name}")
        except Exception as e:
            log.error(f"Error loading data into database: {e}")
        finally:
            engine.dispose()
            log.info("Database connection closed.")

if __name__ == "__main__":
    sap = SAPByDesign(sap_base_url, username, password, top, offset)
    df = sap.fetch_data()
    sap.transform_data(df)
    sap.to_db(pd.read_csv('CSV/sap_byd_data.csv'), os.getenv("DB_TABLE", "sap_byd_data"))
