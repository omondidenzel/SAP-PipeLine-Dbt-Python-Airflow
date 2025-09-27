import pandas as pd
import requests
import os
from dotenv import load_dotenv, find_dotenv
import logging
import sys
from datetime import datetime

sys.path.append(os.path.expanduser('/usr/local/airflow/dags/sap_etl'))

from shared_script import convert_sap_byd_date, load_to_postgres
from mapping import column_mapping

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
        log.info(f"DataFrame shape before transformation: {df.shape}")
        log.info(f"Initial DataFrame columns: {df.columns.tolist()}")
        df.drop(
            columns=[col for col in column_mapping if column_mapping[col] is None], 
            inplace=True, 
        )
        log.info(f"Columns after dropping unwanted columns: {df.columns.tolist()}")

        # Rename columns based on mapping
        df.rename(
            columns= {key: items for key, items in column_mapping.items() if items is not None}, 
            inplace=True
        )
        log.info(f"Columns after renaming: {df.columns.tolist()}")

        # transformation: Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # log.info(f"Columns after renaming: {df.columns}")
        log.info(f"DataFrame shape after dropping unwanted columns: {df.shape}")

        # Convert SAP ByD date strings to standard date format
        date_columns = ['create_date', 'change_date']
        for col in date_columns:
            df[col] = df[col].apply(convert_sap_byd_date)
        
        # process product_id to ensure it's a string and strip any whitespace
        df['product_id'] = df['product_id'].astype(str).str.strip()

        df['company_id'] = 'FKConsulting'

        # DWH load date 
        df['dwh_load_date'] = datetime.now()
        
        log.info("Data transformation complete.")

        # Save transformed data to CSV
        file_path = 'CSV/sap_byd_data.csv'
        try:
            if not os.path.exists('CSV'):
                log.info("Creating CSV directory.")
                os.makedirs('CSV', exist_ok=True)
                log.info("CSV directory created.")
            df.to_csv(f"{file_path}", index=False)
            log.info(f"Data save in {file_path}")
        except Exception as e:
            log.error(f"Error saving data to CSV: {e}")
        
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

def main():
    sap = SAPByDesign(sap_base_url, username, password, top, offset)
    df = sap.fetch_data()
    file_path = 'CSV/sap_byd_data.csv'
    sap.transform_data(df)
    sap.to_db(pd.read_csv(f"{file_path}"), os.getenv("DB_TABLE", "sap_byd_data"))

if __name__ == "__main__":
    main()

# all should work well now