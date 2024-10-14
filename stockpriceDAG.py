from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import logging

def return_snowflake_conn():
    user_id = Variable.get('snowflake_user_id')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')
    
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  
        warehouse='compute_wh',
        database='stock',
        schema='raw_data'
    )
    return conn.cursor()

# Extract task: Fetch stock data from Alpha Vantage API
@task
def extract(symbol):
    api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    
    # Fetch data
    response = requests.get(url)
    if response.status_code == 200:
        logging.info(f"Successfully fetched data for symbol {symbol}")
        return response.json()
    else:
        raise ValueError(f"Failed to fetch data from Alpha Vantage API. Status code: {response.status_code}")

# Transform task: Parse and transform data into a format suitable for Snowflake
@task
def transform(data):
    time_series = data.get('Time Series (Daily)', {})
    
    # Prepare records for loading into Snowflake
    records = []
    for date, values in time_series.items():
        record = {
            'date': date,
            'open': values['1. open'],
            'high': values['2. high'],
            'low': values['3. low'],
            'close': values['4. close'],
            'volume': values['5. volume'],
            'symbol': data.get('Meta Data', {}).get('2. Symbol', 'Unknown')
        }
        records.append(record)
    
    logging.info(f"Transformed {len(records)} records.")
    return records

# Load task: Insert transformed data into Snowflake
@task
def load(cur, records, target_table):
    try:
        cur.execute("BEGIN;")
        
        # Create the table if it doesn't exist
        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_table} (
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                symbol STRING
            );
        """)

        # Use MERGE for upsert (insert or update)
        for record in records:
            symbol = record['symbol'].replace("'", "''")
            logging.info(f"Loading record for symbol: {symbol}, Date: {record['date']}")
            
            cur.execute(f"""
                MERGE INTO {target_table} AS tgt
                USING (SELECT '{record['date']}' AS date,
                              {record['open']} AS open,
                              {record['high']} AS high,
                              {record['low']} AS low,
                              {record['close']} AS close,
                              {record['volume']} AS volume,
                              '{symbol}' AS symbol) AS src
                ON tgt.date = src.date AND tgt.symbol = src.symbol
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.open = src.open,
                        tgt.high = src.high,
                        tgt.low = src.low,
                        tgt.close = src.close,
                        tgt.volume = src.volume
                WHEN NOT MATCHED THEN
                    INSERT (date, open, high, low, close, volume, symbol)
                    VALUES (src.date, src.open, src.high, src.low, src.close, src.volume, src.symbol);
            """)
        
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error while loading data into Snowflake: {e}")
        raise e

# Airflow DAG
with DAG(
    dag_id='AlphaVantageTimeSeriesDaily',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    schedule_interval='@daily',  # Run daily
    tags=['ETL']
) as dag:
    # Define the stock symbol to fetch
    target_table = "raw_data.stock_data"
    symbol = Variable.get("stock_symbol", default_var="AMZN")  
    
    # Set up the Snowflake connection
    cur = return_snowflake_conn()
    
    # Fetch, transform, and load the stock data
    stock_data = extract(symbol)
    transformed_data = transform(stock_data)
    load(cur, transformed_data, target_table)

