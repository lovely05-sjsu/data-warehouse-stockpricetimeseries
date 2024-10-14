from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import datetime
import snowflake.connector
import requests
import logging

# Function to establish Snowflake connection (returns full connection object)
def return_snowflake_conn():
    user_id = Variable.get('snowflake_user_id')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='stockprice',
        schema='raw_data'
    )
    return conn

# Extract task to fetch data from Alpha Vantage API for a given symbol
@task
def extract(symbol):
    api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        logging.info(f"API response for {symbol}: {data}")
        
        # Check if the response contains the expected data
        if 'Time Series (Daily)' not in data:
            logging.error(f"No 'Time Series (Daily)' data found for {symbol}. Full response: {data}")
            return None
        
        # Check if the API rate limit has been hit
        if "Note" in data:
            logging.warning(f"API rate limit hit for {symbol}. Message: {data['Note']}")
            return None

        return data  # Return the JSON response
    else:
        logging.error(f"Failed to fetch data for {symbol}. Status code: {response.status_code}, Response: {response.text}")
        return None

# Transform task to format data for Snowflake
@task
def transform(data):
    if data is None:
        logging.warning(f"No data to transform.")
        return None

    time_series = data.get('Time Series (Daily)', {})
    records = []
    for date, values in time_series.items():
        record = {
            'date': date,
            'open': values['1. open'],
            'max_value': values['2. high'],  # Changed from 'high' to 'max_value'
            'min_value': values['3. low'],    # Changed from 'low' to 'min_value'
            'close': values['4. close'],
            'volume': values['5. volume'],
            'symbol': data.get('Meta Data', {}).get('2. Symbol', 'Unknown')
        }
        records.append(record)
    return records if records else None  # Return transformed records or None

# Load task to insert data into Snowflake
@task
def load(records, symbol):
    if records is None:
        logging.warning(f"No records to load for {symbol}")
        return

    conn = return_snowflake_conn()
    cur = conn.cursor()
    target_table = "raw_data.stock_data"
    
    try:
        for record in records:
            cur.execute(f"""
                MERGE INTO {target_table} AS tgt
                USING (SELECT '{record['date']}' AS date,
                              {record['open']} AS open,
                              {record['max_value']} AS max_value,  -- Changed from 'high' to 'max_value'
                              {record['min_value']} AS min_value,  -- Changed from 'low' to 'min_value'
                              {record['close']} AS close,
                              {record['volume']} AS volume,
                              '{record['symbol']}' AS symbol) AS src
                ON tgt.date = src.date AND tgt.symbol = src.symbol
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.open = src.open,
                        tgt.max_value = src.max_value,  -- Changed from 'high' to 'max_value'
                        tgt.min_value = src.min_value,    -- Changed from 'low' to 'min_value'
                        tgt.close = src.close,
                        tgt.volume = src.volume
                WHEN NOT MATCHED THEN
                    INSERT (date, open, max_value, min_value, close, volume, symbol)
                    VALUES (src.date, src.open, src.max_value, src.min_value, src.close, src.volume, src.symbol);
            """)
        cur.execute("COMMIT;")
        logging.info(f"Successfully loaded data for symbol {symbol}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error loading data into Snowflake: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

# Airflow DAG
with DAG(
    dag_id='AlphaVantageTimeSeriesDaily_AAPL_MSFT',
    start_date=days_ago(1),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    
    # Fetch, transform, and load stock data for AAPL
    symbol_AAPL = "AAPL"
    stock_data_AAPL = extract(symbol_AAPL)
    transformed_data_AAPL = transform(stock_data_AAPL)
    load(transformed_data_AAPL, symbol_AAPL)
    
    # Fetch, transform, and load stock data for MSFT
    symbol_MSFT = "MSFT"
    stock_data_MSFT = extract(symbol_MSFT)
    transformed_data_MSFT = transform(stock_data_MSFT)
    load(transformed_data_MSFT, symbol_MSFT)

