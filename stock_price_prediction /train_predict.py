from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
import snowflake.connector
import logging

# Function to establish Snowflake connection
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
        database='stockprice',
        schema='raw_data'  
    )
    return conn

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """
    conn = return_snowflake_conn()  # Get the connection first
    
    # Create a cursor from the connection
    cur = conn.cursor()

    create_view_sql = f"""
    CREATE OR REPLACE VIEW {train_view} AS SELECT
        CAST(DATE AS TIMESTAMP_NTZ) AS DATE,  -- Casting DATE to TIMESTAMP_NTZ
        CLOSE, 
        SYMBOL
    FROM {train_input_table};"""

    create_model_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        logging.info(f"Model '{forecast_function_name}' trained successfully.")
        # Inspect the accuracy metrics of your model.
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        logging.error(f"Error in training the model: {e}")
        raise

    finally:
        cur.close()  # Always close the cursor after use
        conn.close()  # Close the connection too

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    conn = return_snowflake_conn()
    cur = conn.cursor()  # Create a cursor for prediction execution

    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
        logging.info(f"Predictions stored successfully in {final_table}.")
    except Exception as e:
        logging.error(f"Error in making predictions: {e}")
        raise

    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id='TrainPredict',
    start_date=days_ago(1),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule_interval='@daily'
) as dag:

    # Set all tables and views within the raw_data schema
    train_input_table = "stockprice.raw_data.stock_data"
    train_view = "stockprice.raw_data.market_data_view"
    forecast_table = "stockprice.raw_data.market_data_forecast"
    forecast_function_name = "stockprice.raw_data.predict_stock_price"
    final_table = "stockprice.raw_data.market_data"
    
    # Create a Snowflake connection
    conn = return_snowflake_conn()
    
    # Call tasks
    train(conn, train_input_table, train_view, forecast_function_name)
    predict(conn, forecast_function_name, train_input_table, forecast_table, final_table)
