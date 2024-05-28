import os
import pandas as pd 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from google.cloud import bigquery 
import yfinance as yf 

BASE_DIR = "/".join(os.getcwd().split("/")[: -1])
data_cleaned_dir = "/data/cleaned/yield_curve_historical_rates_MASTER.parquet"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../yieldcurve-422317-510529e47525.json"


def summary_data_query(): 
    # init client 
    client = bigquery.Client() 
    # Define your dataset and table
    dataset_id = 'yieldcurve'
    table_id = 'historical'
    table_ref = client.dataset(dataset_id).table(table_id) 

    query = f""" 
        WITH inversion_date AS (
            SELECT 
                MAX(CAST(Date AS DATE)) AS last_inversion_date
            FROM 
                {table_ref} 
            WHERE 
                `2_Yr` < `10_Yr`
        ),
        contango_data AS (
            SELECT 
                *0
            FROM 
                {table_ref}
            ORDER BY 
                Date DESC 
            LIMIT 1
        )
        SELECT 
            (SELECT CAST(1 + last_inversion_date AS string) FROM inversion_date) AS last_inversion_date,
            (SELECT 1 + DATE_DIFF(CURRENT_DATE(), last_inversion_date, DAY) FROM inversion_date) AS num_days_since_last_inversion, 
            (SELECT ROUND((`3_Mo` - `10_Yr`) / `3_Mo`, 2) FROM contango_data) AS contango_3m_10y, 
            (SELECT ROUND(`3_Mo` - `10_Yr`, 2) FROM contango_data) AS diff_3m_10y, 
            (SELECT ROUND((`2_Yr` - `10_Yr`) / `2_Yr`, 2) FROM contango_data) AS contango_2y_10y,
            (SELECT ROUND(`2_Yr___10_Yr`, 2) FROM contango_data) AS diff_2y_10y, 
        ;
    """
    results = client.query(query)  
    summary_data = pd.DataFrame([dict(_) for _ in results.result()])
    return summary_data

def summary_data_str(): 
    summary_data = summary_data_query() 
    df = pd.read_parquet(BASE_DIR + data_cleaned_dir)
    summary_str = f""" 
        **Summary Statistics for {df.iloc[0]["Date"]}**\n
        **Is inverted**: {True if df.iloc[0]["2 Yr"] > df.iloc[0]["10 Yr"] else False}
        **Last inversion date**: {summary_data["last_inversion_date"].values[0]}
        **Days since last inversion**: {summary_data["num_days_since_last_inversion"].values[0]}
        **2 Year-10 Year Difference**: {summary_data["diff_2y_10y"].values[0]}%
        **3 Month-10 Year Difference**: {summary_data["diff_3m_10y"].values[0]}%
    """
    return summary_str


def clean_date(datetime_date): 
    return datetime_date.strftime('%Y-%m-%d')

def get_historical_market_data(ticker_symbol = "SPY"): 
    # fetches historical ticker data for the past month
    today = datetime.today()
    past = today - timedelta(days = 31)
    # Fetch the historical data
    spy_data = yf.download(ticker_symbol, start=clean_date(past), end=clean_date(today), interval='1d').reset_index() 
    # return only the necessary columns since these are passed into prompt (don't need excess tokens being processed)
    return spy_data[["Date", "Adj Close"]]


def push_to_big_query(data):
    # init client 
    client = bigquery.Client()

    # Define your dataset and table
    dataset_id = 'yieldcurve'
    table_id = 'daily_description'

    table_ref = client.dataset(dataset_id).table(table_id)
    # Insert data into the table
    to_insert = [
        data
    ]
    errors = client.insert_rows_json(table_ref, to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
