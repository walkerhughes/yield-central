# helper functions for: 
#   - get_yc_data.py
#   - get_yc_description.py

import pandas as pd 
from google.cloud import bigquery 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import yfinance as yf 


def clean_date(datetime_date): 
    # returns cleaned datetime date as str 
    return datetime_date.strftime('%Y-%m-%d')


def is_trading_day() -> bool: 
    """ 
        Determines if current day is a trading day on NYSE
        Returns True if it is a trading day, else False. 
    """
    nyse = mcal.get_calendar('NYSE')
    today = datetime.today()
    schedule = nyse.schedule(start_date = today, end_date = today)
    return False if schedule.empty else True 


def get_historical_market_data(ticker_symbol = "SPY"): 
    # fetches historical ticker data for the past month
    today = datetime.today()
    past = today - timedelta(days = 31)
    # Fetch the historical data
    spy_data = yf.download(ticker_symbol, start=clean_date(past), end=clean_date(today), interval='1d').reset_index() 
    # return only the necessary columns since these are passed into prompt (don't need excess tokens being processed)
    return spy_data[["Date", "Adj Close"]]


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
                *
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


def summary_data_str(data_cleaned_dir): 
    summary_data = summary_data_query() 
    df = pd.read_parquet(data_cleaned_dir)
    summary_str = f"""
        **Is inverted**: {True if df.iloc[0]["2 Yr"] > df.iloc[0]["10 Yr"] else False}
        **Last inversion date**: {summary_data["last_inversion_date"].values[0]}
        **Days since last inversion**: {summary_data["num_days_since_last_inversion"].values[0]}
        **2 Year-10 Year Difference**: {summary_data["diff_2y_10y"].values[0]}%
        **3 Month-10 Year Difference**: {summary_data["diff_3m_10y"].values[0]}%
    """
    return summary_str


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


