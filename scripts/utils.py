# helper functions for: 
#   - get_yc_data.py
#   - get_yc_description.py

import pandas as pd 
from google.cloud import bigquery 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import yfinance as yf 
import openai 


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


# Create a function to generate insights
def generate_insight(OVERVIEW_PROMPT: str = "") -> str:
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert on the US Treasury Yield Curve. You provide concise daily commentary on the Yield Curve's recent movements."},
            {"role": "user", "content": f"Please answer the following prompt: {OVERVIEW_PROMPT}"}
        ]
    )
    return response['choices'][0]['message']['content'].strip()

def generate_tldr(insights) -> str:
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert at writing TL;DR versions of long text while maintaining the most vital information."},
            {"role": "user", "content": f"Concisely summarize the following text, returning only your summary: {insights}"}
        ]
    )
    return response['choices'][0]['message']['content'].strip()


def get_prompt(date: str, summary_data: str, historical_yc: str, historical_spy: str) -> str: 
    return f""" 
        Today is {date}. 
        Provide a brief 5-sentence commentary on the current dynamics of the US Treasury Yield Curve based on your analysis of this summary data: {summary_data},
        and this data from the last month of end-of-day yield curve values: {historical_yc}.
        To aid in your commentary is the last month of SPY ETF data: {historical_spy}

        Your analysis and commentary should be intelligent and not juvenile, non-speculative, and not contain financial advice. It should be easily understood by an 8th grader, but compelling 
        enough such that a professional investor would find reading it worthwhile. You may also indicate other areas researchers may wish to study for more context on the macroeconomic environment.

        Feel free to reference how recent interest rate moves fit in the context of the most recent Federal Reserve FOMC Statement released on May 2, 2024: 
        'Recent indicators show solid economic growth with strong job gains and low unemployment. Inflation has eased but remains high, with no recent progress toward the 2% target. The Committee aims for maximum employment and 2% inflation long-term, noting improved balance in achieving these goals despite economic uncertainty. It remains focused on inflation risks. To support its objectives, the Committee keeps the federal funds rate at 5.25-5.5% and will adjust based on incoming data and risk balance. No rate reduction is expected until inflation moves sustainably toward 2%. The Committee will reduce its Treasury securities holdings more slowly, from $60 billion to $25 billion monthly, while maintaining a $35 billion cap on agency securities. The Committee is committed to returning inflation to 2% and will adjust monetary policy as needed, considering labor market, inflation, and global financial developments.'

        **Be sure to utilize the active voice.**
    """