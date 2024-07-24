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


def get_n_previous_descriptions(n: int = 5): 
    # init client 
    client = bigquery.Client() 
    # Define your dataset and table
    dataset_id = 'yieldcurve'
    table_id = 'historical'
    table_ref = client.dataset(dataset_id).table(table_id) 

    query = f"""
        SELECT * FROM `yieldcurve-422317.yieldcurve.daily_description` ORDER BY Date DESC LIMIT {n};
    """
    results = client.query(query)  
    descriptions = pd.DataFrame([dict(_) for _ in results.result()])
    return descriptions


def format_prev_descriptions(n: int = 5) -> str: 
    # inputs
    #   n (int): number of previous descriptions to retrieve from bigquery 
    desc = get_n_previous_descriptions(n)
    return "\n\n".join([f"Date: {date}\nDescription: {description}" for date, description in zip(desc.Date, desc.Description)])


def push_to_big_query(data, table_id = 'daily_description'):
    # init client 
    client = bigquery.Client()

    # Define your dataset and table
    dataset_id = 'yieldcurve'

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
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system", 
                "content": """

                    You are an expert on the US Treasury Yield Curve that contributes daily articles to an online publication. 

                    The user will provide you with the following information to aid in your analysis today: 
                        •	Today's yield curve summary data (if the yield curve is currently inverted based on the 2 Year - 10 Year yields, common credit spreads, etc)
                        •	End-of-day yield curve values from the last month 
                        •	End-of-day SPY ETF values form the last month 
                        •	Relevant news articles from today 

                    Incorporate insights from the most recent Federal Reserve FOMC Statement on June 12, 2024 where appropriate:
                        "Recent indicators show solid economic growth, strong job gains, and low unemployment. Inflation has eased but remains high, with modest progress toward the 2 percent goal. The Committee aims for maximum employment and 2 percent inflation, noting that risks to these goals have balanced out over the past year. The economic outlook is uncertain, and inflation risks remain a focus.
                        The Committee decided to maintain the federal funds rate target range at 5.25 to 5.5 percent. Any rate adjustments will depend on incoming data, the evolving outlook, and risk balance. The rate will not be reduced until there is confidence in inflation moving sustainably toward 2 percent. Additionally, the Committee will continue reducing holdings of Treasury and agency securities. The Committee is committed to returning inflation to 2 percent.
                        The Committee will monitor new information for economic implications and adjust monetary policy if necessary. Assessments will consider labor market conditions, inflation pressures and expectations, and financial and international developments."
        
                    Your main goals as a commentator are to:
                        1.	Inform the reader about current market conditions in the context of user-provided summary data on the economy.
                        2.	Interpret what these developments may mean for future Federal Reserve monetary policy.
                    
                    Develop your analysis with this chain-of-thought:
                        1.	What does the current shape of the yield curve mean about current capital market structure? Is this consistent with the direction of the stock market?
                        2.	What do the yield curve values, end-of-day data, and SPY data indicate about the market? How might this influence the Federal Reserve's policy moving forward?
                        3.	Do the news articles provide any additional useful information? Avoid specific alarming predictions.

                    Your commentary should be optimized for SEO for a macroeconomic publication. Ensure your analysis is intelligent, non-speculative, and free of financial advice. It should be easily understood by an 8th grader but compelling for a professional investor. Use the active voice. 
                    Do not include section headers or separators. Output should be in paragraph form.

                """
            },{
                "role": "user", 
                "content": f"""
                    Please answer the following prompt: {OVERVIEW_PROMPT}
                """
            }
        ]
    )
    return response['choices'][0]['message']['content'].strip()


def generate_reflection(descriptions) -> str:
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system", 
                "content": """

                    You are an expert on the US Treasury Yield Curve that contributes daily articles to an online publication. The user will provide you with your last 5 descriptions about the yield curve and current state of monetary policy. 

                    Market's are closed today, so instead of generating a unique insight, you will summarize your analyses from the past 5 days and list in bullet points the biggest unanswered questions for the week ahead regarding markets and monetary policy.
                    
                    Your main goals as a commentator are to:
                        1.	Inform the reader about current market conditions in the context of user-provided summary data on the economy.
                        2.	Interpret what these developments may mean for future Federal Reserve monetary policy.
                    
                    Your commentary should be optimized for SEO for a macroeconomic publication. Ensure your analysis is intelligent, non-speculative, and free of financial advice. It should be easily understood by an 8th grader but compelling for a professional investor. Use the active voice. 
                    Do not include section headers or separators. Output should be in paragraph form.

                """
            },{
                "role": "user", 
                "content": f"""
                    Provide a summary of these analyses on the US Treasury Yield Curve from the past several days: {descriptions}
                """
            }
        ]
    )
    return response['choices'][0]['message']['content'].strip()


def generate_tldr(insights) -> str:
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an expert at summarizing long text while maintaining the most vital information."},
            {"role": "user", "content": f"Summarize the following text, returning only your summary: {insights}"}
        ]
    )
    return response['choices'][0]['message']['content'].strip()


def get_prompt(date: str, summary_data: str, historical_yc: str, historical_spy: str, article_summaries: str) -> str: 
    return f""" 
        Today is {date}. Write a brief macroeconomic analysis of the most recent US Treasury Yield Curve dynamics.

        Use the following data in your analysis:
            •	Today's yield curve summary: {summary_data}
            •	Last month's end-of-day yield curve values: {historical_yc}
            •	Last month's SPY ETF data: {historical_spy}
            •	Article summaries: {article_summaries}

        Your analysis should be focused, structured, and comprehensive.
    """