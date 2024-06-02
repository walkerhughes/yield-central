# helper functions for: 
#   - get_yc_data.py
#   - get_yc_description.py

import pandas as pd 
from google.cloud import bigquery 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import yfinance as yf 
import openai 

import heapq
import requests
from datetime import datetime 

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


def get_alphavantage_articles() -> dict: 
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics=economy_monetary&apikey=Z1O8XN3WKHVEXJAK'
    response = requests.get(url)
    response_json = response.json()
    return response_json

def get_relevance_score(item: dict) -> float:
    if 'topics' in item and isinstance(item['topics'], list):
        for topic in item['topics']:
            if topic['topic'] == 'Economy - Monetary':
                return float(topic['relevance_score'])
    return 0.0

def get_top_k_relevant_articles(data: dict, k: int) -> list: 
    top_k_articles = heapq.nlargest(k, data["feed"], key = get_relevance_score)
    return top_k_articles

def format_markdown_citation(article):
    # Extract the date
    date_str = article['time_published']
    date_obj = datetime.strptime(date_str, '%Y%m%dT%H%M%S')
    formatted_date = date_obj.strftime('%Y, %B %d')

    # Extract other details
    authors = ', '.join(article['authors'])
    title = article['title']
    source = article['source']
    url = article['url']
    
    # Format the citation with the title as a hyperlink
    citation = f"{authors}. ({formatted_date}). {title}. {source}. [(Read here)]({url})"
    return citation

def get_top_k_summaries(top_k_articles): 
    return "\n\n".join([f"Title: {article['title']}\nSummary: {article['summary']}" for article in top_k_articles])

def get_top_k_citations(top_k_articles): 
    return "**Sources + Relevant Articles**\n\n" + "\n\n".join([format_markdown_citation(article) for article in top_k_articles])

def get_prompt(date: str, summary_data: str, historical_yc: str, historical_spy: str) -> str: 

    data = get_alphavantage_articles()
    top_k_articles = get_top_k_relevant_articles(data, 3) 
    article_summaries = get_top_k_summaries(top_k_articles)
    citations = get_top_k_citations(top_k_articles)

    return f""" 
        Today is {date}. 

        Write a 5-sentence analysis of the most recent US Treasury Yield Curve dynamics. Be sure to use the active voice. 
        
        Write your analysis in the context of today's yield curve summary data: {summary_data}, and the last month of end-of-day yield curve values: {historical_yc}. To aid your analysis is the last month of SPY ETF data: {historical_spy}. 

        Utilize the following article summaries in your analysis, but beware of any specific alarming predictions made: {article_summaries}. 

        Write your analysis in the context of the most recent Federal Reserve FOMC Statements: 
         
            March 30, 2024: 'Recent indicators suggest solid economic growth, with strong job gains and low unemployment. Although inflation has eased over the past year, it remains high. The Committee aims for maximum employment and 2% inflation over the long term and notes improving balance in achieving these goals, though the economic outlook remains uncertain. To support these goals, the Committee has decided to maintain the federal funds rate at 5.25% to 5.5%. They will assess incoming data and the evolving outlook before making any adjustments, emphasizing that they will not lower the rate until inflation is sustainably moving toward 2%. They will also continue to reduce their holdings of Treasury and agency securities. The Committee is committed to returning inflation to 2% and will monitor incoming information to adjust policy as needed. Their assessments will consider various factors, including labor market conditions, inflation pressures, expectations, and financial and international developments.'

            May 2, 2024: 'Recent indicators show solid economic growth with strong job gains and low unemployment. Inflation has eased but remains high, with no recent progress toward the 2% target. The Committee aims for maximum employment and 2% inflation long-term, noting improved balance in achieving these goals despite economic uncertainty. It remains focused on inflation risks. To support its objectives, the Committee keeps the federal funds rate at 5.25-5.5% and will adjust based on incoming data and risk balance. No rate reduction is expected until inflation moves sustainably toward 2%. The Committee will reduce its Treasury securities holdings more slowly, from $60 billion to $25 billion monthly, while maintaining a $35 billion cap on agency securities. The Committee is committed to returning inflation to 2% and will adjust monetary policy as needed, considering labor market, inflation, and global financial developments.'

        Your analysis should be intelligent, non-speculative, and not contain financial advice. It should be easily understood by an 8th grader, but compelling enough that a professional investor would find it worthwhile. Finish with a brief forward-looking statement on what the Federal Reserve might comment on in their next FOMC meeting given the current macro environment.

    """, citations