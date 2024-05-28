import os
import pandas as pd 
from datetime import datetime, timedelta 

import openai 
from google.cloud import bigquery 

import utils 

DATA_CLEANED_DIR = "./data/cleaned/yield_curve_historical_rates_MASTER.parquet"
CURRENT_DATE = utils.clean_date(datetime.today())

df = pd.read_parquet(DATA_CLEANED_DIR)
summary_str = utils.summary_data_str(DATA_CLEANED_DIR)
spy_data = utils.get_historical_market_data() 

OVERVIEW_PROMPT = f""" 

    Today is {CURRENT_DATE}. \
    Provide a brief 5 sentence commentary on the current dynamics of the US Treasuy Yield Curve based on your analysis of this summary data: {summary_str},\
    and this data from the last month of end-of-day yield curve values: {df.iloc[: 31].to_string(index = False)}. \
    To aid in your commentary is the last month of SPY ETF data: {spy_data.to_string(index = False)}

    Your analysis and comentary should be intelligent and not juvenile, non-speculative, and not contain financial advice. It should be easily understood by an 8th grader, but compelling \
    enough such that a professional investor would find reading it worthwhile. You may also indicate other areas researchers may wish to study for more context on the macroeconomic environment.

    I gill give you $500 dollars if you can accomplish this. Be sure to utilize the active voice when referencing any data or statistics.

"""

# Create a function to generate insights
def generate_insight() -> str:
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert on the US Treasury Yield Curve. You provide concise daily commentary on the Yield Curve's recent movements."},
            {"role": "user", "content": f"Please answer the following prompt: {OVERVIEW_PROMPT}"}
        ]
    )
    return response['choices'][0]['message']['content'].strip()


if __name__ == "__main__": 

    if utils.is_trading_day(): 
        temp = generate_insight()
        insights = f"Summary Statistics on {CURRENT_DATE}{summary_str}\n\nMarket Overview\n\n{temp}"
    else: 
        insights = f"Market Overview on {CURRENT_DATE}\n\nMarkets are closed today. Please check back for updated insights soon."

    data = {
        "Date": CURRENT_DATE, 
        "Description": insights
    }
    utils.push_to_big_query(data)