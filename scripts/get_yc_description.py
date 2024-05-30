import os
import pandas as pd 
from datetime import datetime, timedelta 

import openai 
from google.cloud import bigquery 

import utils 

CURRENT_DATE = utils.clean_date(datetime.today())
DATA_CLEANED_DIR = "./data/cleaned/yield_curve_historical_rates_MASTER.parquet"

yc_data = pd.read_parquet(DATA_CLEANED_DIR)
summary_str = utils.summary_data_str(DATA_CLEANED_DIR)
spy_data = utils.get_historical_market_data() 

OVERVIEW_PROMPT = utils.get_prompt(
    date=CURRENT_DATE, 
    summary_data=summary_str, 
    historical_yc=yc_data.iloc[: 31].to_string(index = False), 
    historical_spy=spy_data.to_string(index = False)
)

if __name__ == "__main__": 

    if utils.is_trading_day(): 
        temp = utils.generate_insight(OVERVIEW_PROMPT)
        tldr = utils.generate_tldr(temp)
        insights = f"\n**TL;DR**\n\n{tldr}\n\n**Digging Deeper**\n\n{temp}"
    else: 
        insights = f"\nMarkets are closed today. Please check back for updated insights soon."

    data = {
        "Date": CURRENT_DATE, 
        "Description": insights
    }
    utils.push_to_big_query(data)