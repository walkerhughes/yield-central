import pandas as pd 
from datetime import datetime

import scripts.utils.get_daily_discription as get_daily_discription 

CURRENT_DATE = get_daily_discription.clean_date(datetime.today())
DATA_CLEANED_DIR = "./data/cleaned/yield_curve_historical_rates_MASTER.parquet"

yc_data = pd.read_parquet(DATA_CLEANED_DIR)
summary_str = get_daily_discription.summary_data_str(DATA_CLEANED_DIR)
spy_data = get_daily_discription.get_historical_market_data() 

overview_prompt, citations = get_daily_discription.get_prompt(
    date = CURRENT_DATE, 
    summary_data = summary_str, 
    historical_yc = yc_data.iloc[: 31].to_string(index = False), 
    historical_spy = spy_data.to_string(index = False)
)

if __name__ == "__main__": 

    if get_daily_discription.is_trading_day(): 

        temp_insights = get_daily_discription.generate_insight(overview_prompt)
        insights = f"\n**Digging Deeper**\n\n{temp_insights}\n\n{citations}"

        temp_tldr = get_daily_discription.generate_tldr(temp_insights)
        tldr = f"\n**TL;DR**\n\n{temp_tldr}\n"
        
    else: 
        insights = f"\nMarkets are closed today. Please check back for updated insights soon."
        tldr = "\nValues displayed are from last trading day."

    get_daily_discription.push_to_big_query(
        {
            "Date": CURRENT_DATE, 
            "Description": insights
        },
        table_id = 'daily_description'
    ) 
    get_daily_discription.push_to_big_query(
        {
            "Date": CURRENT_DATE, 
            "TLDR": tldr
        },
        table_id = 'tldr'
    )