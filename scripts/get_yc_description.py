import os
import datetime 
import pandas as pd 

import openai 

from google.cloud import bigquery 


DATA_CLEANED_DIR = "./data/cleaned/yield_curve_historical_rates_MASTER.parquet"
CURRENT_DATE = str(datetime.datetime.today()).split(" ")[0]

PROMPT = f"""
    **US Treasury Yield Curve Movements On {CURRENT_DATE}**

    **Overview:**
    <PROVIDE A BRIEF OVERVIEW OF THE CURRENT STATE OF THE YIELD CURVE AND THE MOST RECENT YIELD CURVE DYNAMICS THAT WOULD BE RELEVANT TO ECONOMISTS AND INVESTORS. COMMENT ON IF THE CURVE STEEPEND OR FLATTENED FOR EXAMPLE.>

    **What it Means**
    1. **Economic Outlook:** <ONE TO THREE SENTENCES ON THE ECONOMIC OUTLOOK BASED ON THE YIELD CURVE>
    
    2. **Monetary Policy:** <ONE TO THREE SENTENCES ON WHAT THE CURRENT YIELD CURVE MIGHT INDICATE FOR MONETARY POLICY>

    3. **Investment Strategies:** <GIVE A BRIEF COMMENT ON WHAT THE CURRENT YIELD CURVE MIGHT INDICATE FOR INVESTORS>
"""

# Create a function to generate insights
def generate_insight(data_summary: pd.DataFrame) -> str:

    openai_api_key = os.environ("OPENAI_API_KEY") 
    openai.api_key = openai_api_key

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an economic expert on the US Treasury Yield Curve. \
                                           You provide intelligent, objective, non-speculative analyses of recent data.\
                                           You provide these analyses daily after the closing values of the Yield Curve are determined."},
            {"role": "user", "content": f"Provide insights based on the following data in the format below.\
                                          Fill in your analyses in the carrots <>:\
                                          The response template you need to use: {PROMPT}. \
                                          The summary data: {data_summary}."}
        ]
    )
    return response['choices'][0]['message']['content'].strip()

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


if __name__ == "__main__": 

    # Get insights for the DataFrame summary
    df = pd.read_parquet(DATA_CLEANED_DIR)
    insights = generate_insight(data_summary=df.iloc[: 31])
    data = {"Date": CURRENT_DATE, "Description": insights}
    push_to_big_query(data)