## YieldCentral: Daily Treasury Yield Curve Insights

Welcome to YieldCentral, a daily-updated hub for U.S. Treasury Yield Curve data. This project leverages web scraping, continuous integration (CI), and continuous deployment (CD) workflows to bring the most current yield curve interest rates directly from treasury.gov to your fingertips. YieldCentral implements this data with an interactive Streamlit app for an engaging way to monitor and analyze U.S. Treasury bond yields over time.

## Features
Daily Updates: EOD data scraped directly from treasury.gov each day and pushed to Google BigQuery via GitHub Actions Workflow.

Insights: Generates insights on the most up-to-date yield curve dynamics with GPT-4o. The model assumes the personality of a macroeconomic expert keen on educating the reader about monetary policy, accomplished via efficient system-level prompting. Current data is fer to the model via the user prompt, and includes: 
> Daily and historical yield curve values
> Yield curve summary statistics, including if the yield curve is inverted, inversion duration, and magnitude of inversion 
> Daily and historical SPY ETF values to indicate dynamics of equity markets from the Yahoo Finance API 
> News article summaries from publicly available publications obtained from the AlphaVantage API 

Automated Workflow: GitHub Actions efficiently automates daily scraping, data processing, insight generation, pushing to Google BigQuery, and publishing on YieldCurveCentral.com.

Interactive and Educational: Learning resources on how to use the Yield Curve also available @ [YieldCurveCentral.com](https://www.yieldcurvecentral.com/).

