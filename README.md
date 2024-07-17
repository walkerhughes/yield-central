## YieldCentral: Daily Treasury Yield Curve Insights

Welcome to Yield Curve Central! This project brings digestible, daily macroeconomic analyses of the most current yield curve interest rate dynamics to your fingertips. Yield Curve Central leverages a data ingestion pipeline automated with GitHub Actions to process data from various sources each day, including data scraped from the web, and obtained through the Yahoo Finance and AlphaVantage APIs. This data is passed to GPT-4o, which generates analyses on [YieldCurveCentral.com](https://www.yieldcurvecentral.com/), a web app deployed on Vercel and bult with TypeScript and Next.js.

## Features
**Daily Updates**: EOD data scraped directly from treasury.gov each day and pushed to Google BigQuery via GitHub Actions Workflow.

**Insights**: Generates insights on the most up-to-date yield curve dynamics with GPT-4o. The model assumes the personality of a macroeconomic expert keen on educating the reader about monetary policy, accomplished via efficient system-level prompting. Daily data is fed to the model via the user prompt, and includes: 
1. Daily and historical yield curve values
2. Yield curve summary statistics, including if the yield curve is inverted, inversion duration, and magnitude of inversion 
3. Daily and historical SPY ETF values to indicate dynamics of equity markets from the Yahoo Finance API 
4. News article summaries from publicly available publications obtained from the AlphaVantage API 

**Automated Workflow**: GitHub Actions efficiently automates daily scraping, data processing, insight generation, pushing to Google BigQuery, and publishing on YieldCurveCentral.com.

**Interactive and Educational**: Learning resources on how to use the Yield Curve also available @ [YieldCurveCentral.com](https://www.yieldcurvecentral.com/).

