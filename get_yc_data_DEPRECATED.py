import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os

def get_current_month():
    today = datetime.today().date()
    year, month = today.year, today.month
    if month < 10:
        month = f"0{month}"
    return f"{year}{month}"

def clean_scraped_date(date_str): 
    date_split = date_str.split("/")
    return "-".join(date_split[-1] + date_split[: 2])

def format_payload_url():
    current_month = get_current_month()
    return f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value_month={current_month}"

def scrape_yield_curve_data_json():
    response = requests.get(format_payload_url())
    soup = BeautifulSoup(response.text, features="html.parser")
    yc_values_on_date = soup.find_all("tr")[-1]
    parsed_data = [_.text.strip() for _ in yc_values_on_date.__dict__["contents"][1::2]]
    yc_data_json = {
        "Date": clean_scraped_date(parsed_data[0]),
        "1 Mo": float(parsed_data[10]),
        "2 Mo": float(parsed_data[11]),
        "3 Mo": float(parsed_data[12]),
        "6 Mo": float(parsed_data[14]),
        "1 Yr": float(parsed_data[15]),
        "2 Yr": float(parsed_data[16]),
        "3 Yr": float(parsed_data[17]),
        "5 Yr": float(parsed_data[18]),
        "7 Yr": float(parsed_data[19]),
        "10 Yr": float(parsed_data[20]),
        "20 Yr": float(parsed_data[21]),
        "30 Yr": float(parsed_data[22]),
        "2 Yr - 10 Yr": float(parsed_data[16]) - float(parsed_data[20]), 
        "2 Yr - 30 Yr": float(parsed_data[16]) - float(parsed_data[22]),
        "10 Yr - 30 Yr": float(parsed_data[20]) - float(parsed_data[22]),
    }
    return yc_data_json, response.status_code

def get_yc_data(verbose = False):
    try:
        if verbose:
            print("Initiating attempt to retrieve Yield Curve data... ", end = " ")
        yc_data_json, status_code = scrape_yield_curve_data_json()
        if verbose:
            print(f"successful with status code {status_code}.")

        return yc_data_json
    except:
        raise ValueError(f"Failed with status code {status_code}. Either 'Month' argument invalid or data cannot be parsed as implemented.")

def save_to_json(dict_obj):
    save_dir = "./data/data_scraped"
    if not os.path.exists(save_dir): 
        os.mkdir(save_dir)
    date = dict_obj["date"].replace("/", "-")
    with open(f"{save_dir}/{date}.json", "w") as newfile:
        json.dump(dict_obj, newfile)

if __name__ == "__main__": 
    save_to_json(
        get_yc_data(verbose = False)
    )
