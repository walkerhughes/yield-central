import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os

class Scraper: 
    def __init__(self): 
        self.today = datetime.today().date().strftime('%Y-%m-%d')
        self.get_current_month() 
        self.format_payload_url() 

    def get_current_month(self):
        year, month = self.today.year, self.today.month
        if month < 10:
            month = f"0{month}"
        self.current_month = f"{year}{month}"

    def clean_scraped_date(self, date_str): 
        date_split = date_str.split("/")
        return "-".join([date_split[-1], date_split[0], date_split[1]])

    def format_payload_url(self):
        self.url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value_month={self.current_month}"

    def scrape_yield_curve_data_json(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, features="html.parser")
        yc_values_on_date = soup.find_all("tr")[-1]
        parsed_data = [_.text.strip() for _ in yc_values_on_date.__dict__["contents"][1::2]]
        self.date_cleaned = self.clean_scraped_date(parsed_data[0])
        yc_data_json = {
            "Date": self.today,
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

    def get_yc_data(self, verbose = False):
        if verbose:
            print("Initiating attempt to retrieve Yield Curve data... ", end = " ")
        try: 
            self.data, self.status_code = self.scrape_yield_curve_data_json()
            if verbose: 
                print(f"successful with status code {self.status_code}.")
        except:
            raise ValueError(f"Attempt failed. Either 'Month' argument invalid or data cannot be parsed as implemented.")

    def save_to_json(self):
        save_dir = "./data/data_scraped"
        if not os.path.exists(save_dir): 
            os.mkdir(save_dir)
        with open(f"{save_dir}/{self.today}.json", "w") as newfile:
            json.dump(self.data, newfile)


if __name__ == "__main__": 

    scraper = Scraper() 
    scraper.get_yc_data(verbose = False) 
    scraper.save_to_json()