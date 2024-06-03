from utils.get_daily_data_scraper import Scraper

def main(): 
    try: 
        scraper = Scraper() 
        scraper.get_yc_data() 
        scraper.merge_with_parquet()
        scraper.push_to_big_query() 
    except: 
        print("Error in Google Credentials or no data to push.")

if __name__ == "__main__": 
    main() 