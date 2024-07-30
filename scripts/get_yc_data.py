from utils.get_daily_data_scraper import Scraper

def main():
    """
    Run scraper to retrieve daily yield curve values 
    """
    try:
        scraper = Scraper()
        scraper.get_yc_data()
        scraper.merge_with_parquet()
        scraper.push_to_big_query()
    except:
        raise ValueError("Error in Google Credentials or no data to push.")

if __name__ == "__main__":
    main()
