from database.db import init_db, get_job_count
from scraper.naukri_scraper import scrape_naukri

def main():
    print("Initializing database...")
    init_db()
    
    print(f"Current job count: {get_job_count()}")
    
    print("\nScraping Naukri...")
    scrape_naukri(
        search_query="data analyst intern",
        location="India",
        pages=3
    )
    
    scrape_naukri(
        search_query="python developer intern",
        location="India", 
        pages=2
    )
    
    scrape_naukri(
        search_query="AI engineer intern",
        location="India",
        pages=2
    )
    
    print(f"\nTotal jobs in database: {get_job_count()}")
    print("\nDone. Run 'streamlit run dashboard/app.py' to view dashboard")

if __name__ == "__main__":
    main()