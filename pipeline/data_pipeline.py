from datetime import timedelta
import multiprocessing
import logging
import sys, os 
from prefect import flow, task, get_run_logger 

# Add scripts folder to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

# Import your script functions
from data_scraper import main_scraper
from clean_jsons import main_clean
from database_save import main_save

# Set multiprocessing context (needed when using Prefect with multiprocessing in some environments)
multiprocessing.set_start_method("spawn", force=True)

@task(retries=1, retry_delay_seconds=300)
def scrape():
    logger = get_run_logger()
    logger.info("Starting data scraping...")
    multiprocessing.set_start_method("spawn", force=True)
    return main_scraper(logger)

@task(retries=1, retry_delay_seconds=300)
def clean(raw_data):
    logger = get_run_logger()
    logger.info("Starting data cleaning...")
    multiprocessing.set_start_method("spawn", force=True)
    return main_clean(raw_data, logger)

@task(retries=1, retry_delay_seconds=300)
def save(cleaned):
    logger = get_run_logger()
    logger.info("Starting data saving...")  
    multiprocessing.set_start_method("spawn", force=True)
    return main_save(cleaned, logger)

@task
def notify():
    multiprocessing.set_start_method("spawn", force=True)
    logger = get_run_logger()
    logger.info("Pipeline completed successfully.")

@flow(name="pipeline", retries=0)
def pipeline():
    raw = scrape()
    cleaned = clean(raw)
    save(cleaned)
    notify()

if __name__ == "__main__":
    pipeline()
