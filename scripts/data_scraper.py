import os
import requests
import json
import time
import random
from dotenv import load_dotenv
from dotenv import find_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from prefect import get_run_logger

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# Load in URLs for scraping
competition_url_template = os.getenv("competition_url_template")  
match_base_url = os.getenv("match_base_url")
event_base_url = os.getenv("event_base_url")

headers = {
    "Authorization": os.getenv("AUTHORIZATION"),
    "User-Agent": os.getenv("USER_AGENT")
}

# Gives a warning if any of the environment variables are not set
if not competition_url_template or not match_base_url or not event_base_url or not headers:
    raise ValueError("One or more environment variables are not set correctly.")


# Collect match ids from competition page
def get_match_ids(competition_id: int, logger) -> list[int]:
    url = competition_url_template.format(competition_id=competition_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()
            return [match["id"] for match in data.get("matches", []) if 'id' in match]
        except json.JSONDecodeError:
            logger.error(f"JSON decode error for competition {competition_id}")
            return []
    else:
        logger.error(f"Failed to fetch match IDs for competition {competition_id}: {response.status_code}")
        return []

# Collect match data 
def collect_match_data(match_id: int, logger) -> dict | None:
    time.sleep(random.uniform(1.5, 3.5)) 
    response = requests.get(f"{match_base_url}{match_id}", headers=headers)
    if response.status_code == 200:
        logger.info(f"Match {match_id} fetched")
        return response.json()
    else:
        logger.warning(f"Match {match_id} failed: {response.status_code}")
        return None

# Collect event data
def collect_event_data(match_id: int, logger) -> dict | None:
    time.sleep(random.uniform(1.5, 3.5)) 
    response = requests.get(f"{event_base_url}{match_id}", headers=headers)
    if response.status_code == 200:
        logger.info(f"Event {match_id} fetched")
        return response.json()
    else:
        logger.warning(f"Event {match_id} failed: {response.status_code}")
        return None

# Main script execution
def fetch_data_parallel(fetch_func, ids: list[int], logger, max_workers=5) -> list[dict]:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_func, id, logger) for id in ids}
        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)
    return results

def scrape_competition(competition_id: int, logger) -> dict:
    logger.info(f"Starting scrape for competition {competition_id}")
    match_ids = list(set(get_match_ids(competition_id, logger)))
    logger.info(f"Found {len(match_ids)} matches for competition {competition_id}")
    
    all_match_data = fetch_data_parallel(collect_match_data, match_ids, logger)
    logger.info(f"Collected data for {len(all_match_data)} matches")
    
    all_event_data = fetch_data_parallel(collect_event_data, match_ids, logger)
    logger.info(f"Collected data for {len(all_event_data)} events")

    return {"dates": all_match_data, "events": all_event_data, "competition_id": competition_id}

# Run the scraper for each competition ID
def main_scraper(logger):
    competition_ids = os.getenv("competition_ids")  
    competition_ids = [int(id.strip()) for id in competition_ids.split(',')] if competition_ids else []
    for competition_id in competition_ids:
        try:
            return scrape_competition(competition_id, logger)
        except Exception as e:
            logger.error(f"Error scraping competition {competition_id}: {e}")
    return None

if __name__ == "__main__":
    main_scraper()

