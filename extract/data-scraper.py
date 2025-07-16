import os 
import requests
import json
import time
import random
import logging 
from dotenv import load_dotenv
from dotenv import find_dotenv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()  # Load environment variables from .env file
print(find_dotenv())  # Debugging line to check if .env is found
# Load in URLs for scraping
competition_ids = os.getenv("competition_ids")
competition_url_template = os.getenv("competition_url_template")    
match_base_url = os.getenv("match_base_url")
event_base_url = os.getenv("event_base_url")

headers = {
    "Authorization": os.getenv("AUTHORIZATION"),
    "User-Agent": os.getenv("USER_AGENT")
}

# Gives a warning if any of the environment variables are not set
if not competition_ids or not competition_url_template or not match_base_url or not event_base_url or not headers:
    raise ValueError("One or more environment variables are not set correctly.")

# Collect match ids from competition page
def get_match_ids(competition_id: int) -> list[int]:
    url = competition_url_template.format(competition_id=competition_id)
    response = requests.get(url, headers=headers) #send request to competition URL

    if response.status_code == 200:
        try:
            data = response.json() 
            return [match["id"] for match in data.get("matches", []) if 'id' in match] #collect ids from matches
        except json.JSONDecodeError:
            logger.error(f"JSON decode error for competition {competition_id}")
            return []
    else:
        logger.error(f"Failed to fetch match IDs for competition {competition_id}: {response.status_code}")
        return []

# Collect match data 
def collect_match_data(match_id: int) -> dict | None:
        time.sleep(random.uniform(1.5, 3.5)) 
        response = requests.get(f"{match_base_url}{match_id}", headers=headers) #send request to match URL
        if response.status_code == 200:
            logger.info(f"Match {match_id} fetched")
            return response.json()
        else:
            logger.warning(f" Match {match_id} failed: {response.status_code}")
            return None


# Collect event data
def collect_event_data(match_id: int) -> dict | None:
        time.sleep(random.uniform(1.5, 3.5)) 
        response = requests.get(f"{event_base_url}{match_id}", headers=headers) #send request to event URL
        if response.status_code == 200:
            logger.info(f"Event {match_id} fetched")
            return response.json()
        else:
            logger.warning(f"Event {match_id} failed: {response.status_code}")
            return None

# Main script execution
def fetch_data_parallel(fetch_func, ids: list[int], max_workers=5) -> list[dict]:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor: # create a thread pool
        futures = {executor.submit(fetch_func, id) for id in ids} # submit tasks to the executor
        for future in as_completed(futures): # process completed futures
            data = future.result() # get the result of the future
            if data:
                results.append(data)    
    return results
        
# Save data to JSON files
def save_data(comp_id: int, matches: list[dict], events: list[dict]) -> None:
    base_dir = Path(__file__).parent # get the directory of the current script
    save_dir = base_dir / 'data' # create a subdirectory named 'data'
    save_dir.mkdir(parents=True, exist_ok=True) # create the directory if it doesn't exist
    

    current_time = time.strftime("%Y%m%d_%H%M%S") # format current time for file naming
    save_path_matches = save_dir / f"competition_{comp_id}_dates.json" # create a file path for matches
    save_path_events = save_dir / f"competition_{comp_id}_events.json" # create a file path for events

    with open(save_path_matches, 'w') as f:
            json.dump(matches, f, indent=4)

    with open(save_path_events, 'w') as f:
            json.dump(events, f, indent=4)        

    logging.info(f"Data for competition {comp_id} saved to: {save_path_matches} and {save_path_events}")

def scrape_competition(competition_id: int) -> None:
    logger.info(f"Starting scrape for competition {competition_id}")
    match_ids = list(set(get_match_ids(competition_id))) # get unique match ids
    logger.info(f"Found {len(match_ids)} matches for competition {competition_id}")
    
    all_match_data = fetch_data_parallel(collect_match_data, match_ids) # collect match data in parallel
    logger.info(f"Collected data for {len(all_match_data)} matches")
    all_event_data = fetch_data_parallel(collect_event_data, match_ids) # collect event data in parallel
    logger.info(f"Collected data for {len(all_event_data)} events")

    save_data(competition_id, all_match_data, all_event_data) # save the collected data

# Run the scraper for each competition ID
if __name__ == "__main__":
    competition_ids = [int(id.strip()) for id in competition_ids.split(',')] if competition_ids else [] # split and convert to int'
    for competition_id in competition_ids: 
        try:
            scrape_competition(competition_id) # scrape each competition
        except Exception as e:
            logger.error(f"Error scraping competition {competition_id}: {e}")
            continue