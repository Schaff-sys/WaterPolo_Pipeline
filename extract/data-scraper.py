import os 
import requests
import json
import time
import random
import logging 
from dotenv import load_dotenv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed



load_dotenv()

# URLs base para scraping
competition_url_template = 
match_base_url = 
event_base_url: os.getenv(event_base_url)

headers = {
    "Authorization": os.getenv("AUTHORIZATION"),
    "User-Agent": os.getenv("USER_AGENT")
}

if 

print(headers)

def get_match_ids(competition_id):
    url = competition_url_template.format(competition_id=competition_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()
            if "matches" in data and isinstance(data["matches"], list):
                return [match["id"] for match in data["matches"]]
            else:
                print(f"❌ Key 'matches' not found for competition {competition_id}")
                return []
        except json.JSONDecodeError:
            print(f"❌ JSON decode error for competition {competition_id}")
            return []
    else:
        print(f"❌ Failed to fetch match IDs for competition {competition_id}: {response.status_code}")
        return []

def collect_match_data(match_id):
        response = requests.get(f"{match_base_url}{match_id}", headers=headers)
        
        if response.status_code == 200:
            print(f"✅ Match {match_id} fetched")
            return response.json()
        else:
            print(f"⚠️ Match {match_id} failed: {response.status_code}")
            return None



def collect_event_data(match_id):
        response = requests.get(f"{event_base_url}{match_id}", headers=headers)
        
        if response.status_code == 200:
            print(f"✅ Event {match_id} fetched")
            return response.json()
        else:
            print(f"⚠️ Event {match_id} failed: {response.status_code}")
            return None

for comp_id in competition_ids:
    print(f"\n📥 Scraping competition ID: {comp_id}")

    match_ids = list(set(get_match_ids(comp_id)))
    print(f"🔎 Found {len(match_ids)} match IDs")

all_match_data = []

all_event_data = []

with ThreadPoolExecutor(max_workers=5) as executor:
        
        futures = {executor.submit(collect_match_data, match_id) for match_id in match_ids}
        for future in as_completed(futures):
             data =future.result()
             if data:
                all_match_data.append(data)    
        time.sleep(random.uniform(1.5, 3.5))

with ThreadPoolExecutor(max_workers=5) as executor:
        
        futures = {executor.submit(collect_event_data, match_id) for match_id in match_ids}
        for future in as_completed(futures):
             data =future.result()
             if data:
                all_event_data.append(data)    
        time.sleep(random.uniform(1.5, 3.5))  
        

    # Guardar datos
base_dir = Path(__file__).parent

save_dir = base_dir / 'data' 

save_dir.mkdir(parents=True, exist_ok=True)

save_path_matches = save_dir/f"competition_{comp_id}_matches.json"

with open(save_path_matches, 'w') as f:
        json.dump(all_match_data, f, indent=4)

save_path_events = save_dir/f"competition_{comp_id}_events.json"

with open(save_path_events, 'w') as f:
        json.dump(all_event_data, f, indent=4)        

print(f"💾 Data for competition {comp_id} saved to: {save_path_matches}") 