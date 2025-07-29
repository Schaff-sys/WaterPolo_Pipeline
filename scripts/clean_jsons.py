import json 
import pandas as pd 
import logging 
import os
import dotenv 
from dotenv import load_dotenv
from pathlib import Path
from data_scraper import scrape_competition, main_scraper


#Logging setup

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s' 
)

#Convert JSON into Dataframe 
def flatten_events(events_json, logger): 
    flat_list = [item for sublist in events_json for item in sublist] #Flatten nested dictionaries in the JSON
    df = pd.json_normalize(flat_list, max_level=2) #Convert from JSON into Dataframe
    logger.info(f"Flattened events data with {len(df)} records")
    return df

#Convert JSON into Dataframe
def flatten_dates(dates_json,logger):
    try:
        df = pd.json_normalize(dates_json, record_path='players') #Convert from JSON into Dataframe
        logger.info(f"Flattened events data with {len(df)} records")
        return df
    except KeyError:
        logger.error("Key 'players' not found in dates JSON")
        raise

#Filter dataframs for specific data 
def filter(df, columns, logger):
    missing_cols = [col for col in columns if col not in df.columns] #Identify if columns are missing 
    if missing_cols:
        logger.warning(f"Missing columns in DataFrame: {missing_cols}")
    filtered_df = df[[col for col in columns if col in df.columns]] #Loop to avoid key errors
    filtered_df.columns = [col.replace('.','_') for col in filtered_df.columns]
    logger.info("Filtered dataframe for necessary columns")
    return filtered_df

#Run main function - convert JSON to filtered dataframes
def main_clean(result, logger):
            events_df = result.get('events')
            dates_df = result.get('dates')
            competition_id = result.get('competition_id')
            #Columns to filter event data 
            events_columns = [
    # Basic event info
    'period', 'minute', 'seconds', 'matchId', 'type',

    # Swimoff
    'swimoff.homeTeamSwimmer.playerId',
    'swimoff.homeTeamSwimmer.teamId',
    'swimoff.awayTeamSwimmer.teamId',
    'swimoff.awayTeamSwimmer.playerId',
    'swimoff.winnerSwimmer.teamId',
    'swimoff.winnerSwimmer.playerId',

    # Shot
    'shot.teamId',
    'shot.type',
    'shot.takenBy.playerId',
    'shot.takenBy.teamId',
    'shot.assistedBy.playerId',
    'shot.isGoal',
    'shot.isFastBreak',
    'shot.isDirectFromFoul',
    'shot.locationX',
    'shot.locationY',
    'shot.targetX',
    'shot.targetY',
    'shot.savedBy.playerId',
    'shot.savedBy.teamId',
    'shot.blockedBy.playerId',
    'shot.blockedBy.teamId',
    'shot.blockedBy.positionId',

    # Turnover
    'turnover.teamId',
    'turnover.type',
    'turnover.lostPossesionPlayer.playerId',
    'turnover.lostPossesionPlayer.teamId',
    'turnover.wonPossesionPlayer.playerId',
    'turnover.wonPossesionPlayer.teamId',

    # Exclusion
    'exclusion.teamId',
    'exclusion.type',
    'exclusion.excludedPlayer.playerId',
    'exclusion.excludedPlayer.teamId',
    'exclusion.fouledPlayer.playerId',
    'exclusion.fouledPlayer.teamId',
    'exclusion.isPenaltyExclusion',
    'exclusion.isDoubleExclusion',
    'exclusion.locationX',
    'exclusion.locationY',

    # Timeout and challenge
    'timeout.teamId',
]

            
            #Columns to filter dates data
            dates_columns = [
        'id','player.name', 'player.id', 'player.surname', 'player.height', 'player.weight', 'player.primaryPosition', 'team.id', 'team.name', 'match.id', 'match.homeTeamId', 'match.awayTeamId'
            ]

            filtered_events = filter(flatten_events(events_df, logger), events_columns, logger)
            filtered_dates = filter(flatten_dates(dates_df, logger), dates_columns, logger)


            return {'events': filtered_events, 'dates': filtered_dates, 'competition_id': competition_id} 

if __name__ == "__main__":
    main_clean()