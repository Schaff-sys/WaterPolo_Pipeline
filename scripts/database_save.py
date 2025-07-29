import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from clean_jsons import main_clean
from pathlib import Path
from data_scraper import main_scraper



env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")

if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Variables de entorno incompletas.")

def main_save(results, logger):
    try:
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        logger.info("Conexión a la base de datos establecida correctamente.")


        filtered_events = results.get('events')

        filtered_dates = results.get('dates')

        competition_id = results.get('competition_id')

        # Convert lists to DataFrames if needed
        if isinstance(filtered_events, list):
            filtered_events = pd.DataFrame(filtered_events)
        if isinstance(filtered_dates, list):
            filtered_dates = pd.DataFrame(filtered_dates)
        
        filtered_events.to_sql(f'events{competition_id}', con=engine, if_exists='replace', index=False)
        filtered_dates.to_sql(f'dates{competition_id}', con=engine, if_exists='replace', index=False)

        logger.info(f"Datos de eventos guardados en la tabla events_{competition_id}.")
        logger.info(f"Datos de fechas guardados en la tabla events_{competition_id}.")

        logger.info("Proceso completado exitosamente.")

    except Exception as e:
            logger.error(f"Error al guardar datos en la base de datos: {e}")
            raise

if __name__ == "__main__":
    main_save()
