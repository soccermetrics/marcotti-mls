import os
import logging

from etl import get_local_handles, ingest_feeds, CSV_ETL_CLASSES
from local import LocalConfig
from interface import Marcotti


LOG_FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
ch = logging.FileHandler(os.path.join(LocalConfig().LOG_DIR, 'marcotti.log'))
ch.setLevel(logging.INFO)
ch.setFormatter(LOG_FORMAT)

logger = logging.getLogger('loader')
logger.setLevel(logging.INFO)
logger.addHandler(ch)


if __name__ == "__main__":
    settings = LocalConfig()
    marcotti = Marcotti(settings)
    logger.info("Data ingestion start")
    with marcotti.create_session() as sess:
        for entity, etl_class in CSV_ETL_CLASSES:
            data_file = settings.CSV_DATA[entity]
            if data_file is None:
                logger.info("Skipping ingestion into %s data model", entity)
            else:
                if type(data_file) is list:
                    data_file = os.path.join(*data_file)
                logger.info("Ingesting %s into %s data model",
                            os.path.join(settings.CSV_DATA_DIR, data_file), entity)
                if entity in ['Salaries', 'Partials', 'FieldStats', 'GkStats', 'LeaguePoints']:
                    params = (sess, settings.COMPETITION_NAME, settings.SEASON_NAME)
                else:
                    params = (sess,)
                ingest_feeds(get_local_handles, settings.CSV_DATA_DIR, data_file, etl_class(*params))
    logger.info("Data ingestion complete")
