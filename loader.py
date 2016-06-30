from etl import get_local_handles, ingest_feeds, CSV_ETL_CLASSES
from local import LocalConfig
from interface import Marcotti


if __name__ == "__main__":
    settings = LocalConfig()
    marcotti = Marcotti(settings)
    with marcotti.create_session() as sess:
        for entity, etl_class in CSV_ETL_CLASSES:
            data_file = settings.CSV_DATA[entity]
            if data_file is None:
                continue
            if entity in ['Salaries', 'Partials', 'FieldStats', 'GkStats', 'LeaguePoints']:
                params = (sess, settings.COMPETITION_NAME, settings.SEASON_NAME)
            else:
                params = (sess,)
            ingest_feeds(get_local_handles, settings.CSV_DATA_DIR, data_file, etl_class(*params))
