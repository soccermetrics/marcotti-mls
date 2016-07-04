import os
import logging

from local import LocalConfig
from base import BaseCSV, SeasonalDataIngest, get_local_handles, ingest_feeds, create_seasons
from overview import (ClubIngest, CountryIngest, CompetitionIngest, CompetitionSeasonIngest,
                      PlayerIngest, PersonIngest)
from financial import (AcquisitionIngest, PlayerSalaryIngest, PartialTenureIngest)
from statistics import (PlayerMinuteIngest, FieldStatIngest, GoalkeeperStatIngest, LeaguePointIngest)


LOG_FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
ch = logging.FileHandler(os.path.join(LocalConfig().LOG_DIR, 'marcotti.log'))
ch.setLevel(logging.INFO)
ch.setFormatter(LOG_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(ch)


CSV_ETL_CLASSES = [
    ('Clubs', ClubIngest),
    ('Competitions', CompetitionIngest),
    ('CompetitionSeasons', CompetitionSeasonIngest),
    ('Players', PlayerIngest),
    ('Acquisitions', AcquisitionIngest),
    ('Salaries', PlayerSalaryIngest),
    ('Partials', PartialTenureIngest),
    ('Minutes', PlayerMinuteIngest),
    ('FieldStats', FieldStatIngest),
    ('GkStats', GoalkeeperStatIngest),
    ('LeaguePoints', LeaguePointIngest)
]
