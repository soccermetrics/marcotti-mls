from base import BaseCSV, get_local_handles, ingest_feeds, create_seasons
from overview import (ClubIngest, CountryIngest, CompetitionIngest, PlayerIngest, PersonIngest)
from financial import (AcquisitionIngest, PlayerSalaryIngest, PartialTenureIngest)
from statistics import (FieldStatIngest, GoalkeeperStatIngest, LeaguePointIngest)


CSV_ETL_CLASSES = [
    ('Clubs', ClubIngest),
    ('Competitions', CompetitionIngest),
    ('Players', PlayerIngest),
    ('Acquisitions', AcquisitionIngest),
    ('Salaries', PlayerSalaryIngest),
    ('Partials', PartialTenureIngest),
    ('FieldStats', FieldStatIngest),
    ('GkStats', GoalkeeperStatIngest),
    ('LeaguePoints', LeaguePointIngest)
]
