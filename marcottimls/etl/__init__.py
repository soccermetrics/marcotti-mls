from base import BaseCSV, SeasonalDataIngest, get_local_handles, ingest_feeds, create_seasons
from overview import (ClubIngest, CountryIngest, CompetitionIngest, CompetitionSeasonIngest,
                      PlayerIngest, PersonIngest)
from financial import (AcquisitionIngest, PlayerSalaryIngest, PartialTenureIngest)
from statistics import (PlayerMinuteIngest, FieldStatIngest, GoalkeeperStatIngest, LeaguePointIngest)


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
