# coding=utf-8
from datetime import date

import pytest
from sqlalchemy.exc import DataError, IntegrityError

from marcottimls.models import *


def test_country_insert(session, country_data):
    """Country 001: Insert a single record into Countries table and verify data."""
    england = Countries(**country_data['england'])
    session.add(england)

    country = session.query(Countries).all()

    assert country[0].name == country_data['england']['name']
    assert country[0].confederation.value == 'UEFA'
    assert repr(country[0]) == "<Country(id={0}, name={1}, confed=UEFA)>".format(country[0].id,
                                                                                 country_data['england']['name'])


def test_country_unicode_insert(session, country_data):
    """Country 002: Insert a single record with Unicode characters into Countries table and verify data."""
    ivory_coast = Countries(**country_data['ivory_coast'])
    session.add(ivory_coast)

    country = session.query(Countries).filter_by(confederation=ConfederationType.africa).one()

    assert country.name == country_data['ivory_coast']['name']
    assert country.confederation.value == 'CAF'


def test_country_name_overflow_error(session, country_data):
    """Country 003: Verify error if country name exceeds field length."""
    too_long_country = Countries(**country_data['overflow'])
    with pytest.raises(DataError):
        session.add(too_long_country)
        session.commit()


def test_competition_insert(session, comp_data):
    """Competition 001: Insert a single record into Competitions table and verify data."""
    domestic_without_country = {key: value for key, value in comp_data['domestic'].items() if key != 'country'}
    record = Competitions(**domestic_without_country)
    session.add(record)

    competition = session.query(Competitions).filter_by(level=1).one()

    assert competition.name == domestic_without_country['name']
    assert competition.level == domestic_without_country['level']


def test_competition_unicode_insert(session, comp_data):
    """Competition 002: Insert a single record with Unicode characters into Competitions table and verify data."""
    record = Competitions(**comp_data['unicode'])
    session.add(record)

    competition = session.query(Competitions).one()

    assert competition.name == comp_data['unicode']['name']


def test_competition_name_overflow_error(session, comp_data):
    """Competition 003: Verify error if competition name exceeds field length."""
    overflow_record = Competitions(**comp_data['overflow'])
    with pytest.raises(DataError):
        session.add(overflow_record)
        session.commit()


def test_domestic_competition_insert(session, comp_data):
    """Domestic Competition 001: Insert domestic competition record and verify data."""
    record = DomesticCompetitions(**comp_data['domestic'])
    session.add(record)

    competition = session.query(DomesticCompetitions).one()

    assert repr(competition) == "<DomesticCompetition(name={0}, country={1}, level={2})>".format(
        comp_data['domestic']['name'], comp_data['domestic']['country'].name, comp_data['domestic']['level'])
    assert competition.name == comp_data['domestic']['name']
    assert competition.level == comp_data['domestic']['level']
    assert competition.country.name == comp_data['domestic']['country'].name


def test_international_competition_insert(session, comp_data):
    """International Competition 001: Insert international competition record and verify data."""
    record = InternationalCompetitions(**comp_data['international'])
    session.add(record)

    competition = session.query(InternationalCompetitions).one()

    assert repr(competition) == "<InternationalCompetition(name={0}, confederation={1})>".format(
        comp_data['international']['name'], comp_data['international']['confederation'].value
    )
    assert competition.level == comp_data['international']['level']


def test_year_insert(session, year_data):
    """Year 001: Insert multiple years into Years table and verify data."""
    years_list = range(*year_data['90_to_94'])
    for yr in years_list:
        record = Years(yr=yr)
        session.add(record)

    years = session.query(Years.yr).all()
    years_from_db = [x[0] for x in years]

    assert set(years_from_db) & set(years_list) == set(years_list)


def test_year_duplicate_error(session, year_data):
    """Year 002: Verify error if year is inserted twice in Years table."""
    for yr in range(*year_data['92_to_95']):
        record = Years(yr=yr)
        session.add(record)

    duplicate = Years(yr=year_data['year_94'])
    with pytest.raises(IntegrityError):
        session.add(duplicate)
        session.commit()


def test_season_insert(session, year_data):
    """Season 001: Insert records into Seasons table and verify data."""
    yr_1994 = Years(yr=year_data['year_94'])
    yr_1995 = Years(yr=year_data['year_95'])

    season_94 = Seasons(start_year=yr_1994, end_year=yr_1994)
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_94)
    session.add(season_9495)

    seasons_from_db = [repr(obj) for obj in session.query(Seasons).all()]
    seasons_test = ["<Season({})>".format(year_data['year_94']),
                    "<Season({}-{})>".format(year_data['year_94'], year_data['year_95'])]

    assert set(seasons_from_db) & set(seasons_test) == set(seasons_test)


def test_season_multiyr_search(session, year_data):
    """Season 002: Retrieve Season record using multi-year season name."""
    yr_1994 = Years(yr=year_data['year_94'])
    yr_1995 = Years(yr=year_data['year_95'])
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_9495)

    record = session.query(Seasons).filter(Seasons.name == '{}-{}'.format(
        year_data['year_94'], year_data['year_95'])).one()
    assert repr(season_9495) == repr(record)


def test_season_multiyr_reference_date(session, year_data):
    """Season 003: Verify that reference date for season across two years is June 30."""
    yr_1994 = Years(yr=year_data['year_94'])
    yr_1995 = Years(yr=year_data['year_95'])
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_9495)

    record = session.query(Seasons).filter(Seasons.start_year == yr_1994).one()
    assert record.reference_date == date(1995, 6, 30)


def test_season_singleyr_search(session, year_data):
    """Season 002: Retrieve Season record using multi-year season name."""
    yr_1994 = Years(yr=year_data['year_94'])
    season_94 = Seasons(start_year=yr_1994, end_year=yr_1994)
    session.add(season_94)

    record = session.query(Seasons).filter(Seasons.name == '{}'.format(year_data['year_94'])).one()
    assert repr(season_94) == repr(record)


def test_season_singleyr_reference_date(session, year_data):
    """Season 005: Verify that reference date for season over one year is December 31."""
    yr_1994 = Years(yr=year_data['year_94'])
    season_94 = Seasons(start_year=yr_1994, end_year=yr_1994)
    session.add(season_94)

    record = session.query(Seasons).filter(Seasons.start_year == yr_1994).one()
    assert record.reference_date == date(1994, 12, 31)


def test_person_generic_insert(session, person_data):
    """Person 001: Insert generic personnel data into Persons model and verify data."""
    generic_person = Persons(**person_data['generic'])
    session.add(generic_person)
    record = session.query(Persons).one()
    assert record.full_name == u"Jim Doe"
    assert record.official_name == u"James Doe"
    assert record.age(date(2010, 1, 1)) == 30
    assert record.exact_age(date(2010, 3, 15)) == (30, 74)
    assert repr(record) == u"<Person(name=Jim Doe, country=Portlandia, DOB=1980-01-01)>"


def test_person_middle_naming_order(session, person_data):
    """Person 002: Return correct format of Person's name with Western Middle naming order."""
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player']]
    session.add_all(persons)

    person_from_db = session.query(Persons).filter(Persons.order == NameOrderType.middle).one()
    assert person_from_db.full_name == u"Miguel Ángel Ponce"
    assert person_from_db.official_name == u"Miguel Ángel Ponce Briseño"


def test_person_eastern_naming_order(session, person_data):
    """Person 003: Return correct format of Person's name with Eastern naming order."""
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player']]
    session.add_all(persons)

    person_from_db = session.query(Persons).filter(Persons.order == NameOrderType.eastern).one()
    assert person_from_db.full_name == u"Son Heung-Min"
    assert person_from_db.full_name == person_from_db.official_name


def test_person_nickname(session, person_data):
    """Person 004: Return correct name of Person with nickname."""
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player'] and 'nick_name' in data]
    session.add_all(persons)

    person_from_db = session.query(Persons).one()

    assert person_from_db.full_name == u"Cristiano Ronaldo"
    assert person_from_db.official_name == u"Cristiano Ronaldo Aveiro dos Santos"


def test_person_full_name_query(session, person_data):
    """Person 005: Query Persons data model on full name and verify results."""
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player']]
    session.add_all(persons)
    generic_person = Persons(**person_data['generic'])
    session.add(generic_person)
    session.flush()

    jim_doe_in_db = session.query(Persons).filter(Persons.full_name == u"Jim Doe")
    assert jim_doe_in_db.count() == 1
    assert jim_doe_in_db[0].full_name == u"Jim Doe"

    ronaldo_in_db = session.query(Persons).filter(Persons.full_name == u"Cristiano Ronaldo")
    assert ronaldo_in_db.count() == 1
    assert ronaldo_in_db[0].full_name == u"Cristiano Ronaldo"


def test_person_age_query(session, person_data):
    """Person 006: Verify record retrieved when Persons is queried for matching ages."""
    reference_date = date(2015, 7, 1)
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player']]
    session.add_all(persons)
    records = session.query(Persons).filter(Persons.age(reference_date) == 22)

    assert records.count() == 1

    son_hm = records.all()[0]
    assert son_hm.age(reference_date) == 22
    assert son_hm.exact_age(reference_date) == (22, 358)


def test_player_insert(session):
    raise NotImplementedError


def test_player_multiple_positions(session):
    raise NotImplementedError
