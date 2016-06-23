# coding=utf-8
from datetime import date

import pytest
from sqlalchemy.exc import DataError, IntegrityError

from models import *


def test_country_insert(session):
    """Country 001: Insert a single record into Countries table and verify data."""
    england = Countries(name=u'England', confederation=ConfederationType.europe)
    session.add(england)

    country = session.query(Countries).all()

    assert country[0].name == u'England'
    assert country[0].confederation.value == 'UEFA'
    assert repr(country[0]) == "<Country(id={0}, name=England, confed=UEFA)>".format(country[0].id)


def test_country_unicode_insert(session):
    """Country 002: Insert a single record with Unicode characters into Countries table and verify data."""
    ivory_coast = Countries(name=u"Côte d'Ivoire", confederation=ConfederationType.africa)
    session.add(ivory_coast)

    country = session.query(Countries).filter_by(confederation=ConfederationType.africa).one()

    assert country.name == u"Côte d'Ivoire"
    assert country.confederation.value == 'CAF'


def test_country_name_overflow_error(session):
    """Country 003: Verify error if country name exceeds field length."""
    too_long_name = "blahblah" * 8
    too_long_country = Countries(name=unicode(too_long_name), confederation=ConfederationType.north_america)
    with pytest.raises(DataError):
        session.add(too_long_country)
        session.commit()


def test_competition_insert(session):
    """Competition 001: Insert a single record into Competitions table and verify data."""
    record = Competitions(name=u"English Premier League", level=1)
    session.add(record)

    competition = session.query(Competitions).filter_by(level=1).one()

    assert competition.name == u"English Premier League"
    assert competition.level == 1


def test_competition_unicode_insert(session):
    """Competition 002: Insert a single record with Unicode characters into Competitions table and verify data."""
    record = Competitions(name=u"Süper Lig", level=1)
    session.add(record)

    competition = session.query(Competitions).one()

    assert competition.name == u"Süper Lig"


def test_competition_name_overflow_error(session):
    """Competition 003: Verify error if competition name exceeds field length."""
    too_long_name = "leaguename" * 9
    record = Competitions(name=unicode(too_long_name), level=2)
    with pytest.raises(DataError):
        session.add(record)
        session.commit()


def test_domestic_competition_insert(session):
    """Domestic Competition 001: Insert domestic competition record and verify data."""
    comp_name = u"Major League Soccer"
    comp_country = u"USA"
    comp_level = 1
    record = DomesticCompetitions(name=comp_name, level=comp_level, country=Countries(
        name=comp_country, confederation=ConfederationType.europe))
    session.add(record)

    competition = session.query(DomesticCompetitions).one()

    assert repr(competition) == "<DomesticCompetition(name={0}, country={1}, level={2})>".format(
        comp_name, comp_country, comp_level)
    assert competition.name == comp_name
    assert competition.level == comp_level
    assert competition.country.name == comp_country


def test_international_competition_insert(session):
    """International Competition 001: Insert international competition record and verify data."""
    comp_name = u"UEFA Champions League"
    comp_confed = ConfederationType.europe
    record = InternationalCompetitions(name=comp_name, level=1, confederation=comp_confed)
    session.add(record)

    competition = session.query(InternationalCompetitions).one()

    assert repr(competition) == "<InternationalCompetition(name={0}, confederation={1})>".format(
        comp_name, comp_confed.value
    )
    assert competition.level == 1


def test_year_insert(session):
    """Year 001: Insert multiple years into Years table and verify data."""
    years_list = range(1990, 1994)
    for yr in years_list:
        record = Years(yr=yr)
        session.add(record)

    years = session.query(Years.yr).all()
    years_from_db = [x[0] for x in years]

    assert set(years_from_db) & set(years_list) == set(years_list)


def test_year_duplicate_error(session):
    """Year 002: Verify error if year is inserted twice in Years table."""
    for yr in range(1992, 1995):
        record = Years(yr=yr)
        session.add(record)

    duplicate = Years(yr=1994)
    with pytest.raises(IntegrityError):
        session.add(duplicate)
        session.commit()


def test_season_insert(session):
    """Season 001: Insert records into Seasons table and verify data."""
    yr_1994 = Years(yr=1994)
    yr_1995 = Years(yr=1995)

    season_94 = Seasons(start_year=yr_1994, end_year=yr_1994)
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_94)
    session.add(season_9495)

    seasons_from_db = [repr(obj) for obj in session.query(Seasons).all()]
    seasons_test = ["<Season(1994)>", "<Season(1994-1995)>"]

    assert set(seasons_from_db) & set(seasons_test) == set(seasons_test)


def test_season_multiyr_search(session):
    """Season 002: Retrieve Season record using multi-year season name."""
    yr_1994 = Years(yr=1994)
    yr_1995 = Years(yr=1995)
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_9495)

    record = session.query(Seasons).filter(Seasons.name == '1994-1995').one()
    assert repr(season_9495) == repr(record)


def test_season_multiyr_reference_date(session):
    """Season 003: Verify that reference date for season across two years is June 30."""
    yr_1994 = Years(yr=1994)
    yr_1995 = Years(yr=1995)
    season_9495 = Seasons(start_year=yr_1994, end_year=yr_1995)
    session.add(season_9495)

    record = session.query(Seasons).filter(Seasons.start_year == yr_1994).one()
    assert record.reference_date == date(1995, 6, 30)


def test_season_singleyr_search(session):
    """Season 002: Retrieve Season record using multi-year season name."""
    yr_1994 = Years(yr=1994)
    season_94 = Seasons(start_year=yr_1994, end_year=yr_1994)
    session.add(season_94)

    record = session.query(Seasons).filter(Seasons.name == '1994').one()
    assert repr(season_94) == repr(record)


def test_season_singleyr_reference_date(session):
    """Season 005: Verify that reference date for season over one year is December 31."""
    yr_1994 = Years(yr=1994)
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
    assert repr(record) == u"<Person(name=John Doe, country=Portlandia, DOB=1980-01-01)>"


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


def test_person_missing_first_name_error(session, person_data):
    """Person 005: Verify error if first name is missing from Persons data."""
    generic_data_without_first = {key: value for key, value in person_data['generic'].items() if key != 'first_name'}
    generic_person = Persons(**generic_data_without_first)
    with pytest.raises(IntegrityError):
        session.add(generic_person)
        session.commit()


def test_person_missing_last_name_error(session, person_data):
    """Person 006: Verify error if last name is missing from Persons data."""
    generic_data_without_last = {key: value for key, value in person_data['generic'].items() if key != 'last_name'}
    generic_person = Persons(**generic_data_without_last)
    with pytest.raises(IntegrityError):
        session.add(generic_person)
        session.commit()


def test_person_missing_birth_date_error(session, person_data):
    """Person 007: Verify error if birth date is missing from Persons data."""
    generic_data_without_dob = {key: value for key, value in person_data['generic'].items() if key != 'birth_date'}
    generic_person = Persons(**generic_data_without_dob)
    with pytest.raises(IntegrityError):
        session.add(generic_person)
        session.commit()


def test_person_age_query(session, person_data):
    """Person 008: Verify record retrieved when Persons is queried for matching ages."""
    reference_date = date(2015, 7, 1)
    persons = [Persons(**data) for key, records in person_data.items()
               for data in records if key in ['player']]
    session.add_all(persons)
    records = session.query(Persons).filter(Persons.age(reference_date) == 22)

    assert records.count() == 1

    son_hm = records.all()[0]
    assert son_hm.age(reference_date) == 22
    assert son_hm.exact_age(reference_date) == (22, 358)
