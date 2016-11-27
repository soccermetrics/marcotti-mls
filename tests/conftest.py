# coding: utf-8
# data fixtures for functional tests

from datetime import date

import pytest

from marcottimls.models import Countries, ConfederationType, NameOrderType, PositionType


@pytest.fixture
def comp_data():
    return {
        'domestic': {
            'name': u"Major League Soccer",
            'level': 1,
            'country': Countries(name=u"USA", confederation=ConfederationType.north_america)
        },
        'unicode': {
            'name': u'Süper Lig'
        },
        'international': {
            'name': u"FIFA Club World Cup",
            'level': 1,
            'confederation': ConfederationType.fifa
        },
        'overflow': {
            'name': u"leaguename" * 9,
            'level': 2
        }
    }


@pytest.fixture
def year_data():
    return {
        '90_to_94': (1990, 1994),
        '92_to_95': (1992, 1995),
        'year_94': 1994,
        'year_95': 1995,
        'year_05': 2005,
        'year_11': 2011
    }


@pytest.fixture
def season_data():
    return {
        'start_year': {
            'yr': 2012
        },
        'end_year': {
            'yr': 2013
        }
    }


@pytest.fixture
def country_data():
    return {
        'england': {
            'name': u'England',
            'confederation': ConfederationType.europe
        },
        'ivory_coast': {
            'name': u"Côte d'Ivoire",
            'confederation': ConfederationType.africa
        },
        'overflow': {
            'name': u"blahblahblah" * 8,
            'confederation': ConfederationType.north_america
        }
    }


@pytest.fixture
def person_data():
    return {
        'generic': {
            'first_name': u"James",
            'known_first_name': u"Jim",
            'last_name': u"Doe",
            'birth_date': date(1980, 1, 1),
            'country': Countries(name=u"Portlandia", confederation=ConfederationType.north_america)
        },
        'player': [
            {
                'first_name': u'Miguel',
                'middle_name': u'Ángel',
                'last_name': u'Ponce',
                'second_last_name': u'Briseño',
                'birth_date': date(1989, 4, 12),
                'country': Countries(name=u"Mexico", confederation=ConfederationType.north_america),
                'order': NameOrderType.middle
            },
            {
                'first_name': u"Cristiano",
                'middle_name': u"Ronaldo",
                'last_name': u"Aveiro",
                'second_last_name': u"dos Santos",
                'nick_name': u"Cristiano Ronaldo",
                'birth_date': date(1985, 2, 5),
                'country': Countries(name=u"Portugal", confederation=ConfederationType.europe),
                'order': NameOrderType.western
            },
            {
                'first_name': u'Heung-Min',
                'last_name': u'Son',
                'birth_date': date(1992, 7, 8),
                'country': Countries(name=u"Korea Republic", confederation=ConfederationType.asia),
                'order': NameOrderType.eastern
            }
        ],
    }


@pytest.fixture
def position_data():
    return {
        'doe': {
            'primary_position': PositionType.goalkeeper
        },
        'ponce': {
            'primary_position': PositionType.defender
        },
        'ronaldo': {
            'primary_position': PositionType.midfielder,
            'secondary_position': PositionType.forward
        },
        'son': {
            'primary_position': PositionType.forward
        }
    }


@pytest.fixture
def club_data():
    return {
        'orlando': {
            'name': u"Orlando City SC",
            'symbol': "ORL",
            'country': Countries(name=u"USA", confederation=ConfederationType.north_america)
        },
        'nyc': {
            'name': u"New York City FC",
            'symbol': "NYCFC",
            'country': Countries(name=u"USA", confederation=ConfederationType.north_america)
        }
    }
