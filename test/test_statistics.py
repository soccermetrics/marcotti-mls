# coding=utf-8
import pytest
from sqlalchemy.exc import DataError, IntegrityError

from models import *


def test_common_stats_insert(session):
    raise NotImplementedError


def test_duplicate_common_stats_error(session):
    raise NotImplementedError


def test_common_stats_default_values(session):
    raise NotImplementedError


def test_common_stats_negative_value_error(session):
    raise NotImplementedError


def test_field_stats_default_values(session):
    raise NotImplementedError


def test_field_stats_negative_value_error(session):
    raise NotImplementedError


def test_goalkeeper_stats_default_values(session):
    raise NotImplementedError


def test_goalkeeper_stats_negative_value_error(session):
    raise NotImplementedError


def test_field_and_goalkeeper_stats_insert(session):
    raise NotImplementedError
