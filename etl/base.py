import os
import csv
import glob
import logging

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from models import Seasons, Years, Competitions, Players


logger = logging.getLogger(__name__)


class BaseIngest(object):

    def __init__(self, session):
        self.session = session

    def get_id(self, model, **conditions):
        """
        Retrieve unique ID of record in database model that satisfies conditions.

        If no unique record exists, communicate error in log file and return None.

        :param model: Marcotti-MLS data model.
        :param conditions: Dictionary of fields/values that describe a record in model.
        :return: Unique ID of database record.
        """
        record_id = None
        try:
            record_id = self.session.query(model).filter_by(**conditions).one().id
        except NoResultFound:
            logger.error("{} has no records in Marcotti database for: {}".format(model.__name__, conditions))
        except MultipleResultsFound:
            logger.error("{} has multiple records in Marcotti database for: {}".format(model.__name__, conditions))
        return record_id

    def record_exists(self, model, **conditions):
        """
        Check for existence of specific record in database.

        :param model: Marcotti-MLS data model.
        :param conditions: Dictionary of fields/values that describe a record in model.
        :return: Boolean value for existence of record in database.
        """
        return self.session.query(model).filter_by(**conditions).count() != 0

    def bulk_insert(self, record_list, threshold):
        """
        Add list of data models to database transaction if enough models are present.

        After bulk insertion, list is reset to empty.

        :param record_list: List of SQLAlchemy objects
        :param threshold: Number of objects in list required to bulk insertions
        :return: tuple of (number of records inserted, list of objects)
        """
        if len(record_list) != threshold:
            inserted = 0
        else:
            self.session.add_all(record_list)
            self.session.commit()
            record_list = []
            inserted = threshold
        return inserted, record_list

    @staticmethod
    def prepare_db_dict(fields, values):
        """
        Create dictionary of columns for insertion/query operations on database model.

        Only return fields whose values exists and are nonnegative.

        :param fields: Field names of database model.
        :param values: Values that correspond to model fields.
        :return: Dictionary of statistical field/value pairs.
        """
        return {field: value for (field, value) in zip(fields, values) if value is not None and value >= 0}

    def load_feed(self, handle):
        raise NotImplementedError


class BaseCSV(BaseIngest):

    def load_feed(self, handle):
        rows = csv.DictReader(handle)
        self.parse_file(rows)

    @staticmethod
    def column(field, **kwargs):
        try:
            value = kwargs[field].strip()
            return value if value != "" else None
        except (AttributeError, KeyError, TypeError) as ex:
            raise ex

    def column_unicode(self, field, **kwargs):
        try:
            return self.column(field, **kwargs).decode('utf-8')
        except (KeyError, AttributeError):
            return None

    def column_int(self, field, **kwargs):
        try:
            return int(self.column(field, **kwargs))
        except (KeyError, TypeError):
            return None

    def column_bool(self, field, **kwargs):
        try:
            return bool(self.column_int(field, **kwargs))
        except (KeyError, TypeError):
            return None

    def column_float(self, field, **kwargs):
        try:
            return float(self.column(field, **kwargs))
        except (KeyError, TypeError):
            return None

    def parse_file(self, rows):
        raise NotImplementedError


class SeasonalDataIngest(BaseCSV):
    """
    Ingestion methods for competition- and season-specific data.
    """

    def __init__(self, session, competition, season):
        super(SeasonalDataIngest, self).__init__(session)
        self.competition_id = self.get_id(Competitions, name=competition)
        self.season_id = self.get_id(Seasons, name=season)

    def get_player_from_name(self, first_name, last_name):
        """
        Retrieve player ID associated with player's full name.

        To avoid ambiguity, some last names include the player's birthdate separated by ':'.
        In this situation, the player is searched by full name and birthdate.

        :param first_name: First name of player (or last name in case of Eastern word order)
        :param last_name: Last name of player that makes up full name.  Can include birthdate separated by ':'.
        :return: Unique ID of player.
        """
        if ':' in last_name:
            last_name_text, birth_date = last_name.split(':')
            full_name = " ".join([first_name, last_name_text]) if first_name else last_name_text
            player_id = self.get_id(Players, full_name=full_name, birth_date=birth_date)
        else:
            full_name = " ".join([first_name, last_name]) if first_name else last_name
            player_id = self.get_id(Players, full_name=full_name)
        return player_id

    def parse_file(self, rows):
        return NotImplementedError


def ingest_feeds(handle_iterator, prefix, pattern, feed_class):
    """Ingest contents of XML files of a common type, as
    described by a common filename pattern.

    :param handle_iterator: A sequence of file handles of XML files.
    :type handle_iterator: return value of generator
    :param prefix: File path, which is also defined as the prefix of the filename.
    :type prefix: string
    :param pattern: Text pattern common to group of XML files.
    :type pattern: string
    :param feed_class: Data feed interface class.
    :type feed_class: class
    """
    for handle in handle_iterator(prefix, pattern):
        feed_class.load_feed(handle)


def get_local_handles(prefix, pattern):
    """Generates a sequence of file handles for XML files of a common type
    that are hosted on a local machine.

    :param prefix: File path, which is also defined as the prefix of the filename.
    :type prefix: string
    :param pattern: Text pattern common to group of files.
    :type pattern: string
    """
    glob_pattern = os.path.join(prefix, "{}".format(pattern))
    for filename in glob.glob(glob_pattern):
        with open(filename) as fh:
            yield fh


def create_seasons(session, start_yr, end_yr):
    """
    Adds Years and calendar and European Seasons records to database.

    :param session: Transaction session object.
    :param start_yr: Start of year interval.
    :param end_yr: End of year interval, inclusive.
    """
    def exists(model, **conditions):
        return session.query(model).filter_by(**conditions).count() != 0

    logger.info("Creating Years between {0} and {1}...".format(start_yr, end_yr))

    year_range = range(start_yr, end_yr+1)
    for yr in year_range:
        if not exists(Years, yr=yr):
            session.add(Years(yr=yr))
    session.commit()
    session.flush()

    logger.info("Creating Seasons...")

    # insert calendar year season record
    for year in year_range:
        try:
            yr_obj = session.query(Years).filter_by(yr=year).one()
        except NoResultFound:
            logger.error("Cannot insert Season record: {} not in database".format(year))
            continue
        except MultipleResultsFound:
            logger.error("Cannot insert Season record: multiple {} records in database".format(year))
            continue
        if not exists(Seasons, start_year=yr_obj, end_year=yr_obj):
            logger.info("Creating record for {0} season".format(yr_obj.yr))
            season_record = Seasons(start_year=yr_obj, end_year=yr_obj)
            session.add(season_record)

    # insert European season record
    for start, end in zip(year_range[:-1], year_range[1:]):
        try:
            start_yr_obj = session.query(Years).filter_by(yr=start).one()
            end_yr_obj = session.query(Years).filter_by(yr=end).one()
        except NoResultFound:
            logger.error("Cannot insert Season record: {} or {} not in database".format(start, end))
            continue
        except MultipleResultsFound:
            logger.error("Cannot insert Season record: multiple {} or {} records in database".format(start, end))
            continue
        if not exists(Seasons, start_year=start_yr_obj, end_year=end_yr_obj):
            logger.info("Creating record for {0}-{1} season".format(start_yr_obj.yr, end_yr_obj.yr))
            season_record = Seasons(start_year=start_yr_obj, end_year=end_yr_obj)
            session.add(season_record)
    session.commit()
    logger.info("Season records committed to database")
    logger.info("Season creation complete.")
