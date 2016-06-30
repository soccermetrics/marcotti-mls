import os
import csv
import glob

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from models import Seasons, Years


class BaseIngest(object):

    def __init__(self, session):
        self.session = session

    def get_id(self, model, **conditions):

        try:
            record_id = self.session.query(model).filter_by(**conditions).one().id
        except NoResultFound as ex:
            print "{} has no records in Marcotti database for: {}".format(model.__name__, conditions)
            raise ex
        except MultipleResultsFound as ex:
            print "{} has multiple records in Marcotti database for: {}".format(model.__name__, conditions)
            raise ex
        return record_id

    def record_exists(self, model, **conditions):
        return self.session.query(model).filter_by(**conditions).count() != 0

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

    print "Creating Seasons..."

    year_range = range(start_yr, end_yr+1)

    for yr in year_range:
        if not exists(Years, yr=yr):
            session.add(Years(yr=yr))
    session.commit()
    session.flush()

    for start, end in zip(year_range[:-1], year_range[1:]):
        try:
            start_yr_obj = session.query(Years).filter_by(yr=start).one()
            end_yr_obj = session.query(Years).filter_by(yr=end).one()
        except (NoResultFound, MultipleResultsFound):
            continue
        # insert calendar year season record
        if not exists(Seasons, start_year=start_yr_obj, end_year=start_yr_obj):
            season_record = Seasons(start_year=start_yr_obj, end_year=start_yr_obj)
            session.add(season_record)
        # insert European season record
        if not exists(Seasons, start_year=start_yr_obj, end_year=end_yr_obj):
            season_record = Seasons(start_year=start_yr_obj, end_year=end_yr_obj)
            session.add(season_record)
    session.commit()
    print "Season creation complete."
