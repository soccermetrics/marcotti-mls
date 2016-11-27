import re
import sys
import logging
from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session

from .version import __version__
from etl import create_seasons, ingest_feeds, get_local_handles, CountryIngest
from models import BaseSchema


logger = logging.getLogger(__name__)


class Marcotti(object):

    def __init__(self, config):
        logger.info("Marcotti-MLS v{0}: Python {1} on {2}".format(
            '.'.join(__version__), sys.version, sys.platform))
        logger.info("Opened connection to {0}".format(self._public_db_uri(config.database_uri)))
        self.engine = create_engine(config.database_uri)
        self.connection = self.engine.connect()
        self.start_year = config.START_YEAR
        self.end_year = config.END_YEAR

    @staticmethod
    def _public_db_uri(uri):
        """
        Strip out database username/password from database URI.

        :param uri: Database URI string.
        :return: Database URI with username/password removed.
        """
        return re.sub(r"//.*@", "//", uri)

    def create_db(self):
        """
        Create database tables from models defined in schema and populate validation tables.
        """
        logger.info("Creating data models...")
        BaseSchema.metadata.create_all(self.connection)
        with self.create_session() as sess:
            create_seasons(sess, self.start_year, self.end_year)
            ingest_feeds(get_local_handles, 'data', 'countries.csv', CountryIngest(sess))

    @contextmanager
    def create_session(self):
        """
        Create a session context that communicates with the database.

        Commits all changes to the database before closing the session, and if an exception is raised,
        rollback the session.
        """
        session = Session(self.connection)
        logger.info("Create session {0} with {1}".format(
            id(session), self._public_db_uri(str(self.engine.url))))
        try:
            yield session
            session.commit()
            logger.info("Commit transactions to database")
        except Exception:
            session.rollback()
            logger.exception("Database transactions rolled back")
        finally:
            logger.info("Session {0} with {1} closed".format(
                id(session), self._public_db_uri(str(self.engine.url))))
            session.close()
