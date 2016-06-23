from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session

from models import BaseSchema


class Marcotti(object):

    def __init__(self, config):
        self.engine = create_engine(config.DATABASE_URI)
        self.connection = self.engine.connect()
        self.start_year = config.START_YEAR
        self.end_year = config.END_YEAR

    def create_db(self):
        """
        Create database tables from models defined in schema.
        """
        BaseSchema.metadata.create_all(self.connection)

    @contextmanager
    def create_session(self):
        """
        Create a session context that communicates with the database.

        Commits all changes to the database before closing the session, and if an exception is raised,
        rollback the session.
        """
        session = Session(self.connection)
        try:
            yield session
            session.commit()
        except Exception as ex:
            session.rollback()
            raise ex
        finally:
            session.close()


if __name__ == "__main__":
    from local import LocalConfig

    marcotti = Marcotti(LocalConfig())
    marcotti.create_db()
