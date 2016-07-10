import pytest
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session

from local import LocalConfig
from models import BaseSchema


class TestConfig(LocalConfig):
    DBNAME = 'test-marcotti-db'


@pytest.fixture('session')
def config():
    return TestConfig()


@pytest.fixture(scope='session')
def db_connection(request, config):
    engine = create_engine(config.database_uri)
    connection = engine.connect()
    BaseSchema.metadata.create_all(connection)

    def fin():
        BaseSchema.metadata.drop_all(connection)
        connection.close()
        engine.dispose()
    request.addfinalizer(fin)
    return connection


@pytest.fixture()
def session(request, db_connection):
    __transaction = db_connection.begin_nested()
    session = Session(db_connection)

    def fin():
        session.close()
        __transaction.rollback()
    request.addfinalizer(fin)
    return session
