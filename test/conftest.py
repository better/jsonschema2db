import datetime as dt

import psycopg2
import pytest
from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer

now = dt.datetime.now()


@pytest.fixture
def datetime_now():
    return now


@pytest.fixture
def connection(scope="session"):
    with PostgresContainer("postgres:11.4") as postgres:
        # yield psycopg2.connect(postgres.get_connection_url())
        engine = create_engine(postgres.get_connection_url())
        yield engine.raw_connection()
