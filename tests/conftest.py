import pytest

from database.database import Base, configure_database


@pytest.fixture(scope="session")
def database_engine():

    engine = configure_database()

    Base.metadata.create_all(bind=engine)

    return engine