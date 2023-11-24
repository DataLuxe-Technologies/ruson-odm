import random

import pytest

from ruson.core.config import Config
from ruson.core.instance import Ruson
from ruson.driver.client import create_client
from ruson.driver.types import Document


@pytest.fixture(scope="session")
def db_uri() -> str:
    return "mongodb://localhost:27017/?retryWrites=false"


@pytest.fixture(scope="session")
def db_name() -> str:
    return "test_db"


@pytest.fixture(scope="session")
def collection_name():
    return "test_collection"


@pytest.fixture(scope="session")
def expected_dbs() -> list[str]:
    return ["admin"]


@pytest.fixture(scope="function", autouse=True)
async def setup_test_db(
    db_uri: str,
    db_name: str,
    collection_name: str,
):
    client = await create_client(db_uri)
    db = client.database(db_name)
    collection = db.collection(collection_name)
    await collection.drop()
    for i in range(10):
        doc = Document(name="test", index=i, random=random.randrange(0, 10))
        await collection.insert_one(doc)


@pytest.fixture(scope="function")
async def setup_connection(db_uri: str, db_name: str, expected_dbs: list[str]):
    config = Config(database_uri=db_uri, database_name=db_name)
    await Ruson.create_connection(config)
    client = Ruson.get_client()
    for db in await client.list_databases():
        if db not in expected_dbs:
            try:
                await client.database(db).drop()
            except:
                pass
