import random

import pytest

from ruson.driver.client import create_client
from ruson.driver.types import Document


@pytest.fixture(scope="session")
def db_uri() -> str:
    return "mongodb://localhost:27017"


@pytest.fixture(scope="session")
def db_name() -> str:
    return "test_db"


@pytest.fixture(scope="session")
def collection_name():
    return "test_collection"


@pytest.fixture(scope="session")
def expected_dbs() -> list[str]:
    return ["admin", "config", "local"]


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
        doc = Document()
        doc["name"] = "test"
        doc["index"] = i
        doc["random"] = random.randrange(0, 10)
        await collection.insert_one(doc)
