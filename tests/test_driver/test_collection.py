from ruson.driver.client import create_client
from ruson.driver.collection import Collection
from ruson.driver.types import Document


async def get_collection(db_uri: str, db_name: str, collection_name: str) -> Collection:
    client = await create_client(db_uri)
    db = client[db_name]
    return db[collection_name]


async def test_find_one(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, f"{collection_name}-empty")
    try:
        await collection.find_one(Document())
    except ValueError as e:
        assert str(e) == "Document not found"

    collection = await get_collection(db_uri, db_name, collection_name)
    doc = await collection.find_one(Document())
    assert doc["name"] == "test"

    filter = Document()
    filter["name"] = "test"
    doc = await collection.find_one(filter=filter)
    assert doc["name"] == "test"

    formatter = lambda x: x["name"]
    doc = await collection.find_one(filter=Document(), formatter=formatter)
    assert doc == "test"

    projection = Document()
    projection["_id"] = 0
    projection["name"] = 1
    doc = await collection.find_one(filter=Document(), projection=projection)
    assert "_id" not in doc
    assert "name" in doc
    assert doc["name"] == "test"

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    doc = await collection.find_one(filter=Document(), session=session)
    assert doc["name"] == "test"


async def test_find_many(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, f"{collection_name}-empty")

    docs = await (await collection.find_many()).tolist()
    assert len(docs) == 0

    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await (await collection.find_many()).tolist()
    assert len(docs) == 10

    filter = Document()
    filter["index"] = 2
    docs = await (await collection.find_many(filter=filter)).tolist()
    assert len(docs) == 1
    doc = docs[0]
    assert doc["index"] == 2

    formatter = lambda x: x["name"]
    docs = await collection.find_many(formatter=formatter)
    async for doc in docs:
        assert doc == "test"

    projection = Document()
    projection["_id"] = 0
    projection["name"] = 1
    docs = await collection.find_many(projection=projection)
    async for doc in docs:
        assert "_id" not in doc
        assert "name" in doc
        assert doc["name"] == "test"

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    docs = await collection.find_many(session=session)
    async for doc in docs:
        assert doc["name"] == "test"

    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await (await collection.find_many(skip=3)).tolist()
    assert len(docs) == 7

    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await (await collection.find_many(limit=3)).tolist()
    assert len(docs) == 3

    sort = Document()
    sort["random"] = 1
    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await collection.find_many(sort=sort)
    prev = None
    async for doc in docs:
        if prev is None:
            prev = doc["random"]
        else:
            assert prev <= doc["random"]
            prev = doc["random"]
