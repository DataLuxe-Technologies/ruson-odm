from ruson.driver.client import create_client
from ruson.driver.collection import Collection
from ruson.driver.types import Document, IndexModel, IndexOptions


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

    filter = Document({"name": "test"})
    doc = await collection.find_one(filter=filter)
    assert doc["name"] == "test"

    formatter = lambda x: x["name"]
    doc = await collection.find_one(filter=Document(), formatter=formatter)
    assert doc == "test"

    projection = Document({"_id": 0, "name": 1})
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

    filter = Document({"index": 2})
    docs = await (await collection.find_many(filter=filter)).tolist()
    assert len(docs) == 1
    doc = docs[0]
    assert doc["index"] == 2

    formatter = lambda x: x["name"]
    docs = await collection.find_many(formatter=formatter)
    async for doc in docs:
        assert doc == "test"

    projection = Document({"_id": 0, "name": 1})
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

    sort = Document({"random": 1})
    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await collection.find_many(sort=sort)
    prev = None
    async for doc in docs:
        if prev is None:
            prev = doc["random"]
        else:
            assert prev <= doc["random"]
            prev = doc["random"]


async def test_insert_one(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    doc = Document({"name": "insert-one"})

    result = await collection.insert_one(doc)
    assert result.inserted_id

    found_doc = await collection.find_one(doc)
    assert str(found_doc["_id"]) == str(result.inserted_id)
    assert found_doc["name"] == doc["name"]

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    doc = Document({"name": "insert-one-with-session"})

    result = await collection.insert_one(doc, session=session)
    assert result.inserted_id

    found_doc = await collection.find_one(doc)
    assert str(found_doc["_id"]) == str(result.inserted_id)
    assert found_doc["name"] == doc["name"]


async def test_insert_many(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    docs = []
    for i in range(3):
        doc = Document({"name": f"insert-many-{i}"})
        docs.append(doc)

    result = await collection.insert_many(docs)
    assert result.inserted_ids

    str_inserted_ids = list(map(str, result.inserted_ids))

    filter = Document({"name": Document({"$regex": "insert-many-*"})})
    found_docs = await collection.find_many(filter=filter)
    async for found_doc in found_docs:
        assert str(found_doc["_id"]) in str_inserted_ids

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    docs = []
    for i in range(3):
        doc = Document({"name": f"insert-with-session-many-{i}"})
        docs.append(doc)

    result = await collection.insert_many(docs, session=session)
    assert result.inserted_ids

    str_inserted_ids = list(map(str, result.inserted_ids))

    filter = Document({"name": Document({"$regex": "insert-with-session-many-*"})})
    found_docs = await collection.find_many(filter=filter)
    async for found_doc in found_docs:
        assert str(found_doc["_id"]) in str_inserted_ids


async def test_update_one(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    update = Document({"$set": Document({"name": "update-one-test"})})

    filter = Document({"index": 1})
    result = await collection.update_one(update=update, filter=filter)
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await collection.find_one(filter=filter)
    assert doc["name"] == update["$set"]["name"]

    collection = await get_collection(db_uri, db_name, collection_name)

    update = Document({"$set": Document({"name": "upsert-one-test"})})
    filter = Document({"index": 90})
    result = await collection.update_one(update=update, filter=filter, upsert=True)
    assert result.matched_count == 0
    assert result.modified_count == 0
    assert result.upserted_id is not None

    doc = await collection.find_one(filter=filter)
    assert doc["name"] == update["$set"]["name"]

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    update = Document({"$set": Document({"name": "update-one-session-test"})})
    filter = Document({"index": 1})
    result = await collection.update_one(update=update, filter=filter, session=session)
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await collection.find_one(filter=filter)
    assert doc["name"] == update["$set"]["name"]


async def test_delete_one(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await (await collection.find_many()).tolist()
    prev_count = len(docs)
    result = await collection.delete_one(Document())
    assert result.deleted_count == 1
    docs = await (await collection.find_many()).tolist()
    new_count = len(docs)
    assert prev_count > new_count

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    docs = await (await collection.find_many()).tolist()
    prev_count = len(docs)
    result = await collection.delete_one(Document(), session=session)
    assert result.deleted_count == 1
    docs = await (await collection.find_many()).tolist()
    new_count = len(docs)
    assert prev_count > new_count


async def test_delete_many(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    filter = Document({"index": Document({"$gte": 2})})
    result = await collection.delete_many(filter=filter)

    assert result.deleted_count == 8
    docs = await (await collection.find_many()).tolist()
    new_count = len(docs)
    assert new_count == 2

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]

    filter = Document({"index": Document({"$lte": 2})})
    result = await collection.delete_many(filter=filter, session=session)
    assert result.deleted_count == 2
    docs = await (await collection.find_many()).tolist()
    new_count = len(docs)
    assert new_count == 0


async def test_distinct(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    result = await collection.distinct(field_name="random")
    assert len(result) < 10

    collection = await get_collection(db_uri, db_name, collection_name)
    filter = Document({"index": Document({"$lt": 2})})
    result = await collection.distinct(field_name="index", filter=filter)
    assert len(result) == 2
    expected_indexes = [0, 1]
    for f in result:
        assert f in expected_indexes

    client = await create_client(db_uri)
    session = await client.create_session()
    collection = client[db_name][collection_name]
    result = await collection.distinct(field_name="random", session=session)
    assert len(result) < 10


async def test_create_indexes(db_uri: str, db_name: str, collection_name: str):
    index_name = "test-create-indexes"

    collection = await get_collection(db_uri, db_name, collection_name)
    index_options = IndexOptions(name=index_name, unique=True)
    index = IndexModel(keys={"index": 1}, options=index_options)
    result = await collection.create_indexes(indexes=[index])
    assert len(result.index_names) == 1
    assert result.index_names[0] in [index_name]

    indexes = await collection.list_indexes()
    async for index in indexes:
        if index.options.name == index_name:
            return
    assert False, f"Index {index_name} not found."


async def test_list_indexes(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    indexes = await (await collection.list_indexes()).tolist()
    assert len(indexes) == 1
    assert indexes[0].options.name == "_id_"


async def test_drop_indexes(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    index_options = IndexOptions(name="test-drop-indexes", unique=True)
    index = IndexModel(keys={"index": 1}, options=index_options)
    await collection.create_indexes(indexes=[index])
    indexes = await (await collection.list_indexes()).tolist()
    assert len(indexes) == 2

    await collection.drop_indexes()  # Cannot pass indexes to servers without replica sets

    collection = await get_collection(db_uri, db_name, collection_name)
    indexes = await (await collection.list_indexes()).tolist()
    assert len(indexes) == 1  # _id cannot be dropped


async def test_count_documents(db_uri: str, db_name: str, collection_name: str):
    collection = await get_collection(db_uri, db_name, collection_name)
    docs = await collection.count_documents()
    assert docs == 10

    filter = Document()
    filter["index"] = 0
    docs = await collection.count_documents(filter=filter)
    assert docs == 1
