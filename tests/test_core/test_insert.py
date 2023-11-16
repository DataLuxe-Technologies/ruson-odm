from ruson.core.ruson_doc import RusonDoc


class InsertDoc(RusonDoc):
    value: str


async def test_insert_one(setup_connection: None):
    user = InsertDoc(value="value1")
    result = await InsertDoc.insert_one(user)
    doc = await InsertDoc.find_one(filter=user)
    assert str(doc["_id"]) == str(result.inserted_id)
    assert doc["value"] == user.value

    another = InsertDoc(value="value2")
    result = await another.insert()
    doc = await InsertDoc.find_one(filter=another)
    assert str(doc["_id"]) == str(result.inserted_id)
    assert doc["value"] == another.value


async def test_insert_many(setup_connection: None):
    users = [
        InsertDoc(value="value1"),
        InsertDoc(value="value2"),
        InsertDoc(value="value3"),
        InsertDoc(value="value4"),
    ]
    result = await InsertDoc.insert_many(users)
    ids = list(map(str, result.inserted_ids))

    docs = await InsertDoc.find_many()
    async for doc in docs:
        id = str(doc["_id"])
        assert ids.remove(id) is None

    assert ids == []
