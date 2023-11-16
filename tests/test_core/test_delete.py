from ruson.core.ruson_doc import RusonDoc


class DeleteDoc(RusonDoc):
    value: str


async def test_delete_one():
    for i in range(2):
        await DeleteDoc.insert_one({"value": f"value{i}"})

    result = await DeleteDoc(value="value0").delete()
    assert result.deleted_count == 1
    try:
        await DeleteDoc.find_one({"value": "value0"})
        assert False, "Deleted value should not be found."
    except:
        pass

    result = await DeleteDoc.delete_one({"value": "value1"})
    assert result.deleted_count == 1
    try:
        await DeleteDoc.find_one({"value": "value1"})
        assert False, "Deleted value should not be found."
    except:
        pass


async def test_delete_many():
    for i in range(5):
        await DeleteDoc.insert_one({"value": f"value{i}"})

    for i in range(5):
        await DeleteDoc.insert_one({"value": f"other{i}"})

    result = await DeleteDoc.delete_many({"value": {"$regex": "value.*"}})
    assert result.deleted_count == 5

    result = await DeleteDoc.find_many(filter={"value": {"$regex": "value.*"}})
    docs = await result.tolist()
    assert len(docs) == 0, "Deleted values should not be found."
