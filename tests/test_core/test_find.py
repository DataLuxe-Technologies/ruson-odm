from ruson.core.ruson_doc import RusonDoc
from ruson.driver.types import Direction, FieldSort


class FindDoc(RusonDoc):
    name: str


async def test_find_one(setup_connection: None):
    for i in range(10):
        await FindDoc(name=f"test-{i}").insert()

    try:
        await FindDoc.find_one(filter={"name": "test-99"})
        assert False, "Should not be able to find a document that does not exist."
    except ValueError:
        pass

    doc = await FindDoc(name="test-0").find()
    assert doc["name"] == "test-0"

    doc = await FindDoc.find_one(filter={"name": "test-0"})
    assert doc["name"] == "test-0"

    doc = await FindDoc.find_one(
        filter={},
        sort=[FieldSort(field="name", direction=Direction.DESCENDING)],
    )
    assert doc["name"] == "test-9"

    doc = await FindDoc.find_one(filter={}, formatter=lambda x: x["name"])
    assert doc == "test-0"

    try:
        await FindDoc.find_one(filter={}, db_name="another")
        assert False, "Should not be able to find a document in another database."
    except ValueError:
        pass


async def test_find_many(setup_connection: None):
    for i in range(10):
        await FindDoc(name=f"test-{i}").insert()

    docs = await (await FindDoc.find_many(filter={"name": "test-99"})).tolist()
    assert len(docs) == 0

    docs = await (
        await FindDoc.find_many(
            filter={"name": "test-0"},
            sort=[FieldSort(field="name", direction=Direction.DESCENDING)],
            skip=5,
        )
    ).tolist()
    assert len(docs) == 0

    docs = await (await FindDoc(name="test-0").find(many=True)).tolist()
    assert len(docs) == 1, str(docs[1]["name"])
    assert docs[0]["name"] == "test-0"

    docs = await (await FindDoc.find_many(filter={"name": "test-0"})).tolist()
    assert len(docs) == 1
    assert docs[0]["name"] == "test-0"

    docs = await FindDoc.find_many(
        sort=[FieldSort(field="name", direction=Direction.DESCENDING)],
    )
    idx = 10
    async for doc in docs:
        idx -= 1
        assert doc["name"] == f"test-{idx}"
    assert idx == 0

    docs = await FindDoc.find_many(
        formatter=lambda x: x["name"],
        sort=[FieldSort(field="name", direction=Direction.DESCENDING)],
    )
    idx = 10
    async for doc in docs:
        idx -= 1
        assert doc == f"test-{idx}"
    assert idx == 0

    docs = await (await FindDoc.find_many(filter={}, db_name="another")).tolist()
    assert len(docs) == 0
