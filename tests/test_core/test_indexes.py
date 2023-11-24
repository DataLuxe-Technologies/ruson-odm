from ruson.core.ruson_doc import RusonDoc
from ruson.driver.types import Document, IndexModel, IndexOptions


class IndexesDoc(RusonDoc):
    another: str
    even_another: str

    @classmethod
    def class_indexes(cls) -> list[IndexModel]:
        return [IndexModel(keys={"another": 1}, options=IndexOptions(unique=True))]


async def test_create_indexes(setup_connection: None):
    result = await IndexesDoc.create_indexes()
    names = result.index_names
    assert len(names) == 1
    assert names[0] == "another_1"


async def test_create_index(setup_connection: None):
    index = IndexModel(keys={"even_another": 1}, options=IndexOptions(unique=True))

    result = await IndexesDoc.create_index(index)
    names = result.index_names
    assert len(names) == 1
    assert names[0] == "even_another_1"


async def test_list_indexes(setup_connection: None):
    await IndexesDoc.create_indexes()
    indexes = await IndexesDoc.list_indexes()
    expected = ["another_1", "_id_"]
    async for index in indexes:
        assert index.options.name in expected


async def test_drop_indexes(setup_connection: None):
    await IndexesDoc.create_indexes()
    await IndexesDoc.drop_indexes()
    indexes = await IndexesDoc.list_indexes()
    expected = ["_id_"]
    async for index in indexes:
        assert index.options.name in expected
