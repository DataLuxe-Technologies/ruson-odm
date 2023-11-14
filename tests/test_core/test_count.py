from ruson.core.ruson_doc import RusonDoc


class CountDoc(RusonDoc):
    name: str


async def test_count(setup_connection: None):
    for i in range(2):
        await CountDoc(name=f"test-{i}").insert()

    count = await CountDoc.count_documents()
    assert count == 2

    filter = {"name": "test-0"}
    count = await CountDoc.count_documents(filter=filter)
    assert count == 1
