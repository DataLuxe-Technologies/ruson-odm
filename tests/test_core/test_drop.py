from ruson.core.ruson_doc import RusonDoc


class DropDoc(RusonDoc):
    pass


async def test_drop(setup_connection: None):
    users = [DropDoc(), DropDoc(), DropDoc(), DropDoc()]
    await DropDoc.insert_many(users)

    docs = await (await DropDoc.find_many()).tolist()
    assert len(docs) == 4

    await DropDoc.drop()

    docs = await (await DropDoc.find_many()).tolist()
    assert len(docs) == 0
