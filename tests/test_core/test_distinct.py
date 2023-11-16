from ruson.core.ruson_doc import RusonDoc


class DistinctDoc(RusonDoc):
    equal: str
    different: str


async def test_distinct():
    for i in range(2):
        await DistinctDoc.insert_one({"equal": "equal", "different": f"value{i}"})

    result = await DistinctDoc.distinct(field_name="equal")
    assert len(result) == 1

    result = await DistinctDoc.distinct(field_name="different")
    assert len(result) == 2

    result = await DistinctDoc.distinct(
        field_name="different", filter={"different": "value0"}
    )
    assert len(result) == 1

    result = await DistinctDoc.distinct(field_name="different", db_name="empty")
    assert len(result) == 0
