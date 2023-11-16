from ruson.core.ruson_doc import RusonDoc


class UpsertDoc(RusonDoc):
    value: str
    upsert_value: str


async def test_upsert_one(setup_connection: None):
    for i in range(3):
        await UpsertDoc.insert_one(
            {"value": f"value{i}", "upsert_value": f"upsert_value{i}"}
        )

    result = await UpsertDoc(value="value0", upsert_value="new_value0").upsert(
        {"upsert_value": "upsert_value0"}
    )
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await UpsertDoc.find_one({"value": "value0"})
    assert doc["value"] == "value0"
    assert doc["upsert_value"] == "new_value0"

    result = await UpsertDoc(value="value55", upsert_value="new_value55").upsert(
        {"upsert_value": "upsert_value55"}
    )
    assert result.matched_count == 0
    assert result.modified_count == 0
    assert result.upserted_id is not None

    doc = await UpsertDoc.find_one({"value": "value55"})
    assert str(doc["_id"]) == str(result.upserted_id)
    assert doc["value"] == "value55"
    assert doc["upsert_value"] == "new_value55"

    result = await UpsertDoc.upsert_one(
        {"$set": {"value": f"value99"}}, {"upsert_value": "upsert_value99"}
    )
    assert result.matched_count == 0
    assert result.modified_count == 0
    assert result.upserted_id is not None

    doc = await UpsertDoc.find_one({"value": "value99"})
    assert str(doc["_id"]) == str(result.upserted_id)
    assert doc["value"] == "value99"
    assert doc["upsert_value"] == "upsert_value99"
