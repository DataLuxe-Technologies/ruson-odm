from ruson.core.ruson_doc import RusonDoc


class UpdateDoc(RusonDoc):
    value: str
    update_value: str


async def test_update_one(setup_connection: None):
    for i in range(3):
        await UpdateDoc.insert_one(
            {"value": f"value{i}", "update_value": f"update_value{i}"}
        )

    result = await UpdateDoc(value="value0", update_value="new_value0").update(
        {"update_value": "update_value0"}, operator="$set"
    )
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await UpdateDoc.find_one({"value": "value0"})
    assert doc["value"] == "value0"
    assert doc["update_value"] == "new_value0"

    result = await UpdateDoc(value="value1", update_value="update_value1").update(
        {"$set": {"update_value": "new_value1"}}
    )
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await UpdateDoc.find_one({"value": "value1"})
    assert doc["value"] == "value1"
    assert doc["update_value"] == "new_value1"

    result = await UpdateDoc.update_one(
        {"$set": {"update_value": "new_value2"}}, {"update_value": "update_value2"}
    )
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    doc = await UpdateDoc.find_one({"value": "value2"})
    assert doc["value"] == "value2"
    assert doc["update_value"] == "new_value2"
