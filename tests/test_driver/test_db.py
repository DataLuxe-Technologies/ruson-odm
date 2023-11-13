from ruson.driver.client import create_client


async def test_list_collections(
    db_uri: str,
    db_name: str,
    collection_name: str,
):
    client = await create_client(db_uri)
    db = client.database(db_name)
    collections = await db.list_collections()

    if collection_name not in collections:
        assert False, f"Excpected collection {collection_name} not found"


async def test_drop(db_uri: str, db_name: str):
    client = await create_client(db_uri)
    db = client.database(db_name)
    try:
        await db.drop()
    except Exception as e:
        assert False, f"Failed to drop database with exception: {e}"
