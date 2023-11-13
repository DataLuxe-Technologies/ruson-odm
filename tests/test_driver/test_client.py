from ruson.driver.client import create_client


async def test_create_client(db_uri: str):
    try:
        await create_client(db_uri)
    except Exception as e:
        assert False, f"Failed to create client with exception: {e}"

    try:
        await create_client("")
        assert False, "Created client with empty uri"
    except:
        pass


async def test_list_databases(db_uri: str, expected_dbs: list[str]):
    client = await create_client(db_uri)
    dbs = await client.list_databases()

    for db in expected_dbs:
        if db not in dbs:
            assert False, f"Expected database {db} not found"


async def test_create_session(db_uri: str):
    client = await create_client(db_uri)
    try:
        await client.create_session()
    except Exception as e:
        assert False, f"Failed to create session with exception: {e}"


async def test_shutdown(db_uri: str):
    client = await create_client(db_uri)
    try:
        await client.shutdown()
    except Exception as e:
        assert False, f"Failed to shutdown client with exception: {e}"
