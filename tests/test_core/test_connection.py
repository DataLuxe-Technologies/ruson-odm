from ruson.core.config import Config
from ruson.core.instance import Ruson


async def test_connection(db_uri: str, db_name: str):
    config = Config(connection_name="test", database_uri=db_uri, database_name=db_name)
    await Ruson.create_connection(config)

    try:
        assert Ruson.get_client()
        assert False, "Retrieved client with unknown connection name."
    except KeyError:
        pass

    assert Ruson.get_client("test") is not None

    new_config = Ruson.get_config("test")
    assert new_config.connection_name == "test"
    assert new_config.database_uri == db_uri
    assert new_config.database_name == db_name
