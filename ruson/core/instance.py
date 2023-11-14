from ..driver.client import Client, create_client
from ..driver.session import Session
from .config import Config


class Ruson:
    _connections: dict[str, tuple[Config, Client]] = {}

    @classmethod
    async def create_connection(cls, config: Config) -> None:
        if config.connection_name in cls._connections:
            return
        client = await create_client(config.database_uri)
        cls._connections[config.connection_name] = (config, client)

    @classmethod
    def get_client(cls, connection_name: str = "default") -> Client:
        _, client = cls._connections[connection_name]
        return client

    @classmethod
    def get_config(cls, connection_name: str = "default") -> Config:
        config, _ = cls._connections[connection_name]
        return config

    @classmethod
    async def create_session(cls, connection_name: str = "default") -> Session:
        client = cls.get_client(connection_name)
        session = await client.create_session()
        return session
