from ..ruson import bindings
from .database import Database
from .session import Session

rust_client = bindings.client


class Client:
    def __init__(self, binding_client):
        self.__binding_client = binding_client

    def database(self, database_name: str) -> Database:
        return Database(rust_client.database(self.__binding_client, database_name))

    def default_database(self) -> Database | None:
        try:
            return Database(rust_client.default_database(self.__binding_client))
        except:
            return None

    async def list_databases(self) -> list[str]:
        return await rust_client.list_database_names(self.__binding_client)

    async def create_session(self) -> Session:
        return Session(await rust_client.create_session(self.__binding_client))

    async def shutdown(self) -> None:
        await rust_client.shutdown(self.__binding_client)

    def __getitem__(self, database_name: str) -> Database:
        return self.database(database_name)


async def create_client(db_uri: str) -> Client:
    return Client(await rust_client.create_client(db_uri))
