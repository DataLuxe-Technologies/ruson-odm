from ..ruson import bindings
from .collection import Collection

rust_database = bindings.database


class Database:
    def __init__(self, binding_database):
        self.__binding_database = binding_database

    def collection(self, collection_name: str) -> Collection:
        return Collection(
            rust_database.collection(self.__binding_database, collection_name)
        )

    async def list_collections(self) -> list[str]:
        return await rust_database.list_collections(self.__binding_database)

    async def drop(self) -> None:
        await rust_database.drop(self.__binding_database)

    def __getitem__(self, collection_name: str) -> Collection:
        return self.collection(collection_name)
