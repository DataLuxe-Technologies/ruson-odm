from ..ruson import bindings
from .results import (
    CreateIndexesResult,
    DeleteResult,
    FindDocumentsIterator,
    FindIndexesIterator,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from .session import Session
from .types import Document, IndexModel

rust_collection = bindings.collection


class Collection:
    def __init__(self, binding_collection):
        self.__binding_collection = binding_collection

    async def find_one(
        self,
        filter: Document,
        session: Session | None = None,
    ) -> Document:
        s = None if session is None else session._get_session()
        return await rust_collection.find_one(self.__binding_collection, filter, s)

    async def find_many(
        self,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> FindDocumentsIterator:
        s = None if session is None else session._get_session()
        return FindDocumentsIterator(
            await rust_collection.find_many(self.__binding_collection, filter, s)
        )

    async def insert_one(
        self,
        document: Document,
        session: Session | None = None,
    ) -> InsertOneResult:
        s = None if session is None else session._get_session()
        return await rust_collection.insert_one(self.__binding_collection, document, s)

    async def insert_many(
        self,
        documents: list[Document],
        session: Session | None = None,
    ) -> InsertManyResult:
        s = None if session is None else session._get_session()
        return await rust_collection.insert_many(
            self.__binding_collection, documents, s
        )

    async def update_one(
        self,
        update: Document,
        filter: Document,
        session: Session | None = None,
    ) -> UpdateResult:
        s = None if session is None else session._get_session()
        return await rust_collection.update_one(
            self.__binding_collection, update, filter, s
        )

    async def delete_one(
        self,
        filter: Document,
        session: Session | None = None,
    ) -> DeleteResult:
        s = None if session is None else session._get_session()
        return await rust_collection.delete_one(self.__binding_collection, filter, s)

    async def delete_many(
        self,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> DeleteResult:
        s = None if session is None else session._get_session()
        return await rust_collection.delete_many(self.__binding_collection, filter, s)

    async def aggregate(
        self,
        pipeline: list[Document],
        session: Session | None = None,
    ) -> FindDocumentsIterator:
        s = None if session is None else session._get_session()
        return FindDocumentsIterator(
            await rust_collection.aggregate(self.__binding_collection, pipeline, s)
        )

    async def distinct(
        self,
        field_name: str,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> list[str]:
        s = None if session is None else session._get_session()
        return await rust_collection.distinct(
            self.__binding_collection, field_name, filter, s
        )

    async def list_indexes(self) -> FindIndexesIterator:
        return FindIndexesIterator(
            await rust_collection.list_indexes(self.__binding_collection)
        )

    async def create_indexes(self, indexes: list[IndexModel]) -> CreateIndexesResult:
        return await rust_collection.create_indexes(self.__binding_collection, indexes)

    async def drop_indexes(self, indexes: list[str]) -> None:
        await rust_collection.drop_indexes(self.__binding_collection, indexes)

    async def count_documents(self, filter: Document | None = None) -> int:
        return await rust_collection.count_documents(self.__binding_collection, filter)

    async def drop(self) -> None:
        await rust_collection.drop(self.__binding_collection)
