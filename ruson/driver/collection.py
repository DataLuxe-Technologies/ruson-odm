from typing import Awaitable, Callable, TypeVar

from ..ruson import bindings
from .results import (
    CreateIndexesResult,
    DeleteResult,
    DocumentsCursor,
    IndexesCursor,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from .session import Session
from .types import Document, IndexModel

rust_collection = bindings.collection

T = TypeVar("T")


def noop_formatter(doc: Document) -> Document:
    return doc


class Collection:
    def __init__(self, binding_collection):
        self.__binding_collection = binding_collection

    async def find_one(
        self,
        filter: Document,
        sort: Document | None = None,
        projection: Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
    ) -> Document:
        s = None if session is None else session._get_session()
        result = await rust_collection.find_one(
            self.__binding_collection, filter, sort, projection, timeout, s
        )
        if result is None:
            raise ValueError("Document not found")

        formatted = formatter(result)
        if isinstance(formatted, Awaitable):
            return await formatted
        return formatted

    async def find_many(
        self,
        filter: Document | None = None,
        skip: int | None = None,
        limit: int | None = None,
        sort: Document | None = None,
        batch_size: int | None = None,
        projection: Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
    ) -> DocumentsCursor[T]:
        s = None if session is None else session._get_session()
        cursor = await rust_collection.find_many(
            self.__binding_collection,
            filter,
            skip,
            limit,
            sort,
            batch_size,
            projection,
            timeout,
            s,
        )
        return DocumentsCursor(cursor, formatter)

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
        upsert: bool | None = None,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
    ) -> UpdateResult:
        s = None if session is None else session._get_session()
        return await rust_collection.update_one(
            self.__binding_collection, update, filter, upsert, array_filters, s
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
        batch_size: int | None = None,
        timeout: int | None = None,
        session: Session | None = None,
    ) -> DocumentsCursor:
        s = None if session is None else session._get_session()
        cursor = await rust_collection.aggregate(
            self.__binding_collection, pipeline, batch_size, timeout, s
        )
        return DocumentsCursor(cursor)

    async def distinct(
        self,
        field_name: str,
        filter: Document | None = None,
        timeout: int | None = None,
        session: Session | None = None,
    ) -> list[str]:
        s = None if session is None else session._get_session()
        return await rust_collection.distinct(
            self.__binding_collection, field_name, filter, timeout, s
        )

    async def list_indexes(
        self,
        timeout: int | None = None,
    ) -> IndexesCursor:
        cursor = await rust_collection.list_indexes(self.__binding_collection, timeout)
        return IndexesCursor(cursor)

    async def create_indexes(
        self,
        indexes: list[IndexModel],
        timeout: int | None = None,
    ) -> CreateIndexesResult:
        return await rust_collection.create_indexes(
            self.__binding_collection, indexes, timeout
        )

    async def drop_indexes(
        self,
        indexes: list[str] | None = None,
        timeout: int | None = None,
    ) -> None:
        await rust_collection.drop_indexes(self.__binding_collection, indexes, timeout)

    async def count_documents(
        self,
        filter: Document | None = None,
        timeout: int | None = None,
    ) -> int:
        return await rust_collection.count_documents(
            self.__binding_collection, filter, timeout
        )

    async def drop(self) -> None:
        await rust_collection.drop(self.__binding_collection)
