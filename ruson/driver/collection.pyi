from typing import Awaitable, Callable, TypeVar

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

T = TypeVar("T")

def noop_formatter(doc: Document) -> Document: ...

class Collection:
    async def find_one(
        self,
        filter: Document,
        skip: int | None = None,
        sort: Document | None = None,
        projection: Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
    ) -> Document: ...
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
    ) -> DocumentsCursor: ...
    async def insert_one(
        self,
        document: Document,
        session: Session | None = None,
    ) -> InsertOneResult: ...
    async def insert_many(
        self,
        documents: list[Document],
        session: Session | None = None,
    ) -> InsertManyResult: ...
    async def update_one(
        self,
        update: Document,
        filter: Document,
        upsert: bool | None = None,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
    ) -> UpdateResult: ...
    async def delete_one(
        self,
        filter: Document,
        session: Session | None = None,
    ) -> DeleteResult: ...
    async def delete_many(
        self,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> DeleteResult: ...
    async def aggregate(
        self,
        pipeline: list[Document],
        batch_size: int | None = None,
        timeout: int | None = None,
        session: Session | None = None,
    ) -> DocumentsCursor: ...
    async def distinct(
        self,
        field_name: str,
        filter: Document | None = None,
        timeout: int | None = None,
        session: Session | None = None,
    ) -> list[str]: ...
    async def list_indexes(
        self,
        timeout: int | None = None,
    ) -> IndexesCursor: ...
    async def create_indexes(
        self,
        indexes: list[IndexModel],
        timeout: int | None = None,
    ) -> CreateIndexesResult: ...
    async def drop_indexes(
        self,
        indexes: list[str],
        timeout: int | None = None,
    ) -> None: ...
    async def count_documents(
        self,
        filter: Document | None = None,
        timeout: int | None = None,
    ) -> int: ...
    async def drop(self) -> None: ...
