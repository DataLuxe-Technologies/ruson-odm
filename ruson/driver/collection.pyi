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

class Collection:
    async def find_one(
        self,
        filter: Document,
        session: Session | None = None,
    ) -> Document: ...
    async def find_many(
        self,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> FindDocumentsIterator: ...
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
        session: Session | None = None,
    ) -> FindDocumentsIterator: ...
    async def distinct(
        self,
        field_name: str,
        filter: Document | None = None,
        session: Session | None = None,
    ) -> list[str]: ...
    async def list_indexes(self) -> FindIndexesIterator: ...
    async def create_indexes(
        self, indexes: list[IndexModel]
    ) -> CreateIndexesResult: ...
    async def drop_indexes(self, indexes: list[str]) -> None: ...
    async def count_documents(self, filter: Document | None = None) -> int: ...
    async def drop(self) -> None: ...
