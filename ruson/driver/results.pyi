from typing import Generic, Self, TypeVar

from .types import BaseTypes, IndexModel

T = TypeVar("T")

class InsertOneResult:
    @property
    def inserted_id(self) -> BaseTypes: ...

class InsertManyResult:
    @property
    def inserted_ids(self) -> list[BaseTypes]: ...

class UpdateResult:
    @property
    def matched_count(self) -> int: ...
    @property
    def modified_count(self) -> int: ...
    @property
    def upserted_id(self) -> BaseTypes | None: ...

class DeleteResult:
    @property
    def deleted_count(self) -> int: ...

class CreateIndexesResult:
    @property
    def index_names(self) -> list[str]: ...

class DocumentsCursor(Generic[T]):
    def __aiter__(self) -> Self: ...
    async def __anext__(self) -> T: ...

class IndexesCursor:
    def __aiter__(self) -> Self: ...
    async def __anext__(self) -> IndexModel | None: ...
