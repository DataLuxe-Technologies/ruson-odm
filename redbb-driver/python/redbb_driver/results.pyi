from typing import Any, Callable, Iterable, Self

class InsertOneResult:
    @property
    def inserted_id(self) -> Any: ...

class InsertManyResult:
    @property
    def inserted_ids(self) -> list[Any]: ...

class UpdateResult:
    @property
    def matched_count(self) -> int: ...
    @property
    def modified_count(self) -> int: ...
    @property
    def upserted_id(self) -> Any | None: ...

class DeleteResult:
    @property
    def deleted_count(self) -> int: ...

class CreateIndexesResult:
    @property
    def index_names(self) -> list[str]: ...
