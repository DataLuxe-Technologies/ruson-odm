from typing import Awaitable, Callable, Generic, Self, TypeVar

from ..ruson import bindings
from .types import Document, IndexModel

InsertOneResult = bindings.types.InsertOneResult
InsertManyResult = bindings.types.InsertManyResult
UpdateResult = bindings.types.UpdateResult
DeleteResult = bindings.types.DeleteResult
CreateIndexesResult = bindings.types.CreateIndexesResult

T = TypeVar("T")


class DocumentsCursor(Generic[T]):
    def __init__(
        self, binding_iterator, formatter: Callable[[Document], T | Awaitable[T]]
    ) -> None:
        self.__binding_iterator = binding_iterator
        self.__formatter = formatter

    async def tolist(self) -> list[T]:
        result = []
        async for item in self:
            result.append(item)
        return result

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            result = await bindings.iterator.document_advance(self.__binding_iterator)
        except:
            raise StopAsyncIteration

        if not result:
            raise StopAsyncIteration

        result = await bindings.iterator.document_current(self.__binding_iterator)
        if result is None:
            raise StopAsyncIteration

        formatted = self.__formatter(result)
        if isinstance(formatted, Awaitable):
            return await formatted
        return formatted


class IndexesCursor:
    def __init__(self, binding_iterator) -> None:
        self.__binding_iterator = binding_iterator

    async def tolist(self) -> list[IndexModel]:
        result = []
        async for item in self:
            result.append(item)
        return result

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> IndexModel:
        try:
            result = await bindings.iterator.index_advance(self.__binding_iterator)
        except:
            raise StopAsyncIteration

        if not result:
            raise StopAsyncIteration

        result = await bindings.iterator.index_current(self.__binding_iterator)
        if not result:
            raise StopAsyncIteration
        return result
