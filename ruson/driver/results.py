from ..ruson import bindings

InsertOneResult = bindings.types.InsertOneResult
InsertManyResult = bindings.types.InsertManyResult
UpdateResult = bindings.types.UpdateResult
DeleteResult = bindings.types.DeleteResult
CreateIndexesResult = bindings.types.CreateIndexesResult


class DocumentsCursor:
    def __init__(self, binding_iterator) -> None:
        self.__binding_iterator = binding_iterator

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            result = await bindings.iterator.document_advance(self.__binding_iterator)
        except:
            raise StopAsyncIteration

        if not result:
            raise StopAsyncIteration

        result = await bindings.iterator.document_current(self.__binding_iterator)
        if result is None:
            raise StopAsyncIteration
        return result


class IndexesCursor:
    def __init__(self, binding_iterator) -> None:
        self.__binding_iterator = binding_iterator

    def __aiter__(self):
        return self

    async def __anext__(self):
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
