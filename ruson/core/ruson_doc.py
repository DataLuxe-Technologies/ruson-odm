from datetime import datetime
from typing import Awaitable, Callable, Literal, Mapping, Self, Type, TypeVar, overload

import pytz
from pydantic import BaseModel, ConfigDict, Field, field_serializer

from ..driver.collection import Collection, noop_formatter
from ..driver.results import (
    CreateIndexesResult,
    DeleteResult,
    DocumentsCursor,
    IndexesCursor,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from ..driver.session import Session
from ..driver.types import (
    BaseTypes,
    CollectionTypes,
    Document,
    DocumentTypes,
    FieldSort,
    Filter,
    IndexModel,
    ObjectId,
    Projection,
    PydanticObjectId,
    Update,
    UpdateOperators,
)
from .instance import Ruson

T = TypeVar("T")


def _get_collection(
    collection_name: str,
    db_name: str | None = None,
    conn_name: str | None = None,
) -> Collection:
    if conn_name is None:
        conn_name = "default"

    if db_name is None:
        config = Ruson.get_config(conn_name)
        db_name = config.database_name

    client = Ruson.get_client(conn_name)
    db = client.database(db_name)
    return db.collection(collection_name)


def _recurse_value(
    value: BaseTypes | CollectionTypes,
) -> BaseTypes | Document | list[BaseTypes | Document]:
    if isinstance(value, Mapping):
        doc = Document()
        for key, val in value.items():
            doc[key] = _recurse_value(val)
        return doc

    if isinstance(value, list):
        return [_recurse_value(v) for v in value]

    return value


def documentify_filter(filter: Filter) -> Document:
    if isinstance(filter, BaseModel):
        filter = filter.model_dump(by_alias=True, exclude_unset=True)

    doc = Document()
    for key, value in filter.items():
        doc[key] = _recurse_value(value)
    return doc


def documentify_sort(sorts: list[FieldSort]) -> Document:
    doc = Document()
    for field_sort in sorts:
        doc[field_sort.field] = field_sort.direction.value
    return doc


def documentify_projection(projection: Projection) -> Document:
    doc = Document()
    for field_projection in projection.field_projections:
        doc[field_projection.field] = 1 if field_projection.include else 0

    if not projection.include_id:
        doc["_id"] = 0

    return doc


def documentify_document(document: DocumentTypes) -> Document:
    if isinstance(document, BaseModel):
        document = document.model_dump(by_alias=True, exclude_unset=True)

    doc = Document()
    for key, value in document.items():
        doc[key] = _recurse_value(value)
    return doc


def documentify_update(update: Update) -> Document:
    if isinstance(update, BaseModel):
        update = update.model_dump(by_alias=True, exclude_unset=True)

    doc = Document()
    for operator, value in update.items():
        doc[operator] = _recurse_value(value)
    return doc


def serialize_datetime(datetime: datetime) -> str:
    return datetime.isoformat()


class RusonDoc(BaseModel):
    id: PydanticObjectId = Field(alias="_id", default_factory=ObjectId)
    created_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    @field_serializer("id")
    def serialize_id(self, value: PydanticObjectId) -> str:
        return str(value)

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return serialize_datetime(value)

    @field_serializer("updated_at")
    def serialize_updated_at(self, value: datetime) -> str:
        return serialize_datetime(value)

    @classmethod
    def class_indexes(cls) -> list[IndexModel]:
        return []

    @classmethod
    async def list_indexes(
        cls: Type[Self],
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> IndexesCursor:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.list_indexes(timeout=timeout)

    @classmethod
    async def create_index(
        cls: Type[Self],
        index: IndexModel,
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> CreateIndexesResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.create_indexes([index], timeout=timeout)

    @classmethod
    async def create_indexes(
        cls: Type[Self],
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> CreateIndexesResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.create_indexes(cls.class_indexes(), timeout=timeout)

    @classmethod
    async def drop_indexes(
        cls: Type[Self],
        indexes: list[str] | None = None,
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> None:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        await collection.drop_indexes(indexes=indexes, timeout=timeout)

    @overload
    async def find(
        self: Self,
        many: Literal[False],
        skip: int | None = None,
        sort: list[FieldSort] | None = None,
        batch_size: int | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> T:
        ...

    @overload
    async def find(
        self: Self,
        many: Literal[True],
        skip: int | None = None,
        sort: list[FieldSort] | None = None,
        batch_size: int | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DocumentsCursor[T]:
        ...

    @overload
    async def find(
        self: Self,
        many=False,
        skip: int | None = None,
        sort: list[FieldSort] | None = None,
        batch_size: int | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> T:
        ...

    async def find(
        self: Self,
        skip: int | None = None,
        sort: list[FieldSort] | None = None,
        batch_size: int | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        many: bool = False,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> T | DocumentsCursor[T]:
        if many:
            return await self.find_many(
                filter=self,
                skip=skip,
                sort=sort,
                batch_size=batch_size,
                projection=projection,
                timeout=timeout,
                formatter=formatter,
                session=session,
                db_name=db_name,
                conn_name=conn_name,
            )

        return await self.find_one(
            filter=self,
            sort=sort,
            projection=projection,
            timeout=timeout,
            formatter=formatter,
            session=session,
            db_name=db_name,
            conn_name=conn_name,
        )

    @classmethod
    async def find_one(
        cls: Type[Self],
        filter: Filter,
        sort: list[FieldSort] | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> T:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        filter = documentify_filter(filter)
        if sort is not None:
            sort = documentify_sort(sort)
        if isinstance(projection, Projection):
            projection = documentify_projection(projection)
        return await collection.find_one(
            filter=filter,
            sort=sort,
            projection=projection,
            timeout=timeout,
            formatter=formatter,
            session=session,
        )

    @classmethod
    async def find_many(
        cls: Type[Self],
        filter: Filter | None = None,
        sort: list[FieldSort] = None,
        projection: Projection | Document | None = None,
        skip: int | None = None,
        limit: int | None = None,
        batch_size: int | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DocumentsCursor[T]:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        if filter is not None:
            filter = documentify_filter(filter)
        if sort is not None:
            sort = documentify_sort(sort)
        if isinstance(projection, Projection):
            projection = documentify_projection(projection)
        return await collection.find_many(
            filter=filter,
            skip=skip,
            limit=limit,
            sort=sort,
            batch_size=batch_size,
            projection=projection,
            timeout=timeout,
            formatter=formatter,
            session=session,
        )

    @classmethod
    async def aggregate(
        cls: Type[Self],
        pipeline: list[Document],
        batch_size: int | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DocumentsCursor[T]:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.aggregate(
            pipeline=pipeline,
            batch_size=batch_size,
            timeout=timeout,
            formatter=formatter,
            session=session,
        )

    @classmethod
    async def distinct(
        cls: Type[Self],
        field_name: str,
        filter: Filter | None = None,
        timeout: int | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> list[str]:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        if filter is not None:
            filter = documentify_filter(filter)
        return await collection.distinct(
            field_name=field_name,
            filter=filter,
            timeout=timeout,
            session=session,
        )

    @classmethod
    async def count_documents(
        cls: Type[Self],
        filter: Filter | None = None,
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> int:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        if filter is not None:
            filter = documentify_filter(filter)
        return await collection.count_documents(filter=filter, timeout=timeout)

    async def insert(
        self: Self,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> InsertOneResult:
        return await self.insert_one(
            document=self,
            session=session,
            db_name=db_name,
            conn_name=conn_name,
        )

    @classmethod
    async def insert_one(
        cls: Type[Self],
        document: DocumentTypes,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> InsertOneResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        document = documentify_document(document)
        return await collection.insert_one(document=document, session=session)

    @classmethod
    async def insert_many(
        cls: Type[Self],
        documents: list[DocumentTypes],
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> InsertManyResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        documents = [documentify_document(document) for document in documents]
        return await collection.insert_many(documents=documents, session=session)

    async def update(
        self: Self,
        update_or_filter: Update | Filter,
        operator: UpdateOperators | None = None,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> UpdateResult:
        suffix = "Self will be used as filter if the operator is None."
        if operator is None:
            update = documentify_update(update_or_filter)
            filter = documentify_document(
                self.model_dump(by_alias=True, exclude_unset=True)
            )
        else:
            update = documentify_update(
                {operator: self.model_dump(by_alias=True, exclude_unset=True)}
            )
            filter = documentify_filter(update_or_filter)

        collection = _get_collection(
            self.__class__.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.update_one(
            update=update,
            filter=filter,
            array_filters=array_filters,
            session=session,
        )

    @classmethod
    async def update_one(
        cls: Type[Self],
        update: Update,
        filter: Filter,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> UpdateResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        update = documentify_update(update)
        filter = documentify_filter(filter)
        return await collection.update_one(
            update=update,
            filter=filter,
            array_filters=array_filters,
            session=session,
        )

    async def upsert(
        self: Self,
        filter: Filter,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> UpdateResult:
        filter = documentify_filter(filter)
        return await self.upsert_one(
            update={"$set": self.model_dump(by_alias=True, exclude={"id"})},
            filter=filter,
            array_filters=array_filters,
            session=session,
            db_name=db_name,
            conn_name=conn_name,
        )

    @classmethod
    async def upsert_one(
        cls: Type[Self],
        update: Update,
        filter: Filter,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> UpdateResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        update = documentify_update(update)
        filter = documentify_filter(filter)
        return await collection.update_one(
            update=update,
            filter=filter,
            upsert=True,
            array_filters=array_filters,
            session=session,
        )

    async def delete(
        self: Self,
        many: bool = False,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DeleteResult:
        if many:
            return await self.delete_many(
                filter=self,
                session=session,
                db_name=db_name,
                conn_name=conn_name,
            )

        return await self.delete_one(
            filter=self,
            session=session,
            db_name=db_name,
            conn_name=conn_name,
        )

    @classmethod
    async def delete_one(
        cls: Type[Self],
        filter: Filter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DeleteResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        filter = documentify_filter(filter)
        return await collection.delete_one(filter=filter, session=session)

    @classmethod
    async def delete_many(
        cls: Type[Self],
        filter: Filter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DeleteResult:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        filter = documentify_filter(filter)
        return await collection.delete_many(filter=filter, session=session)

    @classmethod
    async def drop(
        cls: Type[Self],
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> None:
        collection = _get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        await collection.drop()
