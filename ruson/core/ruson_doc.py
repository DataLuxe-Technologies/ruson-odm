from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Literal, Self, Type, TypeVar, overload

import pytz
from pydantic import BaseModel, ConfigDict, Field

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
    Document,
    DocumentTypes,
    FieldSort,
    Filter,
    IndexModel,
    ObjectId,
    Projection,
    Update,
)
from .instance import Ruson

T = TypeVar("T")


def get_collection(
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
    db = client.database(config.database_name)
    return db.collection(collection_name)


class RusonDoc(BaseModel):
    id: ObjectId = Field(alias="_id", default_factory=ObjectId)
    created_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))

    model_config = ConfigDict(
        json_encoders={ObjectId: str, datetime: lambda dt: dt.isoformat(), Path: str},
        allow_population_by_field_name=True,
    )

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
        collection = get_collection(
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
        collection = get_collection(
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.create_indexes(cls.class_indexes(), timeout=timeout)

    @classmethod
    async def drop_indexes(
        cls: Type[Self],
        indexes: list[str],
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> None:
        collection = get_collection(
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
            skip=skip,
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
        skip: int | None = None,
        sort: list[FieldSort] | None = None,
        projection: Projection | Document | None = None,
        timeout: int | None = None,
        formatter: Callable[[Document], T | Awaitable[T]] = noop_formatter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> T:
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.find_one(
            filter=filter,
            skip=skip,
            sort=sort,
            projection=projection,
            timeout=timeout,
            formatter=formatter,
            session=session,
        )

    @classmethod
    async def find_many(
        cls: Type[Self],
        filter: Filter = None,
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
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
        collection = get_collection(
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.distinct(
            field_name=field_name,
            filter=filter,
            timeout=timeout,
            session=session,
        )

    @classmethod
    async def count_documents(
        cls: Type[Self],
        filter: Filter = None,
        timeout: int | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> int:
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.insert_one(document=document, session=session)

    @classmethod
    async def insert_many(
        cls: Type[Self],
        documents: list[DocumentTypes],
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> InsertManyResult:
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.insert_many(documents=documents, session=session)

    async def update(
        self: Self,
        filter: Filter,
        array_filters: list[Document] | None = None,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> UpdateResult:
        return await self.update_one(
            update=self,
            filter=filter,
            array_filters=array_filters,
            session=session,
            db_name=db_name,
            conn_name=conn_name,
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
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
        return await self.upsert_one(
            update=self,
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
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
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.delete_one(filter=filter, session=session)

    @classmethod
    async def delete_many(
        cls: Type[Self],
        filter: Filter,
        session: Session | None = None,
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> DeleteResult:
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        return await collection.delete_many(filter=filter, session=session)

    @classmethod
    async def drop(
        cls: Type[Self],
        db_name: str | None = None,
        conn_name: str | None = None,
    ) -> None:
        collection = get_collection(
            cls.__name__.lower(), db_name=db_name, conn_name=conn_name
        )
        await collection.drop()
