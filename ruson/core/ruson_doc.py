from abc import abstractclassmethod
from datetime import datetime
from pathlib import Path

import pytz
from pydantic import BaseModel, ConfigDict, Field

from ..driver.collection import Collection
from ..driver.types import IndexModel, IndexOptions, ObjectId
from .instance import Ruson


def get_collection(
    collection_name: str,
    database_name: str | None = None,
    connection_name: str = "default",
) -> Collection:
    if database_name is None:
        config = Ruson.get_config(connection_name)
        database_name = config.database_name

    client = Ruson.get_client(connection_name)
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

    @abstractclassmethod
    def get_indexes(cls) -> list[IndexModel]:
        ...

    @classmethod
    def create_indexes(cls) -> str:
        collection = get_collection(cls.__name__.lower())
        return collection.create_indexes(cls.get_indexes())

    # TODO: define all find, update, delete, etc. methods available at class and instance level
    # TODO: allow passing database name and connection name to each method defaulting to None and "default" respectively
    # TODO: allow passsing session to each method defaulting to None
    # TODO: find one and find many should receive a formatter method or flag to build class from returned results
    # (either a formatter function will be informed, or a flag defaulting to false will be used to construct the class from
    # the returned results)
