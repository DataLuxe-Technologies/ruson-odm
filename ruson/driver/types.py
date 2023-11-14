from datetime import datetime
from enum import Enum
from typing import Any, List, Literal, Mapping, Union

from pydantic import BaseModel
from pydantic_core import core_schema

from ..ruson import bindings

MaxKey = bindings.types.MaxKey
MinKey = bindings.types.MinKey
Symbol = bindings.types.Symbol
JavaScriptCodeWithScope = bindings.types.JavaScriptCodeWithScope
BinarySubtype = bindings.types.BinarySubtype
Binary = bindings.types.Binary
JavaScriptCode = bindings.types.JavaScriptCode
Decimal128 = bindings.types.Decimal128
ObjectId = bindings.types.ObjectId
Regex = bindings.types.Regex
Timestamp = bindings.types.Timestamp
Undefined = bindings.types.Undefined
IndexModel = bindings.types.IndexModel
IndexOptions = bindings.types.IndexOptions
Document = bindings.types.Document
DocumentIter = bindings.types.DocumentIter


BaseTypes = (
    int
    | float
    | bool
    | str
    | ObjectId
    | MaxKey
    | MinKey
    | Symbol
    | JavaScriptCodeWithScope
    | BinarySubtype
    | Binary
    | JavaScriptCode
    | Decimal128
    | Regex
    | Timestamp
    | datetime
    | Undefined
    | None
)
CollectionTypes = (
    List[Union[BaseTypes, "CollectionTypes"]]
    | Mapping[str, Union[BaseTypes, "CollectionTypes"]]
    | Document
)

DocumentTypes = Document | Mapping[str, CollectionTypes | BaseTypes]


class Direction(Enum):
    ASCENDING = 1
    DESCENDING = -1


class FieldSort(BaseModel):
    field: str
    direction: Direction


class FieldProjection(BaseModel):
    field: str
    include: bool


class Projection(BaseModel):
    field_projections: list[FieldProjection]
    include_id: bool = True


UpdateOperators = Literal[
    "$set",
    "$inc",
    "$push",
    "$unset",
    "$replaceRoot",
    "$rename",
    "$addToSet",
    "$pop",
    "$pull",
]

Update = Mapping[UpdateOperators, Mapping[str, CollectionTypes | BaseTypes]]

FilterTypes = int | float | bool | str | Mapping[str, "FilterTypes"]
Filter = Mapping[str, FilterTypes]


class PydanticObjectId:
    @classmethod
    def validate_object_id(cls, v: Any, handler) -> ObjectId:
        if isinstance(v, ObjectId):
            return v

        s = handler(v)
        if ObjectId.is_valid(s):
            return ObjectId.from_str(s)
        else:
            raise ValueError("Invalid ObjectId")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, _handler
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_wrap_validator_function(
            cls.validate_object_id,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema, handler):
        return handler(core_schema.str_schema())
