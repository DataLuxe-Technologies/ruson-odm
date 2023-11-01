use std::sync::Arc;

use mongodb::{
    self,
    bson::{self, oid::ObjectId},
};

use pyo3::{
    exceptions, ffi,
    prelude::*,
    types::{self, PyFloat, PyString},
};

use super::client_binding::ClientSession;
use crate::interface;

use super::bson_binding::Bson;
use super::results_binding::InsertOneResult;

#[pyclass]
pub struct Collection {
    collection: mongodb::Collection<bson::Document>,
}

impl Collection {
    pub(crate) fn new(collection: mongodb::Collection<bson::Document>) -> Self {
        Collection { collection }
    }
}

#[pyfunction]
pub fn get_insert_one(py: Python) -> Vec<InsertOneResult> {
    return vec![
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Double(10.0)).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::String("batata".to_owned())).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Array(vec![bson::Bson::Double(10.0)])).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Int32(10)).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Int64(10)).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Boolean(true)).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Null).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::RegularExpression(bson::Regex {
                pattern: String::from("bata*"),
                options: String::from("i"),
            }))
            .into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::JavaScriptCode(String::from("1 + 2"))).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![104, 101, 108, 108, 111],
            }))
            .into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::ObjectId(bson::oid::ObjectId::new())).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::DateTime(bson::DateTime::now())).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Decimal128(
                "3.14159".parse::<bson::Decimal128>().unwrap(),
            ))
            .into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Symbol("batata".to_owned())).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::Undefined).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::MaxKey).into_py(py),
        },
        InsertOneResult {
            inserted_id: Bson(bson::Bson::MinKey).into_py(py),
        },
    ];
}

// #[pyfunction]
// pub fn get_document() -> Document {
//     let doc = bson::Document::new();
//     Document(doc)
// }

// pub async fn find_one(
//     collection: Collection,
//     filter: DocumentWrap,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<DocumentWrap> {
// }

// pub async fn find_many(
//     collection: Collection,
//     filter: Option<DocumentWrap>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator<DocumentWrap>> {
// }

// pub async fn insert_one(
//     collection: Collection,
//     document: DocumentWrap,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<InsertOneResult> {
// }

// pub async fn insert_many(
//     collection: Collection,
//     documents: impl Iterator<Item = DocumentWrap>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<InsertManyResult> {
// }

// pub async fn update_one(
//     collection: Collection,
//     update: DocumentWrap,
//     filter: DocumentWrap,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<UpdateResult> {
// }

// pub async fn delete_one(
//     collection: Collection,
//     filter: DocumentWrap,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<DeleteResult> {
// }

// pub async fn delete_many(
//     collection: Collection,
//     filter: Option<DocumentWrap>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<DeleteResult> {
// }

// pub async fn aggregate(
//     collection: Collection,
//     pipeline: impl Iterator<Item = DocumentWrap>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator<DocumentWrap>> {
// }

// pub async fn distinct(
//     collection: Collection,
//     field_name: &str,
//     filter: Option<DocumentWrap>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<Vec<Bson>> {
// }

// pub async fn list_indexes(
//     collection: Collection,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator<IndexModel>> {
// }

// pub async fn create_indexes(
//     collection: Collection,
//     indexes: impl Iterator<Item = IndexModel>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<CreateIndexesResult> {
// }

// pub async fn drop_indexes(
//     collection: Collection,
//     indexes: Option<impl Iterator<Item = String>>,
// ) -> PyResult<()> {
// }

// pub async fn count_documents(
//     collection: Collection,
//     filter: Option<DocumentWrap>,
// ) -> PyResult<u64> {
// }
