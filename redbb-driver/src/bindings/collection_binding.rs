use std::{borrow::BorrowMut, sync::Arc};

use mongodb::{
    self,
    bson::{self, oid::ObjectId},
};

use pyo3::{
    exceptions, ffi,
    prelude::*,
    types::{self, PyFloat, PyString},
};

use crate::interface;

use super::{client_binding::ClientSession, results_binding};

use super::bson_binding::Bson;
use super::document_binding::Document;
use super::index_binding::IndexModel;
use super::results_binding::*;

#[pyclass(frozen)]
#[derive(Clone)]
pub struct Collection(pub mongodb::Collection<bson::Document>);

#[pyfunction]
pub fn find_one<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Document,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Document>(py, async move {
        let result = match session {
            Some(s) => {
                let session = s.0.lock().;
                interface::find_one(collection.0, filter.0, Some(*session)).await
            }
            None => interface::find_one(collection.0, filter.0, None).await,
        };
        match result {
            Ok(c) => Ok(Document(c)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

// pub async fn find_many(
//     collection: Collection,
//     filter: Option<Document>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator> {
// }

// pub async fn insert_one(
//     collection: Collection,
//     document: Document,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<InsertOneResult> {
// }

// pub async fn insert_many(
//     collection: Collection,
//     documents: impl Iterator<Item = Document>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<InsertManyResult> {
// }

// pub async fn update_one(
//     collection: Collection,
//     update: Document,
//     filter: Document,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<UpdateResult> {
// }

// pub async fn delete_one(
//     collection: Collection,
//     filter: Document,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<DeleteResult> {
// }

// pub async fn delete_many(
//     collection: Collection,
//     filter: Option<Document>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<DeleteResult> {
// }

// pub async fn aggregate(
//     collection: Collection,
//     pipeline: impl Iterator<Item = Document>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator> {
// }

// pub async fn distinct(
//     collection: Collection,
//     field_name: &str,
//     filter: Option<Document>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<Vec<Bson>> {
// }

// pub async fn list_indexes(
//     collection: Collection,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<ResultIterator> {
// }

// pub async fn create_indexes(
//     collection: Collection,
//     indexes: Vec<IndexModel>,
//     session: Option<&mut ClientSession>,
// ) -> PyResult<CreateIndexesResult> {
// }

// pub async fn drop_indexes(
//     collection: Collection,
//     indexes: Option<impl Iterator<Item = String>>,
// ) -> PyResult<()> {
// }

// pub async fn count_documents(collection: Collection, filter: Option<Document>) -> PyResult<u64> {}
