use std::sync::Arc;

use mongodb::{bson::Document, IndexModel};
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::interface;

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct InsertOneResult {
    pub inserted_id: PyObject,
}

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct InsertManyResult {
    pub inserted_ids: Vec<PyObject>,
}

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: Option<PyObject>,
}

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct CreateIndexesResult {
    pub index_names: Vec<String>,
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct DocumentResultIterator(pub(crate) Arc<Mutex<interface::ResultIterator<Document>>>);

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct IndexResultIterator(pub(crate) Arc<Mutex<interface::ResultIterator<IndexModel>>>);
