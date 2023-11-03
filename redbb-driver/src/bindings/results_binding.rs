use pyo3::prelude::*;

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

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct ResultIterator {}
