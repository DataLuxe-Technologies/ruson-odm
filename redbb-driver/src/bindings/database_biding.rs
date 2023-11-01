use mongodb;

use pyo3::{exceptions, prelude::*};

use super::collection_binding::Collection;

#[pyclass]
pub struct Database {
    pub(crate) database: mongodb::Database,
}

impl Database {
    pub(crate) fn new(database: mongodb::Database) -> Self {
        Database { database }
    }
}

#[pyfunction]
pub fn collection(db: &Database, collection_name: String) -> Collection {
    Collection::new(db.database.collection(collection_name.as_str()))
}

#[pyfunction]
pub fn drop<'a>(py: Python<'a>, db: &Database) -> PyResult<&'a PyAny> {
    let db = db.database.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let result = db.drop(None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn list_collections<'a>(py: Python<'a>, db: &Database) -> PyResult<&'a PyAny> {
    let db = db.database.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let result = db.list_collection_names(None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}
