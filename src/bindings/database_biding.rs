use mongodb;

use pyo3::{exceptions, prelude::*};

use super::collection_binding::Collection;

#[pyclass]
#[repr(transparent)]
pub struct Database(pub(crate) mongodb::Database);

#[pyfunction]
pub fn collection(db: &Database, collection_name: String) -> Collection {
    Collection(db.0.collection(collection_name.as_str()))
}

#[pyfunction]
pub fn list_collections<'a>(py: Python<'a>, db: &Database) -> PyResult<&'a PyAny> {
    let db = db.0.clone();
    pyo3_asyncio::tokio::future_into_py::<_, Vec<String>>(py, async move {
        let result = db.list_collection_names(None).await;
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn drop<'a>(py: Python<'a>, db: &Database) -> PyResult<&'a PyAny> {
    let db = db.0.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let result = db.drop(None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}
