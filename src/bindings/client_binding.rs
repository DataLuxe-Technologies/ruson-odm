use std::sync::Arc;

use mongodb;
use pyo3::{exceptions, prelude::*};
use tokio::sync::Mutex;

use super::database_biding::Database;
use crate::interface;

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct Client(pub(crate) mongodb::Client);

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct ClientSession(pub(crate) Arc<Mutex<mongodb::ClientSession>>);

#[pyfunction]
pub fn create_client(py: Python, db_uri: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Client>(py, async move {
        let result = interface::create_client(db_uri.as_str()).await;
        match result {
            Ok(c) => Ok(Client(c)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn database(client: &Client, database_name: String) -> Database {
    Database(client.0.database(database_name.as_str()))
}

#[pyfunction]
pub fn default_database(client: &Client) -> PyResult<Database> {
    let db = client.0.default_database();
    match db {
        Some(db) => Ok(Database(db)),
        None => Err(PyErr::new::<exceptions::PyValueError, _>(
            "No default database configured. Check your URI.",
        )),
    }
}

#[pyfunction]
pub fn list_database_names<'a>(py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
    let client = client.0.clone();
    pyo3_asyncio::tokio::future_into_py::<_, Vec<String>>(py, async move {
        let future = client.list_database_names(None, None);
        let database_names = future.await;
        match database_names {
            Ok(v) => Ok(v),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn create_session<'a>(py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
    let client = client.0.clone();
    pyo3_asyncio::tokio::future_into_py::<_, ClientSession>(py, async move {
        let session = client.start_session(None).await;
        match session {
            Ok(v) => Ok(ClientSession(Arc::new(Mutex::new(v)))),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn shutdown<'a>(py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
    let client = client.0.clone();
    pyo3_asyncio::tokio::future_into_py::<_, ()>(py, async move { Ok(client.shutdown().await) })
}
