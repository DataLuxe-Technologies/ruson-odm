use mongodb;

use pyo3::{exceptions, prelude::*};

use super::database_biding::Database;
use crate::interface;

#[pyclass(frozen)]
#[derive(Clone)]
pub struct Client {
    pub(crate) client: mongodb::Client,
}

impl Client {
    pub(crate) fn new(client: mongodb::Client) -> Self {
        Client { client }
    }
}

#[pyclass(frozen)]
pub struct ClientSession {
    pub(crate) session: mongodb::ClientSession,
}

impl ClientSession {
    fn new(session: mongodb::ClientSession) -> Self {
        ClientSession { session }
    }
}

#[pyfunction]
pub fn create_client(py: Python, db_uri: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Client>(py, async move {
        let result = interface::create_client(db_uri.as_str()).await;
        match result {
            Ok(c) => Ok(Client::new(c)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn database(client: &Client, database_name: String) -> Database {
    Database::new(client.client.database(database_name.as_str()))
}

#[pyfunction]
pub fn default_database(client: &Client) -> PyResult<Database> {
    let db = client.client.default_database();
    match db {
        Some(db) => Ok(Database::new(db)),
        None => Err(PyErr::new::<exceptions::PyValueError, _>(
            "No default database configured. Check your URI.",
        )),
    }
}

#[pyfunction]
pub fn list_database_names<'a>(py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
    let client = client.client.clone();
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
    let client = client.client.clone();
    pyo3_asyncio::tokio::future_into_py::<_, ClientSession>(py, async move {
        let session = client.start_session(None).await;
        match session {
            Ok(v) => Ok(ClientSession::new(v)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn shutdown<'a>(py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
    let client = client.client.clone();
    pyo3_asyncio::tokio::future_into_py::<_, ()>(py, async move { Ok(client.shutdown().await) })
}
