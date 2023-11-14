use std::sync::Arc;

use mongodb::{
    self,
    bson::{self},
};

use pyo3::{exceptions, prelude::*};
use tokio::sync::Mutex;

use crate::interface;

use super::client_binding::ClientSession;

use super::bson_binding::Bson;
use super::document_binding::Document;
use super::index_binding::IndexModel;
use super::results_binding::*;

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct Collection(pub mongodb::Collection<bson::Document>);

#[pyfunction]
pub fn find_one<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Document,
    sort: Option<Document>,
    projection: Option<Document>,
    timeout: Option<u64>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Option<Document>>(py, async move {
        let sort = match sort {
            Some(s) => Some(s.0),
            None => None,
        };
        let projection = match projection {
            Some(p) => Some(p.0),
            None => None,
        };
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::find_one(
            collection.0,
            filter.0,
            None,
            sort,
            projection,
            timeout,
            session,
        )
        .await;
        match result {
            Ok(d) => Ok(match d {
                Some(d) => Some(Document(d)),
                None => None,
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn find_many<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Option<Document>,
    skip: Option<u64>,
    limit: Option<i64>,
    sort: Option<Document>,
    batch_size: Option<u32>,
    projection: Option<Document>,
    timeout: Option<u64>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DocumentResultIterator>(py, async move {
        let filter = match filter {
            Some(f) => Some(f.0),
            None => None,
        };
        let sort = match sort {
            Some(s) => Some(s.0),
            None => None,
        };
        let projection = match projection {
            Some(p) => Some(p.0),
            None => None,
        };
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::find_many(
            collection.0,
            filter,
            skip,
            limit,
            sort,
            batch_size,
            projection,
            timeout,
            session,
        )
        .await;
        match result {
            Ok(v) => Ok(DocumentResultIterator(Arc::new(Mutex::new(v)))),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn insert_one<'a>(
    py: Python<'a>,
    collection: Collection,
    document: Document,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, InsertOneResult>(py, async move {
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::insert_one(collection.0, document.0, session).await;
        match result {
            Ok(v) => Python::with_gil(|p| {
                Ok(InsertOneResult {
                    inserted_id: Bson(v.inserted_id).into_py(p),
                })
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn insert_many<'a>(
    py: Python<'a>,
    collection: Collection,
    documents: Vec<Document>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, InsertManyResult>(py, async move {
        let docs = documents.into_iter().map(|d| d.0);
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::insert_many(collection.0, docs, session).await;
        match result {
            Ok(v) => Python::with_gil(|p| {
                Ok(InsertManyResult {
                    inserted_ids: v
                        .inserted_ids
                        .into_iter()
                        .map(|(_, id)| Bson(id).into_py(p))
                        .collect::<Vec<PyObject>>(),
                })
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn update_one<'a>(
    py: Python<'a>,
    collection: Collection,
    update: Document,
    filter: Document,
    upsert: Option<bool>,
    array_filters: Option<Vec<Document>>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, UpdateResult>(py, async move {
        let array_filters = match array_filters {
            Some(array_filters) => Some(array_filters.into_iter().map(|d| d.0).collect()),
            None => None,
        };
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::update_one(
            collection.0,
            update.0,
            filter.0,
            upsert,
            array_filters,
            session,
        )
        .await;
        match result {
            Ok(v) => Python::with_gil(|p| {
                let upserted_id = match v.upserted_id {
                    Some(upserted_id) => Some(Bson(upserted_id).into_py(p)),
                    None => None,
                };

                Ok(UpdateResult {
                    matched_count: v.matched_count,
                    modified_count: v.modified_count,
                    upserted_id,
                })
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn delete_one<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Document,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DeleteResult>(py, async move {
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::delete_one(collection.0, filter.0, session).await;
        match result {
            Ok(v) => Ok(DeleteResult {
                deleted_count: v.deleted_count,
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn delete_many<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Option<Document>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DeleteResult>(py, async move {
        let filter = match filter {
            Some(f) => f.0,
            None => bson::Document::new(),
        };
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::delete_many(collection.0, filter, session).await;
        match result {
            Ok(v) => Ok(DeleteResult {
                deleted_count: v.deleted_count,
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn aggregate<'a>(
    py: Python<'a>,
    collection: Collection,
    pipeline: Vec<Document>,
    batch_size: Option<u32>,
    timeout: Option<u64>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DocumentResultIterator>(py, async move {
        let docs = pipeline.into_iter().map(|d| d.0);
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result = interface::aggregate(collection.0, docs, batch_size, timeout, session).await;
        match result {
            Ok(v) => Ok(DocumentResultIterator(Arc::new(Mutex::new(v)))),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn distinct<'a>(
    py: Python<'a>,
    collection: Collection,
    field_name: String,
    filter: Option<Document>,
    timeout: Option<u64>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Vec<PyObject>>(py, async move {
        let filter = match filter {
            Some(v) => Some(v.0),
            None => None,
        };
        let session = match session {
            Some(s) => Some(s.0),
            None => None,
        };
        let result =
            interface::distinct(collection.0, field_name.as_str(), filter, timeout, session).await;
        match result {
            Ok(v) => Python::with_gil(|p| Ok(v.into_iter().map(|b| Bson(b).into_py(p)).collect())),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn list_indexes<'a>(
    py: Python<'a>,
    collection: Collection,
    timeout: Option<u64>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, IndexResultIterator>(py, async move {
        let result = interface::list_indexes(collection.0, timeout).await;
        match result {
            Ok(v) => Ok(IndexResultIterator(Arc::new(Mutex::new(v)))),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn create_indexes<'a>(
    py: Python<'a>,
    collection: Collection,
    indexes: Vec<IndexModel>,
    timeout: Option<u64>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, CreateIndexesResult>(py, async move {
        let mut mongo_indexes = Vec::with_capacity(indexes.len());
        for py_idx in indexes {
            let keys = py_idx.keys.0;
            let options = match py_idx.options {
                Some(options) => Some(
                    mongodb::options::IndexOptions::builder()
                        .name(options.name)
                        .sparse(options.sparse)
                        .unique(options.unique)
                        .default_language(options.default_language)
                        .language_override(options.language_override)
                        .weights(match options.weigths {
                            Some(w) => Some(w.0),
                            None => None,
                        })
                        .bits(options.bits)
                        .max(options.max)
                        .min(options.min)
                        .bucket_size(options.bucket_size)
                        .partial_filter_expression(match options.partial_filter_expression {
                            Some(f) => Some(f.0),
                            None => None,
                        })
                        .wildcard_projection(match options.wildcard_projection {
                            Some(p) => Some(p.0),
                            None => None,
                        })
                        .hidden(options.hidden)
                        .build(),
                ),
                None => None,
            };
            let mongo_idx = mongodb::IndexModel::builder()
                .keys(keys)
                .options(options)
                .build();
            mongo_indexes.push(mongo_idx);
        }

        let result =
            interface::create_indexes(collection.0, mongo_indexes.into_iter(), timeout).await;
        match result {
            Ok(v) => Ok(CreateIndexesResult {
                index_names: v.index_names,
            }),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn drop_indexes<'a>(
    py: Python<'a>,
    collection: Collection,
    indexes: Option<Vec<String>>,
    timeout: Option<u64>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let indexes = match indexes {
            Some(idxs) => Some(idxs.into_iter()),
            None => None,
        };
        let result = interface::drop_indexes(collection.0, indexes, timeout).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn count_documents<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Option<Document>,
    timeout: Option<u64>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, u64>(py, async move {
        let filter = match filter {
            Some(f) => Some(f.0),
            None => None,
        };
        let result = interface::count_documents(collection.0, filter, timeout).await;
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn drop<'a>(py: Python<'a>, collection: Collection) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let result = interface::drop(collection.0).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}
