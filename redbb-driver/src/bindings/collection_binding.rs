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
                let s = s.0.clone();
                interface::find_one(collection.0, filter.0, Some(s)).await
            }
            None => interface::find_one(collection.0, filter.0, None).await,
        };
        match result {
            Ok(c) => Ok(Document(c)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn find_many<'a>(
    py: Python<'a>,
    collection: Collection,
    filter: Option<Document>,
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DocumentResultIterator>(py, async move {
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                match filter {
                    Some(f) => interface::find_many(collection.0, Some(f.0), Some(s)).await,
                    None => interface::find_many(collection.0, None, Some(s)).await,
                }
            }
            None => match filter {
                Some(f) => interface::find_many(collection.0, Some(f.0), None).await,
                None => interface::find_many(collection.0, None, None).await,
            },
        };
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
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::insert_one(collection.0, document.0, Some(s)).await
            }
            None => interface::insert_one(collection.0, document.0, None).await,
        };
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
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::insert_many(collection.0, docs, Some(s)).await
            }
            None => interface::insert_many(collection.0, docs, None).await,
        };
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
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, UpdateResult>(py, async move {
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::update_one(collection.0, update.0, filter.0, Some(s)).await
            }
            None => interface::update_one(collection.0, update.0, filter.0, None).await,
        };
        match result {
            Ok(v) => Python::with_gil(|p| {
                let upserted_id = if let Some(upserted_id) = v.upserted_id {
                    Some(Bson(upserted_id).into_py(p))
                } else {
                    None
                };

                Ok(UpdateResult {
                    matched_count: v.matched_count,
                    modified_count: v.modified_count,
                    upserted_id: upserted_id,
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
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::delete_one(collection.0, filter.0, Some(s)).await
            }
            None => interface::delete_one(collection.0, filter.0, None).await,
        };
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

        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::delete_many(collection.0, filter, Some(s)).await
            }
            None => interface::delete_many(collection.0, filter, None).await,
        };

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
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, DocumentResultIterator>(py, async move {
        let docs = pipeline.into_iter().map(|d| d.0);
        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::aggregate(collection.0, docs, Some(s)).await
            }
            None => interface::aggregate(collection.0, docs, None).await,
        };
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
    session: Option<ClientSession>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Vec<PyObject>>(py, async move {
        let filter = match filter {
            Some(v) => Some(v.0),
            None => None,
        };

        let result = match session {
            Some(s) => {
                let s = s.0.clone();
                interface::distinct(collection.0, field_name.as_str(), filter, Some(s)).await
            }
            None => interface::distinct(collection.0, field_name.as_str(), filter, None).await,
        };
        match result {
            Ok(v) => Python::with_gil(|p| Ok(v.into_iter().map(|b| Bson(b).into_py(p)).collect())),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn list_indexes<'a>(py: Python<'a>, collection: Collection) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, IndexResultIterator>(py, async move {
        let result = interface::list_indexes(collection.0).await;
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
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, CreateIndexesResult>(py, async move {
        let mut mongo_indexes = Vec::with_capacity(indexes.len());
        for py_idx in indexes {
            let keys = Python::with_gil(|p| py_idx.keys.extract::<Document>(p))?.0;
            let options = if let Some(options) = py_idx.options {
                Some(
                    mongodb::options::IndexOptions::builder()
                        .name(options.name)
                        .sparse(options.sparse)
                        .unique(options.unique)
                        .default_language(options.default_language)
                        .language_override(options.language_override)
                        .weights(if let Some(w) = options.weigths {
                            Some(w.0)
                        } else {
                            None
                        })
                        .bits(options.bits)
                        .max(options.max)
                        .min(options.min)
                        .bucket_size(options.bucket_size)
                        .partial_filter_expression(
                            if let Some(f) = options.partial_filter_expression {
                                Some(f.0)
                            } else {
                                None
                            },
                        )
                        .wildcard_projection(if let Some(p) = options.wildcard_projection {
                            Some(p.0)
                        } else {
                            None
                        })
                        .hidden(options.hidden)
                        .build(),
                )
            } else {
                None
            };
            let mongo_idx = mongodb::IndexModel::builder()
                .keys(keys)
                .options(options)
                .build();
            mongo_indexes.push(mongo_idx);
        }

        let result = interface::create_indexes(collection.0, mongo_indexes.into_iter()).await;
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
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let indexes = if let Some(idxs) = indexes {
            Some(idxs.into_iter())
        } else {
            None
        };
        let result = interface::drop_indexes(collection.0, indexes).await;
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
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, u64>(py, async move {
        let filter = if let Some(f) = filter {
            Some(f.0)
        } else {
            None
        };
        let result = interface::count_documents(collection.0, filter).await;
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
