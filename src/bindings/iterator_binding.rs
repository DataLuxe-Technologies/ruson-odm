use super::{
    document_binding::Document,
    index_binding::{IndexModel, IndexOptions},
    results_binding::{DocumentResultIterator, IndexResultIterator},
};

use pyo3::{exceptions, prelude::*};

#[pyfunction]
pub fn document_advance<'a>(
    py: Python<'a>,
    iterator: DocumentResultIterator,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, bool>(py, async move {
        let mut inner_iterator = iterator.0.lock().await;
        let result = inner_iterator.advance().await;
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn document_current<'a>(
    py: Python<'a>,
    iterator: DocumentResultIterator,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, Document>(py, async move {
        let inner_iterator = iterator.0.lock().await;
        let result = inner_iterator.deserialize_current();
        match result {
            Ok(v) => Ok(Document(v)),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn index_advance<'a>(py: Python<'a>, iterator: IndexResultIterator) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, bool>(py, async move {
        let mut inner_iterator = iterator.0.lock().await;
        let result = inner_iterator.advance().await;
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}

#[pyfunction]
pub fn index_current<'a>(py: Python<'a>, iterator: IndexResultIterator) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py::<_, IndexModel>(py, async move {
        let inner_iterator = iterator.0.lock().await;
        let result = inner_iterator.deserialize_current();
        match result {
            Ok(v) => {
                let keys = Document(v.keys);
                let options = match v.options {
                    Some(opts) => Some(IndexOptions {
                        name: opts.name,
                        sparse: opts.sparse,
                        unique: opts.unique,
                        default_language: opts.default_language,
                        language_override: opts.language_override,
                        weigths: match opts.weights {
                            Some(w) => Some(Document(w)),
                            None => None,
                        },
                        bits: opts.bits,
                        max: opts.max,
                        min: opts.min,
                        bucket_size: opts.bucket_size,
                        partial_filter_expression: match opts.partial_filter_expression {
                            Some(f) => Some(Document(f)),
                            None => None,
                        },
                        wildcard_projection: match opts.wildcard_projection {
                            Some(p) => Some(Document(p)),
                            None => None,
                        },
                        hidden: opts.hidden,
                    }),
                    None => None,
                };
                Ok(IndexModel { keys, options })
            }
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}
