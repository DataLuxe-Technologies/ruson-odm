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
                let options = if let Some(opts) = v.options {
                    Some(IndexOptions {
                        name: opts.name,
                        sparse: opts.sparse,
                        unique: opts.unique,
                        default_language: opts.default_language,
                        language_override: opts.language_override,
                        weigths: if let Some(w) = opts.weights {
                            Some(Document(w))
                        } else {
                            None
                        },
                        bits: opts.bits,
                        max: opts.max,
                        min: opts.min,
                        bucket_size: opts.bucket_size,
                        partial_filter_expression: if let Some(f) = opts.partial_filter_expression {
                            Some(Document(f))
                        } else {
                            None
                        },
                        wildcard_projection: if let Some(p) = opts.wildcard_projection {
                            Some(Document(p))
                        } else {
                            None
                        },
                        hidden: opts.hidden,
                    })
                } else {
                    None
                };
                let keys = Python::with_gil(|p| Document(v.keys).into_py(p));
                Ok(IndexModel { keys, options })
            }
            Err(e) => Err(PyErr::new::<exceptions::PyValueError, _>(e.to_string())),
        }
    })
}
