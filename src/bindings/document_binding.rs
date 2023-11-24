use std::collections::HashMap;

use mongodb::{self, bson};

use pyo3::{self, exceptions, iter::IterNextOutput, prelude::*, types::PyDict};

use super::bson_binding::Bson;
use super::utils::key_is_string;

#[pyclass(sequence, module = "ruson.types")]
#[repr(transparent)]
#[derive(Clone)]
pub struct Document(pub(crate) bson::Document);

#[pyclass(module = "ruson.types")]
#[derive(Clone)]
pub struct DocumentIter {
    index: usize,
    len: usize,
    document_items: Vec<(String, Py<PyAny>)>,
}

#[pymethods]
impl DocumentIter {
    pub fn __len__(&self) -> usize {
        self.len
    }

    pub fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    pub fn __next__(&mut self) -> IterNextOutput<(String, PyObject), &'static str> {
        if self.index == self.len {
            IterNextOutput::Return("ACABOU")
        } else {
            self.index += 1;
            match self.document_items.pop() {
                Some(v) => IterNextOutput::Yield(v),
                None => IterNextOutput::Return("SE ACABÓ"),
            }
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ruson.types.DocumentIter(index={}, len={})",
            self.index, self.len
        )
    }
}

#[pymethods]
impl Document {
    #[new]
    #[pyo3(signature = (dict=None, **kwargs))]
    pub fn new(dict: Option<HashMap<String, &PyAny>>, kwargs: Option<&PyDict>) -> PyResult<Self> {
        let mut doc = Document(bson::Document::new());
        if let Some(dict) = dict {
            for (k, v) in dict.into_iter() {
                doc.set(k, v)?;
            }
        }

        if let Some(kwargs) = kwargs {
            for (k, v) in kwargs.into_iter() {
                key_is_string(k)?;
                let k = k.extract::<String>()?;
                doc.set(k, v)?;
            }
        }

        Ok(doc)
    }

    pub fn copy(&self) -> Self {
        Document(self.0.clone())
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn __len__(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, key: &PyAny) -> PyResult<bool> {
        key_is_string(key)?;
        let key = key.extract::<String>()?;
        Ok(self.0.contains_key(key))
    }

    pub fn __contains__(&self, key: &PyAny) -> PyResult<bool> {
        self.contains(key)
    }

    pub fn get(&self, key: &PyAny) -> Option<PyObject> {
        if key_is_string(key).is_err() {
            None
        } else {
            match self.0.get(key.extract::<String>().unwrap()) {
                Some(b) => Python::with_gil(|py| Some(Bson(b.clone()).into_py(py))),
                None => None,
            }
        }
    }

    pub fn __getitem__(&self, key: &PyAny) -> PyResult<PyObject> {
        key_is_string(key)?;
        let string_key = key.extract::<String>()?;
        match self.0.get(&string_key) {
            Some(b) => Ok(Python::with_gil(|py| Bson(b.clone()).into_py(py))),
            None => Err(PyErr::new::<exceptions::PyKeyError, _>(format!(
                "Key not found: '{}'",
                string_key
            ))),
        }
    }

    pub fn set(&mut self, key: String, value: &PyAny) -> PyResult<()> {
        let value = Bson::extract(value)?.0;
        self.0.insert(key, value);
        Ok(())
    }

    pub fn __setitem__(&mut self, key: String, value: &PyAny) -> PyResult<()> {
        self.set(key, value)
    }

    pub fn del(&mut self, key: &PyAny) -> PyResult<()> {
        key_is_string(key)?;
        let key = key.extract::<String>()?;
        match self.0.remove(&key) {
            None => Err(PyErr::new::<exceptions::PyKeyError, _>(format!(
                "Key not found '{}'.",
                key
            ))),
            _ => Ok(()),
        }
    }
    pub fn __delitem__(&mut self, key: &PyAny) -> PyResult<()> {
        self.del(key)
    }

    pub fn keys(&self) -> PyObject {
        let key_vector = self.0.keys().map(|k| k.clone()).collect::<Vec<String>>();
        Python::with_gil(|py| key_vector.into_py(py))
    }

    pub fn values(&self) -> PyObject {
        let values_vector = self
            .0
            .values()
            .map(|k| Bson(k.clone()))
            .collect::<Vec<Bson>>();

        Python::with_gil(|py| values_vector.into_py(py))
    }

    pub fn items(&self) -> DocumentIter {
        self.__iter__()
    }

    pub fn __iter__(&self) -> DocumentIter {
        let items_vector = self
            .0
            .iter()
            .map(|tuple: (&String, &bson::Bson)| {
                Python::with_gil(|py| (tuple.0.clone(), Bson(tuple.1.clone()).into_py(py)))
            })
            .collect::<Vec<(String, PyObject)>>();

        DocumentIter {
            index: 0,
            len: items_vector.len(),
            document_items: items_vector,
        }
    }

    // pub fn update(&mut self, _other: &PyMapping) -> PyResult<()> {
    //     todo!()
    // }

    // pub fn update_if_missing(&mut self, _other: &PyMapping) -> PyResult<()> {
    //     todo!()
    // }
}
