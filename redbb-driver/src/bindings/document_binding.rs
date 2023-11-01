use mongodb::{self, bson};

use pyo3::{self, exceptions, prelude::*, types::PyString};

use super::bson_binding::Bson;

#[pyclass(sequence)]
#[derive(Clone)]
pub struct Document(pub(crate) bson::Document);

#[pyclass]
#[derive(Clone)]
pub struct DocumentIter {
    idx: usize,
    size: usize,
    document_items: Vec<(String, PyObject)>,
}

#[pymethods]
impl DocumentIter {
    pub fn __next__(&mut self) -> Option<(String, PyObject)> {
        if self.idx == self.size {
            None
        } else {
            self.idx += 1;
            self.document_items.pop()
        }
    }
}

fn key_is_string(key: &PyAny) -> PyResult<()> {
    if !key.is_instance_of::<PyString>() {
        return Err(PyErr::new::<exceptions::PyValueError, _>(
            "Document keys must be strings".to_owned(),
        ));
    }
    Ok(())
}

#[pymethods]
impl Document {
    #[new]
    pub fn new() -> Self {
        Document(bson::Document::new())
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

    pub fn __getitem__(&self, key: &PyAny) -> Option<PyObject> {
        self.get(key)
    }

    fn get_with_error(&self, key: &PyAny) -> PyResult<Option<PyObject>> {
        key_is_string(key)?;
        match self.0.get(key.extract::<String>()?) {
            Some(b) => Ok(Python::with_gil(|py| Some(Bson(b.clone()).into_py(py)))),
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: &PyAny, value: &PyAny) -> PyResult<()> {
        key_is_string(key)?;
        let key = key.extract::<String>()?;
        let value = Bson::extract(value)?.0;
        self.0.insert(key, value);
        Ok(())
    }

    pub fn __setitem__(&mut self, key: &PyAny, value: &PyAny) -> PyResult<()> {
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

    pub fn items(&self) -> PyObject {
        let items_vector = self
            .0
            .iter()
            .map(|(k, v)| (k.clone(), Bson(v.clone())))
            .collect::<Vec<(String, Bson)>>();

        Python::with_gil(|py| items_vector.into_py(py))
    }

    pub fn __iter__(&self) -> DocumentIter {
        let items_vector = self
            .0
            .iter()
            .map(|(k, v)| Python::with_gil(|py| (k.clone(), Bson(v.clone()).into_py(py))))
            .collect::<Vec<(String, PyObject)>>();

        DocumentIter {
            idx: 0,
            size: items_vector.len(),
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
