use pyo3::prelude::*;

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct InsertOneResult {
    pub inserted_id: PyObject,
}
