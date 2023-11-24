use pyo3::{exceptions, ffi, prelude::*, types::PyString};

#[allow(non_snake_case)]
pub fn PyNone(py: Python) -> PyObject {
    unsafe { py.from_borrowed_ptr::<PyAny>(ffi::Py_None()).into_py(py) }
}

pub fn key_is_string(key: &PyAny) -> PyResult<()> {
    if !key.is_instance_of::<PyString>() {
        return Err(PyErr::new::<exceptions::PyValueError, _>(
            "Document keys must be strings".to_owned(),
        ));
    }
    Ok(())
}
