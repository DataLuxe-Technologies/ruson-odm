use pyo3::{ffi, prelude::*};

#[allow(non_snake_case)]
pub fn PyNone(py: Python) -> PyObject {
    unsafe { py.from_borrowed_ptr::<PyAny>(ffi::Py_None()).into_py(py) }
}
