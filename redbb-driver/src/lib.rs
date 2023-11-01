use pyo3::prelude::*;

mod bindings;
mod interface;

#[pymodule]
fn redbb_driver(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    bindings::client(py, m)?;
    bindings::database(py, m)?;
    bindings::collection(py, m)?;
    bindings::types(py, m)?;
    Ok(())
}
