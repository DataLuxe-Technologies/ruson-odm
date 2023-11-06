use pyo3::prelude::*;

mod bindings;
mod interface;

#[pymodule]
fn ruson(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let rust_module = PyModule::new(py, "bindings")?;
    bindings::client(py, rust_module)?;
    bindings::database(py, rust_module)?;
    bindings::collection(py, rust_module)?;
    bindings::iterator(py, rust_module)?;
    bindings::types(py, rust_module)?;
    m.add_submodule(rust_module)?;
    Ok(())
}
