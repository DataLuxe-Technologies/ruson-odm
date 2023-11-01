use pyo3::prelude::*;

mod bson_binding;
mod client_binding;
mod collection_binding;
mod database_biding;
mod document_binding;
mod results_binding;
mod utils;

pub fn client(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let client_module = PyModule::new(py, "client")?;
    client_module.add_class::<client_binding::Client>()?;
    client_module.add_class::<client_binding::ClientSession>()?;
    client_module.add_function(wrap_pyfunction!(
        client_binding::create_client,
        client_module
    )?)?;
    client_module.add_function(wrap_pyfunction!(client_binding::database, client_module)?)?;
    client_module.add_function(wrap_pyfunction!(
        client_binding::default_database,
        client_module
    )?)?;
    client_module.add_function(wrap_pyfunction!(
        client_binding::list_database_names,
        client_module
    )?)?;
    client_module.add_function(wrap_pyfunction!(
        client_binding::create_session,
        client_module
    )?)?;
    client_module.add_function(wrap_pyfunction!(client_binding::shutdown, client_module)?)?;
    m.add_submodule(client_module)?;
    Ok(())
}

pub fn database(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let database_module = PyModule::new(py, "database")?;
    database_module.add_class::<database_biding::Database>()?;
    database_module.add_function(wrap_pyfunction!(
        database_biding::collection,
        database_module
    )?)?;
    database_module.add_function(wrap_pyfunction!(database_biding::drop, database_module)?)?;
    database_module.add_function(wrap_pyfunction!(
        database_biding::list_collections,
        database_module
    )?)?;
    m.add_submodule(database_module)?;
    Ok(())
}

pub fn collection(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let collection_module = PyModule::new(py, "collection")?;
    collection_module.add_class::<collection_binding::Collection>()?;
    collection_module.add_function(wrap_pyfunction!(
        collection_binding::get_insert_one,
        collection_module
    )?)?;
    m.add_submodule(collection_module)?;
    Ok(())
}

pub fn types(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let type_module = PyModule::new(py, "types")?;
    type_module.add_class::<bson_binding::MaxKey>()?;
    type_module.add_class::<bson_binding::MinKey>()?;
    type_module.add_class::<bson_binding::Symbol>()?;
    type_module.add_class::<bson_binding::JavaScriptCodeWithScope>()?;
    type_module.add_class::<bson_binding::Binary>()?;
    type_module.add_class::<bson_binding::BinarySubtype>()?;
    type_module.add_class::<bson_binding::JavaScriptCode>()?;
    type_module.add_class::<bson_binding::Decimal128>()?;
    type_module.add_class::<bson_binding::ObjectId>()?;
    type_module.add_class::<bson_binding::Regex>()?;
    type_module.add_class::<bson_binding::Timestamp>()?;
    type_module.add_class::<bson_binding::Undefined>()?;
    type_module.add_class::<document_binding::Document>()?;
    type_module.add_class::<document_binding::DocumentIter>()?;
    type_module.add_class::<results_binding::InsertOneResult>()?;
    m.add_submodule(type_module)?;
    Ok(())
}
