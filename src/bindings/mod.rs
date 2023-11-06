use pyo3::prelude::*;

mod bson_binding;
mod client_binding;
mod collection_binding;
mod database_biding;
mod document_binding;
mod index_binding;
mod iterator_binding;
mod results_binding;
mod utils;

pub fn client(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "client")?;
    submodule.add_class::<client_binding::Client>()?;
    submodule.add_class::<client_binding::ClientSession>()?;
    submodule.add_function(wrap_pyfunction!(client_binding::create_client, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(client_binding::database, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        client_binding::default_database,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        client_binding::list_database_names,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(client_binding::create_session, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(client_binding::shutdown, submodule)?)?;
    // let name = format!("ruson.{}.{}", m.name()?, submodule.name()?);
    // py_run!(
    //     py,
    //     submodule,
    //     format!("import sys; sys.modules['{}'] = submodule", name).as_str()
    // );
    m.add_submodule(submodule)?;
    Ok(())
}

pub fn database(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "database")?;
    submodule.add_class::<database_biding::Database>()?;
    submodule.add_function(wrap_pyfunction!(database_biding::collection, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(database_biding::drop, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        database_biding::list_collections,
        submodule
    )?)?;
    // let name = format!("ruson.{}.{}", m.name()?, submodule.name()?);
    // py_run!(
    //     py,
    //     submodule,
    //     format!("import sys; sys.modules['{}'] = submodule", name).as_str()
    // );
    m.add_submodule(submodule)?;
    Ok(())
}

pub fn collection(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "collection")?;
    submodule.add_class::<collection_binding::Collection>()?;
    submodule.add_function(wrap_pyfunction!(collection_binding::find_one, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::find_many, submodule)?)?;

    submodule.add_function(wrap_pyfunction!(collection_binding::insert_one, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::insert_many,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::update_one, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::delete_one, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::delete_many,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::aggregate, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::distinct, submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::list_indexes,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::create_indexes,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::drop_indexes,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        collection_binding::count_documents,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(collection_binding::drop, submodule)?)?;
    // let name = format!("ruson.{}.{}", m.name()?, submodule.name()?);
    // py_run!(
    //     py,
    //     submodule,
    //     format!("import sys; sys.modules['{}'] = submodule", name).as_str()
    // );
    m.add_submodule(submodule)?;
    Ok(())
}

pub fn iterator(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "iterator")?;
    submodule.add_class::<database_biding::Database>()?;
    submodule.add_function(wrap_pyfunction!(
        iterator_binding::document_advance,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        iterator_binding::document_current,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        iterator_binding::index_advance,
        submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(
        iterator_binding::index_current,
        submodule
    )?)?;
    // let name = format!("ruson.{}.{}", m.name()?, submodule.name()?);
    // py_run!(
    //     py,
    //     submodule,
    //     format!("import sys; sys.modules['{}'] = submodule", name).as_str()
    // );
    m.add_submodule(submodule)?;
    Ok(())
}

pub fn types(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "types")?;
    submodule.add_class::<bson_binding::MaxKey>()?;
    submodule.add_class::<bson_binding::MinKey>()?;
    submodule.add_class::<bson_binding::Symbol>()?;
    submodule.add_class::<bson_binding::JavaScriptCodeWithScope>()?;
    submodule.add_class::<bson_binding::Binary>()?;
    submodule.add_class::<bson_binding::BinarySubtype>()?;
    submodule.add_class::<bson_binding::JavaScriptCode>()?;
    submodule.add_class::<bson_binding::Decimal128>()?;
    submodule.add_class::<bson_binding::ObjectId>()?;
    submodule.add_class::<bson_binding::Regex>()?;
    submodule.add_class::<bson_binding::Timestamp>()?;
    submodule.add_class::<bson_binding::Undefined>()?;
    submodule.add_class::<document_binding::Document>()?;
    submodule.add_class::<document_binding::DocumentIter>()?;
    submodule.add_class::<results_binding::InsertOneResult>()?;
    submodule.add_class::<results_binding::InsertManyResult>()?;
    submodule.add_class::<results_binding::UpdateResult>()?;
    submodule.add_class::<results_binding::DeleteResult>()?;
    submodule.add_class::<results_binding::CreateIndexesResult>()?;
    submodule.add_class::<results_binding::IndexResultIterator>()?;
    submodule.add_class::<index_binding::IndexModel>()?;
    submodule.add_class::<index_binding::IndexOptions>()?;
    // let name = format!("ruson.{}.{}", m.name()?, submodule.name()?);
    // py_run!(
    //     py,
    //     submodule,
    //     format!("import sys; sys.modules['{}'] = submodule", name).as_str()
    // );
    m.add_submodule(submodule)?;
    Ok(())
}
