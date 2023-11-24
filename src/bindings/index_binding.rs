use std::collections::HashMap;

use pyo3::prelude::*;

use super::document_binding::Document;

#[pyclass(frozen, get_all, module = "ruson.types")]
#[derive(Clone)]
pub struct IndexOptions {
    /// Specifies a name outside the default generated name.
    pub name: Option<String>,

    /// If true, the index only references documents with the specified field. The
    /// default value is false.
    pub sparse: Option<bool>,

    /// Forces the index to be unique so the collection will not accept documents where the index
    /// key value matches an existing value in the index. The default value is false.
    pub unique: Option<bool>,

    /// For text indexes, the language that determines the list of stop words and the
    /// rules for the stemmer and tokenizer.
    pub default_language: Option<String>,

    /// For `text` indexes, the name of the field, in the collection’s documents, that
    /// contains the override language for the document.
    pub language_override: Option<String>,

    /// For `text` indexes, a document that contains field and weight pairs.
    pub weigths: Option<Document>,

    /// For `2dsphere` indexes, the number of precision of the stored geohash value of the
    /// location data. The bits value ranges from 1 to 32 inclusive.
    pub bits: Option<u32>,

    /// For `2dsphere` indexes, the upper inclusive boundary for the longitude and latitude
    /// values.
    pub max: Option<f64>,

    /// For `2dsphere` indexes, the lower inclusive boundary for the longitude and latitude
    /// values.
    pub min: Option<f64>,

    /// For `geoHaystack` indexes, specify the number of units within which to group the location
    /// values.
    pub bucket_size: Option<u32>,

    /// If specified, the index only references documents that match the filter
    /// expression. See Partial Indexes for more information.
    pub partial_filter_expression: Option<Document>,

    /// Allows users to include or exclude specific field paths from a wildcard index.
    pub wildcard_projection: Option<Document>,

    /// A flag that determines whether the index is hidden from the query planner. A
    /// hidden index is not evaluated as part of the query plan selection.
    pub hidden: Option<bool>,
}

#[pymethods]
impl IndexOptions {
    #[new]
    fn new(
        name: Option<String>,
        sparse: Option<bool>,
        unique: Option<bool>,
        default_language: Option<String>,
        language_override: Option<String>,
        weigths: Option<Document>,
        bits: Option<u32>,
        max: Option<f64>,
        min: Option<f64>,
        bucket_size: Option<u32>,
        partial_filter_expression: Option<Document>,
        wildcard_projection: Option<Document>,
        hidden: Option<bool>,
    ) -> Self {
        Self {
            name,
            sparse,
            unique,
            default_language,
            language_override,
            weigths,
            bits,
            max,
            min,
            bucket_size,
            partial_filter_expression,
            wildcard_projection,
            hidden,
        }
    }

    fn __repr__(&self) -> String {
        format!("ruson.types.IndexOptions(...)")
    }
}

#[pyclass(frozen, get_all, module = "ruson.types")]
#[derive(Clone)]
pub struct IndexModel {
    /// Specifies the index’s fields. For each field, specify a key-value pair in which the key is
    /// the name of the field to index and the value is index type.
    pub keys: Document,

    /// The options for the index.
    pub options: Option<IndexOptions>,
}

#[pymethods]
impl IndexModel {
    #[new]
    fn new(keys: HashMap<String, &PyAny>, options: Option<IndexOptions>) -> PyResult<Self> {
        Ok(Self {
            keys: Document::new(Some(keys), None)?,
            options,
        })
    }

    fn __repr__(&self) -> String {
        format!("ruson.types.IndexModel(...)")
    }
}
