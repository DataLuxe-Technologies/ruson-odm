use std::{fmt::Display, num::ParseIntError};

use mongodb::{self, bson};

use super::utils::PyNone;
use pyo3::{
    prelude::*,
    types::{PyBool, PyBytes, PyDateTime, PyFloat, PyInt, PyList, PyString, PyType},
};

use super::document_binding::Document;

const BINARY_SUBTYPE_USER_DEFINED: u8 = 0x80;

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct Undefined;

#[pymethods]
impl Undefined {
    fn __repr__(&self) -> String {
        "ruson.types.Undefined".to_owned()
    }

    fn __str__(&self) -> String {
        "Undefined".to_owned()
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct MaxKey;

#[pymethods]
impl MaxKey {
    fn __repr__(&self) -> String {
        "ruson.types.MaxKey".to_owned()
    }

    fn __str__(&self) -> String {
        "MaxKey".to_owned()
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct MinKey;

#[pymethods]
impl MinKey {
    fn __repr__(&self) -> String {
        "ruson.types.MinKey".to_owned()
    }

    fn __str__(&self) -> String {
        "MinKey".to_owned()
    }
}

#[pyclass(get_all, set_all, module = "ruson.types")]
#[derive(Clone)]
pub struct Symbol {
    symbol: String,
}

#[pymethods]
impl Symbol {
    fn __repr__(&self) -> String {
        format!("ruson.types.Symbol(symbol=\"{}\")", self.symbol)
    }

    fn __str__(&self) -> String {
        self.symbol.clone()
    }
}

#[pyclass(get_all, set_all, module = "ruson.types")]
#[derive(Clone)]
pub struct JavaScriptCode {
    code: String,
}

#[pymethods]
impl JavaScriptCode {
    fn __repr__(&self) -> String {
        format!("ruson.types.JavaScriptCode(code=\"{}\")", self.code)
    }

    fn __str__(&self) -> String {
        self.code.clone()
    }
}

#[pyclass(frozen, get_all)]
#[derive(Clone)]
pub struct JavaScriptCodeWithScope {
    code: String,
    scope: PyObject,
}

#[pymethods]
impl JavaScriptCodeWithScope {
    fn __repr__(&self) -> String {
        format!(
            "ruson.types.JavaScriptCodeWithScope(code=\"{}\", scope={})",
            self.code, self.scope
        )
    }

    fn __str__(&self) -> String {
        self.code.to_string()
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct Timestamp {
    #[pyo3(get)]
    timestamp: u32,
    pub(crate) increment: u32,
}

#[pymethods]
impl Timestamp {
    fn __repr__(&self) -> String {
        format!("ruson.types.Timestamp(timestamp={})", self.timestamp)
    }

    fn __str__(&self) -> String {
        self.timestamp.to_string()
    }
}

#[pyclass(get_all, set_all, module = "ruson.types")]
#[derive(Clone)]
pub struct Regex {
    pattern: String,
    options: String,
}

#[pymethods]
impl Regex {
    fn __repr__(&self) -> String {
        format!(
            "ruson.types.Regex(regex=\"{}\", options=\"{}\")",
            self.pattern, self.options
        )
    }

    fn __str__(&self) -> String {
        self.pattern.clone()
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct BinarySubtype {
    pub(crate) id: u8,
    pub(crate) value: u8,
}

#[pymethods]
impl BinarySubtype {
    #[getter]
    fn get_value(&self) -> String {
        format!("{}", self)
    }

    fn __repr__(&self) -> String {
        format!("ruson.types.BinarySubtype.{}", self)
    }

    fn __str__(&self) -> String {
        format!("{}", self)
    }
}

impl Display for BinarySubtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.id == 0 {
            f.write_str("Generic")
        } else if self.id == 1 {
            f.write_str("Function")
        } else if self.id == 2 {
            f.write_str("BinaryOld")
        } else if self.id == 3 {
            f.write_str("UuidOld")
        } else if self.id == 4 {
            f.write_str("Uuid")
        } else if self.id == 5 {
            f.write_str("Md5")
        } else if self.id == 6 {
            f.write_str("Encrypted")
        } else if self.id == 7 {
            f.write_str("Column")
        } else if self.id < BINARY_SUBTYPE_USER_DEFINED {
            f.write_str(format!("Reserved({})", self.value).as_str())
        } else {
            f.write_str(format!("UserDefined({})", self.value).as_str())
        }
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct Binary {
    #[pyo3(get)]
    subtype: BinarySubtype,
    pub(crate) bytes: Vec<u8>,
}

#[pymethods]
impl Binary {
    fn __repr__(&self) -> String {
        let byte_string = self
            .bytes
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        format!(
            "ruson.types.Binary(subtype={}, bytes=\"{}\")",
            self.subtype, byte_string
        )
    }

    fn __str__(&self) -> String {
        self.bytes
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap()
    }

    #[getter]
    fn get_bytes(&self) -> PyObject {
        Python::with_gil(|py| PyBytes::new(py, &self.bytes.as_slice()).into_py(py))
    }

    #[getter]
    fn get_value(&self) -> PyObject {
        let byte_string = self
            .bytes
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        Python::with_gil(|py| byte_string.into_py(py))
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct ObjectId {
    pub(crate) id: [u8; 12],
}

#[pymethods]
impl ObjectId {
    #[new]
    fn new() -> Self {
        Self {
            id: bson::oid::ObjectId::new().bytes(),
        }
    }

    #[staticmethod]
    fn from_str(value: String) -> Self {
        if value.len() != 24 {
            panic!("Value is not a valid ObjectId");
        }

        let s = value.as_str();
        let id_vec: Result<Vec<u8>, ParseIntError> = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect();

        if id_vec.is_err() {
            panic!("Value is not a valid ObjectId, invalid bytes found");
        }

        let mut id: [u8; 12] = [0; 12];
        id.copy_from_slice(id_vec.unwrap().as_slice());
        Self { id }
    }

    #[classmethod]
    fn is_valid(_cls: &PyType, value: String) -> bool {
        let result = bson::oid::ObjectId::parse_str(value.as_str());
        result.is_ok()
    }

    fn __repr__(&self) -> String {
        let byte_string = self
            .id
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        format!("ruson.types.ObjectId(\"{}\")", byte_string)
    }

    fn __str__(&self) -> String {
        self.id
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap()
    }

    #[getter]
    fn get_value(&self) -> PyObject {
        let byte_string = self
            .id
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        Python::with_gil(|py| byte_string.into_py(py))
    }
}

#[pyclass(frozen, module = "ruson.types")]
#[derive(Clone)]
pub struct Decimal128 {
    pub(crate) bytes: [u8; 16],
}

#[pymethods]
impl Decimal128 {
    fn __repr__(&self) -> String {
        let byte_string = self
            .bytes
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        format!("ruson.types.Decimal128(\"{}\")", byte_string)
    }

    fn __str__(&self) -> String {
        self.bytes
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap()
    }

    #[getter]
    fn get_bytes(&self) -> PyObject {
        Python::with_gil(|py| PyBytes::new(py, &self.bytes.as_slice()).into_py(py))
    }

    #[getter]
    fn get_value(&self) -> PyObject {
        let byte_string = self
            .bytes
            .to_vec()
            .iter()
            .map(|b| format!("{:02x?}", b))
            .reduce(|acc, v| acc + &v)
            .unwrap();

        Python::with_gil(|py| byte_string.into_py(py))
    }
}

#[derive(Clone)]
pub(crate) struct Bson(pub(crate) bson::Bson);

impl IntoPy<PyObject> for Bson {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            bson::Bson::Double(v) => v.into_py(py),
            bson::Bson::String(v) => v.into_py(py),
            bson::Bson::Array(v) => v
                .into_iter()
                .map(|v| Bson(v))
                .collect::<Vec<Bson>>()
                .into_py(py),
            bson::Bson::Document(v) => Document(v).into_py(py),
            bson::Bson::Boolean(v) => v.into_py(py),
            bson::Bson::Null => PyNone(py),
            bson::Bson::RegularExpression(v) => {
                let value = Regex {
                    pattern: v.pattern,
                    options: v.options,
                };
                value.into_py(py)
            }
            bson::Bson::JavaScriptCode(code) => JavaScriptCode { code }.into_py(py),
            bson::Bson::JavaScriptCodeWithScope(v) => {
                let code = v.code;
                let scope = Document(v.scope).into_py(py);
                JavaScriptCodeWithScope { code, scope }.into_py(py)
            }
            bson::Bson::Int32(v) => v.into_py(py),
            bson::Bson::Int64(v) => v.into_py(py),
            bson::Bson::Timestamp(v) => {
                let value = Timestamp {
                    timestamp: v.time,
                    increment: v.increment,
                };
                value.into_py(py)
            }
            bson::Bson::Binary(v) => {
                let binary_subtype = match &v.subtype {
                    bson::spec::BinarySubtype::Generic => BinarySubtype { id: 0, value: 0 },
                    bson::spec::BinarySubtype::Function => BinarySubtype { id: 1, value: 0 },
                    bson::spec::BinarySubtype::BinaryOld => BinarySubtype { id: 2, value: 0 },
                    bson::spec::BinarySubtype::UuidOld => BinarySubtype { id: 3, value: 0 },
                    bson::spec::BinarySubtype::Uuid => BinarySubtype { id: 4, value: 0 },
                    bson::spec::BinarySubtype::Md5 => BinarySubtype { id: 5, value: 0 },
                    bson::spec::BinarySubtype::Encrypted => BinarySubtype { id: 6, value: 0 },
                    bson::spec::BinarySubtype::Column => BinarySubtype { id: 7, value: 0 },
                    bson::spec::BinarySubtype::Reserved(v) => BinarySubtype { id: 9, value: *v },
                    bson::spec::BinarySubtype::UserDefined(v) => BinarySubtype {
                        id: BINARY_SUBTYPE_USER_DEFINED,
                        value: *v,
                    },
                    _ => BinarySubtype {
                        id: BINARY_SUBTYPE_USER_DEFINED,
                        value: v.subtype.into(),
                    },
                };

                let value = Binary {
                    subtype: binary_subtype,
                    bytes: v.bytes,
                };
                value.into_py(py)
            }
            bson::Bson::ObjectId(v) => ObjectId { id: v.bytes() }.into_py(py),
            bson::Bson::DateTime(v) => {
                let value = PyDateTime::from_timestamp(py, v.timestamp_millis() as f64, None);
                match value {
                    Ok(v) => v.into_py(py),
                    Err(e) => e.into_py(py),
                }
            }
            bson::Bson::Decimal128(v) => {
                let value = Decimal128 { bytes: v.bytes() };
                value.into_py(py)
            }
            bson::Bson::Symbol(v) => Symbol { symbol: v }.into_py(py),
            bson::Bson::Undefined => Undefined.into_py(py),
            bson::Bson::MaxKey => MaxKey.into_py(py),
            bson::Bson::MinKey => MinKey.into_py(py),
            bson::Bson::DbPointer(_) => panic!("'DBPointer' type is not supported!"),
        }
    }
}

impl<'source> FromPyObject<'source> for Bson {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.is_instance_of::<Symbol>() {
            let value = ob.extract::<Symbol>()?;
            Ok(Bson(bson::Bson::Symbol(value.symbol)))
        } else if ob.is_instance_of::<PyFloat>() {
            let value = ob.extract::<f64>()?;
            Ok(Bson(bson::Bson::Double(value)))
        } else if ob.is_instance_of::<PyString>() {
            let value = ob.extract::<String>()?;
            Ok(Bson(bson::Bson::String(value)))
        } else if ob.is_instance_of::<PyList>() {
            let vector = ob.extract::<Vec<PyObject>>()?;
            let mut bson_vector = Vec::with_capacity(vector.len());
            for value in vector {
                let inner = Python::with_gil(|py| Bson::extract(value.as_ref(py)))?;
                bson_vector.push(inner.0);
            }
            Ok(Bson(bson::Bson::Array(bson_vector)))
        } else if ob.is_instance_of::<Document>() {
            Ok(Bson(bson::Bson::Document(ob.extract::<Document>()?.0)))
        } else if ob.is_instance_of::<PyBool>() {
            let value = ob.extract::<bool>()?;
            Ok(Bson(bson::Bson::Boolean(value)))
        } else if ob.is_none() {
            Ok(Bson(bson::Bson::Null))
        } else if ob.is_instance_of::<Regex>() {
            let regex = ob.extract::<Regex>()?;
            Ok(Bson(bson::Bson::RegularExpression(bson::Regex {
                pattern: regex.pattern,
                options: regex.options,
            })))
        } else if ob.is_instance_of::<JavaScriptCode>() {
            let value = ob.extract::<JavaScriptCode>()?;
            Ok(Bson(bson::Bson::JavaScriptCode(value.code)))
        } else if ob.is_instance_of::<JavaScriptCodeWithScope>() {
            let code = ob.getattr("code")?.extract::<String>()?;
            let scope = ob.getattr("scope")?;
            let scope = Document::extract(scope)?.0;
            let value = bson::JavaScriptCodeWithScope { code, scope };
            Ok(Bson(bson::Bson::JavaScriptCodeWithScope(value)))
        } else if ob.is_instance_of::<PyInt>() {
            let value = ob.extract::<i64>()?;
            Ok(Bson(bson::Bson::Int64(value)))
        } else if ob.is_instance_of::<Binary>() {
            let value = ob.extract::<Binary>()?;
            let value_subtype = value.subtype;
            let subtype = from_subtype(value_subtype.id, value_subtype.value);
            Ok(Bson(bson::Bson::Binary(bson::Binary {
                subtype,
                bytes: value.bytes,
            })))
        } else if ob.is_instance_of::<ObjectId>() {
            let value = ob.extract::<ObjectId>()?;
            Ok(Bson(bson::Bson::ObjectId(bson::oid::ObjectId::from_bytes(
                value.id,
            ))))
        } else if ob.is_instance_of::<Timestamp>() {
            let value = ob.extract::<Timestamp>()?;
            Ok(Bson(bson::Bson::Timestamp(bson::Timestamp {
                time: value.timestamp,
                increment: value.increment,
            })))
        } else if ob.is_instance_of::<PyDateTime>() {
            let year = ob.getattr("year").unwrap().extract::<i32>()?;
            let month = ob.getattr("month").unwrap().extract::<u8>()?;
            let day = ob.getattr("day").unwrap().extract::<u8>()?;
            let hour = ob.getattr("hour").unwrap().extract::<u8>()?;
            let minute = ob.getattr("minute").unwrap().extract::<u8>()?;
            let second = ob.getattr("second").unwrap().extract::<u8>()?;

            let builder = bson::DateTime::builder()
                .year(year)
                .month(month)
                .day(day)
                .hour(hour)
                .minute(minute)
                .second(second);
            Ok(Bson(bson::Bson::DateTime(builder.build().unwrap())))
        } else if ob.is_instance_of::<Undefined>() {
            Ok(Bson(bson::Bson::Undefined))
        } else if ob.is_instance_of::<MaxKey>() {
            Ok(Bson(bson::Bson::MaxKey))
        } else if ob.is_instance_of::<MinKey>() {
            Ok(Bson(bson::Bson::MinKey))
        } else if ob.is_instance_of::<Decimal128>() {
            let value = ob.extract::<Decimal128>()?;
            let decimal = bson::Decimal128::from_bytes(value.bytes);
            Ok(Bson(bson::Bson::Decimal128(decimal)))
        } else {
            panic!("Type {} is not convertible to BSON", ob.get_type().name()?);
        }
    }
}

fn from_subtype(id: u8, value: u8) -> bson::spec::BinarySubtype {
    match id {
        0 => bson::spec::BinarySubtype::Generic,
        1 => bson::spec::BinarySubtype::Function,
        2 => bson::spec::BinarySubtype::BinaryOld,
        3 => bson::spec::BinarySubtype::UuidOld,
        4 => bson::spec::BinarySubtype::Uuid,
        5 => bson::spec::BinarySubtype::Md5,
        6 => bson::spec::BinarySubtype::Encrypted,
        7 => bson::spec::BinarySubtype::Column,
        _ if id < BINARY_SUBTYPE_USER_DEFINED => bson::spec::BinarySubtype::Reserved(value),
        _ => bson::spec::BinarySubtype::UserDefined(value),
    }
}
