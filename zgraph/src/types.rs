use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

pub type RowId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "v")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    Vector(Vec<f32>),
}

pub type Row = HashMap<String, Value>;

impl Value {
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

