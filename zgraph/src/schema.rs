use serde::{Deserialize, Serialize};

use crate::index::Metric;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub vector: Option<VectorSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSpec {
    pub column: String,
    pub dims: Option<usize>,
    pub metric: Metric,
}

impl TableSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vector: None,
        }
    }

    pub fn with_vector(mut self, column: impl Into<String>, dims: Option<usize>, metric: Metric) -> Self {
        self.vector = Some(VectorSpec {
            column: column.into(),
            dims,
            metric,
        });
        self
    }
}

