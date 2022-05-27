use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KeyValueRequest {
    pub key: String,
    pub value: String,
}