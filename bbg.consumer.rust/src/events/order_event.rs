use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OrderEvent {
    pub order_id: String,
    pub amount: f64,
    pub customer: String,
}
