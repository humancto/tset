//! Mixture / subset definitions. v0.1 represents subsets as named
//! `(predicate, default_weight)` pairs stored in the manifest. Actual
//! `WeightedSampler` mechanics live in the consuming pipeline (Python's
//! DataLoader); the Rust core is responsible for declaration + persistence.

use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct Subset {
    pub name: String,
    pub predicate: String,
    pub default_weight: f64,
}

impl Subset {
    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "predicate": self.predicate,
            "default_weight": self.default_weight,
        })
    }
}
