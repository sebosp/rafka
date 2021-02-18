// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use crate::common::feature::base_version::BaseVersionRange;
use std::collections::HashMap;

// Represents an immutable dictionary with key being feature name, and value being
// BaseVersionRange.

#[derive(Debug)]
pub struct Features {
    pub features: HashMap<String, BaseVersionRange>,
}
