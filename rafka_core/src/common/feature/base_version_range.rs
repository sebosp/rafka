// From
// clients/src/main/java/org/apache/kafka/common/feature/BaseVersionRange.java

// Represents an immutable basic version range using 2 attributes: min and max, each of type short.
// The min and max attributes need to satisfy 2 rules:
//  - they are each expected to be >= 1, as we only consider positive version values to be valid.
//  - max should be >= min.

use std::collections::HashMap;
use std::error::Error;
#[derive(Debug)]
pub struct BaseVersionRange {
    // Non-empty label for the min version key, that's used only to convert to/from a map.
    min_key_label: String,

    // The value of the minimum version.
    min_value: i16,

    // Non-empty label for the max version key, that's used only to convert to/from a map.
    max_key_label: String,

    // The value of the maximum version.
    max_value: i16,
}

#[derive(thiserror::Error, Debug)]
pub enum BaseVersionRangeError {
    #[error(
        "Expected minValue >= 1, maxValue >= 1 and maxValue >= minValue, but received minValue: \
         {0}, maxValue: {1}"
    )]
    IncompatibleVersionRange(i16, i16),
    #[error("Expected minKeyLabel to be non-empty.")]
    MinLabelKeyEmpty,
    #[error("Expected maxKeyLabel to be non-empty.")]
    MaxLabelKeyEmpty,
    // From HashMap conversion TODO: maybe Impl From<HashMap>?
    #[error("{0} Absent in {:?}")]
    AbsentKeyInMap(String, &HashMap<String, i16>),
}

impl fmt::Display for BaseVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BaseVersionRange[{}:{}, {}:{}]",
            self.min_key_label, self.min, self.max_key_label, self.max
        )
    }
}
impl BaseVersionRange {
    pub fn new(
        min_key_label: String,
        min_value: i16,
        max_key_label: String,
        max_value: i16,
    ) -> Result<Self, BaseVersionRangeError> {
        if (min_value < 1 || max_value < 1 || max_value < min_value) {
            return Err(IncompatibleVersionRange(min_value, max_value));
        }
        if (min_key_label.is_empty()) {
            return Err(MinLabelKeyEmpty);
        }
        if (max_key_label.is_empty()) {
            return Err(MaxLabelKeyEmpty);
        }
        Ok(Self { min_key_label, min_value, max_key_label, max_value })
    }

    /// Based valueOrThrow(), but returning a Result
    pub fn try_value_from(
        key: &str,
        version_range_map: &HashMap<String, i16>,
    ) -> Result<i16, BaseVersionRangeError> {
        match version_range_map.get(key) {
            None => Err(BaseVersionRangeError::AbsentKeyInMap(key.to_string(), version_range_map)),
            Some(val) => Ok(val),
        }
    }

    /// From valueOrThrow(), panics on error
    pub fn from(key: &str, version_range_map: &HashMap<String, i16>) -> i16 {
        match try_value_from(key, version_range_map) {
            Ok(val) => val,
            Err(err) => panic!(err),
        }
    }
}
