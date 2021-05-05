// From
// clients/src/main/java/org/apache/kafka/common/feature/BaseVersionRange.java

// Represents an immutable basic version range using 2 attributes: min and max, each of type short.
// The min and max attributes need to satisfy 2 rules:
//  - they are each expected to be >= 1, as we only consider positive version values to be valid.
//  - max should be >= min.

use std::collections::HashMap;
use std::fmt;
use tracing::debug;
// RAFKA NOTE: This does not seem to be Serde-derivable because the jsons have different keys for
// Supported vs Finalized Version Ranges.
#[derive(Debug, Clone, PartialEq)]
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
    AbsentKeyInMap(String, HashMap<String, i16>),
    #[error("Serde {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("IncorrectJsonFormat")]
    IncorrectJsonFormat,
    #[error("ParseInt {0:?}")]
    ParseInt(#[from] std::num::ParseIntError),
}

impl fmt::Display for BaseVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BaseVersionRange[{}:{}, {}:{}]",
            self.min_key_label, self.min_value, self.max_key_label, self.max_value
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
        if min_value < 1 || max_value < 1 || max_value < min_value {
            return Err(BaseVersionRangeError::IncompatibleVersionRange(min_value, max_value));
        }
        if min_key_label.is_empty() {
            return Err(BaseVersionRangeError::MinLabelKeyEmpty);
        }
        if max_key_label.is_empty() {
            return Err(BaseVersionRangeError::MaxLabelKeyEmpty);
        }
        Ok(Self { min_key_label, min_value, max_key_label, max_value })
    }

    /// Based valueOrThrow(), but returning a Result
    pub fn try_get_value(
        key: &str,
        version_range_map: &HashMap<String, i16>,
    ) -> Result<i16, BaseVersionRangeError> {
        match version_range_map.get(key) {
            None => Err(BaseVersionRangeError::AbsentKeyInMap(
                key.to_string(),
                version_range_map.clone(),
            )),
            Some(val) => Ok(*val),
        }
    }

    /// From valueOrThrow(), panics on error
    pub fn from(key: &str, version_range_map: &HashMap<String, i16>) -> i16 {
        match BaseVersionRange::try_get_value(key, version_range_map) {
            Ok(val) => val,
            Err(err) => panic!("{}", err),
        }
    }

    /// Returns the minimum value for the version range
    pub fn min(&self) -> i16 {
        self.min_value
    }

    /// Returns the maximum value for the version range
    pub fn max(&self) -> i16 {
        self.max_value
    }
}
