//! From clients/src/main/java/org/apache/kafka/common/config/ConfigException.java

use thiserror::Error;
#[derive(Error, Debug, PartialEq)]
pub enum ConfigException {
    #[error("Invalid value {1} for configuration {0}: {2}")]
    InvalidValue(String, String, String),
}
