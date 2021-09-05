/// This class is used for specifying the set of expected configurations.
/// From clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java
use crate::server::kafka_config::KafkaConfigError;
use serde_derive::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::error;

/// `ConfigDefImportance` provides the levels of importance that different java_properties
/// have.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ConfigDefImportance {
    High,
    Medium,
    Low,
}

/// `ConfigDef` defines the configuration properties, how they can be resolved from other
/// values and their defaults This should be later transformed into a derivable from something like
/// DocOpt.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfigDef<T> {
    importance: ConfigDefImportance,
    doc: String,
    /// The configuration key that is used to apply this value
    pub key: String,
    /// `default` of the value, this would be parsed and transformed into each field type from
    /// KafkaConfig
    default: Option<String>,
    /// Whether or not this variable was provided by the configuration file.
    provided: bool,
    /// The current value, be it the default or overwritten by config
    value: Option<T>,
}

impl<T> Default for ConfigDef<T> {
    fn default() -> Self {
        Self {
            importance: ConfigDefImportance::Low,
            doc: String::from("TODO: Missing Docs"),
            key: String::from("unset.key"),
            default: None,
            provided: false,
            value: None,
        }
    }
}

impl<T> ConfigDef<T>
where
    T: FromStr,
    KafkaConfigError: From<<T as FromStr>::Err>,
    <T as FromStr>::Err: std::fmt::Display,
    T: std::fmt::Debug,
{
    pub fn with_importance(mut self, importance: ConfigDefImportance) -> Self {
        self.importance = importance;
        self
    }

    /// Sets the `key` value, this comes from const &str values in the calling modules
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = key.to_string();
        self
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = doc;
        self
    }

    pub fn with_default(mut self, default: String) -> Self {
        //  Pre-fill the value with the default, if it doesn't parse we should panic as that means
        //  a bug in our code, not the config params
        match default.parse::<T>() {
            Ok(val) => self.value = Some(val),
            Err(err) => {
                error!("Unable to parse default property for {:?}: {}", self, err);
                panic!();
            },
        }
        self.default = Some(default);
        self
    }

    pub fn set_value(&mut self, value: T) {
        self.value = Some(value);
        self.provided = true;
    }

    pub fn try_set_parsed_value(&mut self, value: &str) -> Result<(), KafkaConfigError> {
        match value.parse::<_>() {
            Ok(val) => {
                self.set_value(val);
                Ok(())
            },
            Err(err) => {
                error!("Unable to parse property {:?} : {}. Doc: {}", value, err, self.doc);
                Err(KafkaConfigError::from(err))
            },
        }
    }

    pub fn get_value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn get_importance(&self) -> &ConfigDefImportance {
        &self.importance
    }

    pub fn is_provided(&self) -> bool {
        self.provided
    }
}
