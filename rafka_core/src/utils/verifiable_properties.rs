use std::collections::HashMap;
use std::num;
/// From core/src/main/scala/kafka/utils/VerifiableProperties.scala
use thiserror::Error;
#[derive(Error, Debug)]
pub enum VerifiablePropertiesError {
    #[error(
        "VerifiablePropertiesError: {0:}:{1:}, Invalid config line, expected 2 items separated by \
         =, found {2:} items"
    )]
    InvalidConfigLine(String, usize, usize),
    #[error("Parse error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("BrokerMetadataCheckpoint: Unknown property {0:}")]
    UnknownProperty(String),
    #[error("Missing required property '{0:}'")]
    MissingRequiredKey(String),
    #[error("{0:} has value {1:} which is not in the range of {2:}.")]
    ValueNotInRange(String, i32, String),
    #[error("Unrecognized {0:} of the server meta.properties file: {1:}")]
    UnexpectedValue(String, i32),
}

pub struct VerifiableProperties {
    pub props: HashMap<String, String>,
}

impl VerifiableProperties {
    /// `new` parses a new-line separated key=value pairs into a HashMap
    pub fn new(content: String, source_filename: &str) -> Result<Self, VerifiablePropertiesError> {
        let mut props = HashMap::new();
        for (line_number, config_line) in content.split('\n').enumerate() {
            let config_line_parts: Vec<&str> = config_line.splitn(2, '=').collect();
            if config_line_parts.len() != 2 {
                return Err(VerifiablePropertiesError::InvalidConfigLine(
                    source_filename.to_string(),
                    line_number,
                    config_line_parts.len(),
                ));
            } else {
                props.insert(config_line_parts[0].to_string(), config_line_parts[1].to_string());
            }
        }

        Ok(Self { props })
    }

    pub fn get_required_i32(&self, name: &str) -> Result<i32, VerifiablePropertiesError> {
        // TODO: Make generic
        if let Some(val) = self.props.get(name) {
            Ok(val.parse::<i32>()?)
        } else {
            Err(VerifiablePropertiesError::MissingRequiredKey(name.to_string()))
        }
    }

    pub fn get_optional_string(&self, name: &str, default: Option<String>) -> Option<String> {
        self.props.get(name).map(|val| val.clone())
    }

    pub fn validate_key_has_i32_value(
        &self,
        name: &str,
        rhs: i32,
    ) -> Result<(), VerifiablePropertiesError> {
        // TODO: Make generic
        let existing_value = self.get_required_i32(name)?;
        if existing_value == rhs {
            Ok(())
        } else {
            Err(VerifiablePropertiesError::UnexpectedValue(name.to_string(), existing_value))
        }
    }
}
