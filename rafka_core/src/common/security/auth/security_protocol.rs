//! From clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java

use std::str::FromStr;
use thiserror::Error;

pub const PLAINTEXT: SecurityProtocol = SecurityProtocol::Plaintext(SecurityProtocolDefinition {
    id: 0,
    name: "PLAINTEXT",
});

#[derive(Error, Debug, PartialEq)]
pub enum SecurityProtocolError {
    #[error("Unsupported Security Protocol: {0}")]
    UnsupportedSecurityProtocol(String),
    #[error("Unknown Security Protocol: {0}")]
    UnknownSecurityProtocol(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct SecurityProtocolDefinition {
    pub id: i16,
    pub name: &'static str,
}

// For now only Plaintext
#[derive(Debug, Clone, PartialOrd)]
pub enum SecurityProtocol {
    Plaintext(SecurityProtocolDefinition),
}

impl SecurityProtocol {
    pub fn security_protocol_list() -> Vec<Self> {
        vec![
            PLAINTEXT
        ]
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Plaintext(def) => def.name,
        }
    }
}

impl PartialEq for SecurityProtocol {
    fn eq(&self, rhs: &Self) -> bool {
        match self {
            Self::Plaintext(_) => matches!(rhs, Self::Plaintext(_)),
        }
    }
}

impl FromStr for SecurityProtocol {
    type Err = SecurityProtocolError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "PLAINTEXT" => Ok(PLAINTEXT),
            proto @ ("SSL"|""|"SASL_PLAINTEXT"|"SASL_SSL") => Err(SecurityProtocolError::UnsupportedSecurityProtocol(proto.to_string())),
            proto @ _ => Err(SecurityProtocolError::UnknownSecurityProtocol(proto.to_string())),
        }
    }
}
