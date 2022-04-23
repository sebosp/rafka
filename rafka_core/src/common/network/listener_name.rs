//! From clients/src/main/java/org/apache/kafka/common/network/ListenerName.java
use std::convert::From;
use std::fmt;

use crate::common::security::auth::security_protocol::SecurityProtocol;
#[derive(PartialEq, PartialOrd, Clone, Ord, Eq, Debug, Hash)]
pub struct ListenerName {
    pub value: String,
}

impl fmt::Display for ListenerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "ListenerName({})", self.value)
    }
}

impl ListenerName {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn normalised(input: &str) -> String {
        // RAFKA NOTE: In Java, this uses the Locale and I guess the behavior of to_uppercase may
        // be different in some scenarios/languages/etc?
        input.to_uppercase()
    }
}

impl From<SecurityProtocol> for ListenerName {
    /// Create an instance with the security protocol name as the value.
    fn from(input: SecurityProtocol) -> ListenerName {
        ListenerName::new(input.name().to_string())
    }
}
