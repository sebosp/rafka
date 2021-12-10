//! From clients/src/main/java/org/apache/kafka/common/record/RecordVersion.java
//! Record Format Versions supported by Kafka. It is also known as the `magic`

use std::convert::TryFrom;
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum RecordVersion {
    V0,
    V1,
    V2,
}

impl fmt::Display for RecordVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u8())
    }
}

impl RecordVersion {
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::V0 => 0,
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Checks if the current version precedes another
    pub fn precedes(self, rhs: Self) -> bool {
        self.as_u8() < rhs.as_u8()
    }

    /// lookup remains here just for compatibility with the original source code.
    pub fn lookup(value: u8) -> Result<Self, String> {
        Self::try_from(value)
    }

    pub fn current() -> Self {
        Self::V2
    }
}

impl TryFrom<u8> for RecordVersion {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            _ => Err(format!("Unknown record version: {}", value)),
        }
    }
}
