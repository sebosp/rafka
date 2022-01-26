//! From core/src/main/scala/kafka/cluster/EndPoint.scala

use crate::common::network::listener_name::ListenerName;
use crate::server::kafka_config::KafkaConfigError;
use regex::Regex;
use tracing::error;

#[derive(PartialOrd, PartialEq, Clone, Debug)]
pub struct EndPoint {
    pub host: String,
    pub port: i32,
    pub listener_name: ListenerName,
}

impl EndPoint {
    pub fn create_end_point(connection_string: &str) -> Result<Self, KafkaConfigError> {
        let (listener_name_string, host, port) = Self::uri_parse_exp(connection_string)?;
        let port = port.parse::<i32>()?;
        let listener_name = ListenerName::normalised(listener_name_string);
        Ok(Self { host: host.to_string(), port, listener_name: ListenerName::new(listener_name) })
    }

    pub fn uri_parse_exp(input: &str) -> Result<(&str, &str, &str), KafkaConfigError> {
        // RAFKA NOTE: It seems port could be a negative number?
        let captures =
            Regex::new(r"^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)").unwrap().captures(input);
        if let Some(captures) = captures {
            Ok((
                captures.get(1).map_or("", |m| m.as_str()),
                captures.get(2).map_or("", |m| m.as_str()),
                captures.get(3).map_or("", |m| m.as_str()),
            ))
        } else {
            error!(
                "Does not regex match listener_name_string://[host]:port with input '{}'",
                input
            );
            Err(KafkaConfigError::ListenerMisconfig(format!(
                "Unable to parse {} to a broker endpoint",
                input
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_creates_endpoint() {
        let res = EndPoint::create_end_point(&String::from("PLAINTEXT://localhost:9092")).unwrap();
        assert_eq!(res, EndPoint {
            host: String::from("localhost"),
            port: 9092,
            listener_name: ListenerName::new(String::from("PLAINTEXT")),
        });
    }
}
