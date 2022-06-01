//! From core/src/main/scala/kafka/utils/CoreUtils.scala

use crate::cluster::end_point::EndPoint;
use crate::common::network::listener_name::ListenerName;
use crate::log::log_manager::LogManagerError;
use crate::server::kafka_config::KafkaConfigError;
use regex::Regex;

/// Replaces a current suffix with a new suffix.
/// If the current suffix is not found then Err
pub fn replace_suffix(
    s: &str,
    current_suffix: &str,
    new_suffix: &str,
) -> Result<String, LogManagerError> {
    match s.find(current_suffix) {
        Some(idx) => {
            let (without_suffix, _) = s.split_at(idx);
            Ok(format!("{}{}", without_suffix, new_suffix))
        },
        None => Err(LogManagerError::UnexpectedSuffix(current_suffix.to_string(), s.to_string())),
    }
}

/// Parses comma separated string into Vec
/// The whitespaces \s around the commas are removed
pub fn parse_csv_list(csv_list: &str) -> Vec<String> {
    Regex::new(r"\s*,\s*")
        .unwrap()
        .split(csv_list)
        .collect::<Vec<&str>>()
        .iter()
        .filter(|x| !x.is_empty())
        .map(|x| x.to_string())
        .collect()
}

pub fn validate_endpoint(
    listeners: &str,
    end_points: &Vec<EndPoint>,
) -> Result<(), KafkaConfigError> {
    // filter out port 0 for unit tests
    let mut ports_excluding_zero: Vec<i32> =
        end_points.iter().filter(|x| x.port != 0).map(|x| x.port).collect();
    ports_excluding_zero.sort();
    let mut distinct_ports = ports_excluding_zero.clone();
    distinct_ports.sort();
    distinct_ports.dedup();
    let mut distinct_listener_names: Vec<ListenerName> =
        end_points.iter().map(|x| x.listener_name.clone()).collect();
    distinct_listener_names.sort();
    distinct_listener_names.dedup();

    if distinct_ports.len() != ports_excluding_zero.len() {
        return Err(KafkaConfigError::ListenerMisconfig(format!(
            "Each listener must have a different port, listeners: {}",
            listeners
        )));
    }
    if distinct_listener_names.len() != end_points.len() {
        return Err(KafkaConfigError::ListenerMisconfig(format!(
            "Each listener must have a different name, listeners: {}",
            listeners
        )));
    }
    Ok(())
}

pub fn listener_list_to_end_points(listeners: &str) -> Result<Vec<EndPoint>, KafkaConfigError> {
    tracing::trace!("listener_list_to_end_points: listeners {}", listeners);
    let mut end_points: Vec<EndPoint> = vec![];
    let listener_list = parse_csv_list(listeners);
    tracing::trace!("listener_list_to_end_points parse_csv_list() -> {:?}", listener_list);
    for listener in listener_list {
        match EndPoint::create_end_point(&listener) {
            Ok(val) => {
                tracing::debug!("Successfully parsed endpoint for listener: {}", listener);
                end_points.push(val);
            },
            Err(err) => {
                tracing::error!("Error parsing broker listeners from '{}': {}", listener, err);
                return Err(err);
            },
        }
    }
    validate_endpoint(&listeners, &end_points)?;
    Ok(end_points)
}

#[cfg(test)]
mod tests {
    use crate::common::security::auth::security_protocol::SecurityProtocol;

    use super::*;
    use std::str::FromStr;

    #[test]
    fn it_parses_csv_list() {
        let res =
            parse_csv_list(&String::from("PLAINTEXT://localhost:9091,TRACE://localhost:9092"));
        assert_eq!(res.len(), 2);
        let expected_endpoints = vec![
            EndPoint {
                host: String::from("localhost"),
                listener_name: ListenerName::new(String::from("PLAINTEXT")),
                port: 9091,
                security_protocol: SecurityProtocol::from_str("PLAINTEXT").unwrap(),
            },
            /* For this test to work we need to feed listener protocol security map that
             * registers the 'TRACE' listener to 'PLAINTEXT'
             * EndPoint {
             * host: String::from("localhost"),
             * listener_name: ListenerName::new(String::from("TRACE")),
             * port: 9092,
             * security_protocol: SecurityProtocol::from_str("PLAINTEXT").unwrap(),
             * }, */
        ];
        assert_eq!(EndPoint::create_end_point(&res[0]).unwrap(), expected_endpoints[0]);
        // assert_eq!(EndPoint::create_end_point(&res[1]).unwrap(), expected_endpoints[1]);
    }
}
