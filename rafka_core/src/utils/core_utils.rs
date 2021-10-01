//! From core/src/main/scala/kafka/utils/CoreUtils.scala

use crate::cluster::end_point::EndPoint;
use crate::common::network::listener_name::ListenerName;
use crate::server::kafka_config::KafkaConfigError;
use regex::Regex;
use tracing::{error, debug};

   /// Parses comma separated string into Vec
   /// The whitespaces \s around the commas are removed
  pub fn parse_csv_list(csv_list: &str) -> Vec<String> {
    Regex::new(r"\\s*,\\s*").unwrap().split(csv_list).collect::<Vec<&str>>().iter().filter(|x| **x != "").map(|x|x.to_string()).collect()
  }

    pub  fn validate_endpoint(listeners: &str, end_points: &Vec<EndPoint>) -> Result<(), KafkaConfigError> {
      // filter out port 0 for unit tests
      let mut ports_excluding_zero: Vec<i32> = end_points.iter().filter(|x| x.port != 0).map(|x| x.port).collect();
      ports_excluding_zero.sort();
      let mut distinct_ports = ports_excluding_zero.clone();
      distinct_ports.sort();
      let mut distinct_listener_names: Vec<ListenerName> = end_points.iter().map(|x| x.listener_name).collect();
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

    let mut end_points: Vec<EndPoint> = vec![];
      let listener_list = parse_csv_list(listeners);
      for listener in listener_list {
          match EndPoint::create_end_point(listener){
              Ok(val) => {
                  debug!("Successfully created endpoint for listener: {}", listener);
                  end_points.push(val);
              },
              Err(err) => {
                   error!("Error creating broker listeners from '{}': {}", listener, err);
                    return Err(err)a;
              }
          }
      }
    validate_endpoint(&listeners, &end_points)?;
    Ok(end_points)
  }
