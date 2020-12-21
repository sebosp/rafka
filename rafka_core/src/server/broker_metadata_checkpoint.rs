//! Broker Metadata  Checkpoint saves brokre metada to a file.
//! core/src/main/scala/kafka/server/BrokerMetadataCheckpoint.scala
use std::fs::{remove_file, rename, File};
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::Path;
use tracing::{debug, error, warn};
#[derive(Default, Debug, Eq)]
pub struct BrokerMetadataCheckpoint {
    broker_id: u32,
    cluster_id: Option<String>,
    filename: String,
}

impl BrokerMetadataCheckpoint {
    pub fn to_string(self) -> String {
        format!(
            "BrokerMetadata(brokerId={}, clusterId={})",
            self.broker_id,
            self.cluster_id.unwrap_or(String::from("None"))
        )
    }

    fn to_multiline_string(self) -> String {
        let mut content: String = format!("version=0\nbroker.id={}\n", self.broker_id);
        if let Some(cluster_id) = self.cluster_id {
            content.push_str(&format!("\ncluster.id={}", cluster_id));
        }
        content
    }

    pub fn write(self) {
        let content = self.to_multiline_string();
        let old_path = Path::new(&self.filename);
        let temp_file_name = format!("{}.tmp", self.filename);

        // Open a file in write-only mode, returns `io::Result<File>`
        let new_path = Path::new(&temp_file_name);
        let new_path_display = new_path.display();
        {
            // Use this scope so that the file is closed once we leave it and we can then rename
            // it.
            let mut file = match File::create(&new_path) {
                Err(why) => panic!("couldn't create {}: {}", new_path_display, why),
                Ok(file) => file,
            };

            // Write string to `file`, returns `io::Result<()>`
            match file.write_all(content.as_bytes()) {
                Err(why) => panic!("Failed to write meta.properties due to {}", why),
                Ok(_) => debug!("Successfully wrote to {}", new_path_display),
            }
            file.sync_all();
        };
        match rename(new_path, old_path) {
            Err(err) => panic!("C: {:}", err),
            Ok(()) => debug!("Successfuly moved {}", self.filename),
        }
    }

    /// Since this file is usually 3 or 4 lines, we can pass it here and parse it in memory
    pub fn from_multiline_string(self, content: String) -> Option<BrokerMetadataCheckpoint> {
        let mut broker_id: Option<u32>;
        let mut cluster_id: Option<String> = None;
        let mut version: Option<u32>;
        for (line_number, config_line) in content.split("\n").enumerate() {
            let config_line_parts: Vec<&str> = config_line.splitn(2, '=').collect();
            if config_line_parts.len() != 2 {
                error!(
                    "BrokerMetadataCheckpoint: {}:{}, Invalid config line, expected 2 items \
                     separated by =, found {} items",
                    self.filename,
                    line_number,
                    config_line_parts.len()
                );
            } else {
                match config_line_parts[0].as_ref() {
                    "broker_id" => {
                        broker_id = match config_line_parts[1].to_string().parse() {
                            Ok(num) => Some(num),
                            Err(x) => {
                                error!(
                                    "BrokerMetadataCheckpoint: Unable to parse number for \
                                     broker_id. Found {}",
                                    config_line_parts[1]
                                );
                                None
                            },
                        };
                    },
                    "cluster_id" => cluster_id = Some(config_line_parts[1].to_string()),
                    "version" => {
                        version = match config_line_parts[1].parse::<u32>() {
                            Ok(num) => Some(num),
                            Err(err) => {
                                error!(
                                    "BrokerMetadataCheckpoint: Unable to parse number for \
                                     version. Found: {}",
                                    config_line_parts[1]
                                );
                                None
                            },
                        }
                    },
                }
            }
        }
        match version {
            Some(0) => match broker_id {
                Some(broker_id) => Some(BrokerMetadataCheckpoint {
                    filename: self.filename.clone(),
                    broker_id,
                    cluster_id,
                }),
                None => None,
            },
            Some(version) => {
                error!("Unrecognized version of the server meta.properties file: {}", version);
                None
            },
            None => None,
        }
    }

    pub fn read(self) -> Option<BrokerMetadataCheckpoint> {
        // try to delete any existing temp files for cleanliness
        let temp_file_name = format!("{}.tmp", self.filename);
        let temp_path = Path::new(&temp_file_name);
        if temp_path.is_file() {
            remove_file(temp_path);
        }
        let file_path = Path::new(&self.filename);
        if !file_path.is_file() {
            warn!(
                "No meta.properties file under dir: {}",
                file_path.parent().unwrap_or(Path::new("/")).display()
            );
            return None;
        }
        if let Ok(lines) = read_lines(self.filename) {
            let mut config_content: String;
            for line_data in lines {
                match line_data {
                    Ok(config_line) => config_content.push_str(&config_line),
                    Err(err) => {
                        error!("Unable to read line from file: {:?}", err);
                        return None;
                    },
                }
            }
            self.from_multiline_string(config_content)
        } else {
            None
        }
    }
}

// TODO: This should be moved to a more general place as we're going to use this a lot
// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_config_file() {
        let test_bmc = BrokerMetadataCheckpoint::default();
        let without_cluster_bmc = BrokerMetadataCheckpoint {
            filename: String::from("somepath"),
            cluster_id: None,
            broker_id: 1u32,
        };
        let without_cluster_expected = String::from("version=0\nbroker.id=1");
        assert_eq!(without_cluster_bmc.to_multiline_string(), without_cluster_expected,);
        assert_eq!(
            without_cluster_expected,
            test_bmc.from_multiline_string(without_cluster_expected)
        );
        let with_cluster_bmc = BrokerMetadataCheckpoint {
            filename: String::from("somepath"),
            cluster_id: Some(String::from("rafka1")),
            broker_id: 2u32,
        };
        let with_cluster_expected = String::from("version=0\nbroker.id=1\ncluster.id=rafka1");
        assert_eq!(with_cluster_bmc.to_multiline_string(), with_cluster_expected);
        assert_eq!(with_cluster_expected, test_bmc.from_multiline_string(with_cluster_expected));
        // Test a line that is not a config
        assert_eq!(test_bmc.from_multiline_string(String::from("not.a.config.line")), None);
        // Test a version that is not zero
        assert_eq!(test_bmc.from_multiline_string(String::from("version=1")), None);
        // Test a config without version
        assert_eq!(test_bmc.from_multiline_string(String::from("broker.id=1")), None);
        assert_eq!(
            test_bmc.from_multiline_string(String::from(
                "broker.id=1\nversion=0\ncluster.id=something.with="
            )),
            None
        );
    }
}
