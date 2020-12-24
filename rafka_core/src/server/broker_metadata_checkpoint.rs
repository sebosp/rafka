//! Broker Metadata  Checkpoint saves brokre metada to a file.
//! core/src/main/scala/kafka/server/BrokerMetadataCheckpoint.scala
use std::fmt;
use std::fs::{remove_file, rename, File};
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};

#[derive(Default, Debug, Eq, PartialEq)]
pub struct BrokerMetadata {
    broker_id: u32,
    cluster_id: Option<String>,
}

/// `BrokerMetadataCheckpoint` works as a helper for BrokerMetadata, it handles the I/O operations,
/// reading and writing files as well as maintaining a temporary .tmp to help atomic filesystem
/// move operations. This .tmp may be left over when the process is killed in the process of
/// writing the .tmp and previous to move operation, so the first thing before reading is removing
/// such a file.
pub struct BrokerMetadataCheckpoint {
    filename: PathBuf,
}

impl fmt::Display for BrokerMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerMetadata(brokerId={}, clusterId={})",
            self.broker_id,
            self.cluster_id.as_ref().unwrap_or(&String::from("None"))
        )
    }
}

impl BrokerMetadata {
    /// Since this file is usually 3 or 4 lines, we can pass it here and parse it in memory
    /// The filename is passed along for debug printinf information only
    pub fn from_multiline_string(content: String, filename: &str) -> Option<BrokerMetadata> {
        let mut broker_id: Option<u32> = None;
        let mut cluster_id: Option<String> = None;
        let mut version: Option<u32> = None;
        debug!("from_multiline_string: Starting to work on {}", content);
        for (line_number, config_line) in content.split('\n').enumerate() {
            let config_line_parts: Vec<&str> = config_line.splitn(2, '=').collect();
            if config_line_parts.len() != 2 {
                error!(
                    "BrokerMetadataCheckpoint: {}:{}, Invalid config line, expected 2 items \
                     separated by =, found {} items",
                    filename,
                    line_number,
                    config_line_parts.len()
                );
            } else {
                match config_line_parts[0] {
                    "broker.id" => {
                        broker_id = match config_line_parts[1].to_string().parse() {
                            Ok(num) => Some(num),
                            Err(why) => {
                                error!(
                                    "BrokerMetadataCheckpoint: Unable to parse number for \
                                     broker_id: Found '{}' {}",
                                    config_line_parts[1], why
                                );
                                None
                            },
                        };
                    },
                    "cluster.id" => cluster_id = Some(config_line_parts[1].to_string()),
                    "version" => {
                        version = match config_line_parts[1].parse::<u32>() {
                            Ok(num) => Some(num),
                            Err(why) => {
                                error!(
                                    "BrokerMetadataCheckpoint: Unable to parse number for \
                                     version. Found: '{}' {}",
                                    config_line_parts[1], why
                                );
                                None
                            },
                        }
                    },
                    _ => {
                        error!(
                            "BrokerMetadataCheckpoint: Unknown property {}",
                            config_line_parts[0]
                        );
                    },
                }
            }
        }
        match version {
            Some(0) => match broker_id {
                Some(broker_id) => Some(BrokerMetadata { broker_id, cluster_id }),
                None => None,
            },
            Some(version) => {
                error!("Unrecognized version of the server meta.properties file: {}", version);
                None
            },
            None => None,
        }
    }

    /// `to_multiline_string` transforms the struct into a writable line
    fn to_multiline_string(&self) -> String {
        let mut content: String = format!("version=0\nbroker.id={}\n", self.broker_id);
        if let Some(cluster_id) = &self.cluster_id {
            content.push_str(&format!("cluster.id={}\n", cluster_id));
        }
        debug!("to_multiline_string: {}", content);
        content
    }
}

impl BrokerMetadataCheckpoint {
    /// `new` returns a new instance of the struct or None in case of errors
    pub fn new(filename: &str) -> Self {
        BrokerMetadataCheckpoint { filename: PathBuf::from(filename) }
    }

    /// `write` performs the I/O operation of writing the file
    pub fn write(self, broker_metadata: BrokerMetadata) -> std::io::Result<()> {
        let content = broker_metadata.to_multiline_string();
        let old_path = PathBuf::from(&self.filename);
        let temp_file_name = format!("{}.tmp", self.filename.display());

        // Open a file in write-only mode, returns `io::Result<File>`
        let new_path = PathBuf::from(&temp_file_name);
        let new_path_display = new_path.display();
        {
            // Use this scope so that the file is closed once we leave it and we can then rename
            // it.
            let mut file = File::create(&new_path)?;

            file.write_all(content.as_bytes())?;
            debug!("Successfully wrote to {}", new_path_display);
            file.sync_all()?;
        };
        rename(new_path, old_path)?;
        debug!("Successfully moved {}", self.filename.display());
        Ok(())
    }

    /// `read` performs the I/O operation of reading the file
    pub fn read(self) -> Option<BrokerMetadata> {
        // try to delete any existing temp files for cleanliness
        let temp_file_name = format!("{}.tmp", self.filename.display());
        let temp_path = PathBuf::from(&temp_file_name);
        if temp_path.is_file() {
            match remove_file(temp_path) {
                Err(why) => {
                    error!("Unable to delete .tmp leftover BrokerMetadataCheckpoint file: {}", why);
                    // TODO: PANIC? Maybe one of the filesystems/disks is read-only.
                    return None;
                },
                Ok(()) => {
                    debug!("Successfully deleted leftover .tmp BrokerMetadataCheckpoint file")
                },
            }
        }
        if !self.filename.is_file() {
            warn!(
                "No meta.properties file under dir: {}",
                self.filename.parent().unwrap_or(&Path::new("/")).display()
            );
            return None;
        }

        if let Ok(lines) = read_lines(&self.filename) {
            let mut config_content = String::from("");
            for line_data in lines {
                match line_data {
                    Ok(config_line) => config_content.push_str(&config_line),
                    Err(err) => {
                        error!("Unable to read line from file: {:?}", err);
                        return None;
                    },
                }
            }
            BrokerMetadata::from_multiline_string(
                config_content,
                &self.filename.display().to_string(),
            )
        } else {
            None
        }
    }
}

// TODO: This should be moved to a more general place as we're going to use this a lot
// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: &P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test_env_log::test]
    #[test]
    fn parse_config_string() {
        let filename = String::from("somepath");
        let without_cluster_bm = BrokerMetadata { cluster_id: None, broker_id: 1u32 };
        let without_cluster_expected = String::from("version=0\nbroker.id=1\n");
        assert_eq!(without_cluster_bm.to_multiline_string(), without_cluster_expected);
        assert_eq!(
            Some(BrokerMetadata { cluster_id: None, broker_id: 1u32 }),
            BrokerMetadata::from_multiline_string(without_cluster_expected, &filename)
        );
        let with_cluster_bmc =
            BrokerMetadata { cluster_id: Some(String::from("rafka1")), broker_id: 2u32 };
        let with_cluster_expected = String::from("version=0\nbroker.id=2\ncluster.id=rafka1\n");
        assert_eq!(with_cluster_bmc.to_multiline_string(), with_cluster_expected);
        assert_eq!(
            Some(BrokerMetadata { cluster_id: Some(String::from("rafka1")), broker_id: 2u32 }),
            BrokerMetadata::from_multiline_string(with_cluster_expected, &filename,)
        );
        // Test a line that is not a config
        assert_eq!(
            BrokerMetadata::from_multiline_string(String::from("not.a.config.line"), &filename,),
            None
        );
        // Test a version that is not zero
        assert_eq!(
            BrokerMetadata::from_multiline_string(String::from("version=1"), &filename,),
            None
        );
        // Test a config without version
        assert_eq!(
            BrokerMetadata::from_multiline_string(String::from("broker.id=1"), &filename,),
            None
        );
        assert_eq!(
            BrokerMetadata::from_multiline_string(
                String::from("broker.id=1\nversion=0\ncluster.id=something.with="),
                &filename,
            ),
            Some(BrokerMetadata {
                broker_id: 1,
                cluster_id: Some(String::from("something.with="))
            })
        );
    }
}
