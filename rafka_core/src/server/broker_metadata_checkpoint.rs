//! Broker Metadata  Checkpoint saves brokre metada to a file.
//! core/src/main/scala/kafka/server/BrokerMetadataCheckpoint.scala
use crate::utils::verifiable_properties::{VerifiableProperties, VerifiablePropertiesError};
use std::fmt;
use std::fs::{remove_file, rename, File};
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, trace, warn};

#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct BrokerMetadata {
    broker_id: i32,
    cluster_id: Option<String>,
}

impl BrokerMetadata {
    /// Creates a new instance of the BrokerMetadata
    pub fn new(broker_id: i32, cluster_id: Option<String>) -> Self {
        BrokerMetadata { broker_id, cluster_id }
    }
}

/// `BrokerMetadataCheckpoint` works as a helper for BrokerMetadata, it handles the I/O operations,
/// reading and writing files as well as maintaining a temporary .tmp to help atomic filesystem
/// move operations. This .tmp may be left over when the process is killed in the process of
/// writing the .tmp and previous to move operation, so the first thing before reading is removing
/// such a file.
#[derive(Debug)]
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
    pub fn from_multiline_string(
        content: String,
        filename: &str,
    ) -> Result<BrokerMetadata, VerifiablePropertiesError> {
        debug!("from_multiline_string: Starting to work on {}", content);
        let broker_meta_props = VerifiableProperties::new(content, filename)?;
        let _version = broker_meta_props.validate_key_has_i32_value("version", 0)?;
        let broker_id = broker_meta_props.get_required_i32("broker.id")?;
        let cluster_id = broker_meta_props.get_optional_string("cluster.id", None);
        Ok(BrokerMetadata { broker_id, cluster_id })
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
    pub fn write(&self, broker_metadata: BrokerMetadata) -> std::io::Result<()> {
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
    pub fn read(&self) -> Result<BrokerMetadata, VerifiablePropertiesError> {
        trace!("Reading {}", self.filename.display());
        // try to delete any existing temp files for cleanliness
        let temp_file_name = format!("{}.tmp", self.filename.display());
        let temp_path = PathBuf::from(&temp_file_name);
        if temp_path.is_file() {
            match remove_file(temp_path) {
                Err(err) => {
                    error!("Unable to delete .tmp leftover BrokerMetadataCheckpoint file: {}", err);
                    // TODO: PANIC? Maybe one of the filesystems/disks is read-only.
                    return Err(VerifiablePropertiesError::Io(err));
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
        }

        let lines = read_lines(&self.filename)?;
        let mut config_content = String::from("");
        for line_data in lines {
            match line_data {
                Ok(config_line) => config_content.push_str(&config_line),
                Err(err) => {
                    error!("Unable to read line from file: {:?}", err);
                    return Err(VerifiablePropertiesError::Io(err));
                },
            }
        }
        BrokerMetadata::from_multiline_string(config_content, &self.filename.display().to_string())
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
        let without_cluster_bm = BrokerMetadata { cluster_id: None, broker_id: 1i32 };
        let without_cluster_expected = String::from("version=0\nbroker.id=1\n");
        assert_eq!(without_cluster_bm.to_multiline_string(), without_cluster_expected);
        assert_eq!(
            Ok(BrokerMetadata { cluster_id: None, broker_id: 1i32 }),
            BrokerMetadata::from_multiline_string(without_cluster_expected, &filename)
        );
        let with_cluster_bmc =
            BrokerMetadata { cluster_id: Some(String::from("rafka1")), broker_id: 2i32 };
        let with_cluster_expected = String::from("version=0\nbroker.id=2\ncluster.id=rafka1\n");
        assert_eq!(with_cluster_bmc.to_multiline_string(), with_cluster_expected);
        assert_eq!(
            Ok(BrokerMetadata { cluster_id: Some(String::from("rafka1")), broker_id: 2i32 }),
            BrokerMetadata::from_multiline_string(with_cluster_expected, &filename,)
        );
        // Test a line that is not a config
        assert!(BrokerMetadata::from_multiline_string(
            String::from("not.a.config.line"),
            &filename,
        )
        .is_err());
        // Test a version that is not zero
        assert!(
            BrokerMetadata::from_multiline_string(String::from("version=1"), &filename,).is_err()
        );
        // Test a config without version
        assert!(
            BrokerMetadata::from_multiline_string(String::from("broker.id=1"), &filename,).is_err(),
        );
        assert_eq!(
            BrokerMetadata::from_multiline_string(
                String::from("broker.id=1\nversion=0\ncluster.id=something.with="),
                &filename,
            ),
            Ok(BrokerMetadata { broker_id: 1, cluster_id: Some(String::from("something.with=")) })
        );
    }
}
