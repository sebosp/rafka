//! Broker Metadata  Checkpoint saves brokre metada to a file.
//! core/src/main/scala/kafka/server/BrokerMetadataCheckpoint.scala
use std::fs::{rename, File};
use std::io::prelude::*;
use std::path::Path;
use std::tracing::{debug, error};
#[derive(Debug)]
pub struct BrokerMetadataCheckpoint {
    broker_id: u32,
    cluster_id: Option<String>,
}

impl BrokerMetadataCheckpoint {
    pub fn to_string(self) -> String {
        format!(
            "BrokerMetadata(brokerId={}, clusterId={})",
            self.broker_id,
            self.cluster_id.unwrap_or(String::from("None"))
        )
    }

    pub fn write(self, file: File, filename: &String) {
        let mut content: String = format!("""
        version=0\n
        broker.id={}\n
        """, self.broker_id);
        if let Some(cluster_id) = self.cluster_id {
            content.append(format!("\ncluster.id={}", cluster_id));
        }
        let old_path = Path::new(filename);
        let temp_file_name = format!("{}.tmp", filename);

        // Open a file in write-only mode, returns `io::Result<File>`
        let new_path = Path::new(temp_file_name);
        let new_path_display = new_path.display();
        let mut file = match File::create(&new_path) {
            Err(why) => panic!("couldn't create {}: {}", new_path_display, why),
            Ok(file) => file,
        };

        // Write string to `file`, returns `io::Result<()>`
        match file.write_all(content.as_bytes()) {
            Err(why) => panic!("Failed to write meta.properties due to {}", why),
            Ok(_) => debug!("Successfully wrote to {}", new_path_display),
        }
        file.close();
        match rename(new_path, old_path){
            Err(err) => panic!("C: {:}", err),
            Ok(()) => debug!("Successfuly moved {}", filename);
        }
    }
}
