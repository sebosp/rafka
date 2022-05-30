pub mod core_utils;
pub mod kafka_scheduler;
pub mod verifiable_properties;

use std::io::ErrorKind;
use std::path::Path;

/// Removes a file unless it exists.
// RAFKA TODO: Move to a utils module
pub fn delete_file_if_exists(file: &Path) -> Result<(), std::io::Error> {
    match std::fs::remove_file(file) {
        Ok(()) => {
            tracing::debug!("Successfully deleted leader_epoch_file: {}", file.display());
        },
        Err(err) => match err.kind() {
            ErrorKind::NotFound => {
                tracing::debug!("No need to delete {} as it doesn't exist", file.display())
            },
            errkind @ _ => {
                tracing::error!("Unable to delete {}: {:?}", file.display(), errkind);
                return Err(err);
            },
        },
    };
    Ok(())
}
