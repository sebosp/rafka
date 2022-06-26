//! From org/apache/kafka/common/record/FileRecords.java
//!
//! RAFKA TODO:
//! - The original implementation uses FileChannel which allows multiple process to read and
//! write to the file and share the file between threads. For rust it seems std::fs::File is Unpin
//! and Sync so we can send its instance across processes. But we could create a mpsc-based way to
//! synchronize access to the file to avoid reads while truncation/writes are happening on other
//! processes.

use crate::common::record::file_log_input_stream::FileChannelRecordBatch;
use crate::log::log::LogError;
use std::cmp;
use std::path::PathBuf;

#[derive(Debug)]
pub struct FileRecords {
    file: PathBuf,
    channel: FileChannel,
    start: u64,
    end: u64,
    is_slice: bool,
    size: u64, // RAFKA TODO: Used to be AtomicInteger, figure out why.
    batches: Vec<FileChannelRecordBatch>,
}

impl FileRecords {
    pub fn new(
        file: PathBuf,
        channel: FileChannel,
        start: u64,
        end: u64,
        is_slice: bool,
    ) -> Result<Self, LogError> {
        // RAFKA TODO: We receive signed integers for these start/end/etc and their substraction
        // may not be translatable to u64 given they be negative.
        let mut size = 0u64;
        if is_slice {
            size = end - start;
        } else {
            let channel_size = channel.size()?;
            // RAFKA NOTE: Initially we receive these start/end as i32, so let's preserve the
            // previous behavior. Keep u64 here as it's what [`std::fs::File`] uses for
            // metadata.len() and write_at/read_at
            if channel_size > (i32::MAX as u64) {
                return Err(LogError::SegmentSizeAboveMax(
                    file.display().to_string(),
                    channel_size,
                    i32::MAX,
                ));
            }
            let limit = cmp::min(channel_size, end);
            size = limit - start;

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }

        let batches = vec![]; // TODO: Self::batches_from(start);
        Ok(Self { file, channel, start, end, is_slice, size, batches })
    }

    // pub fn batches_from(start: u64) -> impl IntoIterator<Item = FileChannelRecordBatch> {
    // unimplemented!()
    // }

    /// Close the record set
    pub fn close(&self) -> Result<(), LogError> {
        self.flush()?;
        self.trim();
        self.channel.close();
        Ok(())
    }

    /// Commits all writes to disk: TODO:
    pub fn flush(&self) -> Result<(), LogError> {
        Ok(())
    }

    pub fn trim(&self) -> Result<(), LogError> {
        self.truncate_to(self.size)?;
        Ok(())
    }

    pub fn truncate_to(&self, size: u64) -> Result<u64, LogError> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct FileChannel {
    file: std::fs::File,
    position: u64,
}

impl FileChannel {
    pub fn size(&self) -> Result<u64, LogError> {
        Ok(self.file.metadata()?.len())
    }

    pub fn position(&mut self, position: u64) {
        self.position = position;
    }

    pub fn close(&self) -> Result<(), LogError> {
        Ok(())
    }
}
