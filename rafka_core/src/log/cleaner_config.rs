//! From core/src/main/scala/kafka/log/CleanerConfig.scala
//! Config parameters for the log cleaner

#[derive(Debug)]
pub struct CleanerConfig {
    /// The number of cleaner threads to run
    num_threads: i32,
    /// The total memory used for log deduplication
    ded/upe_buffer_size: u64,
    /// The maximum percent full for the deduplication buffer
    dedupe_buffer_load_factor: f64,
    io_buffer_size: i32,
    /// The maximum size of a message that can appear in the log
    max_message_size: i32,
    /// The maximum read and write I/O that all cleaner threads are allowed to do
    max_io_bytes_per_second: f64,
    /// The amount of time to wait before rechecking if no logs are eligible for cleaning
    back_off_ms: u64,
    /// Allows completely disabling the log cleaner
    enable_cleaner: bool,
    /// The hash algorithm to use in key comparison.
    hash_algorithm: String,
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            num_threads: 1,
            dedupe_buffer_size: 4 * 1024 * 1024,
            dedupe_buffer_load_factor: 0.9, // Used to be 0.9d, what is d?
            io_buffer_size: 1024 * 1024,
            max_message_size: 32 * 1024 * 1024,
            max_io_bytes_per_second: f64::MAX,
            back_off_ms: 15 * 1000,
            enable_cleaner: true,
            hash_algorithm: String::from("MD5"),
        }
    }
}
