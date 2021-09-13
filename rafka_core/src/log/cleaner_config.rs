//! From core/src/main/scala/kafka/log/CleanerConfig.scala
//! Config parameters for the log cleaner

// numThreads The number of cleaner threads to run
// dedupeBufferSize The total memory used for log deduplication
// dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
// maxMessageSize The maximum size of a message that can appear in the log
// maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
// backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
// enableCleaner Allows completely disabling the log cleaner
// hashAlgorithm The hash algorithm to use in key comparison.

pub struct CleanerConfig {
    num_threads: i32,
    dedupe_buffer_size: u64,
    dedupe_buffer_load_factor: f64,
    io_buffer_size: i32,
    max_message_size: i32,
    max_io_bytes_per_second: f64,
    back_off_ms: u64,
    enable_cleaner: bool,
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
