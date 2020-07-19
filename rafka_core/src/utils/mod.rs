pub mod kafka_scheduler;
pub fn default_logger() -> slog::Logger {
    // By default, discard the logs.
    let drain = slog::Discard;
    let root = slog::Logger::root(drain, o!());
    root
}
