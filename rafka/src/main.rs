use rafka_core::server::kafka_server::KafkaServer;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

fn main() {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut kafka_server = KafkaServer::default();
    kafka_server.startup();
}
