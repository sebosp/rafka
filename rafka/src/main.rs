use clap::{App, Arg};
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

    let matches = App::new("Rafka")
        .version("0.0")
        .author("Seb Ospina <kraige@gmail.com>")
        .about("A dive into kafka using rust")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input config file to use")
                .required(true)
                .index(1),
        )
        .arg(Arg::with_name("v").short("v").multiple(true).help("Sets the level of verbosity"))
        .get_matches();
    let mut kafka_server = KafkaServer::default();
    kafka_server.startup();
}
