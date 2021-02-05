use clap::{App, Arg};
use rafka_core::server::kafka_config::KafkaConfig;
use rafka_core::server::kafka_server::KafkaServer;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::Level;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
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
        .arg(
            Arg::with_name("verbosity_level")
                .short("v")
                .default_value("trace")
                .help("Sets the level of verbosity"),
        )
        .get_matches();
    let verbosity = matches.value_of("verbosity_level").unwrap();
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(verbosity.parse::<Level>().unwrap())
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let config_file = matches.value_of("INPUT").unwrap();
    println!("Using input file: {}", config_file);
    let (main_tx, main_rx) = mpsc::channel(4_096); // TODO: Magic number removal
    let kafka_config = KafkaConfig::get_kafka_config(config_file).unwrap();
    let kafka_config_tmp = kafka_config.clone();
    tokio::spawn(async move {
        let mut kafka_server = KafkaServer::new(kafka_config_tmp, Instant::now(), main_tx);
        match kafka_server.startup().await {
            Ok(()) => info!("Kafka startup Complete"),
            Err(err) => error!("Kafka startup failed: {:?}", err),
        };
    });
    rafka_core::tokio::async_coordinator(kafka_config, main_rx).await;
}
