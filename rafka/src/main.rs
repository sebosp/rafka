use clap::{App, Arg};
use rafka_core::majordomo::Coordinator;
use rafka_core::server::kafka_config::KafkaConfig;
use rafka_core::server::kafka_server::KafkaServer;
use rafka_core::zk::kafka_zk_client::KafkaZkClient;
use std::time::Instant;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
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
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let kafka_config = KafkaConfig::get_kafka_config(config_file).unwrap();
    let (kfk_zk_tx, kafka_zk_rx) = mpsc::channel(4_096); // TODO: Magic number removal
    let kafka_zk_thread_handle = KafkaZkClient::coordinator(kafka_config.clone(), kafka_zk_rx);
    let mut majordomo = Coordinator::new(kafka_config.clone(), kfk_zk_tx);
    let kafka_server_async_tx = majordomo.main_tx();
    tokio::spawn(async move {
        let mut kafka_server =
            KafkaServer::new(kafka_config, Instant::now(), kafka_server_async_tx, shutdown_rx);
        match kafka_server.startup().await {
            Ok(()) => info!("Kafka startup Complete"),
            Err(err) => error!("Kafka startup failed: {:?}", err),
        };
        kafka_server.wait_for_shutdown().await;
    });
    // Start the main messaging bus
    majordomo.async_coordinator().await.unwrap();
    let coordinator_shutdown_tx_handle = majordomo.main_tx();
    tokio::spawn(async {
        signal::ctrl_c().await.unwrap();
        error!("ctrl-c received!");
        rafka_core::majordomo::Coordinator::shutdown(coordinator_shutdown_tx_handle).await;
        shutdown_tx.send(()).unwrap();
    });
}
