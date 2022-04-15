use anyhow::Result;
use clap::{App, Arg};
use rafka_core::majordomo::MajordomoCoordinator;
use rafka_core::server::kafka_config::KafkaConfigProperties;
use rafka_core::server::kafka_server::KafkaServer;
use rafka_core::zk::kafka_zk_client::KafkaZkClientCoordinator;
use std::time::Instant;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::Level;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    match main_processor().await {
        Ok(()) => info!("Exiting successfully."),
        Err(err) => error!("Exiting with error: {:?}", err),
    }
}

async fn main_processor() -> Result<()> {
    let matches = App::new("Rafka")
        .version("0.0")
        .author("Seb Ospina <kraige@gmail.com>")
        .about("A dive into kafka using rust")
        .arg(Arg::new("INPUT").help("Sets the input config file to use").required(true).index(1))
        .arg(
            Arg::new("verbosity_level")
                .short('v')
                .default_value("trace")
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::new("override")
                .short('o')
                .multiple_occurrences(true)
                .help("Override properties defined in the config file"),
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
    let mut kafka_config = match KafkaConfigProperties::read_config_file(config_file) {
        Ok(config) => config,
        Err(err) => {
            error!("Unable to use config file {}: {:?}", config_file, err);
            std::process::exit(1);
        },
    };
    if let Some(property_overrides) = matches.values_of("override") {
        for override_property in property_overrides {
            if let Some((property_name, property_value)) = override_property.split_once('=') {
                kafka_config.try_set_property(property_name, property_value)?;
            }
        }
    }
    let kafka_config = kafka_config.build()?;
    let (kafka_server_tx, kafka_server_rx) = mpsc::channel(4_096); // TODO: Magic number removal
    let kafka_config_clone = kafka_config.clone();
    let mut kafka_zk = KafkaZkClientCoordinator::new(kafka_config.clone()).await.unwrap();
    let (majordomo_tx, majordomo_rx) = mpsc::channel(4096); // TODO: Magic number removal
    let majordomo_tx_clone = majordomo_tx.clone();
    tokio::spawn(async move {
        LogManagerCoordinator::new(kafka_config_clone, majordomo_tx_clone).await?
    });
    let kafka_config_clone = kafka_config.clone();
    let majordomo_tx_clone = majordomo_tx.clone();
    tokio::spawn(async move {
        let mut kafka_server = KafkaServer::new(
            kafka_config_clone,
            Instant::now(),
            majordomo_tx_clone.clone(),
            kafka_server_rx,
        );
        kafka_server.init();
        match kafka_server.startup().await {
            Ok(()) => {
                info!("Kafka startup Complete");
                kafka_server.process_message_queue().await.unwrap();
            },
            Err(err) => {
                error!("Exiting. Kafka startup failed: {:?}", err);
                rafka_core::majordomo::MajordomoCoordinator::shutdown(majordomo_tx_clone).await;
            },
        };
    });
    // Start the main messaging bus
    let kafka_zk_async_tx = kafka_zk.main_tx();
    let majordomo_tx_clone = majordomo_tx.clone();
    tokio::spawn(async move {
        MajordomoCoordinator::init_coordinator_thread(
            kafka_config.clone(),
            kafka_zk_async_tx,
            majordomo_tx_clone,
            majordomo_rx,
        )
        .await
        .unwrap();
    });
    kafka_zk.process_message_queue().await.unwrap();
    tokio::spawn(async {
        signal::ctrl_c().await.unwrap();
        info!("ctrl-c received!");
        rafka_core::majordomo::MajordomoCoordinator::shutdown(majordomo_tx).await;
        rafka_core::server::kafka_server::KafkaServer::shutdown(kafka_server_tx).await;
    });
    Ok(())
}
