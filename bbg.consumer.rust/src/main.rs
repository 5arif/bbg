mod consumer;
mod events;

use anyhow::Result;
use clap::Parser;
use tokio::signal;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka brokers (comma-separated)
    #[arg(short, long, default_value = "localhost:9092", env = "KAFKA_BROKERS")]
    brokers: String,

    /// Kafka topic to consume
    #[arg(short, long, default_value = "orders.events", env = "KAFKA_TOPIC")]
    topic: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = Args::parse();
    info!("Starting bbg-consumer-rust with config: {:?}", args);

    // Setup primitive shutdown broadcast
    let (tx, rx) = tokio::sync::broadcast::channel(1);

    // Spawn shutdown listener
    let shutdown_tx = tx.clone();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Ctrl+C received, initiating shutdown");
                let _ = shutdown_tx.send(());
            }
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    if let Err(e) = consumer::order_consumer(&args.brokers, &args.topic, rx).await {
        error!("Consumer error: {:?}", e);
        return Err(e);
    }

    info!("Application exiting gracefully");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests could be added here
}

// Re-importing tracing macro for main visibility if needed (error macro used in main)
// Use tracing error macro
use tracing::error;