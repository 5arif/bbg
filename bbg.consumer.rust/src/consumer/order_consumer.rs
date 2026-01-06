use crate::events::OrderEvent;
use anyhow::{Context, Result};
use rskafka::client::{
    partition::{OffsetAt, UnknownTopicHandling},
    ClientBuilder,
};
use tracing::{error, info, warn};

pub async fn order_consumer(
    brokers: &str,
    topic: &str,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    let client = ClientBuilder::new(vec![brokers.to_owned()])
        .build()
        .await
        .context("Failed to build Kafka client")?;

    let partition_client = client
        .partition_client(topic.to_owned(), 0, UnknownTopicHandling::Retry)
        .await
        .with_context(|| format!("Failed to create partition client for topic '{}'", topic))?;

    info!(topic = %topic, partition = 0, "Consumer started and subscribed");

    // Resolve initial offset
    let mut offset = partition_client
        .get_offset(OffsetAt::Latest)
        .await
        .context("Failed to resolve latest offset")?;

    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                info!("Shutdown signal received, stopping consumer");
                break;
            }
            fetch_result = partition_client.fetch_records(offset, 1..1_000_000, 1000) => {
                let (records, _high_watermark) = fetch_result.context("Failed to fetch records from Kafka")?;

                for record in records {
                    if let Some(payload) = &record.record.value {
                        match serde_json::from_slice::<OrderEvent>(payload) {
                            Ok(event) => {
                                info!(?event, "Received and deserialized event");
                            }
                            Err(e) => {
                                error!(error = %e, "Error deserializing event");
                                if let Ok(s) = std::str::from_utf8(payload) {
                                    warn!(payload = %s, "Malformed payload content");
                                }
                            }
                        }
                    }
                    offset = record.offset + 1;
                }
            }
        }
    }

    info!("Consumer loop exited");
    Ok(())
}
