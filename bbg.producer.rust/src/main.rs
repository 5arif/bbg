use axum::{routing::post, Router, extract::State};
use rskafka::client::{ClientBuilder, partition::UnknownTopicHandling};
use rskafka::record::Record;
use std::sync::Arc;
use chrono::Utc;

// 1. Definisikan State untuk berbagi Producer antar thread API
// Di rskafka, kita biasanya berinteraksi dengan PartitionClient untuk pengiriman ke partisi spesifik
struct AppState {
    partition_client: Arc<rskafka::client::partition::PartitionClient>,
}

#[tokio::main]
async fn main() {
    // Inisialisasi logging
    tracing_subscriber::fmt::init();

    // 2. Konfigurasi Kafka Client
    let kafka_broker = "localhost:9092".to_string();
    let client = ClientBuilder::new(vec![kafka_broker])
        .build()
        .await
        .expect("Gagal membuat Kafka client");

    let topic = "orders.events";
    
    // Mendapatkan partition client (menggunakan partisi 0 secara default)
    let partition_client = Arc::new(client
        .partition_client(
            topic.to_string(),
            0,
            UnknownTopicHandling::Retry,
        )
        .await
        .expect("Gagal mendapatkan partition client"));

    let shared_state = Arc::new(AppState {
        partition_client,
    });

    // 3. Setup Routing API
    let app = Router::new()
        .route("/send", post(kafka_handler))
        .with_state(shared_state);

    // 4. Jalankan Server HTTP
    let addr = "0.0.0.0:8100";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("🚀 Server berjalan di http://{}", addr);
    
    axum::serve(listener, app).await.unwrap();
}

// 5. Handler untuk memproses HTTP Post
async fn kafka_handler(
    State(state): State<Arc<AppState>>,
    body: String,
) -> String {
    let record = Record {
        key: Some(b"producer-rust".to_vec()),
        value: Some(body.into_bytes()),
        headers: std::collections::BTreeMap::new(),
        timestamp: Utc::now(),
    };

    // Kirim secara asinkron ke Kafka
    // rskafka::produce menerima Vec<Record>
    match state.partition_client.produce(vec![record], rskafka::client::partition::Compression::default()).await {
        Ok(offsets) => {
            format!("✅ Berhasil! Offset: {:?}", offsets)
        },
        Err(e) => {
            format!("❌ Gagal mengirim ke Kafka: {:?}", e)
        }
    }
}