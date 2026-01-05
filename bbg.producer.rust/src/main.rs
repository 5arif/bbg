use axum::{routing::post, Router, extract::State};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;

// 1. Definisikan State untuk berbagi Producer antar thread API
struct AppState {
    producer: FutureProducer,
    topic: String,
}

#[tokio::main]
async fn main() {
    // Inisialisasi logging
    tracing_subscriber::fmt::init();

    // 2. Konfigurasi Kafka Producer
    let kafka_broker = "localhost:9092";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_broker)
        .set("message.timeout.ms", "5000")
        .set("linger.ms", "50") // Batching untuk efisiensi
        .create()
        .expect("Gagal membuat Kafka producer");

    let shared_state = Arc::new(AppState {
        producer,
        topic: "orders.events".to_string(),
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
    let record = FutureRecord::to(&state.topic)
        .payload(&body)
        .key("producer-rust");

    // Kirim secara asinkron ke Kafka
    match state.producer.send(record, Duration::from_secs(0)).await {
        Ok(delivery) => {
            format!("✅ Berhasil! Partisi: {}, Offset: {}", delivery.0, delivery.1)
        },
        Err((e, _)) => {
            format!("❌ Gagal mengirim ke Kafka: {:?}", e)
        }
    }
}