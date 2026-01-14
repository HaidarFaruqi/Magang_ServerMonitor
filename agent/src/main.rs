use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::time::sleep;
use rand::Rng;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_servers = "redpanda:9092";
    let topic = "system-logs";
    let node_name = std::env::var("KUBE_NODE_NAME").unwrap_or_else(|_| "server-prod-01".to_string());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;

    println!("Agent aktif di node: {}. Mengirim log ke Redpanda...", node_name);

    let logs_template = [
        "INFO: User login successful",
        "WARN: High memory usage detected",
        "ERROR: Failed to connect to database",
        "INFO: Background job completed",
        "ERROR: Disk space critical on /var/log",
    ];

    loop {
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..logs_template.len());
        let log_msg = format!("[{}] {}", node_name, logs_template[idx]);

        let record = FutureRecord::to(topic)
            .payload(&log_msg)
            .key(&node_name);

        match producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => (), // Berhasil
            Err((e, _)) => eprintln!("Gagal kirim log: {:?}", e),
        }

        // Simulasi beban tinggi: kirim setiap 500ms
        sleep(Duration::from_millis(500)).await;
    }
}