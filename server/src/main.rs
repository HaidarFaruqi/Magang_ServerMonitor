use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde_json::json;

async fn tanya_ai(log: &str) -> Result<String, reqwest::Error> {
    let client = reqwest::Client::new();
    let res = client
        .post("http://host.docker.internal:11434/api/generate")
        .json(&json!({
            "model": "llama3.2:1b",
            "prompt": format!("Kamu adalah Senior SRE. Analisis log ini dan berikan instruksi perbaikan singkat (max 1 baris): {}", log),
            "stream": false
        }))
        .send()
        .await?;

    let body: serde_json::Value = res.json().await?;
    Ok(body["response"].as_str().unwrap_or("Gagal mendapat respon AI").to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "monitoring-group")
        .set("bootstrap.servers", "redpanda:9092")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&["system-logs"])?;

    println!("Central Server aktif. Menunggu stream dari Redpanda...");

    while let Ok(message) = consumer.recv().await {
        let payload = match message.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(_)) => continue,
        };

        println!("LOG DITERIMA: {}", payload);

        // Hanya proses ERROR untuk AI agar tidak kelebihan beban
        if payload.contains("ERROR") {
            println!(">> Mendeteksi anomali. Konsultasi ke AI...");
            match tanya_ai(payload).await {
                Ok(saran) => println!(">> SARAN AI: {}", saran),
                Err(e) => eprintln!(">> Gagal kontak AI: {}", e),
            }
        }
    }
    Ok(())
}