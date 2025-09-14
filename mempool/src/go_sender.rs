// mempool/src/go_sender.rs

use crate::core::CommittedTransactions;
use log::{info, warn};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, Duration};

// Địa chỉ của Go service, bạn có thể đưa ra file config nếu muốn
const GO_SERVICE_ADDR: &str = "127.0.0.1:9002";

// Vòng lặp chính cho task gửi dữ liệu
pub async fn run(mut rx: Receiver<CommittedTransactions>) {
    let mut stream: Option<TcpStream> = None;

    // Vòng lặp sẽ cố gắng kết nối lại nếu mất kết nối
    'main_loop: loop {
        // Nếu chưa kết nối, thử kết nối
        if stream.is_none() {
            match TcpStream::connect(GO_SERVICE_ADDR).await {
                Ok(s) => {
                    info!("[GoSender] Connected to Go TX receiver at {}", GO_SERVICE_ADDR);
                    stream = Some(s);
                }
                Err(e) => {
                    warn!("[GoSender] Failed to connect to Go TX receiver: {}. Retrying in 5s.", e);
                    sleep(Duration::from_secs(5)).await;
                    continue; // Thử lại vòng lặp kết nối
                }
            }
        }

        // Chờ nhận dữ liệu từ Mempool Core
        if let Some(committed_data) = rx.recv().await {
            if let Some(s) = stream.as_mut() {
                let json_data = match serde_json::to_vec(&committed_data) {
                    Ok(json) => json,
                    Err(e) => {
                        warn!("[GoSender] Failed to serialize committed transactions: {}", e);
                        continue;
                    }
                };

                let len = json_data.len() as u32;
                let len_bytes = len.to_be_bytes();

                // Gửi độ dài rồi gửi dữ liệu
                if s.write_all(&len_bytes).await.is_err() || s.write_all(&json_data).await.is_err() {
                    warn!("[GoSender] Failed to send tx data to Go (connection lost). Resetting connection.");
                    stream = None; // Đánh dấu để kết nối lại ở lần lặp sau
                } else {
                     info!(
                        "[GoSender] Sent {} transactions from Block (E:{}, H:{}) to Go",
                        committed_data.transactions.len(), committed_data.epoch, committed_data.height
                    );
                }
            }
        } else {
            // Channel đã bị đóng, nghĩa là Mempool Core đã dừng.
            // Thoát khỏi vòng lặp và kết thúc task.
            info!("[GoSender] Mempool core channel closed. Shutting down.");
            break 'main_loop;
        }
    }
}