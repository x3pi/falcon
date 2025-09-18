// node/src/executor.rs

use log::{error, info};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc::Receiver;

// Định nghĩa kiểu dữ liệu để dễ đọc
type Transaction = Vec<u8>;
type TransactionList = Vec<Transaction>;

pub struct Executor {
    receiver: Receiver<TransactionList>,
    socket_path: PathBuf,
}

impl Executor {
    pub fn new(receiver: Receiver<TransactionList>, socket_path: String) -> Self {
        Self {
            receiver,
            socket_path: PathBuf::from(socket_path),
        }
    }

    /// Kết nối, nối tiếp hóa và gửi danh sách giao dịch qua UDS.
    async fn send_transactions(&self, transactions: TransactionList) {
        info!(
            "Executor: Connecting to socket at {:?}",
            self.socket_path.display()
        );

        match UnixStream::connect(&self.socket_path).await {
            Ok(mut stream) => {
                match serde_json::to_vec(&transactions) {
                    Ok(serialized_txs) => {
                        info!(
                            "Executor: Sending {} bytes of JSON data.",
                            serialized_txs.len()
                        );
                        if let Err(e) = stream.write_all(&serialized_txs).await {
                            error!("Executor: Failed to write to socket: {}", e);
                        }
                    }
                    Err(e) => error!("Executor: Failed to serialize transactions with JSON: {}", e), 
                }
            }
            Err(e) => error!("Executor: Failed to connect to socket: {}", e),
        }
    }

    /// Vòng lặp chính của Executor: chờ dữ liệu từ Consensus và xử lý.
    pub async fn run(&mut self) {
        info!("Executor is running and waiting for committed transactions...");
        while let Some(transactions) = self.receiver.recv().await {
            if !transactions.is_empty() {
                self.send_transactions(transactions).await;
            }
        }
    }
}