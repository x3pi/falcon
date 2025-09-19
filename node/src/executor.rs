// node/src/executor.rs

use consensus::CommittedEpochData;
use log::{info};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc::Receiver;
use anyhow::{Result, Context};

pub struct Executor {
    receiver: Receiver<CommittedEpochData>,
    socket_path: PathBuf,
}

impl Executor {
    pub fn new(receiver: Receiver<CommittedEpochData>, socket_path: String) -> Self {
        Self {
            receiver,
            socket_path: PathBuf::from(socket_path),
        }
    }

    /// Kết nối, nối tiếp hóa và gửi toàn bộ thông tin epoch qua UDS.
    async fn send_epoch_data(&self, epoch_data: CommittedEpochData) -> Result<()> {
        info!(
            "Executor: Connecting to socket at {:?}",
            self.socket_path.display()
        );

        let mut stream = UnixStream::connect(&self.socket_path).await.context("Failed to connect to socket")?;
        
        let serialized_data = serde_json::to_vec(&epoch_data).context("Failed to serialize epoch data with JSON")?;
        
        info!(
            "Executor: Sending {} bytes of JSON data for epoch {}.",
            serialized_data.len(),
            epoch_data.epoch
        );
        stream.write_all(&serialized_data).await.context("Failed to write to socket")?;

        Ok(())
    }

    /// Vòng lặp chính của Executor: chờ dữ liệu từ Consensus và xử lý.
    pub async fn run(&mut self) -> Result<()> {
        info!("Executor is running and waiting for committed epoch data...");
        while let Some(epoch_data) = self.receiver.recv().await {
            if !epoch_data.blocks.is_empty() {
                self.send_epoch_data(epoch_data).await?;
            }
        }
        Ok(())
    }
}