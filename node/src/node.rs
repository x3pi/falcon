// node/src/node.rs

use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block, Consensus, ConsensusError, Protocol, SeqNumber};
use crypto::{Digest, SignatureService};
use log::{error, info, warn};
use mempool::{Mempool, MempoolError};
use serde::Serialize;
use store::{Store, StoreError};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::{sleep, Duration};

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error(transparent)]
    ConsensusError(#[from] ConsensusError),

    #[error(transparent)]
    MempoolError(#[from] MempoolError),
}

// ---- BẮT ĐẦU ĐỊNH NGHĨA LOGIC NOTIFIER ----

// Struct để gửi đi, giống hệt struct bên Go
#[derive(Serialize)]
struct GoNotification {
    #[serde(rename = "Epoch")]
    epoch: SeqNumber,
    #[serde(rename = "Height")]
    height: SeqNumber,
    #[serde(rename = "Transactions")]
    transactions: Vec<Vec<u8>>, // Sẽ rỗng nếu là block rỗng
}

// Task chạy nền để gửi thông báo sang Go
async fn go_notifier_task(mut rx: Receiver<(Vec<Digest>, SeqNumber, SeqNumber)>) {
    const GO_SERVICE_ADDR: &str = "127.0.0.1:9002";
    let mut stream: Option<TcpStream> = None;

    info!("[GoNotifier] Task started.");
    while let Some((digests, epoch, height)) = rx.recv().await {
        // Cố gắng kết nối lại nếu bị mất kết nối
        if stream.is_none() {
            match TcpStream::connect(GO_SERVICE_ADDR).await {
                Ok(s) => {
                    info!(
                        "[GoNotifier] Connected to Go service at {}",
                        GO_SERVICE_ADDR
                    );
                    stream = Some(s);
                }
                Err(e) => {
                    warn!(
                        "[GoNotifier] Failed to connect to Go service: {}. Retrying in 5s.",
                        e
                    );
                    sleep(Duration::from_secs(5)).await;
                    continue; // Bỏ qua thông báo lần này và thử kết nối lại
                }
            }
        }

        // Gửi dữ liệu
        if let Some(s) = stream.as_mut() {
            let notification = GoNotification {
                epoch,
                height,
                // Chuyển đổi Digest thành Vec<u8>
                transactions: digests.into_iter().map(|d| d.0.to_vec()).collect(),
            };

            let json_data = match serde_json::to_vec(&notification) {
                Ok(json) => json,
                Err(e) => {
                    error!("[GoNotifier] Failed to serialize notification: {}", e);
                    continue;
                }
            };
            let len_bytes = (json_data.len() as u32).to_be_bytes();

            if s.write_all(&len_bytes).await.is_err() || s.write_all(&json_data).await.is_err() {
                warn!("[GoNotifier] Connection to Go service lost. Will try to reconnect.");
                stream = None; // Đặt lại để vòng lặp sau kết nối lại
            } else {
                info!(
                    "[GoNotifier] Sent notification for block (E:{}, H:{}) to Go.",
                    epoch, height
                );
            }
        }
    }
    info!("[GoNotifier] Task shutting down.");
}

pub struct Node {
    pub commit: Receiver<Block>,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, NodeError> {
        let (tx_commit, rx_commit) = channel(10000);
        let (tx_consensus, rx_consensus) = channel(10000);
        let (tx_consensus_mempool, rx_consensus_mempool) = channel(10000);

        // ---- TẠO KÊNH VÀ TASK NOTIFIER MỚI ----
        let (tx_commit_notification, rx_commit_notification) = channel(10000);
        tokio::spawn(go_notifier_task(rx_commit_notification));
        // ---- KẾT THÚC ----

        // Đọc cấu hình
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Khởi tạo các thành phần
        let store = Store::new(store_path)?;
        let signature_service = SignatureService::new(secret_key);
        let protocol = match parameters.protocol {
            0 => Protocol::FlexHBBFT,
            _ => {
                warn!("Undefined protocol type!");
                Protocol::Others
            }
        };

        // Chạy Mempool
        Mempool::run(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            signature_service.clone(),
            tx_consensus.clone(),
            rx_consensus_mempool,
        )?;

        // Chạy Consensus, truyền kênh notifier vào
        Consensus::run(
            name,
            committee.consensus,
            parameters.consensus,
            store.clone(),
            signature_service,
            tx_consensus,
            rx_consensus,
            tx_consensus_mempool,
            tx_commit.clone(),      // Kênh này vẫn dùng để nhận block đã commit
            tx_commit_notification, // Kênh mới cho notifier
            protocol,
        )
        .await?;

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), NodeError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
            info!(
                "Block Committed - Epoch: {}, Height: {}",
                _block.epoch, _block.height
            );
        }
    }
}
