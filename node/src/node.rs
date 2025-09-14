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
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
// --- Không cần import Arc và Mutex nữa ---

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

// ---- BẮT ĐẦU CẢI TIẾN LOGIC NOTIFIER ----

type GoNotificationMsg = (Vec<Digest>, SeqNumber, SeqNumber);

#[derive(Serialize)]
struct GoNotificationPayload {
    #[serde(rename = "Epoch")]
    epoch: SeqNumber,
    #[serde(rename = "Height")]
    height: SeqNumber,
    #[serde(rename = "Transactions")]
    transactions: Vec<Vec<u8>>,
}

const GO_SERVICE_ADDR: &str = "127.0.0.1:9002";
const NUM_NOTIFIER_WORKERS: usize = 4;

// Worker giờ nhận một Receiver của riêng nó.
async fn go_worker_task(id: usize, mut work_receiver: Receiver<GoNotificationMsg>) {
    let mut stream: Option<TcpStream> = None;
    info!("[GoWorker-{}] Task started.", id);

    // Vòng lặp chính đơn giản là nhận công việc từ kênh riêng.
    while let Some(job) = work_receiver.recv().await {
        let (digests, epoch, height) = job;

        if stream.is_none() {
            match TcpStream::connect(GO_SERVICE_ADDR).await {
                Ok(s) => {
                    info!("[GoWorker-{}] Connected to Go service.", id);
                    stream = Some(s);
                }
                Err(e) => {
                    warn!("[GoWorker-{}] Failed to connect: {}. Dropping notification (E:{}, H:{}).", id, e, epoch, height);
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        }
        
        if let Some(s) = stream.as_mut() {
            let payload = GoNotificationPayload {
                epoch,
                height,
                transactions: digests.into_iter().map(|d| d.0.to_vec()).collect(),
            };

            let json_data = match serde_json::to_vec(&payload) {
                Ok(json) => json,
                Err(e) => {
                    error!("[GoWorker-{}] Failed to serialize: {}", id, e);
                    continue;
                }
            };
            let len_bytes = (json_data.len() as u32).to_be_bytes();

            if s.write_all(&len_bytes).await.is_err() || s.write_all(&json_data).await.is_err() {
                warn!("[GoWorker-{}] Connection lost. Will reconnect.", id);
                stream = None;
            } else {
                info!("[GoWorker-{}] Sent notification (E:{}, H:{}).", id, epoch, height);
            }
        }
    }
    info!("[GoWorker-{}] Task shutting down.", id);
}

// Dispatcher phân phát công việc cho các worker theo kiểu round-robin.
async fn go_dispatcher_task(
    mut rx_from_core: Receiver<GoNotificationMsg>,
    worker_senders: Vec<Sender<GoNotificationMsg>>,
) {
    let mut next_worker = 0;
    info!("[GoDispatcher] Task started.");

    while let Some(notification) = rx_from_core.recv().await {
        let worker_sender = &worker_senders[next_worker];
        if let Err(e) = worker_sender.send(notification).await {
            error!("[GoDispatcher] Failed to send job to worker {}: {}. Channel closed.", next_worker, e);
            // Có thể thêm logic để loại bỏ worker bị lỗi khỏi danh sách
        }
        
        // Chuyển sang worker tiếp theo cho lần lặp sau.
        next_worker = (next_worker + 1) % worker_senders.len();
    }
    info!("[GoDispatcher] Task shutting down.");
}

// Hàm khởi tạo tạo ra các kênh riêng biệt.
fn setup_go_notifier(rx_from_core: Receiver<GoNotificationMsg>) {
    let mut worker_senders = Vec::new();

    // Khởi tạo các Worker, mỗi worker có một kênh riêng.
    for i in 0..NUM_NOTIFIER_WORKERS {
        let (tx, rx) = channel(1000); // Mỗi worker có buffer riêng
        worker_senders.push(tx);
        tokio::spawn(go_worker_task(i, rx));
    }

    // Khởi tạo Dispatcher với danh sách các đầu gửi của worker.
    tokio::spawn(go_dispatcher_task(rx_from_core, worker_senders));
    
    info!("Go Notifier system with {} workers (channel-only) has been initialized.", NUM_NOTIFIER_WORKERS);
}

// --- (Phần còn lại của file giữ nguyên) ---

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

        let (tx_commit_notification, rx_commit_notification) = channel(10000);
        setup_go_notifier(rx_commit_notification);

        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        let store = Store::new(store_path)?;
        let signature_service = SignatureService::new(secret_key);
        let protocol = match parameters.protocol {
            0 => Protocol::FlexHBBFT,
            _ => {
                warn!("Undefined protocol type!");
                Protocol::Others
            }
        };

        Mempool::run(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            signature_service.clone(),
            tx_consensus.clone(),
            rx_consensus_mempool,
        )?;

        Consensus::run(
            name,
            committee.consensus,
            parameters.consensus,
            store.clone(),
            signature_service,
            tx_consensus,
            rx_consensus,
            tx_consensus_mempool,
            tx_commit.clone(),
            tx_commit_notification,
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
            info!(
                "Block Committed - Epoch: {}, Height: {}",
                _block.epoch, _block.height
            );
        }
    }
}