// node/src/node.rs

use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block, Consensus, ConsensusError, Protocol, SeqNumber};
use crypto::{SignatureService};
use log::{error, info, warn};
use mempool::{Mempool, MempoolError};
use serde::Serialize;
use store::{Store, StoreError};
use thiserror::Error;
// ---- BẮT ĐẦU THAY ĐỔI ----
// Import cả TcpStream và UnixStream
use tokio::io::AsyncWriteExt;
#[cfg(not(unix))]
use tokio::net::{TcpStream};

#[cfg(unix)]
use tokio::net::{UnixStream};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
// ---- KẾT THÚC THAY ĐỔI ----


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

type GoNotificationMsg = (Vec<Vec<u8>>, SeqNumber, SeqNumber);

#[derive(Serialize)]
struct GoNotificationPayload {
    #[serde(rename = "Epoch")]
    epoch: SeqNumber,
    #[serde(rename = "Height")]
    height: SeqNumber,
    #[serde(rename = "Transactions")]
    transactions: Vec<Vec<u8>>,
}

// ---- BẮT ĐẦU THAY ĐỔI ----
// Địa chỉ cho Unix Domain Socket
#[cfg(unix)]
const GO_UDS_ADDR: &str = "/tmp/consensus.sock";
// Địa chỉ TCP dự phòng cho các hệ điều hành không hỗ trợ UDS
#[cfg(not(unix))]
const GO_TCP_ADDR: &str = "127.0.0.1:9002";
// ---- KẾT THÚC THAY ĐỔI ----

const NUM_NOTIFIER_WORKERS: usize = 4;

// ---- PHIÊN BẢN WORKER SỬ DỤNG UNIX DOMAIN SOCKET ----
// Chỉ biên dịch hàm này trên các hệ điều hành Unix-like (Linux, macOS, etc.)
#[cfg(unix)]
async fn go_worker_task(id: usize, mut work_receiver: Receiver<GoNotificationMsg>) {
    let mut stream: Option<UnixStream> = None;
    info!("[GoWorker-{}] Task started (using Unix Domain Socket).", id);

    while let Some(job) = work_receiver.recv().await {
        let (digests, epoch, height) = job;

        // Nếu chưa kết nối, thử kết nối lại.
        if stream.is_none() {
            match UnixStream::connect(GO_UDS_ADDR).await {
                Ok(s) => {
                    info!("[GoWorker-{}] Connected to Go service via UDS.", id);
                    stream = Some(s);
                }
                Err(e) => {
                    warn!("[GoWorker-{}] Failed to connect to UDS ({}): {}. Dropping notification (E:{}, H:{}).", id, GO_UDS_ADDR, e, epoch, height);
                    // Chờ một chút trước khi thử lại ở lần gửi tiếp theo.
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        }
        
        // Gửi dữ liệu nếu đã có kết nối.
        if let Some(s) = stream.as_mut() {
            let payload = GoNotificationPayload {
                epoch,
                height,
                transactions: digests,
            };

            let json_data = match serde_json::to_vec(&payload) {
                Ok(json) => json,
                Err(e) => {
                    error!("[GoWorker-{}] Failed to serialize notification: {}", id, e);
                    continue;
                }
            };
            let len_bytes = (json_data.len() as u32).to_be_bytes();

            // Gửi độ dài và sau đó là dữ liệu JSON.
            if s.write_all(&len_bytes).await.is_err() || s.write_all(&json_data).await.is_err() {
                warn!("[GoWorker-{}] UDS connection lost. Will attempt to reconnect on next message.", id);
                stream = None; // Đặt lại để kết nối lại ở lần sau.
            } else {
                info!("[GoWorker-{}] Sent notification via UDS (E:{}, H:{}).", id, epoch, height);
            }
        }
    }
    info!("[GoWorker-{}] Task shutting down.", id);
}

// ---- PHIÊN BẢN DỰ PHÒNG SỬ DỤNG TCP SOCKET ----
// Chỉ biên dịch hàm này trên các hệ điều hành KHÔNG phải Unix (ví dụ: Windows).
#[cfg(not(unix))]
async fn go_worker_task(id: usize, mut work_receiver: Receiver<GoNotificationMsg>) {
    warn!("[GoWorker-{}] Unix Domain Sockets not supported on this OS. Falling back to TCP.", id);
    let mut stream: Option<TcpStream> = None;
    info!("[GoWorker-{}] Task started (using TCP Socket).", id);

    while let Some(job) = work_receiver.recv().await {
        let (digests, epoch, height) = job;

        if stream.is_none() {
            match TcpStream::connect(GO_TCP_ADDR).await {
                Ok(s) => {
                    info!("[GoWorker-{}] Connected to Go service via TCP.", id);
                    stream = Some(s);
                }
                Err(e) => {
                    warn!("[GoWorker-{}] Failed to connect via TCP ({}): {}. Dropping notification (E:{}, H:{}).", id, GO_TCP_ADDR, e, epoch, height);
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
                    error!("[GoWorker-{}] Failed to serialize notification: {}", id, e);
                    continue;
                }
            };
            let len_bytes = (json_data.len() as u32).to_be_bytes();

            if s.write_all(&len_bytes).await.is_err() || s.write_all(&json_data).await.is_err() {
                warn!("[GoWorker-{}] TCP connection lost. Will attempt to reconnect on next message.", id);
                stream = None;
            } else {
                info!("[GoWorker-{}] Sent notification via TCP (E:{}, H:{}).", id, epoch, height);
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
            error!("[GoDispatcher] Failed to send job to worker {}: {}. Channel may be closed.", next_worker, e);
        }
        
        // Chuyển sang worker tiếp theo cho lần lặp sau.
        next_worker = (next_worker + 1) % worker_senders.len();
    }
    info!("[GoDispatcher] Task shutting down as input channel from core has closed.");
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
    
    info!("Go Notifier system with {} workers has been initialized.", NUM_NOTIFIER_WORKERS);
}

// ---- (Phần còn lại của file giữ nguyên) ----

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

        // Kênh để core gửi tín hiệu commit sang hệ thống notifier
        let (tx_commit_notification, rx_commit_notification) = channel(10000);
        setup_go_notifier(rx_commit_notification);

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

        // Chạy Consensus
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
            tx_commit_notification, // Truyền kênh notifier vào
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
            // Log này hữu ích để xác nhận các khối đang được commit ở phía Rust.
            info!(
                "Block Committed - Epoch: {}, Height: {}",
                _block.epoch, _block.height
            );
        }
    }
}