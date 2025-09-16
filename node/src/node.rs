// node/src/node.rs

use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block, Consensus, ConsensusError, Protocol};
use crypto::{SignatureService};
use log::{error, info, warn};
use mempool::{Mempool, MempoolError};
use store::{Store, StoreError};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};


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
            // NƠI BẠN CÓ THỂ XỬ LÝ LOGIC GỬI GIAO DỊCH ĐI NƠI KHÁC
            // Dữ liệu block đầy đủ có sẵn trong biến `_block`
            info!(
                "Block Committed - Epoch: {}, Height: {}, Payload Digests: {}",
                _block.epoch, _block.height, _block.payload.len()
            );

            // TODO: Thêm logic của bạn ở đây để lấy full transaction từ store và gửi đi.
        }
    }
}