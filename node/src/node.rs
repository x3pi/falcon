use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Consensus, ConsensusError, Protocol};
use crypto::SignatureService;
use log::{info, warn};
use mempool::{Mempool, MempoolError};
use store::{Store, StoreError};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use consensus::Block;
use tokio::net::TcpStream;      // <<-- Thêm dòng này
use tokio::io::AsyncWriteExt;    // <<-- Thêm dòng này

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
    go_connection: Option<TcpStream>, // <<-- THÊM TRƯỜNG MỚI

}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, NodeError> {
        let (tx_commit, rx_commit) = channel(10000); //commit channel
        let (tx_consensus, rx_consensus) = channel(10000); // 协议交流消息
        let (tx_consensus_mempool, rx_consensus_mempool) = channel(10000);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        info!("committee {:?}", committee);
        let secret = Secret::read(key_file)?;
        let name = secret.name; // This is the public key, used as node ID.

        // Derive the wallet address directly from the secret key for verification.
        let public_key_from_secret = secret.secret.to_public();
        let address = public_key_from_secret.to_address();

        // Security check: ensure the public key in the file matches the derived one.
        assert_eq!(name.0, public_key_from_secret.0, "Public key in keyfile does not match the one derived from the secret key!");
        
        let secret_key = secret.secret; // The secret key will be moved later.

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path)?;

        // Run the signature service.
        let signature_service =
            SignatureService::new(secret_key);

        let protocol = match parameters.protocol {
            0 => Protocol::FlexHBBFT,
            _ => {
                warn!("Undefined protocol type!");
                Protocol::Others
            }
        };

        // Make a new mempool.
        Mempool::run(
            //用于交易的缓存
            name,               //公钥->ID
            committee.mempool,  // 节点信息
            parameters.mempool, // mempool参数
            store.clone(),
            signature_service.clone(),
            tx_consensus.clone(), //LOOPBACK
            rx_consensus_mempool, //Get ,Verify,Clean
        )?;

        // Run the consensus core.
        Consensus::run(
            name,
            committee.consensus,
            parameters.consensus,
            store.clone(),
            signature_service,
            tx_consensus,
            rx_consensus,
            tx_consensus_mempool,
            tx_commit,
            protocol,
        )
        .await?;

        info!("Node {} successfully booted", name);
        info!("Wallet Address: {}", address); // Log the derived address.
        Ok(Self { 
            commit: rx_commit,
            go_connection: None,
        })
    }

    pub fn print_key_file(filename: &str) -> Result<(), NodeError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(block) = self.commit.recv().await {
            // Log thông báo cam kết khối như cũ
            info!(
                "Block Committed - Epoch: {}, Height: {}",
                block.epoch, block.height
            );

            // --- BẮT ĐẦU LOGIC GỬI SANG GO ---

            // 1. Kiểm tra và thiết lập kết nối đến Go server nếu chưa có.
            if self.go_connection.is_none() {
                match TcpStream::connect("127.0.0.1:9001").await {
                    Ok(stream) => {
                        info!("Successfully connected to Go server on port 9001");
                        self.go_connection = Some(stream);
                    }
                    Err(e) => {
                        warn!("Failed to connect to Go server: {}. Retrying on next block.", e);
                        continue; // Bỏ qua lần gửi này và thử lại ở khối tiếp theo
                    }
                }
            }
            
            // 2. Gửi khối đi qua kết nối TCP.
            if let Some(stream) = &mut self.go_connection {
                // Tuần tự hóa (serialize) đối tượng Block thành chuỗi JSON.
                let block_json = match serde_json::to_vec(&block) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize block to JSON: {}", e);
                        continue; // Bỏ qua nếu không serialize được
                    }
                };

                // Gửi theo định dạng: [độ dài 4-byte][dữ liệu JSON]
                let len = block_json.len() as u32;

                // Gửi độ dài trước
                if let Err(e) = stream.write_u32(len).await {
                    warn!("Failed to send data to Go server (connection lost): {}. Resetting connection.", e);
                    self.go_connection = None; // Reset kết nối để thử lại lần sau
                    continue;
                }
                
                // Sau đó gửi dữ liệu
                if let Err(e) = stream.write_all(&block_json).await {
                    warn!("Failed to send data to Go server (connection lost): {}. Resetting connection.", e);
                    self.go_connection = None; // Reset kết nối
                }
            }
            // --- KẾT THÚC LOGIC GỬI SANG GO ---
        }
    }
}