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