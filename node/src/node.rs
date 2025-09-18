


use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block,CommittedEpochData, Consensus, ConsensusError, Protocol};
use crypto::{SignatureService};
use log::{info, warn};
use mempool::{Mempool, MempoolError};
use store::{Store, StoreError};
use thiserror::Error;
use crate::executor::Executor;
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
        executor_socket: Option<String>,

    ) -> Result<Self, NodeError> {
        let (tx_commit, rx_commit) = channel(10000); //commit channel
        let (tx_consensus, rx_consensus) = channel(10000); // 协议交流消息
        let (tx_consensus_mempool, rx_consensus_mempool) = channel(10000);
        let (tx_executor, rx_executor) = channel::<CommittedEpochData>(100);

        // 2. KHỞI TẠO VÀ CHẠY EXECUTOR TRONG MỘT TASK RIÊNG
        if let Some(socket_path) = executor_socket {
            info!("Executor is enabled. Will send committed transactions to socket: {}", socket_path);
            let mut executor = Executor::new(rx_executor, socket_path);
            tokio::spawn(async move {
                executor.run().await;
            });
        } else {
            warn!("Executor is disabled. Committed transactions will not be sent anywhere.");
        }
        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        info!("committee {:?}", committee);
        let secret = Secret::read(key_file)?;
        let name = secret.name; //公钥做为ID
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path)?;

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);


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
            Some(tx_executor)
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
            info!("analyze_block {} successfully booted", _block);

        }
    }
}
