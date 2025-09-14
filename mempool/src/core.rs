// mempool/src/core.rs

use crate::config::{Committee, Parameters};
use crate::error::{MempoolError, MempoolResult};
use crate::messages::{Payload}; // Đảm bảo MempoolMessage được import
use crate::payload::PayloadMaker;
use crate::synchronizer::Synchronizer;
use consensus::{Block, ConsensusMempoolMessage, PayloadStatus, SeqNumber};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
#[cfg(feature = "benchmark")]
use log::info;
use log::{error, info, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::Store;
use tokio::io::AsyncWriteExt; // Import trait cần thiết
use tokio::net::TcpStream;   // Import TcpStream
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

// Struct mới để đóng gói dữ liệu gửi sang Go
#[derive(Serialize, Deserialize, Debug)]
struct CommittedTransactions {
    epoch: SeqNumber,
    height: SeqNumber,
    transactions: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MempoolMessage {
    OwnPayload(Payload),
    Payload(Payload),
    PayloadRequest(Vec<Digest>, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    synchronizer: Synchronizer,
    payload_maker: PayloadMaker,
    core_channel: Receiver<MempoolMessage>,
    consensus_channel: Receiver<ConsensusMempoolMessage>,
    network_channel: Sender<NetMessage>,
    queue: HashSet<Digest>,
    go_tx_connection: Option<TcpStream>, // Trường để quản lý kết nối TCP
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        synchronizer: Synchronizer,
        payload_maker: PayloadMaker,
        core_channel: Receiver<MempoolMessage>,
        consensus_channel: Receiver<ConsensusMempoolMessage>,
        network_channel: Sender<NetMessage>,
    ) -> Self {
        let queue = HashSet::with_capacity(parameters.queue_capacity);
        Self {
            name,
            committee,
            parameters,
            store,
            synchronizer,
            core_channel,
            consensus_channel,
            network_channel,
            queue,
            payload_maker,
            go_tx_connection: None, // Khởi tạo là chưa có kết nối
        }
    }

    async fn store_payload(&mut self, key: Vec<u8>, payload: &Payload) {
        let value = bincode::serialize(payload).expect("Failed to serialize payload");
        self.store.write(key, value).await;
    }

    async fn transmit(
        &mut self,
        message: &MempoolMessage,
        to: Option<&PublicKey>,
    ) -> MempoolResult<()> {
        Synchronizer::transmit(
            message,
            &self.name,
            to,
            &self.committee,
            &self.network_channel,
        )
        .await
    }

    async fn process_own_payload(
        &mut self,
        digest: &Digest,
        payload: Payload,
    ) -> MempoolResult<()> {
        ensure!(
            self.queue.len() < self.parameters.queue_capacity,
            MempoolError::MempoolFull
        );
        self.store_payload(digest.to_vec(), &payload).await;
        let message = MempoolMessage::Payload(payload);
        self.transmit(&message, None).await
    }

    async fn handle_own_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        let digest = payload.digest();
        self.process_own_payload(&digest, payload).await?;
        self.queue.insert(digest);
        Ok(())
    }

    async fn handle_others_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        let author = payload.author;
        ensure!(
            self.committee.exists(&author),
            MempoolError::UnknownAuthority(author)
        );
        ensure!(
            payload.size() <= self.parameters.max_payload_size,
            MempoolError::PayloadTooBig
        );
        let digest = payload.digest();
        payload.signature.verify(&digest, &author)?;
        self.store_payload(digest.to_vec(), &payload).await;
        self.queue.insert(digest);
        Ok(())
    }

    async fn handle_request(
        &mut self,
        digests: Vec<Digest>,
        requestor: PublicKey,
    ) -> MempoolResult<()> {
        for digest in &digests {
            if let Some(bytes) = self.store.read(digest.to_vec()).await? {
                let payload = bincode::deserialize(&bytes)?;
                let message = MempoolMessage::Payload(payload);
                self.transmit(&message, Some(&requestor)).await?;
            }
        }
        Ok(())
    }

    async fn get_payload(&mut self, max: usize) -> MempoolResult<Vec<Digest>> {
        if self.queue.is_empty() {
            if let Some(payload) = self.payload_maker.make().await {
                let digest = payload.digest();
                self.process_own_payload(&digest, payload).await?;
                Ok(vec![digest])
            } else {
                Ok(Vec::new())
            }
        } else {
            let digest_len = Digest::default().size();
            let digests: Vec<_> = self.queue.iter().take(max / digest_len).cloned().collect();
            for x in &digests {
                self.queue.remove(x);
            }
            Ok(digests)
        }
    }

    async fn verify_payload(&mut self, block: Box<Block>) -> MempoolResult<bool> {
        self.synchronizer.verify_payload(*block).await
    }

    // --- HÀM CLEANUP ĐÃ ĐƯỢC THAY THẾ HOÀN TOÀN ---
    async fn cleanup(&mut self, digests: Vec<Digest>, epoch: SeqNumber, height: SeqNumber) {
        let mut all_transactions = Vec::new();

        // 1. Lặp qua các digest và đọc payload đầy đủ từ store
        for digest in &digests {
            if let Ok(Some(payload_bytes)) = self.store.read(digest.to_vec()).await {
                if let Ok(payload) = bincode::deserialize::<Payload>(&payload_bytes) {
                    all_transactions.extend(payload.transactions);
                }
            }
        }

        // 2. Chỉ gửi đi nếu có giao dịch để gửi
        if !all_transactions.is_empty() {
            // Thiết lập kết nối nếu chưa có
            if self.go_tx_connection.is_none() {
                match TcpStream::connect("127.0.0.1:9002").await {
                    Ok(stream) => {
                        info!("Mempool connected to Go TX receiver on port 9002");
                        self.go_tx_connection = Some(stream);
                    }
                    Err(e) => warn!("Mempool failed to connect to Go TX receiver: {}", e),
                }
            }

            // Gửi dữ liệu đi
            if let Some(stream) = &mut self.go_tx_connection {
                let committed_data = CommittedTransactions {
                    epoch,
                    height,
                    transactions: all_transactions,
                };

                let json_data = match serde_json::to_vec(&committed_data) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize committed transactions: {}", e);
                        // Vẫn tiếp tục để dọn dẹp
                        self.synchronizer.cleanup(epoch, height).await;
                        for x in &digests {
                            self.queue.remove(x);
                            self.store.delete(x.to_vec()).await;
                        }
                        return;
                    }
                };
                
                let len = json_data.len() as u32;
                let len_bytes = len.to_be_bytes();

                // Gửi độ dài rồi gửi dữ liệu
                if stream.write_all(&len_bytes).await.is_err() || stream.write_all(&json_data).await.is_err() {
                    warn!("Failed to send tx data to Go (connection lost). Resetting.");
                    self.go_tx_connection = None;
                } else {
                    info!("Sent {} committed transactions from block (E:{}, H:{}) to Go", committed_data.transactions.len(), epoch, height);
                }
            }
        }
        
        // 3. Thực hiện logic dọn dẹp ban đầu
        self.synchronizer.cleanup(epoch, height).await;
        for x in &digests {
            self.queue.remove(x);
            self.store.delete(x.to_vec()).await;
        }
    }

    pub async fn run(&mut self) {
        let log = |result: Result<&(), &MempoolError>| match result {
            Ok(()) => (),
            Err(MempoolError::StoreError(e)) => error!("{}", e),
            Err(MempoolError::SerializationError(e)) => error!("Store corrupted. {}", e),
            Err(e) => warn!("{}", e),
        };

        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        MempoolMessage::OwnPayload(payload) => self.handle_own_payload(payload).await,
                        MempoolMessage::Payload(payload) => self.handle_others_payload(payload).await,
                        MempoolMessage::PayloadRequest(digest, sender) => self.handle_request(digest, sender).await,
                    }
                },
                Some(message) = self.consensus_channel.recv() => {
                    match message {
                        ConsensusMempoolMessage::Get(max, sender) => {
                            let result = self.get_payload(max).await;
                            log(result.as_ref().map(|_| &()));
                            let _ = sender.send(result.unwrap_or_default());
                        },
                        ConsensusMempoolMessage::Verify(block, sender) => {
                            let result = self.verify_payload(block).await;
                            log(result.as_ref().map(|_| &()));
                            let status = match result {
                                Ok(true) => PayloadStatus::Accept,
                                Ok(false) => PayloadStatus::Wait,
                                Err(_) => PayloadStatus::Reject,
                            };
                            let _ = sender.send(status);
                        },
                        ConsensusMempoolMessage::Cleanup(digests, epoch, height) => self.cleanup(digests, epoch, height).await,
                    }
                    Ok(())
                },
                else => break,
            };
            log(result.as_ref());
        }
    }
}