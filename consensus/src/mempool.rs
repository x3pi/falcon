// consensus/src/mempool.rs

use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use crypto::Digest;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::mpsc::error::TrySendError;

#[derive(Debug)]
pub enum PayloadStatus {
    Accept,
    Reject,
    Wait,
}

#[derive(Debug)]
pub enum ConsensusMempoolMessage {
    Get(usize, oneshot::Sender<Vec<Digest>>),
    Verify(Box<Block>, oneshot::Sender<PayloadStatus>),
    Cleanup(Vec<Digest>, SeqNumber, SeqNumber),
    // THÊM DÒNG NÀY: Yêu cầu dữ liệu giao dịch đầy đủ
    GetFullTransactions(Vec<Digest>, oneshot::Sender<Vec<Vec<u8>>>),
}

pub struct MempoolDriver {
    mempool_channel: Sender<ConsensusMempoolMessage>,
}

impl MempoolDriver {
    pub fn new(mempool_channel: Sender<ConsensusMempoolMessage>) -> Self {
        Self { mempool_channel }
    }

    pub async fn get(&mut self, max: usize) -> Vec<Digest> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMempoolMessage::Get(max, sender);
        
        match self.mempool_channel.try_send(message) {
            Ok(()) => receiver
                .await
                .expect("Failed to receive payload from mempool"),
            Err(TrySendError::Full(_)) => {
                panic!("[PANIC] Kênh từ Consensus đến Mempool đã đầy khi gọi Get!");
            },
            Err(TrySendError::Closed(_)) => {
                panic!("[PANIC] Kênh từ Consensus đến Mempool đã bị đóng!");
            }
        }
    }

    pub async fn verify(&mut self, block: Box<Block>) -> ConsensusResult<bool> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMempoolMessage::Verify(block, sender);

        if let Err(e) = self.mempool_channel.try_send(message) {
            match e {
                TrySendError::Full(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã đầy khi gọi Verify!"),
                TrySendError::Closed(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã bị đóng!"),
            }
        }
        
        match receiver
            .await
            .expect("Failed to receive payload status from mempool")
        {
            PayloadStatus::Accept => Ok(true),
            PayloadStatus::Wait => Ok(false),
            PayloadStatus::Reject => Err(ConsensusError::InvalidPayload),
        }
    }

    pub async fn cleanup(&mut self, digest: Vec<Digest>, epoch: SeqNumber, height: SeqNumber) {
        let message = ConsensusMempoolMessage::Cleanup(digest, epoch, height);
        
        if let Err(e) = self.mempool_channel.try_send(message) {
            match e {
                TrySendError::Full(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã đầy khi gọi Cleanup!"),
                TrySendError::Closed(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã bị đóng!"),
            }
        }
    }

    // THÊM PHƯƠNG THỨC MỚI NÀY
    pub async fn get_full_transactions(&mut self, digests: Vec<Digest>) -> ConsensusResult<Vec<Vec<u8>>> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMempoolMessage::GetFullTransactions(digests, sender);
        
        if let Err(e) = self.mempool_channel.try_send(message) {
            match e {
                TrySendError::Full(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã đầy khi gọi GetFullTransactions!"),
                TrySendError::Closed(_) => panic!("[PANIC] Kênh từ Consensus đến Mempool đã bị đóng!"),
            }
        }
        
        Ok(receiver.await.expect("Failed to receive full transactions from mempool"))
    }
}