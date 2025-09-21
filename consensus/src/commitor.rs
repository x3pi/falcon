// consensus/src/commitor.rs

use crate::config::Committee;
use crate::{Block, SeqNumber};
use crypto::{Digest, PublicKey, Signature};
use futures::future::try_join_all;
use log::{info, warn};
use prost::Message; // THAY ĐỔI: Import prost::Message
use serde::Deserialize; // Bỏ Serialize
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;
use store::Store;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

// THAY ĐỔI: Bao gồm mã Rust được tạo ra từ file .proto
pub mod executor {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
use executor::{CommittedEpochData, FullBlock}; // Sử dụng struct từ Protobuf

pub const MAX_BLOCK_BUFFER: usize = 100000;

#[derive(Deserialize)] // Bỏ Serialize
struct MempoolPayload {
    pub transactions: Vec<Vec<u8>>,
    pub author: PublicKey,
    pub signature: Signature,
}

// Hàm send_to_executor được cập nhật để gửi dữ liệu Protobuf
async fn send_to_executor(mut rx_executor: Receiver<CommittedEpochData>, socket_path: String) {
    loop {
        info!("Connecting to executor socket at {}...", &socket_path);
        match UnixStream::connect(&socket_path).await {
            Ok(mut stream) => {
                info!("Successfully connected to executor socket.");
                while let Some(epoch_data) = rx_executor.recv().await {
                    let mut buf = Vec::new();
                    // THAY ĐỔI: Serialize bằng Protobuf với độ dài được định sẵn
                    if epoch_data.encode_length_delimited(&mut buf).is_ok() {
                        if let Err(e) = stream.write_all(&buf).await {
                            warn!("Failed to send data to executor socket: {}. Attempting to reconnect...", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to connect to executor socket at {}: {}. Retrying in 1 second...", &socket_path, e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit_digest: Sender<(Vec<Digest>, SeqNumber, SeqNumber)>,
    tx_executor: &Option<Sender<CommittedEpochData>>,
    store: &Store,
) -> usize {
    let mut committed_blocks: Vec<Block> = Vec::new();

    loop {
        if let Some(block) = buffer[cur_ind].take() {
            committed_blocks.push(block);
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER;
        } else if filter[cur_ind] {
            filter[cur_ind] = false;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER;
        } else {
            break;
        }
    }

    if committed_blocks.is_empty() {
        return cur_ind;
    }

    if let Some(tx) = tx_executor {
        let digests_to_read: Vec<_> = committed_blocks
            .iter()
            .flat_map(|block| block.payload.iter().cloned())
            .map(|digest| digest.to_vec())
            .collect();
        
        let read_futures = digests_to_read.iter().map(|digest_vec| {
            let mut store_clone = store.clone();
            async move {
                store_clone.read(digest_vec.clone()).await
            }
        });
        let results = try_join_all(read_futures).await;

        let mut payload_cache = HashMap::new();
        if let Ok(payloads_results) = results {
            for (digest_vec, payload_bytes_opt_res) in digests_to_read.into_iter().zip(payloads_results) {
                 if let Some(payload_bytes) = payload_bytes_opt_res {
                    let digest = Digest::try_from(digest_vec.as_slice()).unwrap();
                    payload_cache.insert(digest, payload_bytes);
                 }
            }
        } else {
            warn!("Failed to read payloads from store in batch.");
        }
        
        let mut epoch_map: HashMap<SeqNumber, Vec<Block>> = HashMap::new();
        for block in &committed_blocks {
            epoch_map.entry(block.epoch).or_default().push(block.clone());
        }

        for (epoch, blocks) in epoch_map {
            let mut full_blocks = Vec::new();
            for block in blocks {
                let mut transactions = Vec::new();
                for digest in &block.payload {
                    if let Some(bytes) = payload_cache.get(digest) {
                        if let Ok(payload) = bincode::deserialize::<MempoolPayload>(&bytes) {
                            transactions.extend(payload.transactions);
                        }
                    }
                }

                if !transactions.is_empty() {
                    // THAY ĐỔI: Tạo struct FullBlock từ Protobuf
                    full_blocks.push(FullBlock {
                        author: block.author.to_string(),
                        epoch: block.epoch,
                        height: block.height,
                        transactions,
                    });
                }
            }

            if !full_blocks.is_empty() {
                // THAY ĐỔI: Tạo struct CommittedEpochData từ Protobuf
                let epoch_data = CommittedEpochData { epoch, blocks: full_blocks };
                if let Err(e) = tx.send(epoch_data).await {
                    warn!("Failed to send epoch data to executor task channel: {}", e);
                }
            }
        }
    }

    let mut all_digests = Vec::new();
    let (last_epoch, last_height) = committed_blocks.last().map_or((0,0), |b| (b.epoch, b.height));

    for block in committed_blocks {
        info!("Committed {}", block);
        if !block.payload.is_empty() {
            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                info!(
                    "Committed B{}({}) epoch {}",
                    block.height,
                    base64::encode(x),
                    block.epoch,
                );
            }
            all_digests.extend(block.payload);
        }
    }

    if !all_digests.is_empty() {
        if let Err(e) = tx_commit_digest.send((all_digests, last_epoch, last_height)).await {
            panic!("Failed to send committed digests to core: {}", e);
        }
    }

    cur_ind
}

// Phần còn lại của file giữ nguyên.
pub struct Commitor {
    tx_block: Sender<Block>,
    tx_filter: Sender<usize>,
}
impl Commitor {
    pub fn new(
        tx_commit: Sender<(Vec<Digest>, SeqNumber, SeqNumber)>,
        committee: Committee,
        executor_socket: Option<String>,
        store: Store,
    ) -> Self {
        let (tx_block, mut rx_block): (_, Receiver<Block>) = channel(10000);
        let (tx_filter, mut rx_filter): (_, Receiver<usize>) = channel(10000);
        let tx_executor = if let Some(socket_path) = executor_socket {
            let (tx_executor, rx_executor) = channel::<CommittedEpochData>(100);
            tokio::spawn(send_to_executor(rx_executor, socket_path));
            Some(tx_executor)
        } else {
            None
        };
        tokio::spawn(async move {
            let mut cur_ind = 0;
            let mut buffer: Vec<Option<Block>> = vec![None; MAX_BLOCK_BUFFER];
            let mut filter: Vec<bool> = vec![false; MAX_BLOCK_BUFFER];
            let store = store;
            loop {
                tokio::select! {
                    Some(block) = rx_block.recv() => {
                        let rank = block.rank(&committee);
                        if buffer[rank].is_some() {
                            warn!("Commitor buffer overflow at rank {}", rank);
                        }
                        buffer[rank] = Some(block);
                    },
                    Some(ind) = rx_filter.recv() => {
                        if filter[ind] {
                             warn!("Commitor filter overflow at index {}", ind);
                        }
                        filter[ind] = true;
                    }
                }
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone(), &tx_executor, &store).await;
            }
        });
        Self {
            tx_block,
            tx_filter,
        }
    }
    pub async fn buffer_block(&self, block: Block) {
        if let Err(e) = self.tx_block.send(block).await {
            panic!("Failed to send block to commitor core: {}", e);
        }
    }
    pub async fn filter_block(&self, ind: usize) {
        if let Err(e) = self.tx_filter.send(ind).await {
            panic!("Failed to filter block to commitor core: {}", e);
        }
    }
}