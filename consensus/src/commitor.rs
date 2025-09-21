// consensus/src/commitor.rs

use crate::config::Committee;
use crate::{Block, SeqNumber};
use crypto::{Digest, PublicKey, Signature};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use store::Store;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const MAX_BLOCK_BUFFER: usize = 100000;

#[derive(Serialize, Deserialize)]
struct MempoolPayload {
    pub transactions: Vec<Vec<u8>>,
    pub author: PublicKey,
    pub signature: Signature,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FullBlock {
    pub author: String,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub transactions: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CommittedEpochData {
    pub epoch: SeqNumber,
    pub blocks: Vec<FullBlock>,
}

// TỐI ƯU 1: Hàm gửi dữ liệu qua socket đã được tách ra.
// Hàm này sẽ chạy trong một task riêng biệt.
async fn send_to_executor(mut rx_executor: Receiver<CommittedEpochData>, socket_path: String) {
    while let Some(epoch_data) = rx_executor.recv().await {
        if let Ok(serialized_data) = serde_json::to_string(&epoch_data) {
            match UnixStream::connect(&socket_path).await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(serialized_data.as_bytes()).await {
                        log::error!("Failed to send data to executor socket: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("Failed to connect to executor socket at {}: {}", &socket_path, e);
                }
            }
        }
    }
}


async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit_digest: Sender<(Vec<Digest>, SeqNumber, SeqNumber)>,
    // TỐI ƯU 2: Thay vì đường dẫn socket, chúng ta nhận một Sender để gửi dữ liệu cho task mạng.
    tx_executor: &Option<Sender<CommittedEpochData>>,
    store: &mut Store,
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

    // TỐI ƯU 3: Chỉ gửi dữ liệu vào channel nếu tx_executor tồn tại.
    // Việc gửi vào channel nhanh hơn nhiều so với kết nối socket.
    if let Some(tx) = tx_executor {
        let mut epoch_map: HashMap<SeqNumber, Vec<Block>> = HashMap::new();
        for block in &committed_blocks {
            epoch_map.entry(block.epoch).or_default().push(block.clone());
        }

        for (epoch, blocks) in epoch_map {
            let mut full_blocks = Vec::new();
            for block in blocks {
                let mut transactions = Vec::new();
                for digest in &block.payload {
                    if let Ok(Some(bytes)) = store.read(digest.to_vec()).await {
                        if let Ok(payload) = bincode::deserialize::<MempoolPayload>(&bytes) {
                            transactions.extend(payload.transactions);
                        }
                    }
                }

                if !transactions.is_empty() {
                    full_blocks.push(FullBlock {
                        author: block.author.to_string(),
                        epoch: block.epoch,
                        height: block.height,
                        transactions,
                    });
                }
            }

            if !full_blocks.is_empty() {
                let epoch_data = CommittedEpochData { epoch, blocks: full_blocks };
                // Gửi vào channel, không block.
                if let Err(e) = tx.send(epoch_data).await {
                    log::error!("Failed to send epoch data to executor task: {}", e);
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

        // TỐI ƯU 4: Tạo channel và task cho việc gửi dữ liệu executor.
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
            let mut store = store;

            loop {
                tokio::select! {
                    Some(block) = rx_block.recv() => {
                        let rank = block.rank(&committee);
                        if buffer[rank].is_some() {
                            log::warn!("Commitor buffer overflow at rank {}", rank);
                        }
                        buffer[rank] = Some(block);
                    },
                    Some(ind) = rx_filter.recv() => {
                        if filter[ind] {
                             log::warn!("Commitor filter overflow at index {}", ind);
                        }
                        filter[ind] = true;
                    }
                }
                // TỐI ƯU 5: Truyền tx_executor vào hàm.
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone(), &tx_executor, &mut store).await;
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