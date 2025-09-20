use crate::config::Committee;
use crate::{Block, SeqNumber};
use crypto::Digest;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::usize;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const MAX_BLOCK_BUFFER: usize = 100000;

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

async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit: Sender<(Vec<Digest>, SeqNumber, SeqNumber)>,
    executor_socket: Option<&str>,
) -> usize {
    let mut data = Vec::new();
    let mut digests = Vec::new();
    let mut epoch_blocks: HashMap<SeqNumber, Vec<Block>> = HashMap::new();

    loop {
        if let Some(block) = buffer[cur_ind].clone() {
            data.push(block.clone());
            // Chỉ thu thập blocks nếu executor được kích hoạt
            if executor_socket.is_some() {
                epoch_blocks.entry(block.epoch).or_default().push(block);
            }
            buffer[cur_ind] = None;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else if filter[cur_ind] {
            filter[cur_ind] = false;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else {
            break;
        }
    }

    // Gửi dữ liệu epoch đã cam kết đến exetps nếu đường dẫn socket được cung cấp
    if let Some(socket_path) = executor_socket {
        for (epoch, blocks) in epoch_blocks {
            if blocks.is_empty() {
                continue;
            }

            let full_blocks: Vec<FullBlock> = blocks
                .into_iter()
                .map(|b| FullBlock {
                    author: b.author.to_string(),
                    epoch: b.epoch,
                    height: b.height,
                    transactions: b.payload.iter().map(|d| d.0.to_vec()).collect(),
                })
                .collect();

            let epoch_data = CommittedEpochData {
                epoch,
                blocks: full_blocks,
            };

            if let Ok(serialized_data) = serde_json::to_string(&epoch_data) {
                match UnixStream::connect(socket_path).await {
                    Ok(mut stream) => {
                        if let Err(e) = stream.write_all(serialized_data.as_bytes()).await {
                            log::error!("Failed to send data to exetps: {}", e);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to connect to exetps socket at {}: {}", socket_path, e);
                    }
                }
            }
        }
    }

    let (mut e, mut h): (SeqNumber, SeqNumber) = (0, 0);
    // Gửi digest của các khối đã cam kết về cho core
    for block in data {
        if !block.payload.is_empty() {
            info!("Committed {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                info!(
                    "Committed B{}({}) epoch {}",
                    block.height,
                    base64::encode(x),
                    block.epoch,
                );
            }
            digests.append(&mut block.payload.clone());
        }
        debug!("Committed {}", block);
        (e, h) = (block.epoch, block.height)
    }
    if !digests.is_empty() {
        if let Err(e) = tx_commit.send((digests, e, h)).await {
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
    ) -> Self {
        let (tx_block, mut rx_block): (_, Receiver<Block>) = channel(10000);
        let (tx_filter, mut rx_filter): (_, Receiver<usize>) = channel(10000);

        tokio::spawn(async move {
            let mut cur_ind = 0;
            let mut buffer: Vec<Option<Block>> = vec![None; MAX_BLOCK_BUFFER];
            let mut filter: Vec<bool> = vec![false; MAX_BLOCK_BUFFER];
            
            // Lấy tham chiếu đến đường dẫn socket để sử dụng trong vòng lặp
            let socket_ref = executor_socket.as_deref();

            loop {
                tokio::select! {
                    Some(block) = rx_block.recv() => {
                        let rank = block.rank(&committee);
                        if buffer[rank].is_some() {
                            // Xử lý lỗi nếu buffer bị đầy, có thể cần tăng kích thước buffer
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
                // Thử cam kết các khối
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone(), socket_ref).await;
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