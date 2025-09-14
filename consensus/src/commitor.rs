use std::usize;

use crate::Block;
use crate::{config::Committee};
use log::{debug, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;

pub const MAX_BLOCK_BUFFER: usize = 10000;

async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit: Sender<Block>, // SỬA ĐỔI: Gửi toàn bộ Block
) -> usize {
    let mut data = Vec::new();
    loop {
        if let Some(block) = buffer[cur_ind].clone() {
            data.push(block);
            buffer[cur_ind] = None;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else if filter[cur_ind] {
            filter[cur_ind] = false;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else {
            break;
        }
    }
    
    // Gửi các khối có thể cam kết đến Core
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
        }
        debug!("Committed {}", block);

        // SỬA ĐỔI: Gửi toàn bộ đối tượng block qua channel
        if let Err(e) = tx_commit.send(block).await {
            panic!("Failed to send committed block to core: {}", e);
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
        tx_commit: Sender<Block>, // SỬA ĐỔI: Kênh nhận Block
        committee: Committee,
    ) -> Self {
        let (tx_block, mut rx_block): (_, Receiver<Block>) = channel(10000);
        let (tx_filter, mut rx_filter): (_, Receiver<usize>) = channel(10000);

        tokio::spawn(async move {
            let mut cur_ind = 0;
            let mut buffer: Vec<Option<Block>> = Vec::with_capacity(MAX_BLOCK_BUFFER);
            let mut filter: Vec<bool> = Vec::with_capacity(MAX_BLOCK_BUFFER);
            for _ in 0..MAX_BLOCK_BUFFER {
                buffer.push(None);
                filter.push(false);
            }
            loop {
                tokio::select! {
                    Some(block) = rx_block.recv()=>{
                        let rank = block.rank(&committee);
                        if let Some(_) = buffer[rank]{
                            // Tốc độ quá nhanh, cần xử lý lỗi hoặc tăng buffer
                        }
                        buffer[rank] = Some(block);
                    }
                    Some(ind) = rx_filter.recv()=>{
                        if filter[ind]{
                            // Tốc độ quá nhanh, cần xử lý lỗi hoặc tăng buffer
                        }
                        filter[ind]=true;
                    }
                }
                // Thử commit
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone()).await;
            }
        });

        Self {
            tx_block,
            tx_filter,
        }
    }

    // --- SỬA HÀM NÀY ---
    pub async fn buffer_block(&self, block: Block) {
        match self.tx_block.try_send(block) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => {
                panic!("[PANIC] Kênh Commitor (tx_block) đã đầy! Core có thể đã bị bế tắc.");
            }
            Err(TrySendError::Closed(_)) => {
                 panic!("[PANIC] Kênh Commitor (tx_block) đã bị đóng!");
            }
        }
    }

    // --- VÀ SỬA HÀM NÀY ---
    pub async fn filter_block(&self, ind: usize) {
        match self.tx_filter.try_send(ind) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => {
                panic!("[PANIC] Kênh Commitor (tx_filter) đã đầy! Core có thể đã bị bế tắc.");
            }
            Err(TrySendError::Closed(_)) => {
                 panic!("[PANIC] Kênh Commitor (tx_filter) đã bị đóng!");
            }
        }
    }
}