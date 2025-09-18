// consensus/src/commitor.rs

use std::usize;

use crate::Block;
use crate::{config::Committee};
use log::{debug, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const MAX_BLOCK_BUFFER: usize = 100000;

async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit: Sender<Vec<Block>>, // <-- THAY ĐỔI Ở ĐÂY
) -> usize {
    let mut committed_blocks = Vec::new();
    loop {
        if let Some(block) = buffer[cur_ind].clone() {
            committed_blocks.push(block);
            buffer[cur_ind] = None;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else if filter[cur_ind] {
            filter[cur_ind] = false;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else {
            break;
        }
    }
    
    // Gửi đi cả danh sách các block đã commit
    if !committed_blocks.is_empty() {
        for block in &committed_blocks {
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
        }
        
        if let Err(e) = tx_commit.send(committed_blocks).await {
            panic!("Failed to send committed blocks to core: {}", e);
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
        tx_commit: Sender<Vec<Block>>, // <-- THAY ĐỔI Ở ĐÂY
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
                            //速率过快 错误处理 增大Buffer
                        }
                        buffer[rank] = Some(block);
                    }
                    Some(ind) = rx_filter.recv()=>{
                        if filter[ind]{
                            //速率过快 错误处理 增大Buffer
                        }
                        filter[ind]=true;
                    }
                }
                //try to commit
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone()).await;
            }
        });

        Self {
            tx_block,
            tx_filter,
        }
    }

    pub async fn buffer_block(&self, block: Block) {
        if let Err(e) = self.tx_block.send(block).await {
            panic!("Failed to send block to commiter core: {}", e);
        }
    }

    pub async fn filter_block(&self, ind: usize) {
        if let Err(e) = self.tx_filter.send(ind).await {
            panic!("Failed to filter block to commiter core: {}", e);
        }
    }
}