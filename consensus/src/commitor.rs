// consensus/src/commitor.rs

use std::usize;

use crate::Block;
use crate::{config::Committee, SeqNumber};
use crypto::Digest;
use log::{debug, info, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;

pub const MAX_BLOCK_BUFFER: usize = 10000;

async fn try_to_commit(
    mut cur_ind: usize,
    buffer: &mut Vec<Option<Block>>,
    filter: &mut Vec<bool>,
    tx_commit: Sender<(Vec<Digest>, SeqNumber, SeqNumber)>,
) -> usize {
    let mut data_to_process = Vec::new();

    // 1. Thu thập tất cả các khối có thể commit
    loop {
        if let Some(block) = buffer[cur_ind].take() { 
            data_to_process.push(block);
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else if filter[cur_ind] {
            filter[cur_ind] = false;
            cur_ind = (cur_ind + 1) % MAX_BLOCK_BUFFER
        } else {
            break;
        }
    }

    // 2. Xử lý từng khối đã thu thập
    for block in data_to_process {
        let mut digests = Vec::new();
        let (epoch, height) = (block.epoch, block.height);

        if !block.payload.is_empty() {
            info!("Committed {}", block);
            digests.extend(block.payload.clone());
        } else {
            debug!("Committed Empty Block {}", block);
        }

        // 3. LUÔN LUÔN GỬI TÍN HIỆU COMMIT CHO MỌI KHỐI
        if let Err(e) = tx_commit.send((digests, epoch, height)).await {
            panic!("Failed to filter block to commiter core: {}", e);
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
    ) -> Self {
        let (tx_block, mut rx_block): (_, Receiver<Block>) = channel(10000);
        let (tx_filter, mut rx_filter): (_, Receiver<usize>) = channel(10000);

        tokio::spawn(async move {
            let mut cur_ind = 0;
            let mut buffer: Vec<Option<Block>> = vec![None; MAX_BLOCK_BUFFER];
            let mut filter: Vec<bool> = vec![false; MAX_BLOCK_BUFFER];
            
            loop {
                tokio::select! {
                    Some(block) = rx_block.recv()=>{
                        let rank = block.rank(&committee);
                        if buffer[rank].is_some(){
                             warn!("Commitor buffer for rank {} is already full! Discarding new block.", rank);
                        } else {
                            buffer[rank] = Some(block);
                        }
                    }
                    Some(ind) = rx_filter.recv()=>{
                        if filter[ind]{
                           warn!("Commitor filter for index {} is already set!", ind);
                        }
                        filter[ind]=true;
                    }
                    else => {
                        break;
                    }
                }
                cur_ind = try_to_commit(cur_ind, &mut buffer, &mut filter, tx_commit.clone()).await;
            }
        });

        Self {
            tx_block,
            tx_filter,
        }
    }

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