use std::collections::{HashMap, HashSet};

use crate::aggregator::Aggregator;
use crate::commitor::{Commitor, MAX_BLOCK_BUFFER};
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::mempool::MempoolDriver;
use crate::messages::{
    ABAOutput, ABAVal, Block, EchoVote, Prepare, RBCProof, ReadyVote,
};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use store::Store;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type SeqNumber = u64; 
pub type HeightNumber = u8;

pub const RBC_ECHO: u8 = 0;
pub const RBC_READY: u8 = 1;

pub const PRE_ONE: u8 = 0;
pub const PRE_TWO: u8 = 1;

pub const VAL_PHASE: u8 = 0;
pub const MUX_PHASE: u8 = 1;

pub const OPT: u8 = 1;
pub const PES: u8 = 0;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    RBCValMsg(Block),
    RBCEchoMsg(EchoVote),
    RBCReadyMsg(ReadyVote),
    ABAValMsg(ABAVal),
    ABAMuxMsg(ABAVal),
    ABAOutputMsg(ABAOutput),
    PrePareMsg(Prepare),
    LoopBackMsg(Block),
    SyncRequestMsg(SeqNumber, SeqNumber, PublicKey),
    SyncReplyMsg(Block),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    _tx_core: Sender<ConsensusMessage>,
    rx_core: Receiver<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    _commit_channel: Sender<Block>,
    rx_commit_signal: Receiver<(Vec<Digest>, SeqNumber, SeqNumber)>,
    fallback: SeqNumber,
    epoch: SeqNumber,
    height: SeqNumber,
    aggregator: Aggregator,
    commitor: Commitor,
    // ---- KÊNH GỬI THÔNG BÁO SANG GO ----
    tx_commit_notification: Sender<(Vec<Vec<u8>>, SeqNumber, SeqNumber)>,
    buffers: HashMap<(SeqNumber, SeqNumber), bool>,
    rbc_proofs: HashMap<(SeqNumber, SeqNumber, u8), RBCProof>,
    rbc_ready: HashSet<(SeqNumber, SeqNumber)>,
    rbc_epoch_outputs: HashMap<SeqNumber, HashSet<SeqNumber>>,
    prepare_flags: HashSet<(SeqNumber, SeqNumber)>,
    aba_values: HashMap<(SeqNumber, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_values_flag: HashMap<(SeqNumber, SeqNumber, SeqNumber), [bool; 2]>,
    aba_mux_values: HashMap<(SeqNumber, SeqNumber, SeqNumber), [HashSet<PublicKey>; 2]>,
    aba_mux_flags: HashMap<(SeqNumber, SeqNumber, SeqNumber), [bool; 2]>,
    aba_outputs: HashMap<(SeqNumber, SeqNumber, SeqNumber), HashSet<PublicKey>>,
    aba_ends: HashMap<(SeqNumber, SeqNumber), bool>,
    aba_timeouts: HashMap<(SeqNumber, SeqNumber), Instant>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        tx_core: Sender<ConsensusMessage>,
        rx_core: Receiver<ConsensusMessage>,
        network_filter: Sender<FilterInput>,
        _commit_channel: Sender<Block>,
        // ---- THÊM THAM SỐ MỚI ----
        tx_commit_notification: Sender<(Vec<Vec<u8>>, SeqNumber, SeqNumber)>,
    ) -> Self {
        let (tx_commit_signal, rx_commit_signal) = channel(10000);
        let aggregator = Aggregator::new(committee.clone());
        let commitor = Commitor::new(tx_commit_signal.clone(), committee.clone());
        Self {
            fallback: parameters.fallback,
            epoch: 0,
            height: committee.id(name) as u64,
            name,
            committee,
            parameters,
            signature_service,
            store,
            mempool_driver,
            synchronizer,
            network_filter,
            _commit_channel,
            rx_commit_signal,
            _tx_core: tx_core,
            rx_core,
            aggregator,
            commitor,
            tx_commit_notification,
            buffers: HashMap::new(),
            rbc_proofs: HashMap::new(),
            rbc_ready: HashSet::new(),
            rbc_epoch_outputs: HashMap::new(),
            prepare_flags: HashSet::new(),
            aba_values: HashMap::new(),
            aba_mux_values: HashMap::new(),
            aba_values_flag: HashMap::new(),
            aba_mux_flags: HashMap::new(),
            aba_outputs: HashMap::new(),
            aba_ends: HashMap::new(),
            aba_timeouts: HashMap::new(),
        }
    }

    pub fn rank(epoch: SeqNumber, height: SeqNumber, committee: &Committee) -> usize {
        let r = ((epoch as usize) * committee.size() + (height as usize)) % MAX_BLOCK_BUFFER;
        r
    }

    async fn store_block(&mut self, block: &Block) {
        self.buffers.insert((block.epoch, block.height), true);
        let key: Vec<u8> = block.rank(&self.committee).to_le_bytes().into();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn cleanup(
        &mut self,
        digest: Vec<Digest>,
        epoch: SeqNumber,
        height: SeqNumber,
    ) -> ConsensusResult<()> {
        let size = self.committee.size() as SeqNumber;
        let rank = epoch * size + height;
        self.aggregator.cleanup(epoch, height);
        self.mempool_driver.cleanup(digest, epoch, height).await;
        self.buffers.retain(|(e, h, ..), _| e * size + h > rank);
        self.rbc_proofs.retain(|(e, h, ..), _| e * size + h > rank);
        self.rbc_ready.retain(|(e, h)| e * size + h > rank);
        self.aba_values.retain(|(e, h, ..), _| e * size + h > rank);
        self.aba_mux_values
            .retain(|(e, h, ..), _| e * size + h > rank);
        self.aba_values_flag
            .retain(|(e, h, ..), _| e * size + h > rank);
        self.aba_mux_flags
            .retain(|(e, h, ..), _| e * size + h > rank);
        self.aba_outputs.retain(|(e, h, ..), _| e * size + h > rank);

        if epoch > self.parameters.pruning_threshold {
            let prune_epoch = epoch - self.parameters.pruning_threshold;
            
            for h in 0..self.committee.size() as SeqNumber {
                let rank_to_prune = Self::rank(prune_epoch, h, &self.committee);
                let key_to_prune: Vec<u8> = rank_to_prune.to_le_bytes().into();
                self.store.delete(key_to_prune).await;
            }

            self.rbc_epoch_outputs.retain(|e, _| *e >= prune_epoch);
            debug!("Pruned data from epoch {}", prune_epoch);
        }

        Ok(())
    }

    async fn handle_sync_request(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        debug!("processing sync request epoch {} height {}", epoch, height);
        let rank = Core::rank(epoch, height, &self.committee);
        if let Some(bytes) = self.store.read(rank.to_le_bytes().into()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::SyncReplyMsg(block);
            Synchronizer::transmit(
                message,
                &self.name,
                Some(&sender),
                &self.network_filter,
                &self.committee,
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_sync_reply(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!(
            "processing sync reply epoch {} height {}",
            block.epoch, block.height
        );
        block.verify(&self.committee)?;
        self.store_block(block).await;
        self.process_rbc_output(block.epoch, block.height).await?;
        Ok(())
    }

    /************* RBC Protocol ******************/
    #[async_recursion]
    async fn generate_rbc_proposal(&mut self) -> ConsensusResult<()> {
        if self.height < self.parameters.fault {
            return Ok(());
        }
        debug!("start rbc epoch {}", self.epoch);

        info!("[BƯỚC 3] Consensus core: Đang yêu cầu payload từ mempool driver cho epoch {}.", self.epoch);

        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;

        if payload.is_empty() {
            info!("[BƯỚC 4] Consensus core: Mempool trả về payload trống. Bỏ qua đề xuất khối lần này.");
        } else {
            info!("[BƯỚC 4] Consensus core: Mempool trả về payload với {} digest.", payload.len());
        }

        let block = Block::new(
            self.name,
            self.epoch,
            self.height,
            payload,
            self.signature_service.clone(),
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                info!(
                    "Created B{}({}) epoch {}",
                    block.height,
                    base64::encode(x),
                    block.epoch
                );
            }
        }
        debug!("Created {:?}", block);

        let message = ConsensusMessage::RBCValMsg(block.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
        )
        .await?;
        self.handle_rbc_val(&block).await?;

        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;

        Ok(())
    }
    async fn handle_rbc_val(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!(
            "processing RBC val epoch {} height {}",
            block.epoch, block.height
        );
        block.verify(&self.committee)?;
        if self.parameters.exp > 0 {
            if !self.mempool_driver.verify(block.clone()).await? {
                return Ok(());
            }
        }

        self.store_block(block).await;

        let vote = EchoVote::new(
            self.name,
            block.epoch,
            block.height,
            block,
            self.signature_service.clone(),
        )
        .await;
        let message = ConsensusMessage::RBCEchoMsg(vote.clone());

        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
        )
        .await?;

        self.handle_rbc_echo(&vote).await?;
        Ok(())
    }

    async fn handle_rbc_echo(&mut self, vote: &EchoVote) -> ConsensusResult<()> {
        debug!(
            "processing RBC echo_vote epoch {} height {}",
            vote.epoch, vote.height
        );
        vote.verify(&self.committee)?;

        if let Some(proof) = self.aggregator.add_rbc_echo_vote(vote.clone())? {
            self.rbc_proofs
                .insert((proof.epoch, proof.height, proof.tag), proof);
            self.rbc_ready.insert((vote.epoch, vote.height));
            let ready = ReadyVote::new(
                self.name,
                vote.epoch,
                vote.height,
                vote.digest.clone(),
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::RBCReadyMsg(ready.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.invoke_prepare(vote.epoch, vote.height, OPT).await?;
            self.handle_rbc_ready(&ready).await?;
        }

        Ok(())
    }

    #[async_recursion]
    async fn handle_rbc_ready(&mut self, vote: &ReadyVote) -> ConsensusResult<()> {
        debug!(
            "processing RBC ready_vote epoch {} height {}",
            vote.epoch, vote.height
        );
        vote.verify(&self.committee)?;

        if let Some(proof) = self.aggregator.add_rbc_ready_vote(vote.clone())? {
            let flag = self.rbc_ready.contains(&(vote.epoch, vote.height));

            self.rbc_proofs
                .insert((proof.epoch, proof.height, proof.tag), proof.clone());

            if !flag && proof.votes.len() as Stake == self.committee.random_coin_threshold() {
                self.rbc_ready.insert((vote.epoch, vote.height));
                let ready = ReadyVote::new(
                    self.name,
                    vote.epoch,
                    vote.height,
                    vote.digest.clone(),
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::RBCReadyMsg(ready.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                self.invoke_prepare(vote.epoch, vote.height, OPT).await?;
                self.handle_rbc_ready(&ready).await?;
                return Ok(());
            }
            if proof.votes.len() as Stake == self.committee.quorum_threshold() {
                self.process_rbc_output(vote.epoch, vote.height).await?;
            }
        }

        Ok(())
    }

    #[async_recursion]
    async fn process_rbc_output(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
    ) -> ConsensusResult<()> {
        debug!("processing RBC output epoch {} height {}", epoch, height);
        let outputs = self
            .rbc_epoch_outputs
            .entry(epoch)
            .or_insert(HashSet::new());
        if !outputs.contains(&height) {
            if let Some(block) = self
                .synchronizer
                .block_request(epoch, height, &self.committee)
                .await?
            {
                outputs.insert(height);
                self.commitor.buffer_block(block.clone()).await;

                if outputs.len() as Stake == self.committee.quorum_threshold() {
                    self.rbc_advance(epoch + 1).await?;
                    self.fallback(epoch).await?;
                }
            }
        }
        Ok(())
    }

    async fn fallback(&mut self, cur_epoch: SeqNumber) -> ConsensusResult<()> {
        if cur_epoch >= self.fallback {
            let fall_epoch = cur_epoch - self.fallback;
            for height in 0..(self.committee.size() as SeqNumber) {
                if !self.prepare_flags.contains(&(fall_epoch, height)) {
                    self.invoke_prepare(fall_epoch, height, PES).await?;
                }
            }
        }
        Ok(())
    }

    async fn rbc_advance(&mut self, epoch: SeqNumber) -> ConsensusResult<()> {
        if epoch > self.epoch {
            self.epoch = epoch;
            self.generate_rbc_proposal().await?; 
        }
        Ok(())
    }
    /************* RBC Protocol ******************/

    /************* PrePare Protocol ******************/
    async fn invoke_prepare(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        val: u8,
    ) -> ConsensusResult<()> {
        if self.prepare_flags.insert((epoch, height)) {
            let prepare = Prepare::new(
                self.name,
                epoch,
                height,
                PRE_ONE,
                val,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::PrePareMsg(prepare.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.handle_prepare(&prepare).await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn handle_prepare(&mut self, prepare: &Prepare) -> ConsensusResult<()> {
        debug!(
            "processing prepare epoch {} height {} phase {} tag {}",
            prepare.epoch, prepare.height, prepare.phase, prepare.val
        );
        prepare.verify(&self.committee)?;
        if let Some((val, flag)) = self.aggregator.add_prepare_vote(prepare.clone())? {
            debug!("prepare=> val {}", val);
            if flag {
                if prepare.phase == PRE_ONE {
                    self.process_rbc_output(prepare.epoch, prepare.height)
                        .await?;
                } else if prepare.phase == PRE_TWO {
                    self.commitor
                        .filter_block(Self::rank(prepare.epoch, prepare.height, &self.committee))
                        .await;
                }
            } else {
                if prepare.phase == PRE_ONE {
                    let pre2 = Prepare::new(
                        self.name,
                        prepare.epoch,
                        prepare.height,
                        PRE_TWO,
                        val,
                        self.signature_service.clone(),
                    )
                    .await;
                    let message = ConsensusMessage::PrePareMsg(pre2.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter,
                        &self.committee,
                    )
                    .await?;
                    self.handle_prepare(&pre2).await?;
                } else if prepare.phase == PRE_TWO {
                    self.aba_timeouts.insert((prepare.epoch, prepare.height), Instant::now());

                    let aba_val = ABAVal::new(
                        self.name,
                        prepare.epoch,
                        prepare.height,
                        0,
                        val as usize,
                        VAL_PHASE,
                        self.signature_service.clone(),
                    )
                    .await;
                    let message = ConsensusMessage::ABAValMsg(aba_val.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter,
                        &self.committee,
                    )
                    .await?;
                    self.handle_aba_val(&aba_val).await?;
                }
            }
        }
        Ok(())
    }
    /************* PrePare Protocol ******************/

    /************* ABA Protocol ******************/
    #[async_recursion]
    async fn handle_aba_val(&mut self, aba_val: &ABAVal) -> ConsensusResult<()> {
        debug!(
            "processing aba val epoch {} height {}",
            aba_val.epoch, aba_val.height
        );

        aba_val.verify()?;

        let values = self
            .aba_values
            .entry((aba_val.epoch, aba_val.height, aba_val.round))
            .or_insert([HashSet::new(), HashSet::new()]);

        if values[aba_val.val].insert(aba_val.author) {
            let mut nums = values[aba_val.val].len() as Stake;
            if nums == self.committee.random_coin_threshold()
                && !values[aba_val.val].contains(&self.name)
            {
                let other = ABAVal::new(
                    self.name,
                    aba_val.epoch,
                    aba_val.height,
                    aba_val.round,
                    aba_val.val,
                    VAL_PHASE,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ABAValMsg(other);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                values[aba_val.val].insert(self.name);
                nums += 1;
            }

            if nums == self.committee.quorum_threshold() {
                let values_flag = self
                    .aba_values_flag
                    .entry((aba_val.epoch, aba_val.height, aba_val.round))
                    .or_insert([false, false]);

                if !values_flag[OPT as usize] && !values_flag[PES as usize] {
                    values_flag[aba_val.val] = true;
                    let mux = ABAVal::new(
                        self.name,
                        aba_val.epoch,
                        aba_val.height,
                        aba_val.round,
                        aba_val.val,
                        MUX_PHASE,
                        self.signature_service.clone(),
                    )
                    .await;
                    let message = ConsensusMessage::ABAMuxMsg(mux.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter,
                        &self.committee,
                    )
                    .await?;
                    self.handle_aba_mux(&mux).await?;
                } else {
                    values_flag[aba_val.val] = true;
                }
            }
        }
        Ok(())
    }
    

    async fn handle_aba_mux(&mut self, aba_mux: &ABAVal) -> ConsensusResult<()> {
        debug!(
            "processing aba mux epoch {} height {}",
            aba_mux.epoch, aba_mux.height
        );
        aba_mux.verify()?;
    
        if *self.aba_ends.entry((aba_mux.epoch, aba_mux.height)).or_insert(false) {
            return Ok(());
        }
    
        if let Some(start_time) = self.aba_timeouts.get(&(aba_mux.epoch, aba_mux.height)) {
            let timeout_duration = Duration::from_millis(self.parameters.timeout_delay * 2);
            if start_time.elapsed() > timeout_duration {
                warn!(
                    "ABA TIMEOUT on epoch {}, height {}, round {}. Deterministically choosing 1 (OPT) to break deadlock.",
                    aba_mux.epoch, aba_mux.height, aba_mux.round
                );
    
                return self.process_aba_output(aba_mux.epoch, aba_mux.height, aba_mux.round, OPT as usize).await;
            }
        }
    
        let mux_votes = self
            .aba_mux_values
            .entry((aba_mux.epoch, aba_mux.height, aba_mux.round))
            .or_insert_with(|| [HashSet::new(), HashSet::new()]);
    
        if !mux_votes[aba_mux.val].insert(aba_mux.author) {
            return Ok(());
        }
    
        let num_opt_votes = mux_votes[OPT as usize].len() as Stake;
        let num_pes_votes = mux_votes[PES as usize].len() as Stake;
        
        let val_flags = self
            .aba_values_flag
            .entry((aba_mux.epoch, aba_mux.height, aba_mux.round))
            .or_insert([false, false]);
    
        let mut decision: Option<usize> = None;
    
        if num_opt_votes >= self.committee.quorum_threshold() {
            decision = Some(OPT as usize);
        } else if num_pes_votes >= self.committee.quorum_threshold() {
            decision = Some(PES as usize);
        }
        else if num_opt_votes + num_pes_votes >= self.committee.quorum_threshold() {
            if val_flags[OPT as usize] && val_flags[PES as usize] {
                 decision = Some(OPT as usize);
            } 
            else if val_flags[OPT as usize] {
                decision = Some(OPT as usize);
            } else if val_flags[PES as usize] {
                decision = Some(PES as usize);
            }
        }
    
        if let Some(decided_value) = decision {
            return self.process_aba_output(aba_mux.epoch, aba_mux.height, aba_mux.round, decided_value).await;
        }
    
        Ok(())
    }

    async fn handle_aba_output(&mut self, output: &ABAOutput) -> ConsensusResult<()> {
        debug!(
            "processing aba output epoch {} height {}",
            output.epoch, output.height
        );
        output.verify()?;
        let used = self
            .aba_outputs
            .entry((output.epoch, output.height, output.round))
            .or_insert(HashSet::new());
        if used.insert(output.author)
            && used.len() == self.committee.random_coin_threshold() as usize
        {
            if !used.contains(&self.name) {
                let output = ABAOutput::new(
                    self.name,
                    output.epoch,
                    output.height,
                    output.round,
                    output.val,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ABAOutputMsg(output);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
                used.insert(self.name);
            }
            self.process_aba_output(output.epoch, output.height, output.round, output.val)
                .await?;
        }

        Ok(())
    }

    async fn process_aba_output(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
    ) -> ConsensusResult<()> {
        if *self.aba_ends.entry((epoch, height)).or_insert(false) {
            return Ok(());
        }
        debug!("ABA(epoch {} height {}) end output({})", epoch, height, val);
        let used = self
            .aba_outputs
            .entry((epoch, height, round))
            .or_insert(HashSet::new());
        if used.insert(self.name) {
            let output = ABAOutput::new(
                self.name,
                epoch,
                height,
                round,
                val,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::ABAOutputMsg(output);
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
        }

        self.aba_ends.insert((epoch, height), true);

        if val == OPT as usize {
            self.process_rbc_output(epoch, height).await?;
        } else {
            self.commitor
                .filter_block(Self::rank(epoch, height, &self.committee))
                .await;
        }

        Ok(())
    }
    /************* ABA Protocol ******************/
    pub async fn run(&mut self) {
        if let Err(e) = self.generate_rbc_proposal().await {
            panic!("protocol invoke failed! error {}", e);
        }
        let mut previous_epoch = self.epoch;
        let mut epoch_start_time = Instant::now();
        let epoch_timeout = Duration::from_millis(self.parameters.timeout_delay * 5); 

        loop {
            let timer = sleep(Duration::from_millis(5)); 
            tokio::pin!(timer);

            let result = tokio::select! {
                Some(message) = self.rx_core.recv() => {
                    if self.height<self.parameters.fault{
                        continue;
                    }
                    match message {
                        ConsensusMessage::RBCValMsg(block)=> self.handle_rbc_val(&block).await,
                        ConsensusMessage::RBCEchoMsg(evote)=> self.handle_rbc_echo(&evote).await,
                        ConsensusMessage::RBCReadyMsg(rvote)=> self.handle_rbc_ready(&rvote).await,
                        ConsensusMessage::ABAValMsg(val)=>self.handle_aba_val(&val).await,
                        ConsensusMessage::ABAMuxMsg(mux)=> self.handle_aba_mux(&mux).await,
                        ConsensusMessage::ABAOutputMsg(output)=>self.handle_aba_output(&output).await,
                        ConsensusMessage::PrePareMsg(prepare)=>self.handle_prepare(&prepare).await,
                        ConsensusMessage::LoopBackMsg(block) => self.handle_loopback(&block).await, 
                        ConsensusMessage::SyncRequestMsg(epoch,height, sender) => self.handle_sync_request(epoch,height, sender).await,
                        ConsensusMessage::SyncReplyMsg(block) => self.handle_sync_reply(&block).await,
                    }
                },
                Some((digests, epoch, height)) = self.rx_commit_signal.recv() => {

                    let full_transactions = match self.mempool_driver.get_full_transactions(digests.clone()).await {
                        Ok(txs) => txs,
                        Err(e) => {
                            error!("[ConsensusCore] Failed to get full transactions for block (E:{}, H:{}): {}", epoch, height, e);
                            continue; // Bỏ qua nếu không lấy được dữ liệu.
                        }
                    };
                    
                    // GỬI TÍN HIỆU CHO NOTIFIER (KHÔNG CHẶN)
                    let notification = (full_transactions, epoch, height);
                    if let Err(TrySendError::Full(_)) = self.tx_commit_notification.try_send(notification) {
                        warn!("[ConsensusCore] Go-Notifier channel is full. Dropping notification for block (E:{}, H:{}).", epoch, height);
                    }
                    

                    // GỌI CLEANUP MỘT CÁCH ĐỘC LẬP
                    // Việc dọn dẹp mempool vẫn diễn ra như bình thường và không liên quan đến việc gửi sang Go.
                    self.cleanup(digests, epoch, height).await
                },

                () = &mut timer, if epoch_start_time.elapsed() > epoch_timeout => {
                    warn!("Epoch {} has timed out. Forcing fallback to un-stick the protocol.", self.epoch);
                    let fallback_result = self.fallback(self.epoch).await;
                    epoch_start_time = Instant::now(); 
                    fallback_result
                },
    

                else => break,
            };

            if self.epoch > previous_epoch {
                info!("Advanced to new epoch {}", self.epoch);
                epoch_start_time = Instant::now(); 
                previous_epoch = self.epoch; 
            }

            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }

    async fn handle_loopback(&mut self, block: &Block) -> ConsensusResult<()> {
        info!(
            "Resuming processing for block epoch {}, height {} after sync.",
            block.epoch, block.height
        );
        self.store_block(block).await;
        
        self.process_rbc_output(block.epoch, block.height).await
    }
}