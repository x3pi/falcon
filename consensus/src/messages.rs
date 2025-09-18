use crate::commitor::MAX_BLOCK_BUFFER;
use crate::config::Committee;
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
// crypto::Hash đã bao gồm cả crypto::Digest
use crypto::{Hash, PublicKey, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use std::fmt;

// ... (struct Block và các struct khác giữ nguyên) ...

// === PHẦN SỬA LỖI QUAN TRỌNG: THỐNG NHẤT HASHING ===

impl Hash for Block {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        for x in &self.payload {
            bytes.extend_from_slice(x.as_ref());
        }
        bytes.as_slice().digest()
    }
}

impl Hash for EchoVote {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(self.digest.as_ref());
        bytes.as_slice().digest()
    }
}

impl Hash for ReadyVote {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(self.digest.as_ref());
        bytes.as_slice().digest()
    }
}

impl Hash for Prepare {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(&self.val.to_le_bytes());
        bytes.extend_from_slice(&self.phase.to_le_bytes());
        bytes.as_slice().digest()
    }
}

impl Hash for ABAVal {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.round.to_le_bytes());
        bytes.as_slice().digest()
    }
}

impl Hash for ABAOutput {
    fn digest(&self) -> crypto::Digest {
        let mut bytes = self.author.0.to_vec();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(&self.round.to_le_bytes());
        bytes.as_slice().digest()
    }
}

// ... (các phần còn lại của file như impl Block, impl fmt::Debug, v.v. giữ nguyên) ...
// Hãy đảm bảo các struct và các hàm khác vẫn còn đó. Đoạn code trên chỉ thay thế
// các khối `impl Hash for ...`

// Dưới đây là toàn bộ file để bạn dễ thay thế:
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub payload: Vec<crypto::Digest>,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        author: PublicKey,
        epoch: SeqNumber,
        height: SeqNumber,
        payload: Vec<crypto::Digest>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            author,
            epoch,
            height,
            payload,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(self.author));
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: B(author {}, epoch {},  height {}, payload_len {})", self.digest(), self.author, self.epoch, self.height, self.payload.iter().map(|x| x.size()).sum::<usize>())
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: B(author {}, epoch {},  height {}, payload_len {})", self.digest(), self.author, self.epoch, self.height, self.payload.iter().map(|x| x.size()).sum::<usize>())
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct EchoVote {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub digest: crypto::Digest,
    pub signature: Signature,
}

impl EchoVote {
    pub async fn new(author: PublicKey, epoch: SeqNumber, height: SeqNumber, block: &Block, mut signature_service: SignatureService) -> Self {
        let mut vote = Self { author, epoch, height, digest: block.digest(), signature: Signature::default() };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        vote
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(self.author));
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}

impl fmt::Debug for EchoVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: EchoVote(author {}, epoch {},  height {})", self.digest(), self.author, self.epoch, self.height)
    }
}

impl fmt::Display for EchoVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: EchoVote(author {}, epoch {},  height {})", self.digest(), self.author, self.epoch, self.height)
    }
}


#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ReadyVote {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub digest: crypto::Digest,
    pub signature: Signature,
}

impl ReadyVote {
    pub async fn new(author: PublicKey, epoch: SeqNumber, height: SeqNumber, digest: crypto::Digest, mut signature_service: SignatureService) -> Self {
        let mut vote = Self { author, epoch, height, digest, signature: Signature::default() };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        vote
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(self.author));
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}

impl fmt::Debug for ReadyVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: ReadyVote(author {}, epoch {},  height {})", self.digest(), self.author, self.epoch, self.height)
    }
}

impl fmt::Display for ReadyVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: ReadyVote(author {}, epoch {},  height {})", self.digest(), self.author, self.epoch, self.height)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct RBCProof {
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub votes: Vec<(PublicKey, Signature)>,
    pub tag: u8,
}

impl RBCProof {
    pub fn new(epoch: SeqNumber, height: SeqNumber, votes: Vec<(PublicKey, Signature)>, tag: u8) -> Self {
        Self { epoch, height, votes, tag }
    }
}

impl fmt::Debug for RBCProof {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "RBCProof(epoch {}, height {},tag {})", self.epoch, self.height, self.tag)
    }
}

impl fmt::Display for RBCProof {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "RBCProof(epoch {},  height {},tag {})", self.epoch, self.height, self.tag)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Prepare {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub val: u8,
    pub phase: u8,
    pub signature: Signature,
}

impl Prepare {
    pub async fn new(author: PublicKey, epoch: SeqNumber, height: SeqNumber, phase: u8, val: u8, mut signature_service: SignatureService) -> Self {
        let mut prepare = Self { author, epoch, height, val, phase, signature: Signature::default() };
        prepare.signature = signature_service.request_signature(prepare.digest()).await;
        prepare
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(self.author));
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}


impl fmt::Debug for Prepare {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}: Prepare(author {}, epoch {},  height {}, phase {}, val {})", self.digest(), self.author, self.epoch, self.height, self.phase, self.val)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ABAVal {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub phase: u8,
    pub val: usize,
    pub signature: Signature,
}

impl ABAVal {
    pub async fn new(author: PublicKey, epoch: SeqNumber, height: SeqNumber, round: SeqNumber, val: usize, phase: u8, mut signature_service: SignatureService) -> Self {
        let mut aba_val = Self { author, epoch, height, round, val, phase, signature: Signature::default() };
        aba_val.signature = signature_service.request_signature(aba_val.digest()).await;
        aba_val
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}


impl fmt::Debug for ABAVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ABAVal(author{},epoch {},height {},round {},phase {},val {})", self.author, self.epoch, self.height, self.round, self.phase, self.val)
    }
}

impl fmt::Display for ABAVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ABAVal(author{},epoch {},height {},round {},phase {},val {})", self.author, self.epoch, self.height, self.round, self.phase, self.val)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ABAOutput {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub val: usize,
    pub signature: Signature,
}

impl ABAOutput {
    pub async fn new(author: PublicKey, epoch: SeqNumber, height: SeqNumber, round: SeqNumber, val: usize, mut signature_service: SignatureService) -> Self {
        let mut out = Self { author, epoch, height, round, val, signature: Signature::default() };
        out.signature = signature_service.request_signature(out.digest()).await;
        out
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }

    pub fn rank(&self, committee: &Committee) -> usize {
        ((self.epoch as usize) * committee.size() + (self.height as usize)) % MAX_BLOCK_BUFFER
    }
}

impl fmt::Debug for ABAOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ABAOutput(author {},epoch {},height {},round {},val {})", self.author, self.epoch, self.height, self.round, self.val)
    }
}