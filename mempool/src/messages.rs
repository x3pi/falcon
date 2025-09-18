// mempool/src/messages.rs

use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type Transaction = Vec<u8>;

#[derive(Deserialize, Serialize)]
pub struct Payload {
    pub transactions: Vec<Transaction>,
    pub author: PublicKey,
    pub signature: Signature,
}

// ... (impl Payload giữ nguyên) ...

impl Hash for Payload {
    fn digest(&self) -> Digest {
        let mut bytes = self.author.0.to_vec();
        for transaction in &self.transactions {
            bytes.extend_from_slice(transaction);
        }
        bytes.as_slice().digest()
    }
}

// ... (impl fmt::Debug giữ nguyên) ...
// Để chắc chắn, dưới đây là toàn bộ file:
impl Payload {
    pub async fn new(transactions: Vec<Transaction>, author: PublicKey, mut signature_service: SignatureService) -> Self {
        let payload = Self { transactions, author, signature: Signature::default() };
        let signature = signature_service.request_signature(payload.digest()).await;
        Self { signature, ..payload }
    }

    pub fn size(&self) -> usize {
        self.transactions.iter().map(|x| x.len()).sum()
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Payload({}, {})", self.digest(), self.size())
    }
}