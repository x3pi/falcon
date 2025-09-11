use serde::{de, ser, Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use threshold_crypto::{
    PublicKeySet, PublicKeyShare, SecretKeySet, SecretKeyShare, SignatureShare,
};
use threshold_crypto::serde_impl::SerdeSecret;
use sha3::{Digest as Sha3Digest, Keccak256};
use libsecp256k1::{Message, PublicKey as SecpPublicKey, SecretKey as SecpSecretKey, Signature as EcdsaSignature};
use rand::rngs::OsRng;


#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = libsecp256k1::Error;

#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(&self.0))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(&self.0).get(0..16).unwrap())
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for Digest {
    type Error = std::array::TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> {
        Ok(Digest(item.try_into()?))
    }
}

pub trait Hash {
    fn digest(&self) -> Digest;
}

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        let mut hasher = Keccak256::new();
        hasher.update(self);
        Digest(hasher.finalize().into())
    }
}

// Sử dụng 65 bytes cho public key không nén của secp256k1.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PublicKey(pub [u8; 65]);

impl Default for PublicKey {
    fn default() -> Self {
        PublicKey([0; 65])
    }
}

impl PublicKey {
    pub fn to_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..65]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }

    /// Chuyển đổi khóa công khai thành địa chỉ ví Ethereum.
    pub fn to_address(&self) -> String {
        let mut hasher = Keccak256::new();
        // Băm khóa công khai (bỏ qua byte tiền tố 0x04).
        hasher.update(&self.0[1..]);
        let hash = hasher.finalize();

        // Lấy 20 byte cuối của kết quả băm.
        let address_bytes = &hash[hash.len() - 20..];

        // Định dạng thành chuỗi hex với tiền tố "0x".
        format!("0x{}", hex::encode(address_bytes))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_base64())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_base64().get(0..16).unwrap())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::from_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

// Sử dụng 32 bytes cho secp256k1 secret key.
pub struct SecretKey(pub [u8; 32]);

impl SecretKey {
    pub fn to_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..32]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::from_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.0.iter_mut().for_each(|x| *x = 0);
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    generate_keypair()
}

pub fn generate_keypair() -> (PublicKey, SecretKey) {
    let secret = SecpSecretKey::random(&mut OsRng);
    let public = SecpPublicKey::from_secret_key(&secret);
    (PublicKey(public.serialize()), SecretKey(secret.serialize()))
}

// Chữ ký secp256k1 được tuần tự hóa thành 64 bytes.
#[derive(Clone, Debug)]
pub struct Signature(pub [u8; 64]);

impl Default for Signature {
    fn default() -> Self {
        Signature([0; 64])
    }
}

impl Signature {
    pub fn to_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..64]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }

    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        let secret_key = SecpSecretKey::parse(&secret.0).expect("Unable to load secret key");
        let message = Message::parse(&digest.0);
        let signature = libsecp256k1::sign(&message, &secret_key);
        Self(signature.0.serialize())
    }

    pub fn verify(&self, digest: &Digest, public_key: &PublicKey) -> Result<(), CryptoError> {
        let signature = EcdsaSignature::parse_standard_slice(&self.0)
            .map_err(|_| CryptoError::InvalidSignature)?;
        let public_key = SecpPublicKey::parse_slice(&public_key.0, None)
            .map_err(|_| CryptoError::InvalidPublicKey)?;
        let message = Message::parse(&digest.0);

        if libsecp256k1::verify(&message, &signature, &public_key) {
            Ok(())
        } else {
            Err(CryptoError::InvalidSignature)
        }
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::from_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    tss_channel: Option<Sender<(Digest, oneshot::Sender<SignatureShare>)>>,
}

impl SignatureService {
    pub fn new(secret: SecretKey, tss_secret: Option<SecretKeyShare>) -> Self {
        let (tx, mut rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                let signature = Signature::new(&digest, &secret);
                let _ = sender.send(signature);
            }
        });
        let (tss_tx, mut tss_rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        if let Some(secret_share) = tss_secret {
            tokio::spawn(async move {
                while let Some((digest, sender)) = tss_rx.recv().await {
                    let signature_share = secret_share.sign(digest);
                    let _ = sender.send(signature_share);
                }
            });
            return Self { channel: tx, tss_channel: Some(tss_tx) };
        }
        Self { channel: tx, tss_channel: None }
    }

    pub async fn request_signature(&mut self, digest: Digest) -> Signature {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }

    pub async fn request_tss_signature(&mut self, digest: Digest) -> Option<SignatureShare> {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Some(channel) = &self.tss_channel {
            if let Err(e) = channel.send((digest, sender)).await {
                panic!("Failed to send message TSS Signature Service: {}", e);
            }
            return Some(receiver
                .await
                .expect("Failed to receive tss signature share from TSS Signature Service"));
        }
        return None;
    }
}

// Wrapper for threshold signature key shares
#[derive(Serialize, Deserialize, Debug)]
pub struct SecretShare {
    pub id: usize,
    pub name: PublicKeyShare,
    pub secret: SerdeSecret<SecretKeyShare>,
    pub pkset: PublicKeySet,
}

impl SecretShare {
    pub fn new(id: usize, name: PublicKeyShare, secret: SerdeSecret<SecretKeyShare>, pkset: PublicKeySet) -> Self {
        Self { id, name, secret, pkset }
    }
}

impl Default for SecretShare {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let sk_set = SecretKeySet::random(0, &mut rng);
        let pk_set = sk_set.public_keys();
        let sk_share = sk_set.secret_key_share(0);
        let pk_share = pk_set.public_key_share(0);
        Self{ id: 0, name: pk_share, secret: SerdeSecret(sk_share), pkset: pk_set}
    }
}