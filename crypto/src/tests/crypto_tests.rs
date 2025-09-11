use super::*;
use libsecp256k1::{PublicKey as SecpPublicKey, SecretKey as SecpSecretKey};

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_base64())
    }
}

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    (0..4).map(|_| generate_keypair()).collect()
}

#[test]
fn import_export_public_key() {
    let (public_key, _) = keys().pop().unwrap();
    let export = public_key.to_base64();
    let import = PublicKey::from_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let (_, secret_key) = keys().pop().unwrap();
    let export = secret_key.to_base64();
    let import = SecretKey::from_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    let bad_message: &[u8] = b"Bad message!";
    let digest = bad_message.digest();
    assert!(signature.verify(&digest, &public_key).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key, None);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest.clone()).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn generate_ethereum_address() {
    // Khóa bí mật và địa chỉ ví đã biết để kiểm tra.
    // a10d6e6340b6d5e6d775560a3d5714a1939dc110a99cb1d156cd861346a32aab
    
    let secret_bytes = hex::decode("a10d6e6340b6d5e6d775560a3d5714a1939dc110a99cb1d156cd861346a32aab").unwrap();
    let secret_key_secp = SecpSecretKey::parse_slice(&secret_bytes).unwrap();
    let public_key_secp = SecpPublicKey::from_secret_key(&secret_key_secp);

    let public_key = PublicKey(public_key_secp.serialize());

    let expected_address = "0x924897dc867f06a1e3c5579bb0b75df3025d5e9d";
    let calculated_address = public_key.to_address();

    assert_eq!(calculated_address, expected_address);
}