//! Ed25519 audit-log signing.
//!
//! Each audit-log entry's `entry_hash` (BLAKE3 of the canonical
//! `{seq, timestamp, event_type, payload}`) is signed with a writer-held
//! Ed25519 private key. The corresponding public key lives in the
//! manifest's audit_log section, not the entry itself, so all entries
//! share one pubkey per shard.
//!
//! On read, every entry's `signature` field is verified against the
//! manifest's `writer_public_key`. A missing signature is allowed for
//! backward compat with v0.1–v0.3 shards (where signing was absent);
//! mixed-signature/no-signature within a single audit log is rejected
//! to prevent drop-the-signature downgrade attacks.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::error::{TsetError, TsetResult};

pub const SIGNATURE_LEN: usize = 64;
pub const PUBLIC_KEY_LEN: usize = 32;
pub const SECRET_KEY_LEN: usize = 32;

/// Convenience wrapper around `SigningKey`. Construction is deliberately
/// explicit — you either generate (random; OS RNG via `rand`) or load
/// an existing 32-byte secret. We never default-impl a key.
pub struct AuditSigner {
    inner: SigningKey,
}

impl AuditSigner {
    pub fn from_secret_bytes(bytes: &[u8]) -> TsetResult<Self> {
        if bytes.len() != SECRET_KEY_LEN {
            return Err(TsetError::BadManifest(
                "Ed25519 secret key must be 32 bytes",
            ));
        }
        let mut secret = [0u8; SECRET_KEY_LEN];
        secret.copy_from_slice(bytes);
        Ok(Self {
            inner: SigningKey::from_bytes(&secret),
        })
    }

    pub fn generate() -> Self {
        let mut bytes = [0u8; SECRET_KEY_LEN];
        getrandom_fill(&mut bytes);
        Self {
            inner: SigningKey::from_bytes(&bytes),
        }
    }

    pub fn public_key_bytes(&self) -> [u8; PUBLIC_KEY_LEN] {
        self.inner.verifying_key().to_bytes()
    }

    pub fn secret_bytes(&self) -> [u8; SECRET_KEY_LEN] {
        self.inner.to_bytes()
    }

    pub fn sign(&self, message: &[u8]) -> [u8; SIGNATURE_LEN] {
        self.inner.sign(message).to_bytes()
    }
}

pub fn verify_signature(
    public_key_bytes: &[u8],
    message: &[u8],
    signature_bytes: &[u8],
) -> bool {
    if public_key_bytes.len() != PUBLIC_KEY_LEN || signature_bytes.len() != SIGNATURE_LEN {
        return false;
    }
    let mut pk_arr = [0u8; PUBLIC_KEY_LEN];
    pk_arr.copy_from_slice(public_key_bytes);
    let pk = match VerifyingKey::from_bytes(&pk_arr) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let mut sig_arr = [0u8; SIGNATURE_LEN];
    sig_arr.copy_from_slice(signature_bytes);
    let sig = Signature::from_bytes(&sig_arr);
    pk.verify(message, &sig).is_ok()
}

fn getrandom_fill(out: &mut [u8]) {
    use rand::RngCore;
    let mut rng = rand::rngs::OsRng;
    rng.fill_bytes(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let signer = AuditSigner::generate();
        let pk = signer.public_key_bytes();
        let msg = b"hello world";
        let sig = signer.sign(msg);
        assert!(verify_signature(&pk, msg, &sig));
        assert!(!verify_signature(&pk, b"tampered", &sig));
    }

    #[test]
    fn from_secret_bytes_rejects_wrong_len() {
        assert!(AuditSigner::from_secret_bytes(&[0u8; 31]).is_err());
        assert!(AuditSigner::from_secret_bytes(&[0u8; 32]).is_ok());
        assert!(AuditSigner::from_secret_bytes(&[0u8; 33]).is_err());
    }

    #[test]
    fn rejects_malformed_inputs_without_panic() {
        assert!(!verify_signature(b"short", b"msg", &[0u8; 64]));
        assert!(!verify_signature(&[0u8; 32], b"msg", b"short"));
    }
}
