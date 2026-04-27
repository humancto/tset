use crate::constants::{FOOTER_SIZE, MAGIC_FOOTER, TRUNCATED_HASH_SIZE};
use crate::error::{TsetError, TsetResult};

/// Layout (40 bytes total):
///   [ 0.. 8]  manifest_size      u64 LE
///   [ 8..36]  manifest_hash[..28] (first 28 bytes of BLAKE3)
///   [36..40]  magic = b"TEND"
#[derive(Debug, Clone)]
pub struct Footer {
    pub manifest_size: u64,
    pub manifest_hash28: [u8; TRUNCATED_HASH_SIZE],
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut out = [0u8; FOOTER_SIZE];
        out[0..8].copy_from_slice(&self.manifest_size.to_le_bytes());
        out[8..36].copy_from_slice(&self.manifest_hash28);
        out[36..40].copy_from_slice(MAGIC_FOOTER);
        out
    }

    pub fn decode(data: &[u8]) -> TsetResult<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(TsetError::BadManifest("footer buffer too small"));
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[36..40]);
        if &magic != MAGIC_FOOTER {
            return Err(TsetError::BadFooterMagic(magic));
        }
        let manifest_size = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let mut manifest_hash28 = [0u8; TRUNCATED_HASH_SIZE];
        manifest_hash28.copy_from_slice(&data[8..36]);
        Ok(Self {
            manifest_size,
            manifest_hash28,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let f = Footer {
            manifest_size: 4242,
            manifest_hash28: [3u8; 28],
        };
        let enc = f.encode();
        let dec = Footer::decode(&enc).unwrap();
        assert_eq!(dec.manifest_size, 4242);
        assert_eq!(dec.manifest_hash28, [3u8; 28]);
    }
}
