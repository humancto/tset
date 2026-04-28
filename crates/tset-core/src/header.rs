use crate::constants::{
    HASH_SIZE, HEADER_SIZE, MAGIC_HEADER, SUPPORTED_MINOR_VERSIONS, VERSION_MAJOR,
};
use crate::error::{TsetError, TsetResult};
use crate::hashing::Hash;

#[derive(Debug, Clone)]
pub struct Header {
    pub version_major: u8,
    pub version_minor: u8,
    pub flags: u32,
    pub manifest_offset: u64,
    pub manifest_size: u64,
    pub shard_merkle_root: Hash,
    pub manifest_hash: Hash,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut out = [0u8; HEADER_SIZE];
        out[0..4].copy_from_slice(MAGIC_HEADER);
        out[4] = self.version_major;
        out[5] = self.version_minor;
        out[8..12].copy_from_slice(&self.flags.to_le_bytes());
        out[16..24].copy_from_slice(&self.manifest_offset.to_le_bytes());
        out[24..32].copy_from_slice(&self.manifest_size.to_le_bytes());
        out[32..64].copy_from_slice(&self.shard_merkle_root);
        out[64..96].copy_from_slice(&self.manifest_hash);
        out
    }

    pub fn decode(data: &[u8]) -> TsetResult<Self> {
        if data.len() < HEADER_SIZE {
            return Err(TsetError::BadManifest("header buffer too small"));
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[0..4]);
        if &magic != MAGIC_HEADER {
            return Err(TsetError::BadHeaderMagic(magic));
        }
        let version_major = data[4];
        let version_minor = data[5];
        if version_major != VERSION_MAJOR || !SUPPORTED_MINOR_VERSIONS.contains(&version_minor) {
            return Err(TsetError::UnsupportedVersion {
                major: version_major,
                minor: version_minor,
            });
        }
        let flags = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if flags != 0 {
            return Err(TsetError::UnexpectedFlags(flags));
        }
        let manifest_offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let manifest_size = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let mut shard_merkle_root = [0u8; HASH_SIZE];
        shard_merkle_root.copy_from_slice(&data[32..64]);
        let mut manifest_hash = [0u8; HASH_SIZE];
        manifest_hash.copy_from_slice(&data[64..96]);
        Ok(Self {
            version_major,
            version_minor,
            flags,
            manifest_offset,
            manifest_size,
            shard_merkle_root,
            manifest_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let h = Header {
            version_major: 0,
            version_minor: 2,
            flags: 0,
            manifest_offset: 8192,
            manifest_size: 1234,
            shard_merkle_root: [7u8; 32],
            manifest_hash: [9u8; 32],
        };
        let enc = h.encode();
        let dec = Header::decode(&enc).unwrap();
        assert_eq!(dec.manifest_offset, 8192);
        assert_eq!(dec.shard_merkle_root, [7u8; 32]);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");
        assert!(matches!(
            Header::decode(&buf),
            Err(TsetError::BadHeaderMagic(_))
        ));
    }

    #[test]
    fn rejects_nonzero_flags() {
        let h = Header {
            version_major: 0,
            version_minor: 2,
            flags: 0,
            manifest_offset: 0,
            manifest_size: 0,
            shard_merkle_root: [0u8; 32],
            manifest_hash: [0u8; 32],
        };
        let mut enc = h.encode();
        enc[8..12].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(
            Header::decode(&enc),
            Err(TsetError::UnexpectedFlags(1))
        ));
    }

    #[test]
    fn rejects_short_buffer_without_panic() {
        let buf = [0u8; 10];
        assert!(matches!(
            Header::decode(&buf),
            Err(TsetError::BadManifest(_))
        ));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut h = Header {
            version_major: 0,
            version_minor: 2,
            flags: 0,
            manifest_offset: 0,
            manifest_size: 0,
            shard_merkle_root: [0u8; 32],
            manifest_hash: [0u8; 32],
        };
        h.version_minor = 9;
        let enc = h.encode();
        assert!(matches!(
            Header::decode(&enc),
            Err(TsetError::UnsupportedVersion { major: 0, minor: 9 })
        ));
    }
}
