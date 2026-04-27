pub const MAGIC_HEADER: &[u8; 4] = b"TSET";
pub const MAGIC_FOOTER: &[u8; 4] = b"TEND";
pub const MAGIC_DOC_BLOCK: &[u8; 4] = b"DBLK";
pub const MAGIC_VIEW: &[u8; 4] = b"TVEW";

pub const VERSION_MAJOR: u8 = 0;
pub const VERSION_MINOR: u8 = 2;
pub const SUPPORTED_MINOR_VERSIONS: &[u8] = &[1, 2];

pub const HEADER_SIZE: usize = 4096;
pub const FOOTER_SIZE: usize = 40;

pub const HASH_SIZE: usize = 32;
pub const TRUNCATED_HASH_SIZE: usize = 28;

pub const VIEW_HEADER_SIZE: usize = 52;
pub const CHUNK_HEADER_SIZE: usize = 24;

/// Token IDs are stored as little-endian u32 in v0.1/v0.2.
pub const TOKEN_BYTES: usize = 4;

pub const ZSTD_LEVEL: i32 = 6;
