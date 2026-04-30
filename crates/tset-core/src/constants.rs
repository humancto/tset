pub const MAGIC_HEADER: &[u8; 4] = b"TSET";
pub const MAGIC_FOOTER: &[u8; 4] = b"TEND";
pub const MAGIC_DOC_BLOCK: &[u8; 4] = b"DBLK";
pub const MAGIC_VIEW: &[u8; 4] = b"TVEW";
// On-disk binary sections (v0.4+; opt-in additive in v0.3 writers).
pub const MAGIC_SMT: &[u8; 4] = b"TSMT";
pub const MAGIC_AUDIT_LOG: &[u8; 4] = b"TLOG";
pub const MAGIC_COLUMNS: &[u8; 4] = b"TCOL";

pub const VERSION_MAJOR: u8 = 0;
pub const VERSION_MINOR: u8 = 4;
pub const SUPPORTED_MINOR_VERSIONS: &[u8] = &[1, 2, 3, 4];

pub const HEADER_SIZE: usize = 4096;
pub const FOOTER_SIZE: usize = 40;

pub const HASH_SIZE: usize = 32;
pub const TRUNCATED_HASH_SIZE: usize = 28;

pub const VIEW_HEADER_SIZE: usize = 52;
pub const CHUNK_HEADER_SIZE: usize = 24;

/// Token IDs are stored as little-endian u32 in v0.1/v0.2.
pub const TOKEN_BYTES: usize = 4;

pub const ZSTD_LEVEL: i32 = 6;
