use thiserror::Error;

#[derive(Debug, Error)]
pub enum TsetError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("zstd decompression failed: {0}")]
    Zstd(String),

    #[error("hex decode: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("bad header magic: expected TSET, got {0:?}")]
    BadHeaderMagic([u8; 4]),

    #[error("bad footer magic: expected TEND, got {0:?}")]
    BadFooterMagic([u8; 4]),

    #[error("bad block magic: expected DBLK, got {0:?}")]
    BadBlockMagic([u8; 4]),

    #[error("bad view magic: expected TVEW, got {0:?}")]
    BadViewMagic([u8; 4]),

    #[error("unsupported version {major}.{minor}")]
    UnsupportedVersion { major: u8, minor: u8 },

    #[error("unexpected header flags: 0x{0:08x}")]
    UnexpectedFlags(u32),

    #[error("manifest hash mismatch ({0})")]
    ManifestHashMismatch(&'static str),

    #[error("manifest size mismatch between header and footer")]
    ManifestSizeMismatch,

    #[error("shard merkle root mismatch ({0})")]
    ShardMerkleRootMismatch(&'static str),

    #[error("audit log integrity check failed")]
    AuditLogIntegrityFailed,

    #[error("document hash mismatch on read")]
    DocumentHashMismatch,

    #[error("document content_size mismatch")]
    DocumentContentSizeMismatch,

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("tokenization view not found: {0}")]
    ViewNotFound(String),

    #[error("chunk compressed_size mismatch with manifest")]
    ChunkCompressedSizeMismatch,

    #[error("chunk num_tokens mismatch with manifest")]
    ChunkNumTokensMismatch,

    #[error("chunk decompressed size mismatch")]
    ChunkUncompressedSizeMismatch,

    #[error("chunk content_hash mismatch (compressed payload tampered)")]
    ChunkContentHashMismatch,

    #[error("view config_hash on disk disagrees with manifest")]
    ViewConfigHashMismatch,

    #[error("view total_tokens on disk ({on_disk}) differs from manifest ({manifest})")]
    ViewTotalTokensMismatch { on_disk: u64, manifest: u64 },

    #[error("view num_chunks on disk ({on_disk}) differs from manifest ({manifest})")]
    ViewNumChunksMismatch { on_disk: u64, manifest: u64 },

    #[error("token id {0} >= vocab_size {1}")]
    TokenIdOutOfRange(u32, u32),

    #[error("manifest field invalid or missing: {0}")]
    BadManifest(&'static str),
}

pub type TsetResult<T> = std::result::Result<T, TsetError>;
