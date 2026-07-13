//! Error type for the crate. One enum, `thiserror` derive — see
//! IMPLEMENTATION.md Step 0.

#[derive(Debug, thiserror::Error)]
pub enum SalamanderError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt record at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: String },

    #[error("offset {0} beyond head")]
    OffsetBeyondHead(u64),

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error(
        "unsupported payload format version {found}: this build supports {supported}. \
         The data dir was written by a newer SalamanderDB; upgrade to read it."
    )]
    UnsupportedFormat { found: u32, supported: u32 },

    #[error("unsupported storage format version {found}: this build writes version {supported}; migrate the source directory offline")]
    UnsupportedStorageFormat { found: u32, supported: u32 },

    #[error("invalid segment file name: {0}")]
    InvalidSegmentName(String),

    #[error("namespace {0} already exists")]
    NamespaceExists(String),

    #[error("data dir is locked by another process: {0}")]
    Locked(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid format: {0}")]
    InvalidFormat(String),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("resource limit exceeded: {resource} is {actual}, maximum is {maximum}")]
    ResourceLimit {
        resource: &'static str,
        actual: u64,
        maximum: u64,
    },

    #[error("migration error: {0}")]
    Migration(String),

    #[error("migration is incomplete for {0}; resume it with the migration command")]
    MigrationIncomplete(String),

    #[error("stream revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: String, actual: String },

    #[error("event ID already exists with different content")]
    EventIdConflict,

    #[error("idempotency key already exists with different content")]
    IdempotencyConflict,

    #[error("batch ID already exists with different content")]
    BatchIdConflict,

    #[error("branch not found: {0}")]
    BranchNotFound(String),

    #[error("branch already exists: {0}")]
    BranchExists(String),

    #[error("branch is archived: {0}")]
    BranchArchived(String),

    #[error("invalid branch ancestry: {0}")]
    InvalidBranchAncestry(String),

    #[error("position {0} is not a committed batch boundary")]
    NotBatchBoundary(u64),
}

pub type Result<T> = std::result::Result<T, SalamanderError>;
