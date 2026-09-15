//! Error type shared by every layer of the engine.

use std::fmt;
use std::io;

/// Result alias used throughout emberdb.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by emberdb.
#[derive(Debug)]
pub enum Error {
    /// An underlying filesystem operation failed.
    Io(io::Error),
    /// On-disk data failed validation (bad checksum, bad magic, malformed encoding).
    Corruption(String),
    /// The caller supplied an invalid argument or option.
    InvalidArgument(String),
    /// A previous background flush or compaction failed; the database is now read-only
    /// until it is reopened.
    Background(String),
}

impl Error {
    pub(crate) fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(msg) => write!(f, "corruption: {msg}"),
            Error::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Error::Background(msg) => write!(f, "background error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
