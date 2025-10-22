use thiserror::Error;

pub type ZgResult<T> = Result<T, ZgError>;

#[derive(Error, Debug)]
pub enum ZgError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

