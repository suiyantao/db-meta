use derive_more::Display;
use log::error;
use thiserror::Error;

#[derive(Debug, Display, Error)]
pub enum ServiceError {
    #[display("{_0}")]
    BadRequest(String),

    #[display("{_0}")]
    DbException(String),

    #[display("参数错误: {_0}")]
    InvalidArgument(String),
}

impl From<sqlx::Error> for ServiceError {
    fn from(value: sqlx::Error) -> Self {
        match value {
            e => {
                error!("{:?}", e);
                ServiceError::DbException(format!("{}", e))
            }
        }
    }
}

impl From<std::io::Error> for ServiceError {
    fn from(value: std::io::Error) -> Self {
        match value {
            e => {
                error!("{:?}", e);
                ServiceError::BadRequest(format!("{}", e))
            }
        }
    }
}
