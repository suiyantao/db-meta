#![allow(dead_code, unused_variables)]

use derive_more::Display;
use log::error;
use thiserror::Error;

#[derive(Debug, Display, Error)]
pub enum MetaError {
    #[display("{_0}")]
    BadRequest(String),

    #[display("{_0}")]
    DbException(String),

    #[display("参数错误: {_0}")]
    InvalidArgument(String),
}

impl From<sqlx::Error> for MetaError {
    fn from(value: sqlx::Error) -> Self {
        match value {
            e => {
                error!("{:?}", e);
                MetaError::DbException(format!("{}", e))
            }
        }
    }
}

impl From<std::io::Error> for MetaError {
    fn from(value: std::io::Error) -> Self {
        match value {
            e => {
                error!("{:?}", e);
                MetaError::BadRequest(format!("{}", e))
            }
        }
    }
}
