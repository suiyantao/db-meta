#![allow(dead_code, unused_variables)]
use async_trait::async_trait;

use crate::{
    error::MetaError,
    // 推测这里可能是拼写错误，应该是 `model` 而非 `modal`
    modal::{ConnConfig, DbType, Metadata, TableInfo, ViewsInfo},
    mysql_meta::MysqlMeta,
    pg_meta::PgMeta,
};

// 数据库元数据采集
#[derive(Debug, Clone)]
pub struct MetadataService {
    pub connection: ConnConfig,
}

impl MetadataService {
    pub fn new(connection_config: ConnConfig) -> Result<Self, MetaError> {
        connection_config.validate()?;
        Ok(Self { connection: connection_config })
    }

    async fn create_metadata_handler(&self) -> Result<Box<dyn MetaTrait>, MetaError> {
        match self.connection.db_type {
            DbType::Postgresql => Ok(Box::new(PgMeta::new(&self.connection).await?)),
            DbType::MySql => Ok(Box::new(MysqlMeta::new(&self.connection).await?)),
            DbType::MariaDb => Err(MetaError::InvalidArgument("暂不支持MariaDB".into())),
            DbType::Sqlite => Err(MetaError::InvalidArgument("暂不支持SQLite".into())),
        }
    }

    pub async fn get_metadata(&self) -> Result<Metadata, MetaError> {
        // 在 get_metadata 方法中调用抽取的方法
        let metadata_handler = self.create_metadata_handler().await?;

        let mut tables_info = metadata_handler.get_tables().await?;
        metadata_handler.set_primary_key(&mut tables_info).await?;
        metadata_handler.set_index_key(&mut tables_info).await?;
        metadata_handler.set_columns(&mut tables_info).await?;

        let mut views_info = metadata_handler.get_views().await?;
        metadata_handler.set_view_columns(&mut views_info).await?;
        Ok(Metadata {
            tables: tables_info,
            views: views_info,
        })
    }
}

type MetadataResult<T> = Result<T, MetaError>;

#[async_trait]
pub trait MetaTrait: Send + Sync {
    /// 获取表
    async fn get_tables(&self) -> MetadataResult<Vec<TableInfo>>;

    /// 设置表的主键
    async fn set_primary_key(&self, tables: &mut Vec<TableInfo>) -> MetadataResult<()>;

    /// 设置表的索引
    async fn set_index_key(&self, tables: &mut Vec<TableInfo>) -> MetadataResult<()>;

    /// 设置表的字段
    async fn set_columns(&self, tables: &mut Vec<TableInfo>) -> MetadataResult<()>;

    /// 获取视图
    async fn get_views(&self) -> MetadataResult<Vec<ViewsInfo>>;

    /// 设置视图的字段
    async fn set_view_columns(&self, views: &mut Vec<ViewsInfo>) -> MetadataResult<()>;

    /// 执行sql
    async fn count(&self, sql: &str) -> MetadataResult<i64>;

    /// query
    async fn query(&self, sql: &str) -> MetadataResult<Vec<Vec<String>>>;
}
