#![allow(dead_code, unused_variables)]
use async_trait::async_trait;

use crate::{
    error::MetaError,
    modal::{ConnConfig, DbType, MetaData, TableInfo, ViewsInfo},
    mysql_meta::MysqlMeta,
    pg_meta::PgMeta,
};

// 数据库元数据采集
#[derive(Debug, Clone)]
pub struct MetaDataService {
    pub connection: ConnConfig,
}

impl MetaDataService {
    pub fn new(cc: ConnConfig) -> Result<Self, MetaError> {
        cc.validate()?;
        Ok(Self { connection: cc })
    }

    async fn create_meta(&self) -> Result<Box<dyn MetaTrait>, MetaError> {
        return match self.connection.db_type {
            DbType::Postgresql => Ok(Box::new(PgMeta::new(&self.connection).await?)),
            DbType::MySql => Ok(Box::new(MysqlMeta::new(&self.connection).await?)),
            DbType::MariaDB => return Err(MetaError::InvalidArgument("暂不支持MariaDB".into())),
            DbType::SQLite => return Err(MetaError::InvalidArgument("暂不支持SQLite".into())),
        };
    }

    pub async fn get_meta(&self) -> Result<MetaData, MetaError> {
        // 在 get_meta 方法中调用抽取的方法
        let meta = self.create_meta().await?;

        let mut table_info_vec = meta.get_tables().await?;
        meta.set_pk_key(&mut table_info_vec).await?;
        meta.set_index_key(&mut table_info_vec).await?;
        meta.set_column(&mut table_info_vec).await?;

        let mut view_info_view = meta.get_views().await?;
        meta.set_view_column(&mut view_info_view).await?;
        Ok(MetaData {
            tables: table_info_vec,
            views: view_info_view,
        })
    }
}

type MyResult<T> = Result<T, MetaError>;

#[async_trait]
pub trait MetaTrait: Send + Sync {
    /// 获取表
    async fn get_tables(&self) -> MyResult<Vec<TableInfo>>;

    /// 设置表的主键
    async fn set_pk_key(&self, table_vec: &mut Vec<TableInfo>) -> MyResult<()>;

    /// 设置表的索引
    async fn set_index_key(&self, table_vec: &mut Vec<TableInfo>) -> MyResult<()>;

    /// 设置表的字段
    async fn set_column(&self, table_vec: &mut Vec<TableInfo>) -> MyResult<()>;

    /// 获取视图
    async fn get_views(&self) -> MyResult<Vec<ViewsInfo>>;

    /// 设置视图的字段
    async fn set_view_column(&self, view_vec: &mut Vec<ViewsInfo>) -> MyResult<()>;

    /// 执行sql
    async fn count(&self, sql: &str) -> MyResult<i64>;

    /// query
    async fn query(&self, sql: &str) -> MyResult<Vec<Vec<String>>>;
}
