use crate::error::ServiceError;
use crate::modal::{Column, ConnConfig, FiledTypeEnum, IndexInfo, TableInfo, ViewsInfo};
use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool, Row};
use std::collections::HashMap;
use std::time::Duration;

use super::meta::MetaTrait;

#[derive(Debug, Clone)]
pub struct MysqlMeta {
    pub(crate) pool: Pool<MySql>,
    pub(crate) conn_config: ConnConfig,
}

impl MysqlMeta {
    pub(crate) async fn new(conn_config: &ConnConfig) -> Result<Self, ServiceError> {
        let url = format!(
            "mysql://{user_name}:{password}@{host}:{port}/{dbname}",
            user_name = conn_config.username,
            password = conn_config.password,
            host = conn_config.url,
            port = conn_config.port,
            dbname = conn_config.database
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(30)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await?;
        Ok(Self {
            pool,
            conn_config: conn_config.clone(),
        })
    }

    async fn get_columns(
        &self,
        table_names: Vec<String>,
        pk_map: HashMap<String, String>,
    ) -> Result<HashMap<String, Vec<Column>>, ServiceError> {
        let tables_str = table_names.join("','");

        let sql = format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    COLUMN_COMMENT,
                    EXTRA
             FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = '{schema}'
               AND TABLE_NAME IN ('{tables_str}')",
            schema = &self.conn_config.database
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let mut column_map = HashMap::new();

        for row in rows {
            let table: String = row.get(0);
            let column_name: String = row.get(1);
            let type_name: String = row.get(2);

            // 处理评论
            let comment: String = row.get(7);
            let comment = if comment.is_empty() {
                None
            } else {
                Some(comment)
            };

            // 处理额外信息
            let extra: Option<String> = row.get(8);
            let auto_increment = extra.as_ref().map(|x| x.to_lowercase() == "auto_increment");

            // 处理列定义
            let column_def: String = row.get(8);
            let column_def = if column_def.is_empty() {
                None
            } else {
                Some(column_def)
            };

            // 处理长度
            let length = row.get::<Option<i64>, usize>(4).unwrap_or(-1);

            // 处理数字精度
            let digit =
                if ["DECIMAL", "FLOAT", "DOUBLE"].contains(&type_name.to_uppercase().as_str()) {
                    row.get::<Option<u32>, usize>(5)
                } else {
                    None
                };

            // 检查是否为主键
            let is_pk = pk_map.get(&table) == Some(&column_name);

            column_map
                .entry(table)
                .or_insert_with(Vec::new)
                .push(Column {
                    name: column_name,
                    c_type: FiledTypeEnum::mysql_filed_type(row.get(2)),
                    type_name: row.get(2),
                    length: length as i32,
                    digit: digit.map(|x| x as i32),
                    is_nullable: row.get::<String, usize>(6) == "YES",
                    comment,
                    auto_increment,
                    column_def,
                    is_pk,
                });
        }
        Ok(column_map)
    }
}

#[async_trait]
impl MetaTrait for MysqlMeta {
    async fn get_tables(&self) -> Result<Vec<TableInfo>, ServiceError> {
        let sql = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_TYPE = 'BASE TABLE'",
            db_name = &self.conn_config.database
        );
        let rows = sqlx::query(&sql)
            .map(|row: sqlx::mysql::MySqlRow| {
                let schema = row.get(0);
                let table_name = row.get(1);
                let comment = row.get(2);
                TableInfo::new(schema, table_name, Some(comment))
            })
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn set_pk_key(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), ServiceError> {
        let sql = format!(
            "SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE`
            WHERE TABLE_SCHEMA = '{schema}' AND CONSTRAINT_NAME = 'PRIMARY'",
            schema = &self.conn_config.database
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let pk_map: HashMap<String, String> =
            rows.iter().map(|row| (row.get(0), row.get(1))).collect();

        for table in table_vec {
            if let Some(name) = pk_map.get(&table.table_name) {
                table.set_pk_column(name.clone())
            }
        }

        Ok(())
    }

    async fn set_index_key(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), ServiceError> {
        let sql = format!(
            "SELECT
        a.TABLE_SCHEMA,
        a.TABLE_NAME,
        a.index_name,
        GROUP_CONCAT(column_name ORDER BY seq_in_index) AS `Columns`
    FROM information_schema.statistics a
    WHERE a.table_schema = '{schema}' AND index_name <> 'PRIMARY'
    GROUP BY a.TABLE_SCHEMA, a.TABLE_NAME, a.index_name",
            schema = self.conn_config.database
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let mut index_map: HashMap<String, Vec<IndexInfo>> = HashMap::new();
        for row in rows {
            index_map
                .entry(row.get(1))
                .or_insert_with(Vec::new)
                .push(IndexInfo {
                    column_name: row.get(3),
                    index_name: row.get(2),
                    index_def: "".to_string(),
                });
        }

        for table in table_vec {
            if let Some(indexes) = index_map.get(&table.table_name) {
                table.set_index_columns(indexes.clone());
            }
        }

        Ok(())
    }

    async fn set_column(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), ServiceError> {
        let table_names = table_vec.iter().map(|x| x.table_name.clone()).collect();

        let pk_map: HashMap<String, String> = table_vec
            .iter()
            .filter(|t| !t.pk_column.is_empty())
            .map(|t| (t.table_name.clone(), t.pk_column.clone()))
            .collect();

        let column_map = self.get_columns(table_names, pk_map).await?;

        for table in table_vec {
            if let Some(columns) = column_map.get(&table.table_name) {
                table.set_columns(columns.clone());
            }
        }

        Ok(())
    }

    async fn get_views(&self) -> Result<Vec<ViewsInfo>, ServiceError> {
        let sql = format!(
            "SELECT TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_COMMENT
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = '{schema}'
               AND TABLE_TYPE = 'VIEW'",
            schema = self.conn_config.database
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let views = rows
            .iter()
            .map(|x| ViewsInfo::new(x.get(0), x.get(1)))
            .collect();
        Ok(views)
    }

    async fn set_view_column(&self, view_vec: &mut Vec<ViewsInfo>) -> Result<(), ServiceError> {
        let view_names = view_vec.iter().map(|x| x.view_name.clone()).collect();
        let column_map = self.get_columns(view_names, HashMap::new()).await?;

        for view in view_vec {
            if let Some(columns) = column_map.get(&view.view_name) {
                view.set_columns(columns.clone());
            }
        }

        Ok(())
    }

    async fn count(&self, sql: &str) -> Result<i64, ServiceError> {
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        Ok(row.get(0))
    }

    /// query
    async fn query(&self, sql: &str) -> Result<Vec<Vec<String>>, ServiceError> {
        let result = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let rows = result
            .iter()
            .map(|mysql_row| (0..mysql_row.len()).map(|i| mysql_row.get(i)).collect())
            .collect();

        Ok(rows)
    }
}
