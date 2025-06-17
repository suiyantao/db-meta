use crate::error::MetaError;
use crate::modal::{Column, ConnConfig, IndexInfo, TableInfo, ViewsInfo, FieldTypeEnum};

use super::meta::MetaTrait;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use std::time::Duration;

/// PostgreSQL元数据操作结构体
#[derive(Debug, Clone)]
pub struct PgMeta {
    /// PostgreSQL连接池
    pub(crate) pool: Pool<Postgres>,
}

impl PgMeta {
    /// 创建PgMeta实例
    pub async fn new(conn_config: &ConnConfig) -> Result<Self, MetaError> {
        let url = format!(
            "postgres://{user_name}:{password}@{host}:{port}/{dbname}",
            user_name = conn_config.username,
            password = conn_config.password,
            host = conn_config.url,
            port = conn_config.port,
            dbname = conn_config.database
        );

        let pool = PgPoolOptions::new()
            .max_connections(30)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await?;

        Ok(Self { pool })
    }
}

/// MetaTrait trait的异步实现
#[async_trait]
impl MetaTrait for PgMeta {
    /// 获取所有表信息
    async fn get_tables(&self) -> Result<Vec<TableInfo>, MetaError> {
        let sql = r"SELECT
       n.nspname AS TABLE_SCHEM,
       c.relname AS TABLE_NAME,
       d.description AS REMARKS
FROM pg_catalog.pg_namespace n,
     pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_description d
                   ON (c.oid = d.objoid AND d.objsubid = 0 and d.classoid = 'pg_class'::regclass)
WHERE c.relnamespace = n.oid and n.nspname = 'public' and c.relkind = 'r';";

        let result = sqlx::query(sql).fetch_all(&self.pool).await?;

        let tables = result
            .iter()
            .map(|row| {
                let schema: String = row.get(0);
                let table_name: String = row.get(1);
                let comment: Option<String> = row.get(2);
                TableInfo::new(schema, table_name, comment)
            })
            .collect();

        Ok(tables)
    }

    /// 设置表的主键信息
    async fn set_primary_key(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), MetaError> {
        let sql = "SELECT result.TABLE_SCHEMA, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME
FROM (SELECT NULL AS TABLE_CAT,
             n.nspname AS TABLE_SCHEMA,
             ct.relname AS TABLE_NAME,
             a.attname AS COLUMN_NAME,
             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
             ci.relname AS PK_NAME,
             information_schema._pg_expandarray(i.indkey) AS KEYS,
             a.attnum AS A_ATTNUM
      FROM pg_catalog.pg_class ct
               JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
               JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
               JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
               JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE ci.relname like '%_pkey') result
ORDER BY result.table_name, result.pk_name, result.key_seq";

        let result = sqlx::query(sql).fetch_all(&self.pool).await?;

        let pk_map: HashMap<String, (String, String)> = result
            .into_iter()
            .map(|row| (row.get(1), (row.get(2), row.get(4))))
            .collect();

        for table in table_vec {
            if let Some(pk) = pk_map.get(&table.table_name) {
                table.set_pk_name(pk.clone().1);
                table.set_pk_column(pk.clone().0);
            }
        }

        Ok(())
    }

    /// 设置表的索引信息
    async fn set_index_key(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), MetaError> {
        let sql = "SELECT result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME, indexdef
FROM (SELECT NULL AS TABLE_CAT,
             n.nspname AS TABLE_SCHEM,
             ct.relname AS TABLE_NAME,
             a.attname AS COLUMN_NAME,
             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
             ci.relname AS PK_NAME,
             information_schema._pg_expandarray(i.indkey) AS KEYS,
             a.attnum AS A_ATTNUM,
             p.indexdef
      FROM pg_catalog.pg_class ct
               JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
               JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
               JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
               JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
               JOIN pg_indexes p on p.indexname = ci.relname
      WHERE ci.relname not like '%_pkey') result
ORDER BY result.table_name, result.pk_name, result.key_seq;";

        let result = sqlx::query(sql).fetch_all(&self.pool).await?;

        let mut index_map: HashMap<String, Vec<IndexInfo>> = HashMap::new();
        for row in result {
            let table_name = row.get(1);
            let column_name = row.get(2);
            let index_name = row.get(4);
            let index_def = row.get(5);
            index_map
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(IndexInfo {
                    column_name,
                    index_name,
                    index_def,
                });
        }

        for table in table_vec {
            if let Some(indexes) = index_map.get(&table.table_name) {
                table.set_index_columns(indexes.clone());
            }
        }

        Ok(())
    }

    /// 设置表的列信息
    async fn set_columns(&self, table_vec: &mut Vec<TableInfo>) -> Result<(), MetaError> {
        let tables: Vec<_> = table_vec
            .iter()
            .map(|table| table.table_name.clone())
            .collect();
        let tables_str = tables.join("','");

        let sql = format!(
            "select
    col.table_schema,
    col.table_name,
    col.column_name,
    col.udt_name,
    coalesce(character_maximum_length, numeric_precision, -1) as column_length,
    col.numeric_scale,
    des.description,
    col.is_nullable,
    col.ordinal_position,
    col.column_default
from
    information_schema.columns col left join pg_description des on
        col.table_name::regclass = des.objoid
            and col.ordinal_position = des.objsubid
where
    table_schema = 'public' and col.table_name in ('{}')",
            tables_str
        );

        let result = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let mut column_map = HashMap::new();
        let pk_map: HashMap<_, _> = table_vec
            .into_iter()
            .map(|table| (table.table_name.clone(), table.pk_column.clone()))
            .collect();

        for row in result {
            let is_nullable = row.get::<String, usize>(7) != "NO";
            let table_name = row.get::<String, usize>(1);
            let column_name = row.get::<String, usize>(2);
            let is_pk = pk_map.get(&table_name) == Some(&column_name);
            let column_def = row.get::<Option<String>, usize>(9);
            let auto_increment = column_def
                .clone()
                .map(|def| is_pk && def.to_lowercase().starts_with("nextval"));

            let column = Column {
                name: column_name,
                column_type: FieldTypeEnum::pg_field_type(row.get(3)),
                type_name: row.get(3),
                length: row.get::<i32, usize>(4),
                digit: row.get(5),
                comment: row.get(6),
                auto_increment,
                column_def,
                is_nullable,
                is_pk,
            };

            column_map
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(column);
        }

        for table in table_vec {
            if let Some(columns) = column_map.get(&table.table_name) {
                table.set_columns(columns.clone());
            }
        }

        Ok(())
    }

    /// 获取所有视图信息
    async fn get_views(&self) -> Result<Vec<ViewsInfo>, MetaError> {
        let sql = r"SELECT
       n.nspname AS TABLE_SCHEM,
       c.relname AS TABLE_NAME,
       d.description AS REMARKS
FROM pg_catalog.pg_namespace n,
     pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_description d
                   ON (c.oid = d.objoid AND d.objsubid = 0 and d.classoid = 'pg_class'::regclass)
WHERE c.relnamespace = n.oid and n.nspname = 'public' and c.relkind = 'v';";

        let result = sqlx::query(sql).fetch_all(&self.pool).await?;

        let views = result
            .iter()
            .map(|row| {
                let schema: String = row.get(0);
                let view_name: String = row.get(1);
                ViewsInfo::new(schema, view_name)
            })
            .collect();

        Ok(views)
    }

    /// 设置视图的列信息
    async fn set_view_columns(&self, view_vec: &mut Vec<ViewsInfo>) -> Result<(), MetaError> {
        let views: Vec<_> = view_vec.iter().map(|view| view.view_name.clone()).collect();
        let views_str = views.join("','");

        let sql = format!(
            "select
    col.table_schema,
    col.table_name,
    col.column_name,
    col.udt_name,
    coalesce(character_maximum_length, numeric_precision, -1) as column_length,
    col.numeric_scale,
    des.description,
    col.is_nullable,
    col.ordinal_position,
    col.column_default
from
    information_schema.columns col left join pg_description des on
        col.table_name::regclass = des.objoid
            and col.ordinal_position = des.objsubid
where
    table_schema = 'public' and col.table_name in ('{}')",
            views_str
        );

        let result = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let mut column_map = HashMap::new();

        for row in result {
            let is_nullable = row.get::<String, usize>(7) != "NO";
            let view_name = row.get::<String, usize>(1);
            let column_name = row.get::<String, usize>(2);
            let column_def = row.get::<Option<String>, usize>(9);

            let column = Column {
                name: column_name,
                column_type: FieldTypeEnum::pg_field_type(row.get(3)),
                type_name: row.get(3),
                length: row.get(4),
                digit: row.get(5),
                comment: row.get(6),
                auto_increment: None,
                column_def,
                is_nullable,
                is_pk: false,
            };

            column_map
                .entry(view_name)
                .or_insert_with(Vec::new)
                .push(column);
        }

        for view in view_vec {
            if let Some(columns) = column_map.get(&view.view_name) {
                view.set_columns(columns.clone());
            }
        }

        Ok(())
    }

    /// 执行计数SQL查询
    async fn count(&self, sql: &str) -> Result<i64, MetaError> {
        let result = sqlx::query(&sql).fetch_one(&self.pool).await?;
        Ok(result.get(0))
    }

    /// 执行查询并返回结果集
    async fn query(&self, sql: &str) -> Result<Vec<Vec<String>>, MetaError> {
        let result = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let rows = result
            .iter()
            .map(|pg_row| (0..pg_row.len()).map(|i| pg_row.get(i)).collect())
            .collect();

        Ok(rows)
    }
}
