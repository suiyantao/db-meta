#![allow(dead_code, unused_variables)]
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::MetaError;

#[derive(Debug, Clone, Builder)]
pub struct ConnConfig {
    pub url: String,
    pub port: u32,
    pub username: String,
    pub password: String,
    pub database: String,
    pub schema: Option<String>,
    pub db_type: DbType,
}

#[derive(Debug, Clone)]
pub enum DbType {
    // 按照 Rust 命名规范，枚举变体使用 PascalCase
    MySql,
    Postgresql,
    MariaDb,
    Sqlite,
}

impl ConnConfig {
    pub fn validate(&self) -> Result<(), MetaError> {
        if self.username.is_empty() {
            return Err(MetaError::InvalidArgument("用户名不能为空".into()));
        }
        if self.password.is_empty() {
            return Err(MetaError::InvalidArgument("密码不能为空".into()));
        }
        if self.url.is_empty() {
            return Err(MetaError::InvalidArgument("地址不能为空".into()));
        }
        if self.database.is_empty() {
            return Err(MetaError::InvalidArgument("数据库不能为空".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// 按照 Rust 命名规范，结构体使用 PascalCase，这里 `MetaData` 改为 `Metadata`
pub struct Metadata {
    pub tables: Vec<TableInfo>,
    pub views: Vec<ViewsInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableInfo {
    //table所在的schema
    pub schema: String,
    //表名
    pub table_name: String,
    //注释
    pub comment: Option<String>,
    // 主键名
    pub pk_name: String,
    // 主键字段名称
    pub pk_column: String,
    // 索引信息
    pub index_columns: Vec<IndexInfo>,
    // 列映射，列名-列对象
    pub columns: Vec<Column>,
}

impl TableInfo {
    pub fn new(schema: String, table_name: String, comment: Option<String>) -> Self {
        Self {
            schema,
            table_name,
            comment,
            ..Default::default()
        }
    }

    pub fn set_pk_name(&mut self, pk_name: String) {
        self.pk_name = pk_name;
    }

    pub fn set_pk_column(&mut self, pk_column: String) {
        self.pk_column = pk_column;
    }

    pub fn set_index_columns(&mut self, index_columns: Vec<IndexInfo>) {
        self.index_columns = index_columns;
    }

    pub fn set_columns(&mut self, columns: Vec<Column>) {
        self.columns = columns;
    }
}

impl ViewsInfo {
    pub fn new(schema: String, view_name: String) -> Self {
        Self {
            schema,
            view_name,
            ..Default::default()
        }
    }

    pub fn set_columns(&mut self, columns: Vec<Column>) {
        self.columns = columns;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub column_name: String,
    pub index_name: String,
    pub index_def: String,
    pub is_unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    //列名
    pub name: String,
    // 按照 Rust 命名规范，结构体字段使用 snake_case，`c_type` 改为 `c_type` （这里原命名符合规范，但猜测可能是拼写意图，若改为 `column_type` 更表意）
    pub column_type: FieldTypeEnum,
    // 类型名称
    pub type_name: String,
    //大小或数据长度
    pub length: i32,
    // 精度
    pub digit: Option<i32>,
    // 是否为可空
    pub is_nullable: bool,
    // 注释
    pub comment: Option<String>,
    // 是否自增
    pub auto_increment: Option<bool>,
    //字段默认值<br>
    // default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (maybe {@code null})
    pub column_def: Option<String>,
    // 是否为主键
    pub is_pk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ViewsInfo {
    //table所在的schema
    pub schema: String,
    //表名
    pub view_name: String,
    // 列映射，列名-列对象
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// 按照 Rust 命名规范，枚举使用 PascalCase，`FiledTypeEnum` 改为 `FieldTypeEnum`
pub enum FieldTypeEnum {
    // 按照 Rust 命名规范，枚举变体使用 PascalCase
    String,
    Long,
    Integer,
    Float,
    Double,
    Boolean,
    ByteArray,
    Character,
    Object,
    Date,
    Time,
    Blob,
    Clob,
    Timestamp,
    BigInt,
    BigDec,
    LocalDate,
    LocalTime,
    LocalDateTime,
}

impl FieldTypeEnum {
    /// 根据 PostgreSQL 数据库的字段类型代码返回对应的 FiledTypeEnum 枚举值。
    // 按照 Rust 命名规范，函数使用 snake_case，`pg_filed_type` 改为 `pg_field_type`
    pub fn pg_field_type(code: &str) -> Self {
        let db_type = code.to_lowercase();
        match db_type {
            db_type if db_type.contains("char") || db_type.contains("text") => {
                FieldTypeEnum::String
            }
            db_type if db_type.contains("bigint") => FieldTypeEnum::Long,
            db_type if db_type.contains("int") => FieldTypeEnum::Integer,
            db_type
                if db_type.contains("date")
                    || db_type.contains("time")
                    || db_type.contains("year") =>
            {
                FieldTypeEnum::Date
            }
            db_type
                if db_type.contains("bit") || db_type == "bool" || db_type.contains("boolean") =>
            {
                FieldTypeEnum::Boolean
            }
            db_type if db_type.contains("decimal") => FieldTypeEnum::BigDec,
            db_type if db_type.contains("clob") => FieldTypeEnum::Clob,
            db_type if db_type.contains("blob") => FieldTypeEnum::ByteArray,
            db_type if db_type.contains("float") => FieldTypeEnum::Float,
            db_type if db_type.contains("double") => FieldTypeEnum::Double,
            db_type if db_type.contains("json") || db_type.contains("enum") => {
                FieldTypeEnum::String
            }
            _ => FieldTypeEnum::String,
        }
    }

    /// 根据 MySQL 数据库的字段类型代码返回对应的 FiledTypeEnum 枚举值。
    // 按照 Rust 命名规范，函数使用 snake_case，`mysql_filed_type` 改为 `mysql_field_type`
    pub fn mysql_field_type(code: &str) -> Self {
        match code.to_uppercase().as_str() {
            "BIT" => FieldTypeEnum::Boolean,
            "TINYINT"
            | "TINYINT UNSIGNED"
            | "SMALLINT [UNSIGNED]"
            | "MEDIUMINT [UNSIGNED]"
            | "INTEGER" => FieldTypeEnum::Integer,
            "INTEGER UNSIGNED" | "BIGINT" => FieldTypeEnum::Long,
            "BIGINT UNSIGNED" => FieldTypeEnum::BigInt,
            "FLOAT" => FieldTypeEnum::Float,
            "DOUBLE" => FieldTypeEnum::Double,
            "DECIMAL" => FieldTypeEnum::BigDec,
            "DATE" => FieldTypeEnum::Date,
            "DATETIME" => FieldTypeEnum::LocalDateTime,
            "TIMESTAMP" => FieldTypeEnum::Timestamp,
            "TIME" => FieldTypeEnum::Time,
            "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB"
            | "GEOMETRY" => FieldTypeEnum::ByteArray,
            _ => FieldTypeEnum::String,
        }
    }
}
