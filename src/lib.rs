pub mod error;
pub mod meta;
pub mod modal;
pub mod mysql_meta;
pub mod pg_meta;

#[cfg(test)]
mod test {

    use crate::{
        meta::MetadataService, modal::{ConnConfig, DbType}
    };
    use std::error::Error;

    #[actix_rt::test]
    async fn test_mysql_meta() -> Result<(), Box<dyn Error>> {
        let cc = ConnConfig {
            url: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "root".to_string(),
            database: "sys".to_string(),
            db_type: DbType::MySql,
            schema: None,
        };

        let meta_service = MetadataService::new(cc).unwrap();

        let tables = meta_service.get_metadata().await.unwrap();

        println!("tables={}", tables.tables.len());

        Ok(())
    }
}
