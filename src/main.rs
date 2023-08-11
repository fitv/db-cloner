use log::{debug, error, info, warn};
use mysql_async::prelude::*;
use mysql_async::{Conn, Pool, Result, Row, Value};
use std::env;
use std::iter;
use std::str::FromStr;
use tokio::task::JoinSet;

const MAX_CONCURRENT: usize = 15;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().expect("Failed to read .env file");
    setup_logger();

    let ignore_tables = ignore_tables();
    let pool_source = mysql_async::Pool::new(source_database_url().as_str());
    let pool_target = mysql_async::Pool::new(target_database_url().as_str());

    let mut conn_source = pool_source.get_conn().await?;
    let source_tables = get_tables(&mut conn_source).await?;

    let mut task_set = JoinSet::new();
    let mut processed_tables = 0;
    let total_tables = source_tables
        .iter()
        .filter(|t| !ignore_tables.contains(t))
        .count();

    for table in source_tables.iter() {
        if ignore_tables.contains(table) {
            info!("Ignored table `{}`", table);
            continue;
        }

        while task_set.len() >= MAX_CONCURRENT {
            let result = task_set.join_next().await.unwrap().unwrap();

            if let Err(e) = result {
                error!("{} ", e);
            }
            upgrade_progress(&mut processed_tables, total_tables);
        }
        task_set.spawn(clone_table(
            pool_source.clone(),
            pool_target.clone(),
            table.to_string(),
        ));
    }

    while let Some(result) = task_set.join_next().await {
        if let Err(e) = result.unwrap() {
            error!("{} ", e);
        }
        upgrade_progress(&mut processed_tables, total_tables);
    }

    drop(conn_source);

    pool_source.disconnect().await?;
    pool_target.disconnect().await?;

    Ok(())
}

async fn clone_table(pool_source: Pool, pool_target: Pool, table: String) -> Result<()> {
    let mut conn_source = pool_source.get_conn().await?;
    let mut conn_target = pool_target.get_conn().await?;

    debug!("Dropping table `{}`", table);
    conn_target
        .query_drop(format!("DROP TABLE IF EXISTS `{}`", table))
        .await?;

    debug!("Creating table `{}`", table);
    conn_target
        .query_drop(get_table_structure(&mut conn_source, &table).await?)
        .await?;

    let rows = conn_source
        .query::<Row, _>(format!("SELECT * FROM `{}`", table))
        .await?;

    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() > 1_000_000 {
        warn!(
            "Table `{}` has more than 1 million rows, consider using IGNORE_TABLES to ignore this table.",
            table
        )
    }

    debug!("Inserting into table `{}` with {} rows.", table, rows.len());

    for row in rows.iter() {
        let column_names = row
            .columns()
            .iter()
            .map(|col| format!("`{}`", col.name_str()))
            .collect::<Vec<_>>();
        let column_values = row
            .columns()
            .iter()
            .map(|col| row.get::<Value, _>(col.name_str().as_ref()))
            .collect::<Vec<_>>();

        let insert_sql = format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            table,
            column_names.join(", "),
            iter::repeat("?")
                .take(row.len())
                .collect::<Vec<_>>()
                .join(", ")
        );

        conn_target.exec_drop(insert_sql, column_values).await?;
    }

    debug!("Inserted into table `{}` with {} rows.", table, rows.len());

    drop(conn_source);
    drop(conn_target);

    Ok(())
}

async fn get_tables(conn: &mut Conn) -> Result<Vec<String>> {
    Ok(conn
        .query::<Row, _>("SHOW TABLES")
        .await?
        .iter()
        .map(|row| row.get(0).unwrap())
        .collect())
}

async fn get_table_structure(conn: &mut Conn, table: &str) -> Result<String> {
    Ok(conn
        .query_first::<Row, _>(format!("SHOW CREATE TABLE `{}`", table))
        .await?
        .unwrap()
        .get(1)
        .unwrap())
}

fn setup_logger() {
    let level = env::var("LOG_LEVEL").unwrap_or("info".to_string());

    env_logger::builder()
        .format_target(false)
        .filter_level(log::LevelFilter::from_str(&level).unwrap())
        .init();
}

fn upgrade_progress(processed: &mut usize, total: usize) {
    *processed += 1;

    info!(
        "Progress {:.0}% ({}/{})",
        (*processed as f64 / total as f64) * 100.0,
        processed,
        total,
    );
}

fn ignore_tables() -> Vec<String> {
    env::var("IGNORE_TABLES")
        .unwrap_or("".to_string())
        .split(",")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn get_env(key: &str) -> String {
    env::var(key).expect(&format!("{} is not set in .env", key))
}

fn source_database_url() -> String {
    format!(
        "mysql://{}:{}@{}:{}/{}",
        get_env("SOURCE_DB_USERNAME"),
        get_env("SOURCE_DB_PASSWORD"),
        get_env("SOURCE_DB_HOST"),
        get_env("SOURCE_DB_PORT"),
        get_env("SOURCE_DB_DATABASE"),
    )
}

fn target_database_url() -> String {
    format!(
        "mysql://{}:{}@{}:{}/{}",
        get_env("TARGET_DB_USERNAME"),
        get_env("TARGET_DB_PASSWORD"),
        get_env("TARGET_DB_HOST"),
        get_env("TARGET_DB_PORT"),
        get_env("TARGET_DB_DATABASE"),
    )
}
