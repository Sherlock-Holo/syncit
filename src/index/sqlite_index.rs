use std::ffi::OsStr;
use std::io::ErrorKind;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use std::{error, io};

use async_trait::async_trait;
use futures_util::{Stream, TryStreamExt};
use sqlx::{FromRow, QueryBuilder, Sqlite, SqlitePool, Transaction};
use tap::TapFallible;
use thiserror::Error;
use tracing::{error, info, instrument};

use super::{BlockChain, FileDetail, FileKind, Index, IndexFile, IndexGuard};

#[derive(Debug, Error)]
pub enum Error {
    #[error("sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("other error: {0}")]
    Custom(Box<dyn error::Error + Send + 'static>),
}

#[derive(Debug, FromRow)]
struct DbIndexFile {
    filename: String,
    kind: String,
    gen: i64,
    update_time: i64,
    update_by: String,
}

#[derive(Debug, FromRow, Eq, PartialEq)]
struct DbFileDetail {
    filename: String,
    gen: i64,
    hash_sum: String,
    block_chain: Option<String>,
    deleted: bool,
}

#[derive(Debug)]
pub struct SqliteIndex {
    db_poll: SqlitePool,
}

#[async_trait]
impl Index for SqliteIndex {
    type Error = Error;
    type IndexStream<'a> = Pin<Box<dyn Stream<Item=Result<IndexFile, Self::Error>> + 'a>> where Self: 'a;
    type Guard = SqliteIndexGuard;

    #[inline]
    #[instrument]
    async fn list_all_files(&self) -> Result<Self::IndexStream<'_>, Self::Error> {
        let stream = async_stream::try_stream! {
            let mut index_guard = self.begin().await?;

            info!("create index guard done");

            let mut stream = index_guard.list_all_files().await?;
            while let Some(file) = stream.try_next().await? {
                yield file
            }
        };

        Ok(Box::pin(stream))
    }

    #[inline]
    #[instrument]
    async fn get_file(&self, filename: &OsStr) -> Result<Option<IndexFile>, Self::Error> {
        let mut index_guard = self.begin().await?;

        info!("create index guard done");

        index_guard.get_file(filename).await
    }

    #[inline]
    #[instrument]
    async fn begin(&self) -> Result<Self::Guard, Self::Error> {
        let transaction = self
            .db_poll
            .begin()
            .await
            .tap_err(|err| error!(%err, "create a transaction failed"))?;

        info!("create transaction done");

        Ok(SqliteIndexGuard { transaction })
    }
}

#[derive(Debug)]
pub struct SqliteIndexGuard {
    transaction: Transaction<'static, Sqlite>,
}

impl SqliteIndexGuard {
    async fn construct_file(
        &mut self,
        db_index_file: DbIndexFile,
    ) -> Result<IndexFile, sqlx::Error> {
        let file_kind = db_index_file.kind.parse::<FileKind>().map_err(|err| {
            error!(%err, filename = %db_index_file.filename, "parse file kind failed");

            sqlx::Error::Decode(Box::new(io::Error::new(ErrorKind::Other, err)))
        })?;

        let db_file_details: Vec<DbFileDetail> = sqlx::query_as(
            "SELECT * FROM file_details WHERE filename=? ORDER BY gen DESC",
        )
        .bind(&db_index_file.filename)
        .fetch_all(&mut self.transaction)
        .await
        .tap_err(
            |err| error!(%err, filename = %db_index_file.filename, "select file details failed"),
        )?;

        if db_file_details.is_empty() {
            error!(filename = %db_index_file.filename, "db file details is empty");

            return Err(sqlx::Error::Decode(Box::new(io::Error::new(
                ErrorKind::Other,
                "db file details is empty",
            ))));
        }

        info!(filename = %db_index_file.filename, "select all file details done");

        let mut file_details = db_file_details
            .into_iter()
            .map(|db_detail| {
                let hash_sum = if db_detail.hash_sum.is_empty() {
                    [0; 32]
                } else {
                    let hex_sum = hex::decode(&db_detail.hash_sum).map_err(|err| {
                        error!(%err, hash_sum = %db_detail.hash_sum, "decode hash sum failed");

                        sqlx::Error::Decode(Box::new(err))
                    })?;

                    hex_sum.try_into().map_err(|_| {
                        error!(hash_sum = %db_detail.hash_sum, "hash sum invalid");

                        sqlx::Error::Decode(Box::new(io::Error::new(
                            ErrorKind::Other,
                            format!("invalid hash sum: {}", db_detail.hash_sum),
                        )))
                    })?
                };

                let file_detail = match db_detail.block_chain {
                    None => FileDetail {
                        gen: db_detail.gen as _,
                        hash_sum,
                        block_chain: None,
                        deleted: db_detail.deleted,
                    },

                    Some(block_chain) => {
                        let block_chain = serde_json::from_str::<BlockChain>(&block_chain)
                            .map_err(|err| {
                                error!(%err, %block_chain, "parse block chain failed");

                                sqlx::Error::Decode(Box::new(err))
                            })?;

                        FileDetail {
                            gen: db_detail.gen as _,
                            hash_sum,
                            block_chain: Some(block_chain),
                            deleted: db_detail.deleted,
                        }
                    }
                };

                Ok(file_detail)
            })
            .collect::<Result<Vec<FileDetail>, sqlx::Error>>()?;

        info!(?file_details, "collect file details done");

        let file_detail = file_details.remove(0);

        Ok(IndexFile {
            filename: db_index_file.filename.into(),
            kind: file_kind,
            detail: file_detail,
            previous_details: file_details,
            update_time: SystemTime::UNIX_EPOCH
                + Duration::from_secs(db_index_file.update_time as _),
            update_by: db_index_file.update_by,
        })
    }

    #[instrument(err)]
    async fn update_or_insert_file_detail(
        &mut self,
        filename: &str,
        file_detail: &FileDetail,
    ) -> Result<(), Error> {
        let hash_sum = if file_detail.hash_sum == [0; 32] {
            String::new()
        } else {
            hex::encode(file_detail.hash_sum)
        };

        let block_chain = file_detail.block_chain.as_ref().map(serde_json::to_string).transpose()
            .map_err(|err| {
                error!(filename, %err, block_chain = ?file_detail.block_chain, "marshal block chain failed");

                Error::Custom(Box::new(err))
            })?;

        info!(filename, ?block_chain, "marshal block chain done");

        let new_db_file_detail = DbFileDetail {
            filename: filename.to_string(),
            gen: file_detail.gen as _,
            hash_sum,
            block_chain,
            deleted: file_detail.deleted,
        };

        let db_file_detail: DbFileDetail = match sqlx::query_as(
            "SELECT * FROM file_details WHERE filename = ? AND gen = ?",
        )
        .bind(filename)
        .bind(file_detail.gen as i64)
        .fetch_one(&mut self.transaction)
        .await
        {
            Err(sqlx::Error::RowNotFound) => {
                let result = sqlx::query("INSERT INTO file_details (filename, gen, hash_sum, block_chain, deleted) VALUES (?, ?, ?, ?, ?)")
                    .bind(new_db_file_detail.filename)
                    .bind(new_db_file_detail.gen)
                    .bind(new_db_file_detail.hash_sum)
                    .bind(new_db_file_detail.block_chain)
                    .bind(new_db_file_detail.deleted)
                    .execute(&mut self.transaction)
                    .await.tap_err(|err| error!(%err, "insert db file detail failed"))?;

                let rows_affected = result.rows_affected();
                if rows_affected != 1 {
                    error!(rows_affected, "rows affected invalid, should be 1");

                    return Err(Error::SqlError(sqlx::Error::Io(io::Error::new(
                        ErrorKind::Other,
                        format!("rows affected {} invalid, should be 1", rows_affected),
                    ))));
                }

                return Ok(());
            }

            Err(err) => {
                error!(%err, "select db file detail failed");

                return Err(err.into());
            }

            Ok(db_file_detail) => db_file_detail,
        };

        info!("select db file detail done");

        if db_file_detail == new_db_file_detail {
            info!("db file detail no need update");

            return Ok(());
        }

        let result = sqlx::query("UPDATE file_details SET hash_sum = ?, block_chain = ?, deleted = ? WHERE filename = ? AND gen = ?")
            .bind(new_db_file_detail.hash_sum)
            .bind(new_db_file_detail.block_chain)
            .bind(new_db_file_detail.deleted)
            .bind(new_db_file_detail.filename)
            .bind(new_db_file_detail.gen)
            .execute(&mut self.transaction).await.tap_err(|err| error!(%err, "update db file detail failed"))?;

        let rows_affected = result.rows_affected();
        if rows_affected != 1 {
            error!(rows_affected, "rows affected invalid, should be 1");

            return Err(Error::SqlError(sqlx::Error::Io(io::Error::new(
                ErrorKind::Other,
                format!("rows affected {} invalid, should be 1", rows_affected),
            ))));
        }

        Ok(())
    }
}

#[async_trait]
impl IndexGuard for SqliteIndexGuard {
    type Error = Error;
    type IndexStream<'a> = Pin<Box<dyn Stream<Item=Result<IndexFile, Self::Error>> + 'a>> where Self: 'a;

    #[instrument]
    async fn list_all_files(&mut self) -> Result<Self::IndexStream<'_>, Self::Error> {
        let db_index_files: Vec<DbIndexFile> = sqlx::query_as("SELECT * FROM index_files")
            .fetch_all(&mut self.transaction)
            .await
            .tap_err(|err| error!(%err, "select all index files failed"))?;

        info!("select all index files done");

        let stream = async_stream::try_stream! {
            for db_index_file in db_index_files {
                let index_file = self.construct_file(db_index_file).await?;

                yield index_file
            }
        };

        Ok(Box::pin(stream))
    }

    #[instrument]
    async fn create_file(&mut self, file: &IndexFile) -> Result<(), Self::Error> {
        let db_index_file = DbIndexFile {
            filename: file.filename.to_string_lossy().to_string(),
            kind: file.kind.to_string(),
            gen: file.detail.gen as _,
            update_time: file
                .update_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as _,
            update_by: file.update_by.clone(),
        };

        let db_file_details = [&file.detail]
            .into_iter()
            .chain(file.previous_details.iter())
            .map(|file_detail| {
                let block_chain = match &file_detail.block_chain {
                    None => None,
                    Some(block_chain) => {
                        Some(serde_json::to_string(&block_chain).map_err(|err| {
                            error!(%err, ?block_chain, "marshal block chain failed");

                            Error::Custom(Box::new(err))
                        })?)
                    }
                };

                let hash_sum = if file_detail.hash_sum == [0; 32] {
                    String::new()
                } else {
                    hex::encode(file_detail.hash_sum)
                };

                Ok(DbFileDetail {
                    filename: file.filename.to_string_lossy().to_string(),
                    gen: file_detail.gen as _,
                    hash_sum,
                    block_chain,
                    deleted: file_detail.deleted,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        info!(?db_file_details, "collect db file details done");

        sqlx::query("INSERT INTO index_files (filename, kind, gen, update_time, update_by) VALUES (?, ?, ?, ?, ?)")
            .bind(&db_index_file.filename)
            .bind(&db_index_file.kind)
            .bind(db_index_file.gen)
            .bind(db_index_file.update_time)
            .bind(&db_index_file.update_by)
            .execute(&mut self.transaction)
            .await
            .tap_err(|err| error!(%err, ?db_index_file, "insert db index file failed"))?;

        info!(?db_index_file, "insert db index file done");

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO file_details (filename, gen, hash_sum, block_chain, deleted) ",
        );
        let query = query_builder
            .push_values(db_file_details, |mut b, db_file_detail| {
                b.push_bind(db_file_detail.filename)
                    .push_bind(db_file_detail.gen)
                    .push_bind(db_file_detail.hash_sum)
                    .push_bind(db_file_detail.block_chain)
                    .push_bind(db_file_detail.deleted);
            })
            .build();

        query
            .execute(&mut self.transaction)
            .await
            .tap_err(|err| error!(%err, "insert db file details failed"))?;

        info!("insert db file details done");

        Ok(())
    }

    #[instrument(err)]
    async fn get_file(&mut self, filename: &OsStr) -> Result<Option<IndexFile>, Self::Error> {
        let db_index_file: DbIndexFile =
            match sqlx::query_as("SELECT * FROM index_files WHERE filename=?")
                .bind(filename.to_string_lossy())
                .fetch_one(&mut self.transaction)
                .await
            {
                Err(sqlx::Error::RowNotFound) => {
                    info!("index file not found");

                    return Ok(None);
                }

                Err(err) => {
                    error!(%err, "select index file failed");

                    return Err(err.into());
                }

                Ok(db_index_file) => db_index_file,
            };

        info!("get db index file done");

        let index_file = self.construct_file(db_index_file).await?;

        info!("construct index file done");

        Ok(Some(index_file))
    }

    #[instrument(err)]
    async fn update_file(&mut self, file: &IndexFile) -> Result<(), Self::Error> {
        let filename = file.filename.to_string_lossy();

        sqlx::query("DELETE FROM index_files WHERE filename = ?")
            .bind(&filename)
            .execute(&mut self.transaction)
            .await
            .tap_err(|err| error!(?filename, %err, "delete exists index file failed"))?;

        info!(?filename, "delete exists index file done");

        sqlx::query("DELETE FROM file_details WHERE filename = ?")
            .bind(&filename)
            .execute(&mut self.transaction)
            .await
            .tap_err(|err| error!(?filename, %err, "delete exists db file details failed"))?;

        info!(?filename, "delete exists db file details done");

        self.create_file(file).await
    }

    #[instrument]
    async fn commit(self) -> Result<(), Self::Error> {
        self.transaction
            .commit()
            .await
            .tap_err(|err| error!(%err, "commit transaction failed"))?;

        Ok(())
    }
}
