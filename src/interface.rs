use mongodb::{
    bson::{Bson, Document},
    error::Error,
    error::Result,
    results::{CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Client, ClientSession, Collection, Cursor, IndexModel, SessionCursor,
};
use serde::Deserialize;
use std::{iter::Iterator, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub(crate) enum CursorType<T> {
    Session(SessionCursor<T>, Arc<Mutex<ClientSession>>),
    Plain(Cursor<T>),
}

pub(crate) struct ResultIterator<T> {
    cursor: CursorType<T>,
}

impl<'a, T> ResultIterator<T> {
    pub(crate) fn new(cursor: CursorType<T>) -> Self {
        ResultIterator { cursor }
    }

    pub(crate) async fn advance(&mut self) -> Result<bool> {
        match &mut self.cursor {
            CursorType::Session(c, s) => {
                let mut session = s.lock().await;
                c.advance(&mut session).await
            }
            CursorType::Plain(c) => c.advance().await,
        }
    }

    pub(crate) fn deserialize_current(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        match &self.cursor {
            CursorType::Session(c, _) => c.deserialize_current(),
            CursorType::Plain(c) => c.deserialize_current(),
        }
    }
}

pub(crate) async fn create_client(db_uri: &str) -> Result<Client> {
    Client::with_uri_str(db_uri).await
}

pub(crate) async fn find_one(
    collection: Collection<Document>,
    filter: Document,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<Document> {
    let timeout_options = mongodb::options::FindOneOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    let out = match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .find_one_with_session(filter, timeout_options, &mut session)
                .await?
        }
        None => collection.find_one(filter, timeout_options).await?,
    };
    out.ok_or(Error::custom("No document found"))
}

pub(crate) async fn find_many(
    collection: Collection<Document>,
    filter: Option<Document>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<ResultIterator<Document>> {
    let timeout_options = mongodb::options::FindOptions::builder()
        .max_time(Duration::from_secs(5))
        .max_await_time(Duration::from_secs(5))
        .no_cursor_timeout(false)
        .cursor_type(mongodb::options::CursorType::NonTailable)
        .build();
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .find_with_session(filter, timeout_options, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.find(filter, timeout_options).await?;
            Ok(ResultIterator::new(CursorType::Plain(cursor)))
        }
    }
}

pub(crate) async fn insert_one(
    collection: Collection<Document>,
    document: Document,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<InsertOneResult> {
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .insert_one_with_session(document, None, &mut session)
                .await
        }
        None => collection.insert_one(document, None).await,
    }
}

pub(crate) async fn insert_many(
    collection: Collection<Document>,
    documents: impl Iterator<Item = Document>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<InsertManyResult> {
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .insert_many_with_session(documents, None, &mut session)
                .await
        }
        None => collection.insert_many(documents, None).await,
    }
}

pub(crate) async fn update_one(
    collection: Collection<Document>,
    update: Document,
    filter: Document,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<UpdateResult> {
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .update_one_with_session(filter, update, None, &mut session)
                .await
        }
        None => collection.update_one(filter, update, None).await,
    }
}

pub(crate) async fn delete_one(
    collection: Collection<Document>,
    filter: Document,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<DeleteResult> {
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .delete_one_with_session(filter, None, &mut session)
                .await
        }
        None => collection.delete_one(filter, None).await,
    }
}

pub(crate) async fn delete_many(
    collection: Collection<Document>,
    filter: Document,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<DeleteResult> {
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .delete_many_with_session(filter, None, &mut session)
                .await
        }
        None => collection.delete_many(filter, None).await,
    }
}

pub(crate) async fn aggregate(
    collection: Collection<Document>,
    pipeline: impl Iterator<Item = Document>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<ResultIterator<Document>> {
    let timeout_options = mongodb::options::AggregateOptions::builder()
        .max_time(Duration::from_secs(5))
        .max_await_time(Duration::from_secs(5))
        .build();
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .aggregate_with_session(pipeline, timeout_options, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.aggregate(pipeline, timeout_options).await?;
            Ok(ResultIterator::new(CursorType::Plain(cursor)))
        }
    }
}

pub(crate) async fn distinct(
    collection: Collection<Document>,
    field_name: &str,
    filter: Option<Document>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<Vec<Bson>> {
    let timeout_options = mongodb::options::DistinctOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .distinct_with_session(field_name, filter, timeout_options, &mut session)
                .await
        }
        None => {
            collection
                .distinct(field_name, filter, timeout_options)
                .await
        }
    }
}

pub(crate) async fn list_indexes(
    collection: Collection<Document>,
) -> Result<ResultIterator<IndexModel>> {
    let timeout_options = mongodb::options::ListIndexesOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    let cursor = collection.list_indexes(timeout_options).await?;
    Ok(ResultIterator::new(CursorType::Plain(cursor)))
}

pub(crate) async fn create_indexes(
    collection: Collection<Document>,
    indexes: impl Iterator<Item = IndexModel>,
) -> Result<CreateIndexesResult> {
    let timeout_options = mongodb::options::CreateIndexOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    collection.create_indexes(indexes, timeout_options).await
}

pub(crate) async fn drop_indexes(
    collection: Collection<Document>,
    indexes: Option<impl Iterator<Item = String>>,
) -> Result<()> {
    let timeout_options = mongodb::options::DropIndexOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    match indexes {
        Some(idxs) => {
            let session_options = mongodb::options::SessionOptions::builder()
                .default_transaction_options(
                    mongodb::options::TransactionOptions::builder()
                        .max_commit_time(Duration::from_secs(5))
                        .build(),
                )
                .build();
            let mut session = collection.client().start_session(session_options).await?;
            for index in idxs {
                collection
                    .drop_index_with_session(index, timeout_options.clone(), &mut session)
                    .await?;
            }
            session.commit_transaction().await
        }
        None => collection.drop_indexes(timeout_options).await,
    }
}

pub(crate) async fn count_documents(
    collection: Collection<Document>,
    filter: Option<Document>,
) -> Result<u64> {
    let timeout_options = mongodb::options::CountOptions::builder()
        .max_time(Duration::from_secs(5))
        .build();
    collection.count_documents(filter, timeout_options).await
}

pub(crate) async fn drop(collection: Collection<Document>) -> Result<()> {
    collection.drop(None).await
}
