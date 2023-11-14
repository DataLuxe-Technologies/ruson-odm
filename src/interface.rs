use mongodb::{
    bson::{Bson, Document},
    error::Result,
    options::{
        AggregateOptions, CountOptions, CreateIndexOptions, DistinctOptions, DropIndexOptions,
        FindOneOptions, FindOptions, ListIndexesOptions, TransactionOptions, UpdateOptions,
    },
    results::{CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Client, ClientSession, Collection, Cursor, IndexModel, SessionCursor,
};
use serde::Deserialize;
use std::{iter::Iterator, sync::Arc, time::Duration};
use tokio::sync::Mutex;

const DEFAULT_TIMEOUT: u64 = 5;

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
    skip: Option<u64>,
    sort: Option<Document>,
    projection: Option<Document>,
    timeout: Option<u64>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<Option<Document>> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = FindOneOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .skip(skip)
        .sort(sort)
        .projection(projection)
        .build();
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .find_one_with_session(filter, options, &mut session)
                .await
        }
        None => collection.find_one(filter, options).await,
    }
}

pub(crate) async fn find_many(
    collection: Collection<Document>,
    filter: Option<Document>,
    skip: Option<u64>,
    limit: Option<i64>,
    sort: Option<Document>,
    batch_size: Option<u32>,
    projection: Option<Document>,
    timeout: Option<u64>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<ResultIterator<Document>> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = FindOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .max_await_time(Duration::from_secs(timeout_value))
        .no_cursor_timeout(false)
        .cursor_type(mongodb::options::CursorType::NonTailable)
        .skip(skip)
        .limit(limit)
        .sort(sort)
        .batch_size(batch_size)
        .projection(projection)
        .build();
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .find_with_session(filter, options, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.find(filter, options).await?;
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
    upsert: Option<bool>,
    array_filters: Option<Vec<Document>>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<UpdateResult> {
    let options = UpdateOptions::builder()
        .upsert(upsert)
        .array_filters(array_filters)
        .build();
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .update_one_with_session(filter, update, options, &mut session)
                .await
        }
        None => collection.update_one(filter, update, options).await,
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
    batch_size: Option<u32>,
    timeout: Option<u64>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<ResultIterator<Document>> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = AggregateOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .max_await_time(Duration::from_secs(timeout_value))
        .batch_size(batch_size)
        .build();
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .aggregate_with_session(pipeline, options, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.aggregate(pipeline, options).await?;
            Ok(ResultIterator::new(CursorType::Plain(cursor)))
        }
    }
}

pub(crate) async fn distinct(
    collection: Collection<Document>,
    field_name: &str,
    filter: Option<Document>,
    timeout: Option<u64>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<Vec<Bson>> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = DistinctOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .build();
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .distinct_with_session(field_name, filter, options, &mut session)
                .await
        }
        None => collection.distinct(field_name, filter, options).await,
    }
}

pub(crate) async fn list_indexes(
    collection: Collection<Document>,
    timeout: Option<u64>,
) -> Result<ResultIterator<IndexModel>> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = ListIndexesOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .build();
    let cursor = collection.list_indexes(options).await?;
    Ok(ResultIterator::new(CursorType::Plain(cursor)))
}

pub(crate) async fn create_indexes(
    collection: Collection<Document>,
    indexes: impl Iterator<Item = IndexModel>,
    timeout: Option<u64>,
) -> Result<CreateIndexesResult> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = CreateIndexOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .build();
    collection.create_indexes(indexes, options).await
}

pub(crate) async fn drop_indexes(
    collection: Collection<Document>,
    indexes: Option<impl Iterator<Item = String>>,
    timeout: Option<u64>,
) -> Result<()> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = DropIndexOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .build();
    match indexes {
        Some(idxs) => {
            let mut session = collection.client().start_session(None).await?;
            let transaction_options = TransactionOptions::builder()
                .max_commit_time(Duration::from_secs(timeout_value))
                .build();
            session.start_transaction(transaction_options).await?;
            for index in idxs {
                collection
                    .drop_index_with_session(index, options.clone(), &mut session)
                    .await?;
            }
            session.commit_transaction().await
        }
        None => collection.drop_indexes(options).await,
    }
}

pub(crate) async fn count_documents(
    collection: Collection<Document>,
    filter: Option<Document>,
    timeout: Option<u64>,
) -> Result<u64> {
    let timeout_value = match timeout {
        Some(t) => t,
        None => DEFAULT_TIMEOUT,
    };
    let options = CountOptions::builder()
        .max_time(Duration::from_secs(timeout_value))
        .build();
    collection.count_documents(filter, options).await
}

pub(crate) async fn drop(collection: Collection<Document>) -> Result<()> {
    collection.drop(None).await
}
