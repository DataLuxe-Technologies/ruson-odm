use mongodb::{
    bson::{Bson, Document},
    error::Error,
    error::Result,
    results::{CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Client, ClientSession, Collection, Cursor, IndexModel, SessionCursor,
};
use serde::Deserialize;
use std::{iter::Iterator, sync::Arc};
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
    let out = match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .find_one_with_session(filter, None, &mut session)
                .await?
        }
        None => collection.find_one(filter, None).await?,
    };
    out.ok_or(Error::custom("No document found"))
}

pub(crate) async fn find_many(
    collection: Collection<Document>,
    filter: Option<Document>,
    session: Option<Arc<Mutex<ClientSession>>>,
) -> Result<ResultIterator<Document>> {
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .find_with_session(filter, None, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.find(filter, None).await?;
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
    match session {
        Some(s) => {
            let copy = s.clone();
            let mut session = s.lock().await;
            let cursor = collection
                .aggregate_with_session(pipeline, None, &mut session)
                .await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, copy)))
        }
        None => {
            let cursor = collection.aggregate(pipeline, None).await?;
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
    match session {
        Some(s) => {
            let mut session = s.lock().await;
            collection
                .distinct_with_session(field_name, filter, None, &mut session)
                .await
        }
        None => collection.distinct(field_name, filter, None).await,
    }
}

pub(crate) async fn list_indexes(
    collection: Collection<Document>,
) -> Result<ResultIterator<IndexModel>> {
    let cursor = collection.list_indexes(None).await?;
    Ok(ResultIterator::new(CursorType::Plain(cursor)))
}

pub(crate) async fn create_indexes(
    collection: Collection<Document>,
    indexes: impl Iterator<Item = IndexModel>,
) -> Result<CreateIndexesResult> {
    collection.create_indexes(indexes, None).await
}

pub(crate) async fn drop_indexes(
    collection: Collection<Document>,
    indexes: Option<impl Iterator<Item = String>>,
) -> Result<()> {
    match indexes {
        Some(idxs) => {
            let mut session = collection.client().start_session(None).await?;
            for index in idxs {
                collection
                    .drop_index_with_session(index, None, &mut session)
                    .await?;
            }
            session.commit_transaction().await
        }
        None => collection.drop_indexes(None).await,
    }
}

pub(crate) async fn count_documents(
    collection: Collection<Document>,
    filter: Option<Document>,
) -> Result<u64> {
    collection.count_documents(filter, None).await
}

pub(crate) async fn drop(collection: Collection<Document>) -> Result<()> {
    collection.drop(None).await
}
