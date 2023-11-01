use mongodb::{
    bson::{Bson, Document, RawDocument},
    error::Error,
    error::Result,
    results::{CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Client, ClientSession, Collection, Cursor, IndexModel, SessionCursor,
};
use serde::Deserialize;
use std::iter::Iterator;

pub(crate) enum CursorType<'a, T> {
    Session(SessionCursor<T>, &'a mut ClientSession),
    Plain(Cursor<T>),
}

pub(crate) struct ResultIterator<'a, T> {
    cursor: CursorType<'a, T>,
}

impl<'a, T> ResultIterator<'a, T> {
    pub(crate) fn new(cursor: CursorType<'a, T>) -> Self {
        ResultIterator { cursor }
    }

    pub(crate) async fn advance(&mut self) -> Result<bool> {
        match &mut self.cursor {
            CursorType::Session(c, s) => c.advance(s).await,
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

    pub(crate) fn current(&self) -> &RawDocument {
        match &self.cursor {
            CursorType::Session(c, _) => c.current(),
            CursorType::Plain(c) => c.current(),
        }
    }
}

pub(crate) async fn create_client(db_uri: &str) -> Result<Client> {
    Client::with_uri_str(db_uri).await
}

pub(crate) async fn find_one(
    collection: Collection<Document>,
    filter: Document,
    session: Option<&mut ClientSession>,
) -> Result<Document> {
    let out = match session {
        Some(s) => collection.find_one_with_session(filter, None, s).await?,
        None => collection.find_one(filter, None).await?,
    };
    out.ok_or(Error::custom("No document found"))
}

pub(crate) async fn find_many(
    collection: Collection<Document>,
    filter: Option<Document>,
    session: Option<&mut ClientSession>,
) -> Result<ResultIterator<Document>> {
    match session {
        Some(s) => {
            let cursor = collection.find_with_session(filter, None, s).await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, s)))
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
    session: Option<&mut ClientSession>,
) -> Result<InsertOneResult> {
    match session {
        Some(s) => collection.insert_one_with_session(document, None, s).await,
        None => collection.insert_one(document, None).await,
    }
}

pub(crate) async fn insert_many(
    collection: Collection<Document>,
    documents: impl Iterator<Item = Document>,
    session: Option<&mut ClientSession>,
) -> Result<InsertManyResult> {
    match session {
        Some(s) => {
            collection
                .insert_many_with_session(documents, None, s)
                .await
        }
        None => collection.insert_many(documents, None).await,
    }
}

pub(crate) async fn update_one(
    collection: Collection<Document>,
    update: Document,
    filter: Document,
    session: Option<&mut ClientSession>,
) -> Result<UpdateResult> {
    match session {
        Some(s) => {
            collection
                .update_one_with_session(filter, update, None, s)
                .await
        }
        None => collection.update_one(filter, update, None).await,
    }
}

pub(crate) async fn delete_one(
    collection: Collection<Document>,
    filter: Document,
    session: Option<&mut ClientSession>,
) -> Result<DeleteResult> {
    match session {
        Some(s) => collection.delete_one_with_session(filter, None, s).await,
        None => collection.delete_one(filter, None).await,
    }
}

pub(crate) async fn delete_many(
    collection: Collection<Document>,
    filter: Option<Document>,
    session: Option<&mut ClientSession>,
) -> Result<DeleteResult> {
    match filter {
        Some(f) => match session {
            Some(s) => collection.delete_many_with_session(f, None, s).await,
            None => collection.delete_many(f, None).await,
        },
        None => match session {
            Some(s) => {
                collection
                    .delete_many_with_session(Document::new(), None, s)
                    .await
            }
            None => collection.delete_many(Document::new(), None).await,
        },
    }
}

pub(crate) async fn aggregate(
    collection: Collection<Document>,
    pipeline: impl Iterator<Item = Document>,
    session: Option<&mut ClientSession>,
) -> Result<ResultIterator<Document>> {
    match session {
        Some(s) => {
            let cursor = collection.aggregate_with_session(pipeline, None, s).await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, s)))
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
    session: Option<&mut ClientSession>,
) -> Result<Vec<Bson>> {
    match session {
        Some(s) => {
            collection
                .distinct_with_session(field_name, filter, None, s)
                .await
        }
        None => collection.distinct(field_name, filter, None).await,
    }
}

pub(crate) async fn list_indexes(
    collection: Collection<Document>,
    session: Option<&mut ClientSession>,
) -> Result<ResultIterator<IndexModel>> {
    match session {
        Some(s) => {
            let cursor = collection.list_indexes_with_session(None, s).await?;
            Ok(ResultIterator::new(CursorType::Session(cursor, s)))
        }
        None => {
            let cursor = collection.list_indexes(None).await?;
            Ok(ResultIterator::new(CursorType::Plain(cursor)))
        }
    }
}

pub(crate) async fn create_indexes(
    collection: Collection<Document>,
    indexes: impl Iterator<Item = IndexModel>,
    session: Option<&mut ClientSession>,
) -> Result<CreateIndexesResult> {
    match session {
        Some(s) => {
            collection
                .create_indexes_with_session(indexes, None, s)
                .await
        }
        None => collection.create_indexes(indexes, None).await,
    }
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
