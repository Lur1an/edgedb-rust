use std::mem::ManuallyDrop;
use std::sync::Arc;

use bytes::BytesMut;
use edgedb_errors::{
    Error, ErrorKind, NoDataError, NoResultExpected, ProtocolEncodingError, SHOULD_RETRY,
};
use edgedb_protocol::common::{Capabilities, Cardinality, CompilationOptions, IoFormat};
use edgedb_protocol::model::Json;
use edgedb_protocol::query_arg::{Encoder, QueryArgs};
use edgedb_protocol::QueryResult;
use tokio::sync::oneshot::{channel, Sender};
use tokio::time::sleep;

use crate::raw::{Connection, Options, Pool, PoolConnection, QueryCapabilities};
use crate::state::State;

#[derive(Debug)]
pub struct ManualTransaction {
    options: Arc<Options>,
    conn: ManuallyDrop<PoolConnection>,
    committed: bool,
    rollback_tx: ManuallyDrop<Sender<(PoolConnection, Arc<Options>)>>,
}

pub(crate) async fn start_transaction(
    pool: &Pool,
    options: &Arc<Options>,
) -> Result<ManualTransaction, Error> {
    let mut conn = pool.acquire().await?;
    let (tx, rx) = channel::<(PoolConnection, Arc<Options>)>();
    tokio::spawn(async move {
        if let Ok((mut conn, options)) = rx.await {
            log::debug!("Rolling back transaction");
            conn.statement("ROLLBACK", &options.state)
                .await
                .expect("rollback failed");
        }
    });
    conn.statement("START TRANSACTION", &options.state).await?;
    Ok(ManualTransaction {
        options: options.clone(),
        conn: ManuallyDrop::new(conn),
        committed: false,
        rollback_tx: ManuallyDrop::new(tx),
    })
}

impl Drop for ManualTransaction {
    fn drop(&mut self) {
        let (conn, tx) = unsafe {
            (
                ManuallyDrop::take(&mut self.conn),
                ManuallyDrop::take(&mut self.rollback_tx),
            )
        };
        if !self.committed {
            tx.send((conn, self.options.clone()))
                .expect("failed to send rollback signal");
        }
    }
}

impl ManualTransaction {
    pub async fn commit(mut self) -> Result<(), Error> {
        self.conn.statement("COMMIT", &self.options.state).await?;
        self.committed = true;
        Ok(())
    }

    /// Execute a query and return a collection of results.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Vec<String> = pool.query("SELECT 'hello'", &());
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query<R, A>(&mut self, query: &str, arguments: &A) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let caps = Capabilities::MODIFICATIONS;
        let mut iteration = 0;
        let conn = &mut self.conn.inner();
        loop {
            let state = &self.options.state;
            match conn.query(query.as_ref(), arguments, state, caps).await {
                Ok(resp) => return Ok(resp.data),
                Err(e) => {
                    let allow_retry = match e.get::<QueryCapabilities>() {
                        // Error from a weird source, or just a bug
                        // Let's keep on the safe side
                        None => false,
                        Some(QueryCapabilities::Unparsed) => true,
                        Some(QueryCapabilities::Parsed(c)) => c.is_empty(),
                    };
                    if allow_retry && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a query and return a single result
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_single::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Option<String> = pool.query_single(
    ///     "SELECT 'hello'",
    ///     &()
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(
        &mut self,
        query: &str,
        arguments: &A,
    ) -> Result<Option<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let mut iteration = 0;
        let caps = Capabilities::MODIFICATIONS;
        let state = &self.options.state;
        let conn = self.conn.inner();
        loop {
            match conn
                .query_single(query.as_ref(), arguments, state, caps)
                .await
            {
                Ok(resp) => return Ok(resp.data),
                Err(e) => {
                    let allow_retry = match e.get::<QueryCapabilities>() {
                        // Error from a weird source, or just a bug
                        // Let's keep on the safe side
                        None => false,
                        Some(QueryCapabilities::Unparsed) => true,
                        Some(QueryCapabilities::Parsed(c)) => c.is_empty(),
                    };
                    if allow_retry && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a query and return a single result
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_required_single::<String, _>(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// // or
    /// let greeting: String = pool.query_required_single(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_required_single<R, A>(
        &mut self,
        query: &str,
        arguments: &A,
    ) -> Result<R, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        self.query_single(query, arguments)
            .await?
            .ok_or_else(|| NoDataError::with_message("query row returned zero results"))
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Json, Error> {
        let mut iteration = 0;
        let allow_capabilities = Capabilities::MODIFICATIONS;
        let conn = &mut self.conn;
        loop {
            let flags = CompilationOptions {
                implicit_limit: None,
                implicit_typenames: false,
                implicit_typeids: false,
                explicit_objectids: true,
                allow_capabilities,
                io_format: IoFormat::Json,
                expected_cardinality: Cardinality::Many,
            };
            let desc = match conn
                .parse(&flags, query.as_ref(), &self.options.state)
                .await
            {
                Ok(parsed) => parsed,
                Err(e) => {
                    if e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };
            let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

            let mut arg_buf = BytesMut::with_capacity(8);
            arguments.encode(&mut Encoder::new(
                &inp_desc.as_query_arg_context(),
                &mut arg_buf,
            ))?;

            let res = conn
                .execute(
                    &flags,
                    query.as_ref(),
                    &self.options.state,
                    &desc,
                    &arg_buf.freeze(),
                )
                .await;
            let data = match res {
                Ok(data) => data,
                Err(e) => {
                    if desc.capabilities == Capabilities::empty() && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };

            let out_desc = desc.output().map_err(ProtocolEncodingError::with_source)?;
            match out_desc.root_pos() {
                Some(root_pos) => {
                    let ctx = out_desc.as_queryable_context();
                    // JSON objects are returned as strings :(
                    let mut state = String::prepare(&ctx, root_pos)?;
                    let bytes = data
                        .into_iter()
                        .next()
                        .and_then(|chunk| chunk.data.into_iter().next());
                    if let Some(bytes) = bytes {
                        // we trust database to produce valid json
                        let s = String::decode(&mut state, &bytes)?;
                        return Ok(Json::new_unchecked(s));
                    } else {
                        return Err(NoDataError::with_message("query row returned zero results"));
                    }
                }
                None => return Err(NoResultExpected::build()),
            }
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised.
    pub async fn query_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Option<Json>, Error> {
        let mut iteration = 0;
        let conn = &mut self.conn;
        let allow_capabilities = Capabilities::MODIFICATIONS;
        loop {
            let flags = CompilationOptions {
                implicit_limit: None,
                implicit_typenames: false,
                implicit_typeids: false,
                explicit_objectids: true,
                allow_capabilities,
                io_format: IoFormat::Json,
                expected_cardinality: Cardinality::AtMostOne,
            };
            let desc = match conn.parse(&flags, query, &self.options.state).await {
                Ok(parsed) => parsed,
                Err(e) => {
                    if e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };
            let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

            let mut arg_buf = BytesMut::with_capacity(8);
            arguments.encode(&mut Encoder::new(
                &inp_desc.as_query_arg_context(),
                &mut arg_buf,
            ))?;

            let res = conn
                .execute(&flags, query, &self.options.state, &desc, &arg_buf.freeze())
                .await;
            let data = match res {
                Ok(data) => data,
                Err(e) => {
                    if desc.capabilities == Capabilities::empty() && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };

            let out_desc = desc.output().map_err(ProtocolEncodingError::with_source)?;
            match out_desc.root_pos() {
                Some(root_pos) => {
                    let ctx = out_desc.as_queryable_context();
                    // JSON objects are returned as strings :(
                    let mut state = String::prepare(&ctx, root_pos)?;
                    let bytes = data
                        .into_iter()
                        .next()
                        .and_then(|chunk| chunk.data.into_iter().next());
                    if let Some(bytes) = bytes {
                        // we trust database to produce valid json
                        let s = String::decode(&mut state, &bytes)?;
                        return Ok(Some(Json::new_unchecked(s)));
                    } else {
                        return Ok(None);
                    }
                }
                None => return Err(NoResultExpected::build()),
            }
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    pub async fn query_required_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Json, Error> {
        self.query_single_json(query, arguments)
            .await?
            .ok_or_else(|| NoDataError::with_message("query row returned zero results"))
    }

    /// Execute a query and don't expect result
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn execute<A>(&mut self, query: &str, arguments: &A) -> Result<(), Error>
    where
        A: QueryArgs,
    {
        let caps = Capabilities::MODIFICATIONS;
        let mut iteration = 0;
        let conn = self.conn.inner();
        loop {
            let state = &self.options.state;
            match conn.execute(query.as_ref(), arguments, state, caps).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let allow_retry = match e.get::<QueryCapabilities>() {
                        // Error from a weird source, or just a bug
                        // Let's keep on the safe side
                        None => false,
                        Some(QueryCapabilities::Unparsed) => true,
                        Some(QueryCapabilities::Parsed(c)) => c.is_empty(),
                    };
                    if allow_retry && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }
}
