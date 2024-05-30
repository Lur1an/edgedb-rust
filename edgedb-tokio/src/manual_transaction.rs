use std::sync::Arc;

use edgedb_errors::Error;
use tokio::sync::oneshot::{channel, Sender};

use crate::raw::{Options, Pool, PoolConnection, PoolState};

#[derive(Debug)]
pub struct ManualTransaction {
    state: Arc<PoolState>,
    rollback_tx: Option<Sender<()>>,
}

pub(crate) async fn start_transaction(pool: &Pool) -> Result<ManualTransaction, Error> {
    let conn = pool.acquire().await?;
    let (tx, rx) = channel::<()>();
    tokio::spawn(async move {
        match rx.await {
            Ok(_) => todo!(),
            Err(e) => todo!(),
        }
    });
    todo!()
}

impl Drop for ManualTransaction {
    fn drop(&mut self) {}
}
