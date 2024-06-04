use std::mem::ManuallyDrop;
use std::sync::Arc;

use edgedb_errors::Error;
use tokio::sync::oneshot::{channel, Sender};

use crate::raw::{Options, Pool, PoolConnection, PoolState};

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
    let conn = pool.acquire().await?;
    let (tx, rx) = channel::<(PoolConnection, Arc<Options>)>();
    tokio::spawn(async move {
        if let Ok((mut conn, options)) = rx.await {
            log::debug!("Rolling back transaction");
            conn.statement("ROLLBACK", &options.state)
                .await
                .expect("rollback failed");
        }
    });
    Ok(ManualTransaction {
        options: options.clone(),
        conn: ManuallyDrop::new(conn),
        committed: false,
        rollback_tx: ManuallyDrop::new(tx),
    })
}

impl Drop for ManualTransaction {
    fn drop(&mut self) {
        if !self.committed {
            ManuallyDrop::take(slot)
        }
    }
}
