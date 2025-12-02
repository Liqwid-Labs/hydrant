use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::signal;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod codec;
mod db;
mod indexer;
mod sync;
mod tx;
mod writer;
use db::Db;

use crate::indexer::oracle::OracleIndexer;
use crate::sync::Sync;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!(version = env!("CARGO_PKG_VERSION"), "Starting...");

    let db = Db::new("./db/hydrant", 1024 * 1024 * 1024 * 20)?;
    let indexer = Arc::new(Mutex::new(OracleIndexer::new(&db.env)?));

    // Example logging Oracle UTxOs
    {
        let indexer = indexer.lock().unwrap();
        for (tx_pointer, utxo) in indexer.utxos()?.iter() {
            println!("{}", hex::encode(*tx_pointer.hash));
            if let Some(datum_hash) = &utxo.datum_hash {
                println!("{:?}", indexer.datum(datum_hash)?);
            }
        }
    };

    let mut sync = Sync::new(&db, &indexer).await?;

    // Listen for chain-sync events until shutdown or error
    info!("Starting sync...");
    let sync_result = tokio::select! {
        res = sync.run() => res,
        _ = shutdown_signal() => {
            tracing::info!("Received shutdown signal");
            Ok(())
        }
    };
    if let Err(error) = sync_result {
        error!(?error, "Error while syncing");
    }

    info!("Stopping sync...");
    if let Err(error) = sync.stop().await {
        error!(?error, "Error while writing");
    }

    info!("Persisting database...");
    db.persist()?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
