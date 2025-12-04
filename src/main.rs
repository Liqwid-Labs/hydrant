use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use pallas::network::facades::PeerClient;
use tokio::signal;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod db;
mod indexer;
mod primitives;
mod sync;
mod writer;

use db::Db;
use indexer::oracle::OracleIndexer;
use sync::Sync;

const NODE_HOST: &str = "localhost:3001";
const MAGIC: u64 = 764824073; // mainnet

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!(version = env!("CARGO_PKG_VERSION"), "Starting...");

    let db = Db::new("./db/hydrant", 2160)?;
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

    info!("Connecting to node...");
    let node = PeerClient::connect(NODE_HOST, MAGIC)
        .await
        .context("failed to connect to node")?;

    // Listen for chain-sync events until shutdown or error
    info!("Starting sync...");
    let mut sync = Sync::new(node, &db, &vec![indexer]).await?;
    let sync_result = tokio::select! {
        res = sync.run() => res,
        res = shutdown_signal() => {
            tracing::info!("Received shutdown signal");
            res
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

async fn shutdown_signal() -> Result<()> {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .context("failed to install Ctrl+C handler")
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("failed to install signal handler")?
            .recv()
            .await;
        Ok(())
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        res = ctrl_c => res,
        res = terminate => res,
    }
}
