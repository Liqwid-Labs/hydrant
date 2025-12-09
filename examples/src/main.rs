use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use hydrant::primitives::{AssetId, Hash, Policy};
use hydrant::{Db, Sync, UtxoIndexerBuilder};
use pallas::network::facades::PeerClient;
use tokio::signal;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

const MAX_ROLLBACK_BLOCKS: usize = 2160;
const DB_PATH: &str = "../db/hydrant";
const NODE_HOST: &str = "localhost:3001";
const MAGIC: u64 = 764824073; // mainnet

const POLICY_ID: Policy = Hash([
    0x0f, 0xde, 0x77, 0xa0, 0xea, 0x08, 0x33, 0x50, 0x2b, 0x38, 0x6d, 0x34, 0xe3, 0x3d, 0x78, 0xf8,
    0x6c, 0x75, 0x4b, 0xad, 0x30, 0x9e, 0xe8, 0xbf, 0x00, 0x8d, 0x7a, 0x9d,
]);

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!(version = env!("CARGO_PKG_VERSION"), "Starting...");

    let db = Db::new(DB_PATH, MAX_ROLLBACK_BLOCKS)?;
    let indexer = UtxoIndexerBuilder::new("utxo")
        .asset(AssetId::new(POLICY_ID, None))
        .build(&db.env)?;
    let indexer = Arc::new(Mutex::new(indexer));

    // Example logging UTxOs
    {
        let indexer = indexer.lock().unwrap();
        for (txo_pointer, _) in indexer.utxos()?.iter() {
            println!("{}", hex::encode(*txo_pointer.hash));
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
