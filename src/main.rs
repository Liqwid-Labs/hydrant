use anyhow::Result;
use tokio::signal;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod db;
mod sync;
mod tx;
mod writer;
use db::Db;

use crate::sync::Sync;
use crate::writer::Writer;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "Starting...");

    let db = Db::new("./db/hydrant")?;

    let writer = Writer::new(&db);
    let mut sync = Sync::new(&db).await?;

    // Listen for chain-sync events until shutdown or error
    info!("Starting sync...");
    let sync_result = tokio::select! {
        res = sync.next(&writer) => res,
        _ = shutdown_signal() => {
            tracing::info!("Received shutdown signal");
            Ok(())
        }
    };
    if let Err(error) = sync_result {
        error!(?error);
    }

    info!("Stopping sync...");
    sync.stop().await;

    info!("Stopping writer...");
    if let Err(error) = writer.stop().await {
        error!(?error);
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
