use anyhow::Result;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::NodeClient;
use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::{BlockContent, NextResponse, Tip};
use tokio::signal;
use tokio::sync::mpsc;

mod db;
mod tx;
use db::Db;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

const BUFFER_SIZE: usize = 10000;
const NODE_SOCKET: &str = "./db/node/socket";
const MAGIC: u64 = 764824073; // mainnet

enum ProcessorEvent {
    /// Rolled forward to a new block
    RollForward(BlockContent, Tip),
    /// Rolled back to a point in the chain
    RollBackward(Point),
    /// Reached the tip of the chain, waiting for new blocks
    Await,
    /// Program shutting down
    Shutdown,
}

// TODO: safe shutdown on error
#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let db = Db::new("./db/hydrant")?;

    tracing::info!("Connecting to node...");
    let mut peer = NodeClient::connect(NODE_SOCKET, MAGIC).await?;
    let client = peer.chainsync();
    if let Ok(Some(tip)) = db.tip() {
        tracing::info!(?tip, "Requesting intersection");
        client.find_intersect(vec![tip]).await?;
    } else {
        tracing::info!("No tip, starting from origin");
        client.intersect_origin().await?;
    }
    tracing::info!("Connected to node");

    // Process new blocks in a background task
    let (tx, mut rx) = mpsc::channel::<ProcessorEvent>(BUFFER_SIZE);
    let processor_db = db.clone();
    let processor_task = tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            if matches!(response, ProcessorEvent::Shutdown) {
                return Ok(());
            }
            if let Err(error) = process_event(response, &processor_db) {
                tracing::error!(?error, "error processing chain-sync response");
                return Err(error);
            }
        }
        Ok(())
    });

    tokio::select! {
        // Fetch blocks from node
        result = async {
            loop {
                if processor_task.is_finished() {
                    tracing::error!("Processor task exited early, persisting database and exiting...");
                    db.persist()?;
                    return Ok(());
                }

                let next = match client.has_agency() {
                    true => client.request_next().await?,
                    false => client.recv_while_must_reply().await?,
                };
                tx.send(match next {
                    NextResponse::RollForward(cbor, tip) => ProcessorEvent::RollForward(cbor, tip),
                    NextResponse::RollBackward(point, _) => ProcessorEvent::RollBackward(point),
                    NextResponse::Await => ProcessorEvent::Await,
                }).await?;
            }
        } => {
            result
        }

        // Shutdown
        _ = shutdown_signal() => {
            tracing::info!("Received shutdown signal, flushing processor...");
            tx.send(ProcessorEvent::Shutdown).await?;
            let _ = processor_task.await;
            tracing::info!("Persisting database...");
            db.persist()?;
            Ok(())
        }
    }
}

fn process_event(event: ProcessorEvent, db: &Db) -> Result<()> {
    match event {
        ProcessorEvent::RollForward(cbor, tip) => {
            let block = MultiEraBlock::decode(&cbor)?;
            db.roll_forward(&block)?;

            let tip_slot = tip.0.slot_or_default();
            if tip_slot < block.slot().saturating_sub(1000) || block.number() % 1000 == 0 {
                tracing::info!(slot = block.slot(), number = block.number(), "RollForward");
            }

            Ok(())
        }
        ProcessorEvent::RollBackward(point) => {
            db.roll_backward(&point)?;
            match &point {
                Point::Origin => tracing::info!(slot = 0, origin = true, "RollBackward"),
                Point::Specific(slot, _) => tracing::info!(?slot, origin = false, "RollBackward"),
            };
            Ok(())
        }
        _ => Ok(()),
    }
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
