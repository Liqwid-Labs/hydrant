use anyhow::Result;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::NodeClient;
use pallas::network::miniprotocols::chainsync::{BlockContent, NextResponse, Tip};
use pallas::network::miniprotocols::{Point, chainsync};
use tokio::signal;
use tokio::sync::mpsc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod db;
mod tx;
use db::Db;

const BUFFER_SIZE: usize = 10000;
const NODE_SOCKET: &str = "./db/node/socket";
const MAGIC: u64 = 764824073; // mainnet

enum ProcessorEvent {
    /// Rolled forward to a new block
    RollForward(BlockContent, Tip),
    /// Rolled back to a point in the chain
    RollBackward(Point),
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let db = Db::new("./db/hydrant")?;
    let peer = setup_node_connection(&db).await?;
    let result = run_sync_loop(peer, db.clone()).await;

    tracing::info!("Persisting database...");
    db.persist()?;

    result
}

async fn setup_node_connection(db: &Db) -> Result<NodeClient> {
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
    Ok(peer)
}

async fn run_sync_loop(mut peer: NodeClient, db: Db) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<ProcessorEvent>(BUFFER_SIZE);

    // Push blocks to database on a background task
    let processor = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(error) = process_event(event, &db) {
                tracing::error!(?error, "error processing chain-sync response");
                return Err(error);
            }
        }
        Ok(())
    });

    // Listen for chain-sync events until shutdown or error
    let sync_result = tokio::select! {
        res = fetch_chainsync_event(peer.chainsync(), tx.clone()) => res,
        _ = shutdown_signal() => {
            tracing::info!("Received shutdown signal, flushing processor...");
            Ok(())
        }
    };

    // Stop sending new events and wait for processor to drain
    drop(tx);
    let _ = processor.await?; // TODO: handle error

    sync_result
}

async fn fetch_chainsync_event(
    client: &mut chainsync::N2CClient,
    tx: mpsc::Sender<ProcessorEvent>,
) -> Result<()> {
    loop {
        let next = match client.has_agency() {
            true => client.request_next().await?,
            false => client.recv_while_must_reply().await?,
        };

        let event = match next {
            NextResponse::RollForward(cbor, tip) => ProcessorEvent::RollForward(cbor, tip),
            NextResponse::RollBackward(point, _) => ProcessorEvent::RollBackward(point),
            NextResponse::Await => continue,
        };

        // If send fails, receiver has been dropped, so we should stop
        if tx.send(event).await.is_err() {
            return Ok(());
        }
    }
}

fn process_event(event: ProcessorEvent, db: &Db) -> Result<()> {
    match event {
        ProcessorEvent::RollForward(cbor, tip) => {
            let block = MultiEraBlock::decode(&cbor)?;
            db.roll_forward(&block)?;

            let tip_slot = tip.0.slot_or_default();
            if tip_slot.saturating_sub(1000) < block.slot() || block.number() % 1000 == 0 {
                tracing::info!(
                    slot = block.slot(),
                    block = block.number(),
                    ?tip_slot,
                    "RollForward"
                );
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
