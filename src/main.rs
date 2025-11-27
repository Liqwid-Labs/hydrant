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

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: safe shutdown on error
    let db = Db::new("./db/hydrant")?;

    let mut peer = NodeClient::connect(NODE_SOCKET, MAGIC).await?;
    let client = peer.chainsync();
    if let Ok(Some(tip)) = db.tip() {
        println!("tip is {:?}, requesting intersection", tip);
        client.find_intersect(vec![tip]).await?;
    } else {
        println!("no tip, starting from origin");
        client.intersect_origin().await?;
    }

    // Process new blocks in a background task
    let (tx, mut rx) = mpsc::channel::<ProcessorEvent>(BUFFER_SIZE);
    let processor_db = db.clone();
    let processor_task = tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            if matches!(response, ProcessorEvent::Shutdown) {
                return Ok(());
            }
            if let Err(e) = process_event(response, &processor_db) {
                eprintln!("Error processing response: {}", e);
                return Err(e);
            }
        }
        Ok(())
    });

    tokio::select! {
        // Fetch blocks from node
        result = async {
            loop {
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
            println!("Received shutdown signal, stopping processor and persisting database...");
            tx.send(ProcessorEvent::Shutdown).await?;
            let _ = processor_task.await;
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

            if tip.0.slot_or_default() < block.slot().saturating_sub(1000)
                || block.number() % 500 == 0
            {
                println!(
                    "RollForward: slot {} | block {}",
                    block.slot(),
                    block.number(),
                );
            }

            Ok(())
        }
        ProcessorEvent::RollBackward(point) => {
            db.roll_backward(&point)?;
            match &point {
                Point::Origin => println!("RollBackward: to origin"),
                Point::Specific(slot, _) => println!("RollBackward: slot {}", slot),
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
