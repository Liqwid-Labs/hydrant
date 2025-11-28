use anyhow::{Context, Result};
use pallas::network::facades::NodeClient;
use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::{BlockContent, NextResponse, Tip};
use tracing::info;

use crate::db::Db;
use crate::writer::Writer;

const NODE_SOCKET: &str = "./db/node/socket";
const MAGIC: u64 = 764824073; // mainnet

#[derive(Debug)]
pub enum SyncEvent {
    /// Rolled forward to a new block
    RollForward(BlockContent, Tip),
    /// Rolled back to a point in the chain
    RollBackward(Point),
}

pub struct Sync {
    node: NodeClient,
}

impl Sync {
    pub async fn new(db: &Db) -> Result<Self> {
        info!("Connecting to node...");
        let mut node = NodeClient::connect(NODE_SOCKET, MAGIC)
            .await
            .context("failed to connect to node")?;

        let chainsync = node.chainsync();
        if let Ok(Some(tip)) = db.tip() {
            info!(?tip, "Requesting intersection");
            chainsync
                .find_intersect(vec![tip])
                .await
                .context("failed to request intersection")?;
        } else {
            info!("No tip, starting from origin");
            chainsync
                .intersect_origin()
                .await
                .context("failed to start from origin")?;
        }

        Ok(Self { node })
    }

    pub async fn next(&mut self, writer: &Writer) -> Result<()> {
        let chainsync = self.node.chainsync();
        loop {
            let next = match chainsync.has_agency() {
                true => chainsync.request_next().await?,
                false => chainsync.recv_while_must_reply().await?,
            };

            let event = match next {
                NextResponse::RollForward(cbor, tip) => SyncEvent::RollForward(cbor, tip),
                NextResponse::RollBackward(point, _) => SyncEvent::RollBackward(point),
                NextResponse::Await => continue,
            };

            writer.send(event).await?
        }
    }

    pub async fn stop(self) {
        self.node.abort().await
    }
}

