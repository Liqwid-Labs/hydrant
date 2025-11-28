use anyhow::{Context, Result};
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::{ PeerClient};
use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::{ NextResponse, Tip};
use tracing::info;

use crate::db::Db;
use crate::writer::Writer;

const NODE_PATH: &str = "localhost:3001";
const MAGIC: u64 = 764824073; // mainnet

#[derive(Debug)]
pub enum SyncEvent {
    /// Rolled forward to a new block
    RollForward(Vec<u8>, Tip),
    /// Rolled back to a point in the chain
    RollBackward(Point),
}

pub struct Sync {
    node: PeerClient,
}

impl Sync {
    pub async fn new(db: &Db) -> Result<Self> {
        info!("Connecting to node...");
        let mut node = PeerClient::connect(NODE_PATH, MAGIC)
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
        loop {
            let next = {
                let chainsync = self.node.chainsync();
                match chainsync.has_agency() {
                    true => chainsync.request_next().await?,
                    false => chainsync.recv_while_must_reply().await?,
                }
            };

            let event = match next {
                NextResponse::RollForward(header, tip) => {
                    let header = match header.byron_prefix {
                        Some((subtag, _)) => {
                            MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor)
                        }
                        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
                    }?;
                    let slot = header.slot();
                    let hash = header.hash();
                    let point = Point::Specific(slot, hash.to_vec());

                    let block = self.node.blockfetch().fetch_single(point.clone()).await?;

                    SyncEvent::RollForward(block, tip)
                }
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
