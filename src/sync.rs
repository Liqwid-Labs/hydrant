use anyhow::{Context, Result};
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::{NextResponse, Tip};
use tracing::info;

use crate::db::Db;
use crate::writer::Writer;

const NODE_PATH: &str = "localhost:3001";
const MAGIC: u64 = 764824073; // mainnet
const BLOCKFETCH_CONCURRENCY: usize = 200;

#[derive(Debug)]
pub enum SyncEvent {
    /// Rolled forward to a new block
    RollForward(Vec<u8>, Tip),
    /// Rolled back to a point in the chain
    RollBackward(Point),
}

pub struct Sync {
    node: PeerClient,
    pending_fetches: Vec<(Point, Tip)>,
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

        Ok(Self {
            node,
            pending_fetches: vec![],
        })
    }

    pub async fn next(&mut self, writer: &Writer) -> Result<()> {
        loop {
            // Collect multiple headers first
            let next = {
                let chainsync = self.node.chainsync();
                match chainsync.has_agency() {
                    true => chainsync.request_next().await?,
                    false => chainsync.recv_while_must_reply().await?,
                }
            };

            match next {
                NextResponse::RollForward(header, tip) => {
                    let subtag = header.byron_prefix.map(|(subtag, _)| subtag);
                    let header = MultiEraHeader::decode(header.variant, subtag, &header.cbor)?;
                    let point = Point::Specific(header.slot(), header.hash().to_vec());
                    let is_at_tip = point == tip.0;

                    self.pending_fetches.push((point, tip));
                    if self.pending_fetches.len() >= BLOCKFETCH_CONCURRENCY || is_at_tip {
                        self.flush_pending_fetches(writer).await?;
                    }
                }
                NextResponse::RollBackward(point, _) => {
                    self.flush_pending_fetches(writer).await?;
                    writer.send(SyncEvent::RollBackward(point)).await?;
                }
                NextResponse::Await => {
                    self.flush_pending_fetches(writer).await?;
                    break;
                }
            };
        }

        Ok(())
    }

    async fn flush_pending_fetches(&mut self, writer: &Writer) -> Result<()> {
        if let Some((start, _)) = self.pending_fetches.first()
            && let Some((end, tip)) = self.pending_fetches.last()
        {
            let blocks = self
                .node
                .blockfetch()
                .fetch_range((start.clone(), end.clone()))
                .await?;
            assert!(blocks.len() == self.pending_fetches.len());
            for block in blocks {
                writer
                    .send(SyncEvent::RollForward(block, tip.clone()))
                    .await?;
            }
        }
        self.pending_fetches.clear();
        Ok(())
    }

    pub async fn stop(self) {
        self.node.abort().await
    }
}
