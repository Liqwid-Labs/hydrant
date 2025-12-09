use std::time::Duration;

use anyhow::{Context, Result};
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::Point;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse, Tip};
use tokio::time::sleep;
use tracing::info;

use crate::db::Db;
use crate::indexer::IndexerList;
use crate::writer::Writer;

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
    writer: Writer,
    pending_fetches: Vec<(Point, Tip)>,
}

impl Sync {
    pub async fn new(mut node: PeerClient, db: &Db, indexer: &IndexerList) -> Result<Self> {
        let tip = db.tip()?;
        match db.tip()? {
            Point::Origin => {
                info!("No tip, starting from origin");
                node.chainsync()
                    .intersect_origin()
                    .await
                    .context("failed to start from origin")?;
            }
            Point::Specific(_, _) => {
                info!(?tip, "Requesting intersection");
                node.chainsync()
                    .find_intersect(vec![tip])
                    .await
                    .context("failed to request intersection")?;
            }
        };

        Ok(Self {
            node,
            writer: Writer::new(db, indexer),
            pending_fetches: vec![],
        })
    }

    pub async fn next(&mut self) -> Result<NextResponse<HeaderContent>> {
        let next = {
            let chainsync = self.node.chainsync();
            match chainsync.has_agency() {
                true => chainsync.request_next().await?,
                false => chainsync.recv_while_must_reply().await?,
            }
        };

        match next {
            NextResponse::RollForward(ref header, ref tip) => {
                let subtag = header.byron_prefix.map(|(subtag, _)| subtag);
                let header = MultiEraHeader::decode(header.variant, subtag, &header.cbor)?;
                let point = Point::Specific(header.slot(), header.hash().to_vec());
                let is_at_tip = point == tip.0;

                self.pending_fetches.push((point, tip.clone()));
                if self.pending_fetches.len() >= BLOCKFETCH_CONCURRENCY || is_at_tip {
                    self.flush_pending_fetches().await?;
                }
            }
            NextResponse::RollBackward(ref point, _) => {
                self.flush_pending_fetches().await?;
                self.writer
                    .send(SyncEvent::RollBackward(point.clone()))
                    .await?;
            }
            NextResponse::Await => {
                self.flush_pending_fetches().await?;
            }
        };

        Ok(next)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let next = self.next().await?;
            if matches!(next, NextResponse::Await) {
                sleep(Duration::from_millis(10)).await;
            }
        }
    }

    pub async fn run_until_synced(&mut self) -> Result<()> {
        loop {
            if matches!(self.next().await?, NextResponse::Await) {
                self.writer.wait_until_flushed().await?;
                return Ok(());
            }
        }
    }

    async fn flush_pending_fetches(&mut self) -> Result<()> {
        if let Some((start, _)) = self.pending_fetches.first()
            && let Some((end, tip)) = self.pending_fetches.last()
        {
            let blocks = self
                .node
                .blockfetch()
                .fetch_range((start.clone(), end.clone()))
                .await?;
            if blocks.len() != self.pending_fetches.len() {
                return Err(anyhow::anyhow!(
                    "fetched {} blocks, expected {}",
                    blocks.len(),
                    self.pending_fetches.len()
                ));
            }
            for block in blocks {
                self.writer
                    .send(SyncEvent::RollForward(block, tip.clone()))
                    .await?;
            }
        }
        self.pending_fetches.clear();
        Ok(())
    }

    pub async fn stop(self) -> Result<()> {
        self.node.abort().await;
        self.writer.stop().await.context("error while writing")
    }
}
