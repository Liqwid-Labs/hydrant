use anyhow::{Context, Result};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tokio::sync::mpsc;

use crate::db::{Db, Filter};
use crate::sync::SyncEvent;

const BUFFER_SIZE: usize = 5000;

pub struct Writer {
    tx: mpsc::Sender<SyncEvent>,
    shutdown_tx: mpsc::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Writer {
    pub fn new(db: &Db, filter: Box<Filter>) -> Self {
        let (tx, mut rx) = mpsc::channel::<SyncEvent>(BUFFER_SIZE);
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let db = db.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    Some(event) = rx.recv() => {
                        let buffer_usage = (BUFFER_SIZE - rx.capacity()) as f64 / BUFFER_SIZE as f64 * 100.;
                        Writer::write_event(event, &db, &filter, buffer_usage)?;
                    }
                    else => break,
                }
            }
            Ok(())
        });
        Self {
            tx,
            shutdown_tx,
            task,
        }
    }

    pub async fn send(&self, event: SyncEvent) -> Result<()> {
        self.tx.send(event).await.context("Writer channel closed")?;
        Ok(())
    }

    pub async fn stop(self) -> Result<()> {
        drop(self.tx);
        if let Err(e) = self.shutdown_tx.send(()).await {
            tracing::error!(error = ?e, "error while sending shutdown signal to writer");
        }
        self.task.await?
    }

    fn write_event(event: SyncEvent, db: &Db, filter: &Filter, buffer_usage: f64) -> Result<()> {
        match event {
            SyncEvent::RollForward(cbor, tip) => {
                let block = MultiEraBlock::decode(&cbor)?;
                db.roll_forward(&block, filter)?;

                let tip_slot = tip.0.slot_or_default();
                if tip_slot.saturating_sub(1000) < block.slot() || block.number() % 1000 == 0 {
                    let sync_progress = block.slot() as f64 / tip_slot as f64 * 100.;
                    tracing::info!(
                        block = block.number(),
                        slot = block.slot(),
                        diff_to_expected = tip_slot.saturating_sub(block.slot()),
                        sync_progress = format!("{sync_progress:.2}%"),
                        buffer_usage = format!("{buffer_usage:.2}%"),
                        "RollForward"
                    );
                }
            }
            SyncEvent::RollBackward(point) => {
                db.roll_backward(&point)?;
                match &point {
                    Point::Origin => tracing::info!(slot = 0, origin = true, "RollBackward"),
                    Point::Specific(slot, _) => {
                        tracing::info!(?slot, origin = false, "RollBackward")
                    }
                };
            }
        }
        Ok(())
    }
}
