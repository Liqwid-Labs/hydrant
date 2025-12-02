use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tokio::sync::mpsc;

use crate::db::Db;
use crate::indexer::Indexer;
use crate::sync::SyncEvent;

const BUFFER_SIZE: usize = 5000;

pub struct Writer {
    tx: mpsc::Sender<SyncEvent>,
    shutdown_tx: mpsc::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Writer {
    pub fn new(db: &Db, indexer: &Arc<Mutex<impl Indexer + Send + 'static>>) -> Self {
        let (tx, mut rx) = mpsc::channel::<SyncEvent>(BUFFER_SIZE);
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let db = db.clone();
        let indexer = indexer.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    Some(event) = rx.recv() => {
                        let buffer_usage = (BUFFER_SIZE - rx.capacity()) as f64 / BUFFER_SIZE as f64 * 100.;
                        Writer::write_event(event, &indexer, &db, buffer_usage)?;
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

    fn write_event(
        event: SyncEvent,
        indexer: &Arc<Mutex<impl Indexer>>,
        db: &Db,
        buffer_usage: f64,
    ) -> Result<()> {
        match event {
            SyncEvent::RollForward(cbor, tip) => {
                let block = MultiEraBlock::decode(&cbor)?;
                db.roll_forward(indexer, &block)?;

                let tip_slot = tip.0.slot_or_default();
                let at_tip = tip_slot.saturating_sub(1000) < block.slot();
                if at_tip || block.number() % 10000 == 0 {
                    db.trim_volatile()?;
                    db.persist()?;

                    let sync_progress = block.slot() as f64 / tip_slot as f64 * 100.;
                    tracing::info!(
                        block = block.number(),
                        slot = block.slot(),
                        slots_to_tip = tip_slot.saturating_sub(block.slot()),
                        sync_progress = format!("{sync_progress:.2}%"),
                        buffer_usage = format!("{buffer_usage:.2}%"),
                        "RollForward"
                    );
                }
            }
            SyncEvent::RollBackward(point) => {
                db.roll_backward(indexer, &point)?;
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
