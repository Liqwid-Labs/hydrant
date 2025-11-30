use anyhow::{Context, Result};
use fjall::{Batch, Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use pallas::codec::minicbor;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tracing::{error, info};

use crate::tx::{Block, BlockHash, Datum, DatumHash, Tx, TxHash, TxOutputPointer, UnspentTxOutput};

mod partition;
use partition::Partition;

#[derive(Clone)]
pub struct Db {
    keyspace: Keyspace,
    state: PartitionHandle,
    pub slots: Partition<u64, BlockHash>,
    pub blocks: Partition<BlockHash, Block>,
    pub txs: Partition<TxHash, Tx>,
    pub datums: Partition<DatumHash, Datum>,
    pub unspent_tx_outputs: Partition<TxOutputPointer, UnspentTxOutput>,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        info!(?path, "Creating/opening database...");
        let keyspace = Config::new(path)
            .max_write_buffer_size(1024 * 1024 * 1024) // 1GB
            .max_journaling_size(1024 * 1024 * 1024) // 1GB
            .open()?;

        let options = PartitionCreateOptions::default().max_memtable_size(1024 * 1024 * 64); // 64MB
        let state = keyspace.open_partition("state", options.clone())?;
        let slots = keyspace.open_partition("slots", options.clone())?;
        let blocks = keyspace.open_partition("blocks", options.clone())?;
        let txs = keyspace.open_partition("txs", options.clone())?;
        let datums = keyspace.open_partition("datums", options.clone())?;
        let unspent_tx_outputs = keyspace.open_partition("unspent_tx_outputs", options.clone())?;

        Ok(Self {
            keyspace,
            state,
            slots: slots.into(),
            blocks: blocks.into(),
            txs: txs.into(),
            datums: datums.into(),
            unspent_tx_outputs: unspent_tx_outputs.into(),
        })
    }

    pub fn tip(&self) -> Result<Option<Point>> {
        Ok(self.state.get("tip")?.map(|v| {
            minicbor::decode::<Point>(&v).expect("failed to decode tip, consider wiping the db")
        }))
    }

    fn set_tip(&self, batch: &mut Batch, tip: &Point) -> Result<()> {
        let mut buffer = [0u8; 40];
        minicbor::encode(tip, buffer.as_mut()).expect("failed to encode tip");
        batch.insert(&self.state, "tip", buffer);
        Ok(())
    }

    pub fn roll_forward(&self, block: &MultiEraBlock) -> Result<()> {
        // TODO: verify block network id

        let mut batch = self.keyspace.batch();

        let mut tx_hashes = vec![];
        let mut datum_hashes = vec![];
        for raw_tx in block.txs().iter() {
            let (tx, datums) = Tx::parse(raw_tx);

            // Datum Hash -> Datum
            for (hash, data) in datums {
                if !self.datums.contains_key(&hash)? {
                    datum_hashes.push(hash.clone());
                    self.datums.insert(&mut batch, &hash, &data)?;
                }
            }

            // Mark outputs as unspent
            for (index, output) in tx.unspent().enumerate() {
                let pointer = TxOutputPointer::new(tx.hash.clone(), index);
                let utxo = UnspentTxOutput::new(&pointer, output);
                self.unspent_tx_outputs
                    .insert(&mut batch, &pointer, &utxo)?;
            }

            // Mark inputs as spent
            for pointer in tx.spent() {
                self.unspent_tx_outputs.remove(&mut batch, pointer)?;
            }

            // Tx Hash -> Tx
            tx_hashes.push(tx.hash.clone());
            self.txs.insert(&mut batch, &tx.hash, &tx)?;
        }

        // Block Hash -> Block
        let block = Block::parse(block, tx_hashes, datum_hashes);
        self.blocks.insert(&mut batch, &block.hash, &block)?;

        // Slot -> Block Hash
        self.slots.insert(&mut batch, &block.slot, &block.hash)?;

        self.set_tip(
            &mut batch,
            &Point::Specific(block.slot, block.hash.to_vec()),
        )?;

        batch.commit()?;

        Ok(())
    }

    pub fn roll_backward(&self, point: &Point) -> Result<()> {
        // TODO: cleanup datums
        let mut batch = self.keyspace.batch();

        let slot = match point {
            Point::Origin => 0,
            Point::Specific(slot, _) => *slot + 1,
        };
        for res in self.slots.range(slot..)?.rev() {
            let (slot, block_hash) = res?;
            let Some(block) = self.blocks.get(&block_hash)? else {
                error!(
                    "Block not found, the db could be corrupted: {:?}",
                    block_hash
                );
                continue;
            };

            // NOTE: reverse order because a tx may spend outputs from a previous tx
            // in the same block
            for tx_hash in block.txs.iter().rev() {
                self.txs.remove(&mut batch, tx_hash)?;

                let Some(tx) = self.txs.get(tx_hash)? else {
                    error!("Tx not found, the db could be corrupted: {:?}", tx_hash);
                    continue;
                };

                // Remove outputs from unspent_tx_outputs
                for (index, _) in tx.unspent().enumerate() {
                    let pointer = TxOutputPointer::new(tx.hash.clone(), index);
                    self.unspent_tx_outputs.remove(&mut batch, &pointer)?;
                }

                // Add inputs back to unspent_tx_outputs
                for pointer in tx.spent() {
                    let utxo = self.txs.get(&pointer.hash)?.and_then(|spent_tx| {
                        spent_tx
                            .outputs
                            .get(pointer.index as usize)
                            .map(|spent_tx_output| UnspentTxOutput::new(pointer, spent_tx_output))
                    });

                    if let Some(utxo) = utxo {
                        self.unspent_tx_outputs.insert(&mut batch, pointer, &utxo)?;
                    } else {
                        error!(
                            "Tx or output not found, the db could be corrupted: {:?}",
                            pointer
                        );
                    }
                }
            }

            self.slots.remove(&mut batch, &slot)?;
            self.blocks.remove(&mut batch, &block_hash)?;
        }

        self.set_tip(&mut batch, point)?;

        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        self.keyspace
            .persist(fjall::PersistMode::SyncAll)
            .context("failed to persist db")
    }
}
