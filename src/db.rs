use anyhow::{Context, Result};
use heed::byteorder::BigEndian;
use heed::types::{Bytes, Str, U64};
use heed::{Database, Env, EnvOpenOptions};
use pallas::codec::minicbor;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tracing::info;

use crate::codec::RkyvCodec;
use crate::tx::{Block, BlockHash, Datum, DatumHash, Tx, TxHash, TxOutputPointer};

#[derive(Clone)]
pub struct Db {
    pub env: Env,
    state: Database<Str, Bytes>,
    pub slots: Database<U64<BigEndian>, RkyvCodec<BlockHash>>,
    pub blocks: Database<RkyvCodec<BlockHash>, RkyvCodec<Block>>,
    pub txs: Database<RkyvCodec<TxHash>, RkyvCodec<Tx>>,
    pub utxos: Database<RkyvCodec<TxOutputPointer>, RkyvCodec<()>>,
    pub datums: Database<RkyvCodec<DatumHash>, RkyvCodec<Datum>>,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        info!(?path, "Creating/opening database...");
        std::fs::create_dir_all(path)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(64)
                .flags(
                    heed::EnvFlags::NO_SYNC // manually fsync data
                    | heed::EnvFlags::NO_META_SYNC // manually fsync metadata
                    | heed::EnvFlags::WRITE_MAP, // assume no memory unsafety in this program
                )
                .map_size(1024 * 1024 * 1024 * 500) // maximum size of LMDB (500GB)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let state = env.create_database(&mut wtxn, Some("state"))?;
        let slots = env.create_database(&mut wtxn, Some("slots"))?;
        let blocks = env.create_database(&mut wtxn, Some("blocks"))?;
        let txs = env.create_database(&mut wtxn, Some("txs"))?;
        let utxos = env.create_database(&mut wtxn, Some("utxos"))?;
        let datums = env.create_database(&mut wtxn, Some("datums"))?;
        wtxn.commit()?;

        Ok(Self {
            env,
            state,
            slots,
            blocks,
            txs,
            utxos,
            datums,
        })
    }

    pub fn tip(&self) -> Result<Option<Point>> {
        let rtxn = self.env.read_txn()?;
        self.state
            .get(&rtxn, "tip")?
            .map(|v| {
                minicbor::decode::<Point>(v)
                    .context("failed to decode tip, the db could be corrupted")
            })
            .transpose()
    }

    fn set_tip(&self, wtxn: &mut heed::RwTxn, tip: &Point) -> Result<()> {
        let mut buffer = [0u8; 40];
        minicbor::encode(tip, buffer.as_mut()).context("failed to encode tip")?;
        self.state.put(wtxn, "tip", &buffer)?;
        Ok(())
    }

    pub fn roll_forward(&self, block: &MultiEraBlock) -> Result<()> {
        // TODO: verify block network id

        let mut wtxn = self.env.write_txn()?;

        let mut tx_hashes = vec![];
        let mut datum_hashes = vec![];
        for raw_tx in block.txs().iter() {
            let (tx, datums) = Tx::parse(raw_tx);

            // Datum Hash -> Datum
            for (hash, data) in datums {
                if self.datums.get(&wtxn, &hash)?.is_none() {
                    datum_hashes.push(hash.clone());
                    self.datums.put(&mut wtxn, &hash, &data)?;
                }
            }

            // Mark outputs as unspent
            for (index, _) in tx.unspent().enumerate() {
                let pointer = TxOutputPointer::new(tx.hash.clone(), index);
                self.utxos.put(&mut wtxn, &pointer, &())?;
            }

            // Mark inputs as spent
            for pointer in tx.spent() {
                self.utxos.delete(&mut wtxn, pointer)?;
            }

            // Tx Hash -> Tx
            tx_hashes.push(tx.hash.clone());
            self.txs.put(&mut wtxn, &tx.hash, &tx)?;
        }

        // Block Hash -> Block
        let block = Block::parse(block, tx_hashes, datum_hashes);
        self.blocks.put(&mut wtxn, &block.hash, &block)?;

        // Slot -> Block Hash
        self.slots.put(&mut wtxn, &block.slot, &block.hash)?;

        self.set_tip(&mut wtxn, &Point::Specific(block.slot, block.hash.to_vec()))?;

        Ok(wtxn.commit()?)
    }

    pub fn roll_backward(&self, point: &Point) -> Result<()> {
        // TODO: cleanup datums
        let slot = match point {
            Point::Origin => return self.clear(),
            Point::Specific(slot, _) => *slot + 1,
        };

        let rtxn = self.env.read_txn()?;
        for res in self.slots.rev_range(&rtxn, &(slot..))? {
            let mut wtxn = self.env.write_txn()?;
            let (slot, block_hash) = res?;
            info!(?slot, "Rolling back slot");
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;

            let block = self.blocks.get(&rtxn, &block_hash)?.with_context(|| {
                format!(
                    "Block not found, the db could be corrupted: {:?}",
                    block_hash
                )
            })?;

            // NOTE: reverse order because a tx may spend outputs from a previous tx
            // in the same block
            for tx_hash in block.txs.iter().rev() {
                let tx_hash = rkyv::deserialize::<TxHash, rkyv::rancor::Error>(tx_hash)?;

                let tx = self.txs.get(&wtxn, &tx_hash)?.with_context(|| {
                    format!("Tx not found, the db could be corrupted: {:?}", tx_hash)
                })?;
                let tx = rkyv::deserialize::<Tx, rkyv::rancor::Error>(tx)?;

                // Mark outputs as spent
                for (index, _) in tx.unspent().enumerate() {
                    let pointer = TxOutputPointer::new(tx.hash.clone(), index);
                    self.utxos.delete(&mut wtxn, &pointer)?;
                }

                // Mark inputs as unspent
                for pointer in tx.spent() {
                    self.utxos.put(&mut wtxn, pointer, &())?;
                }

                self.txs.delete(&mut wtxn, &tx_hash)?;
            }

            self.slots.delete(&mut wtxn, &slot)?;
            self.blocks.delete(&mut wtxn, &block_hash)?;
            self.set_tip(&mut wtxn, &Point::Specific(slot, block_hash.to_vec()))?;
            wtxn.commit()?;
        }

        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.state.clear(&mut wtxn)?;
        self.slots.clear(&mut wtxn)?;
        self.blocks.clear(&mut wtxn)?;
        self.txs.clear(&mut wtxn)?;
        self.datums.clear(&mut wtxn)?;
        self.utxos.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        self.env.force_sync()?;
        Ok(())
    }
}
