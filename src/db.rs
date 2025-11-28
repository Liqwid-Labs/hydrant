use bincode::config::{BigEndian, Configuration};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, Result};
use pallas::codec::minicbor;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;

use crate::tx::Tx;

static CONFIG: Configuration<BigEndian> = bincode::config::standard().with_big_endian();

#[derive(Clone)]
pub struct Db {
    keyspace: Keyspace,
    state: PartitionHandle,
    slots: PartitionHandle,
    blocks: PartitionHandle,
    txs: PartitionHandle,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        let keyspace = Config::new(path).fsync_ms(Some(1000)).open()?;

        let state = keyspace.open_partition("state", PartitionCreateOptions::default())?;
        let slots = keyspace.open_partition("slots", PartitionCreateOptions::default())?;
        let blocks = keyspace.open_partition("blocks", PartitionCreateOptions::default())?;
        let txs = keyspace.open_partition("txs", PartitionCreateOptions::default())?;

        Ok(Self {
            keyspace,
            state,
            slots,
            blocks,
            txs,
        })
    }

    pub fn tip(&self) -> Result<Option<Point>> {
        Ok(self.state.get("tip")?.map(|v| {
            minicbor::decode::<Point>(&v).expect("failed to decode tip, consider wiping the db")
        }))
    }

    fn set_tip(&self, tip: &Point) -> Result<()> {
        // actual length is 38 bytes (2 byte cbor header, 8 byte slot, 28 byte hash)
        let mut buffer = [0u8; 64];
        minicbor::encode(tip, buffer.as_mut()).expect("failed to encode tip");
        self.state.insert("tip", buffer)?;
        Ok(())
    }

    pub fn roll_forward(&self, block: &MultiEraBlock) -> Result<()> {
        let mut tx_hashes = vec![];
        for tx in block.txs() {
            let hash: [u8; 32] = *tx.hash();
            tx_hashes.push(hash);
            let tx = bincode::encode_to_vec(Tx::from(tx), CONFIG).expect("failed to encode tx");
            self.txs.insert(hash, tx)?;
        }

        let tx_hashes =
            bincode::encode_to_vec(&tx_hashes, CONFIG).expect("failed to encode tx hashes");
        self.blocks.insert(*block.hash(), tx_hashes)?;

        let slot = bincode::encode_to_vec(block.slot(), CONFIG).expect("failed to encode slot");
        self.slots.insert(slot, *block.hash())?;

        self.set_tip(&Point::Specific(block.slot(), (*block.hash()).to_vec()))?;

        Ok(())
    }

    pub fn roll_backward(&self, point: &Point) -> Result<()> {
        let slot = match point {
            Point::Origin => 0,
            Point::Specific(slot, _) => *slot + 1,
        };
        let slot = bincode::encode_to_vec(slot, CONFIG).expect("failed to encode slot");

        let blocks = self.slots.range(slot..);
        for (slot, block) in blocks.flatten() {
            if let Some(txs) = self.blocks.get(&block)? {
                let (tx_hashes, _): (Vec<[u8; 32]>, usize) =
                    bincode::decode_from_slice(&txs, CONFIG).expect("failed to decode txs");
                for tx_hash in tx_hashes.iter() {
                    self.txs.remove(tx_hash)?;
                }
            }

            self.slots.remove(slot)?;
            self.blocks.remove(block)?;
        }

        self.set_tip(point)?;

        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)
    }
}
