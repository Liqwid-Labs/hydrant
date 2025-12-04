use std::collections::HashMap;

use pallas::ledger::traverse::MultiEraBlock;
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

pub type BlockHash = Hash<32>;

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
pub struct Block {
    // TODO: epoch? requires genesis values
    pub era: Era,
    pub hash: BlockHash,
    pub number: u64,
    pub slot: u64,
    pub size: usize,

    pub txs: Vec<Tx>,
    pub datums: HashMap<DatumHash, Datum>,
}

impl From<&MultiEraBlock<'_>> for Block {
    fn from(block: &MultiEraBlock) -> Self {
        let mut txs = Vec::with_capacity(block.txs().len());
        let mut datums = HashMap::new();
        for raw_tx in block.txs().iter() {
            let (tx, tx_datums) = Tx::parse(raw_tx);
            datums.extend(tx_datums);
            txs.push(tx);
        }

        Self {
            era: block.era().into(),
            hash: block.hash().into(),
            number: block.number(),
            slot: block.slot(),
            size: block.size(),

            txs,
            datums,
        }
    }
}
