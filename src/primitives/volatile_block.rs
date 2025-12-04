use pallas::ledger::traverse::{
      MultiEraBlock,
};
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct VolatileBlock {
    pub hash: BlockHash,
    pub number: u64,
    pub slot: u64,
    pub txs: Vec<TxHash>,
    pub datums: Vec<DatumHash>,
}

impl VolatileBlock {
    pub fn parse(block: &MultiEraBlock, txs: Vec<Hash<32>>, datums: Vec<Hash<32>>) -> Self {
        Self {
            hash: block.hash().into(),
            number: block.number(),
            slot: block.slot(),
            txs,
            datums,
        }
    }
}
