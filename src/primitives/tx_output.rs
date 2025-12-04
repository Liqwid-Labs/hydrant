use std::ops::RangeInclusive;

use pallas::ledger::primitives::conway::PseudoDatumOption;
use pallas::ledger::traverse::{ComputeHash, MultiEraInput, MultiEraOutput};
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

pub type Datum = Vec<u8>;
pub type DatumHash = Hash<32>;
pub type Address = Vec<u8>;

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct TxOutput {
    pub address: Address,
    pub lovelace: u64,
    pub assets: Vec<Asset>,
    pub datum_hash: Option<DatumHash>,
    // TODO: script ref
}

impl TxOutput {
    pub fn parse(output: MultiEraOutput) -> (Self, Option<(DatumHash, Datum)>) {
        let address = output.address().expect("failed to decode address").to_vec();
        let lovelace = output.value().coin();
        let assets = Asset::from_assets(output.value().assets());
        let datum_hash = output.datum().map(|d| {
            match d {
                PseudoDatumOption::Hash(x) => x,
                PseudoDatumOption::Data(data) => data.compute_hash(),
            }
            .into()
        });
        let datum = output.datum().and_then(|d| match d {
            PseudoDatumOption::Hash(_) => None,
            PseudoDatumOption::Data(data) => {
                Some((data.compute_hash().into(), data.raw_cbor().to_vec()))
            }
        });

        (
            Self {
                address,
                lovelace,
                assets,
                datum_hash,
            },
            datum,
        )
    }
}

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct TxOutputPointer {
    pub hash: TxHash,
    pub index: u64,
}
impl TxOutputPointer {
    pub fn new(hash: Hash<32>, index: usize) -> Self {
        Self {
            hash,
            index: index as u64,
        }
    }

    pub fn range(hash: &TxHash) -> RangeInclusive<Self> {
        let start = Self {
            hash: hash.clone(),
            index: 0,
        };
        let end = Self {
            hash: hash.clone(),
            index: u64::MAX,
        };
        start..=end
    }
}
impl From<MultiEraInput<'_>> for TxOutputPointer {
    fn from(input: MultiEraInput) -> Self {
        let hash = input.hash().into();
        let index = input.index();
        Self { hash, index }
    }
}
