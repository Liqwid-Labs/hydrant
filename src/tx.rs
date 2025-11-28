use std::ops::Deref;

use bincode::{Decode, Encode};
use pallas::ledger::primitives::conway::PseudoDatumOption;
use pallas::ledger::traverse::{
    ComputeHash, MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx,
};

// Block

pub type BlockHash = Hash<32>;

#[derive(Clone, Debug, Encode, Decode)]
pub struct Block {
    pub hash: BlockHash,
    pub number: u64,
    pub slot: u64,
    pub txs: Vec<TxHash>,
    pub datums: Vec<DatumHash>,
}

impl Block {
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

// Tx

pub type TxHash = Hash<32>;

#[derive(Clone, Debug, Encode, Decode)]
pub struct Tx {
    pub hash: TxHash,
    pub inputs: Vec<TxOutputPointer>,
    pub outputs: Vec<TxOutput>,

    pub collateral: Vec<TxOutputPointer>,
    pub reference_inputs: Vec<TxOutputPointer>,
    // pub scripts: Option<Vec<Script>>,
    pub mints: Vec<Mint>,
    pub valid: bool,
}

impl Tx {
    pub fn parse(tx: &MultiEraTx) -> (Self, Vec<(DatumHash, Datum)>) {
        let inputs = tx.inputs_sorted_set().into_iter().map(Into::into).collect();
        let (outputs, datums): (Vec<TxOutput>, Vec<Option<(DatumHash, Datum)>>) =
            tx.outputs().into_iter().map(|x| TxOutput::parse(x)).unzip();
        let collateral = tx.collateral().into_iter().map(Into::into).collect();
        let reference_inputs = tx.reference_inputs().into_iter().map(Into::into).collect();
        let mints = Mint::from_assets(tx.mints_sorted_set());

        (
            Self {
                hash: tx.hash().into(),
                inputs,
                outputs,
                collateral,
                reference_inputs,
                mints,
                valid: tx.is_valid(),
            },
            datums.into_iter().flatten().collect(),
        )
    }
}

// Tx Output

pub type Datum = Vec<u8>;
pub type DatumHash = Hash<32>;
pub type Address = Vec<u8>;

#[derive(Clone, Debug, Encode, Decode)]
pub struct TxOutput {
    pub address: Address,
    pub lovelace: u64,
    pub assets: Vec<Asset>,
    pub datum_hash: Option<DatumHash>,
    // TODO: script ref, inline datum
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

// Unspent Tx Output
#[derive(Clone, Debug, Encode, Decode)]
pub struct UnspentTxOutput {
    /// Hash of the transaction
    pub hash: TxHash,
    /// Index of the output in the transaction
    pub index: u64,
    pub address: Vec<u8>,
    pub lovelace: u64,
    pub assets: Vec<Asset>,
    pub datum_hash: Option<DatumHash>,
}
impl UnspentTxOutput {
    pub fn new(pointer: &TxOutputPointer, output: &TxOutput) -> Self {
        let pointer = pointer.clone();
        let output = output.clone();
        Self {
            hash: pointer.hash,
            index: pointer.index,
            address: output.address,
            lovelace: output.lovelace,
            assets: output.assets,
            datum_hash: output.datum_hash,
        }
    }
}

// Tx Pointer (input)

#[derive(Clone, Debug, Encode, Decode)]
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
}
impl From<MultiEraInput<'_>> for TxOutputPointer {
    fn from(input: MultiEraInput) -> Self {
        let hash = input.hash().into();
        let index = input.index();
        Self { hash, index }
    }
}

// Hash

#[derive(Clone, Debug, Encode, Decode)]
pub struct Hash<const BYTES: usize>([u8; BYTES]);
impl<const BYTES: usize> From<[u8; BYTES]> for Hash<BYTES> {
    fn from(bytes: [u8; BYTES]) -> Self {
        Self(bytes)
    }
}
impl<const BYTES: usize> From<pallas::ledger::primitives::Hash<BYTES>> for Hash<BYTES> {
    fn from(hash: pallas::crypto::hash::Hash<BYTES>) -> Self {
        Self(*hash)
    }
}
impl<const BYTES: usize> From<&pallas::ledger::primitives::Hash<BYTES>> for Hash<BYTES> {
    fn from(hash: &pallas::crypto::hash::Hash<BYTES>) -> Self {
        Self(**hash)
    }
}
impl<const BYTES: usize> Deref for Hash<BYTES> {
    type Target = [u8; BYTES];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Assets

pub type Policy = Hash<28>;
pub type AssetName = Vec<u8>;

#[derive(Clone, Debug, Encode, Decode)]
pub struct Mint {
    pub policy: Policy,
    pub name: AssetName,
    pub quantity: i64,
}

impl Mint {
    fn from_assets(assets: Vec<MultiEraPolicyAssets>) -> Vec<Self> {
        assets
            .iter()
            .flat_map(|a| a.assets())
            .map(|a| Mint {
                policy: a.policy().into(),
                name: a.name().to_vec(),
                quantity: a
                    .mint_coin()
                    .expect("missing mint amount in asset. is this an output asset?"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Asset {
    pub policy: Policy,
    pub name: AssetName,
    pub quantity: u64,
}

impl Asset {
    fn from_assets(assets: Vec<MultiEraPolicyAssets>) -> Vec<Self> {
        assets
            .iter()
            .flat_map(|a| a.assets())
            .map(|a| Asset {
                policy: a.policy().into(),
                name: a.name().to_vec(),
                quantity: a
                    .output_coin()
                    .expect("missing output amount in asset. is this a mint asset?"),
            })
            .collect()
    }
}
