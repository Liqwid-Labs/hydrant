use std::ops::Deref;

use bincode::{Decode, Encode};
use pallas::ledger::traverse::{MultiEraInput, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Tx {
    pub hash: Hash<32>,
    pub inputs: Vec<TxOutputPointer>,
    pub outputs: Vec<TxOutput>,

    pub collateral: Vec<TxOutputPointer>,
    pub reference_inputs: Vec<TxOutputPointer>,
    // pub scripts: Option<Vec<Script>>,
    pub mints: Vec<Mint>,
    pub valid: bool,
}

impl From<&MultiEraTx<'_>> for Tx {
    fn from(tx: &MultiEraTx) -> Self {
        let inputs = tx.inputs().into_iter().map(Into::into).collect();
        let outputs = tx.outputs().into_iter().map(Into::into).collect();
        let collateral = tx.collateral().into_iter().map(Into::into).collect();
        let reference_inputs = tx.reference_inputs().into_iter().map(Into::into).collect();
        let mints = Mint::from_assets(tx.mints());

        Self {
            hash: tx.hash().into(),
            inputs,
            outputs,
            collateral,
            reference_inputs,
            mints,
            valid: tx.is_valid(),
        }
    }
}

// Tx Output

pub type DatumHash = Hash<32>;

#[derive(Clone, Debug, Encode, Decode)]
pub struct TxOutput {
    address: Vec<u8>,
    lovelace: u64,
    assets: Vec<Asset>,
    datum_hash: Option<DatumHash>,
    // TODO: script ref, inline datum
}

impl From<MultiEraOutput<'_>> for TxOutput {
    fn from(output: MultiEraOutput) -> Self {
        let address = output.address().expect("failed to decode address").to_vec();
        let lovelace = output.value().coin();
        let assets = Asset::from_assets(output.value().assets());
        Self {
            address,
            lovelace,
            assets,
            datum_hash: None, // TODO:
        }
    }
}

// Tx Pointer (input)

#[derive(Clone, Debug, Encode, Decode)]
pub struct TxOutputPointer(Hash<32>, u64);

impl From<MultiEraInput<'_>> for TxOutputPointer {
    fn from(input: MultiEraInput) -> Self {
        let hash = input.hash().into();
        let index = input.index();
        Self(hash, index)
    }
}

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

#[derive(Clone, Debug, Encode, Decode)]
pub struct Mint {
    pub policy: Policy,
    pub name: Vec<u8>,
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
    pub name: Vec<u8>,
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
