use std::collections::HashMap;

use pallas::ledger::traverse::MultiEraTx;
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

pub type TxHash = Hash<32>;

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct Tx {
    pub hash: TxHash,
    pub inputs: Vec<TxOutputPointer>,
    pub outputs: Vec<TxOutput>,

    pub collateral: Vec<TxOutputPointer>,
    pub collateral_return: Option<TxOutput>,
    /// NOTE: It is possible for this to include duplicates
    /// https://github.com/input-output-hk/cardano-ledger/commit/a342b74f5db3d3a75eae3e2abe358a169701b1e7
    pub reference_inputs: Vec<TxOutputPointer>,
    pub mints: Vec<Mint>,

    pub scripts: Vec<Script>,
    pub native_scripts: Vec<NativeScript>,

    pub valid: bool,
}

impl Tx {
    pub fn parse(tx: &MultiEraTx) -> (Self, HashMap<DatumHash, Datum>) {
        let inputs = tx.inputs_sorted_set().into_iter().map(Into::into).collect();
        let (outputs, mut datums): (Vec<TxOutput>, Vec<Option<(DatumHash, Datum)>>) =
            tx.outputs().into_iter().map(|x| TxOutput::parse(x)).unzip();

        let collateral = tx.collateral().into_iter().map(Into::into).collect();
        let collateral_return = tx.collateral_return().map(|cr| {
            let (collateral_return, datum) = TxOutput::parse(cr);
            if !tx.is_valid() {
                datums.push(datum);
            }
            collateral_return
        });

        let reference_inputs = tx.reference_inputs().into_iter().map(Into::into).collect();
        let mints = Mint::from_assets(tx.mints_sorted_set());

        let scripts = tx
            .plutus_v1_scripts()
            .iter()
            .map(Into::into)
            .chain(tx.plutus_v2_scripts().iter().map(Into::into))
            .chain(tx.plutus_v3_scripts().iter().map(Into::into))
            .collect();
        let native_scripts = tx.aux_native_scripts().iter().map(Into::into).collect();

        (
            Self {
                hash: tx.hash().into(),
                valid: tx.is_valid(),

                inputs,
                outputs,
                collateral,
                collateral_return,
                reference_inputs,
                mints,
                scripts,
                native_scripts,
            },
            datums.into_iter().flatten().collect(),
        )
    }

    pub fn spent(&self) -> impl Iterator<Item = &TxOutputPointer> + '_ {
        if self.valid {
            self.inputs.iter()
        } else {
            self.collateral.iter()
        }
    }

    pub fn unspent(&self) -> impl Iterator<Item = &TxOutput> + '_ {
        self.outputs.iter().filter(|_| self.valid)
    }
}
