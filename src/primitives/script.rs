use pallas::ledger::primitives::PlutusScript;
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

pub type ScriptHash = Hash<28>;

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub enum Script {
    V1(Vec<u8>),
    V2(Vec<u8>),
    V3(Vec<u8>),
}

impl<const VERSION: usize> From<&PlutusScript<VERSION>> for Script {
    fn from(script: &PlutusScript<VERSION>) -> Self {
        let bytes = script.0.to_vec();
        match VERSION {
            1 => Script::V1(bytes),
            2 => Script::V2(bytes),
            3 => Script::V3(bytes),
            _ => panic!("unsupported script version"),
        }
    }
}

pub type AddressKeyHash = Hash<28>;

/// This is a bit mind-numbing because of the recursive types
/// https://github.com/rkyv/rkyv/blob/8ec288833989b126f7442cf9c4e94b481ae8e2eb/rkyv/examples/json_like_schema.rs#L5
#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum NativeScript {
    ScriptPubkey(AddressKeyHash),
    ScriptAll(#[rkyv(omit_bounds)] Vec<NativeScript>),
    ScriptAny(#[rkyv(omit_bounds)] Vec<NativeScript>),
    ScriptNOfK(u32, #[rkyv(omit_bounds)] Vec<NativeScript>),
    InvalidBefore(u64),
    InvalidHereafter(u64),
}
impl From<&pallas::ledger::primitives::conway::NativeScript> for NativeScript {
    fn from(script: &pallas::ledger::primitives::conway::NativeScript) -> Self {
        match script {
            pallas::ledger::primitives::conway::NativeScript::ScriptPubkey(hash) => {
                Self::ScriptPubkey(hash.into())
            }
            pallas::ledger::primitives::conway::NativeScript::ScriptAll(scripts) => {
                Self::ScriptAll(scripts.iter().map(Into::into).collect())
            }
            pallas::ledger::primitives::conway::NativeScript::ScriptAny(scripts) => {
                Self::ScriptAny(scripts.iter().map(Into::into).collect())
            }
            pallas::ledger::primitives::conway::NativeScript::ScriptNOfK(n, scripts) => {
                Self::ScriptNOfK(*n, scripts.iter().map(Into::into).collect())
            }
            pallas::ledger::primitives::conway::NativeScript::InvalidBefore(slot) => {
                Self::InvalidBefore(*slot)
            }
            pallas::ledger::primitives::conway::NativeScript::InvalidHereafter(slot) => {
                Self::InvalidHereafter(*slot)
            }
        }
    }
}
