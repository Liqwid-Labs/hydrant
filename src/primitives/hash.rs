use std::ops::Deref;

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[rkyv(compare(PartialEq))]
pub struct Hash<const BYTES: usize>(pub [u8; BYTES]);

impl<const BYTES: usize> Deref for Hash<BYTES> {
    type Target = [u8; BYTES];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<const BYTES: usize> std::fmt::Display for Hash<BYTES> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.deref()).fmt(f)
    }
}
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

impl<'a, C, const BYTES: usize> minicbor::Decode<'a, C> for Hash<BYTES> {
    fn decode(
        d: &mut minicbor::Decoder<'a>,
        _ctx: &mut C,
    ) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        if bytes.len() == BYTES {
            let mut hash = [0; BYTES];
            hash.copy_from_slice(bytes);
            Ok(Self(hash))
        } else {
            // TODO: minicbor does not allow for expecting a specific size byte array
            //       (in fact cbor is not good at it at all anyway)
            Err(minicbor::decode::Error::message("Invalid hash size"))
        }
    }
}

impl<const BYTES: usize> Deref for ArchivedHash<BYTES> {
    type Target = [u8; BYTES];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<const BYTES: usize> PartialEq for ArchivedHash<BYTES> {
    fn eq(&self, other: &Self) -> bool {
        &*self == &*other
    }
}
impl<const BYTES: usize> Eq for ArchivedHash<BYTES> {}
impl<const BYTES: usize> std::hash::Hash for ArchivedHash<BYTES> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (&self.0).hash(state);
    }
}
