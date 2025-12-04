use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Archive, Deserialize, Serialize)]
#[non_exhaustive]
pub enum Era {
    Byron,
    Shelley,
    Allegra, // time-locks
    Mary,    // multi-assets
    Alonzo,  // smart-contracts
    Babbage, // CIP-31/32/33
    Conway,  // governance CIP-1694
}

impl From<pallas::ledger::traverse::Era> for Era {
    fn from(era: pallas::ledger::traverse::Era) -> Self {
        match era {
            pallas::ledger::traverse::Era::Byron => Self::Byron,
            pallas::ledger::traverse::Era::Shelley => Self::Shelley,
            pallas::ledger::traverse::Era::Allegra => Self::Allegra,
            pallas::ledger::traverse::Era::Mary => Self::Mary,
            pallas::ledger::traverse::Era::Alonzo => Self::Alonzo,
            pallas::ledger::traverse::Era::Babbage => Self::Babbage,
            pallas::ledger::traverse::Era::Conway => Self::Conway,
            _ => panic!("unsupported era"), // TODO:
        }
    }
}
