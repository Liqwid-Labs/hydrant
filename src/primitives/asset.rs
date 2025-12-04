use pallas::ledger::traverse::MultiEraPolicyAssets;
use rkyv::{Archive, Deserialize, Serialize};

use super::*;

pub type Policy = Hash<28>;
pub type AssetName = Vec<u8>;

#[derive(Clone, Debug, Archive, Deserialize, Serialize, PartialEq)]
#[rkyv(compare(PartialEq))]
pub struct AssetId {
    pub policy: Policy,
    pub name: Option<AssetName>,
}

impl AssetId {
    pub fn new(policy: Policy, name: Option<AssetName>) -> Self {
        Self { policy, name }
    }
}

impl PartialEq<Asset> for AssetId {
    fn eq(&self, other: &Asset) -> bool {
        self.policy == other.policy && self.name.as_ref().is_none_or(|name| name == &other.name)
    }
}

impl From<Asset> for AssetId {
    fn from(asset: Asset) -> Self {
        Self {
            policy: asset.policy,
            name: Some(asset.name),
        }
    }
}
impl From<&Asset> for AssetId {
    fn from(asset: &Asset) -> Self {
        Self {
            policy: asset.policy.clone(),
            name: Some(asset.name.clone()),
        }
    }
}

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct Mint {
    pub policy: Policy,
    pub name: AssetName,
    pub quantity: i64,
}

impl Mint {
    pub fn from_assets(assets: Vec<MultiEraPolicyAssets>) -> Vec<Self> {
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

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
#[rkyv(compare(PartialEq))]
pub struct Asset {
    pub policy: Policy,
    pub name: AssetName,
    pub quantity: u64,
}

impl Asset {
    pub fn from_assets(assets: Vec<MultiEraPolicyAssets>) -> Vec<Self> {
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
