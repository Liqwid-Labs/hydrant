use anyhow::{Context, Result};
use heed::{Database, DatabaseFlags, RwTxn};

use crate::db::{Db, Env, RkyvCodec};
use crate::indexer::Indexer;
use crate::primitives::{Address, AssetId, Tx, TxOutput, TxOutputPointer};

pub struct UtxoIndexerBuilder {
    addresses: Option<Vec<Address>>,
    assets: Option<Vec<AssetId>>,
}

impl UtxoIndexerBuilder {
    pub fn new() -> Self {
        Self {
            addresses: None,
            assets: None,
        }
    }

    pub fn address(mut self, addresses: Address) -> Self {
        self.addresses = Some(
            self.addresses
                .unwrap_or_default()
                .into_iter()
                .chain(vec![addresses])
                .collect(),
        );
        self
    }

    pub fn asset(mut self, assets: AssetId) -> Self {
        self.assets = Some(
            self.assets
                .unwrap_or_default()
                .into_iter()
                .chain(vec![assets])
                .collect(),
        );
        self
    }

    pub fn build(self, env: &Env) -> Result<UtxoIndexer> {
        UtxoIndexer::new(env, self.addresses, self.assets)
    }
}

#[derive(Clone)]
pub struct UtxoIndexer {
    env: Env,
    utxos: Database<RkyvCodec<TxOutputPointer>, RkyvCodec<TxOutput>>,
    by_address: Database<RkyvCodec<Address>, RkyvCodec<TxOutputPointer>>,
    by_asset: Database<RkyvCodec<AssetId>, RkyvCodec<TxOutputPointer>>,
    addresses: Option<Vec<Address>>,
    assets: Option<Vec<AssetId>>,
}

impl UtxoIndexer {
    pub fn new(
        env: &Env,
        addresses: Option<Vec<Address>>,
        assets: Option<Vec<AssetId>>,
    ) -> Result<Self> {
        let env = env.clone();

        let mut wtxn = env.write_txn()?;
        let utxos = env.create_database(&mut wtxn, "utxos")?;
        let by_address =
            env.create_database_with_flags(&mut wtxn, "by_address", DatabaseFlags::DUP_SORT)?;
        let by_asset =
            env.create_database_with_flags(&mut wtxn, "by_asset", DatabaseFlags::DUP_SORT)?;
        wtxn.commit()?;

        Ok(Self {
            env,
            utxos,
            by_address,
            by_asset,
            addresses,
            assets,
        })
    }

    pub fn utxos(&self) -> Result<Vec<(TxOutputPointer, TxOutput)>> {
        let txn = self.env.read_txn()?;
        self.utxos
            .iter(&txn)?
            .map(|res| {
                let pointer = rkyv::deserialize::<TxOutputPointer, rkyv::rancor::Error>(res?.0)?;
                let txo = self.utxos.get(&txn, &pointer)?.context("missing txo")?;
                let txo = rkyv::deserialize::<TxOutput, rkyv::rancor::Error>(txo)?;
                Ok((pointer, txo))
            })
            .collect::<Result<Vec<_>>>()
    }

    fn insert_output(
        &self,
        wtxn: &mut RwTxn,
        pointer: &TxOutputPointer,
        output: &TxOutput,
    ) -> Result<bool> {
        // Filter based on address
        if let Some(addresses) = &self.addresses
            && addresses.contains(&output.address)
        {
            return Ok(false);
        }
        // Filter based on asset
        if let Some(assets) = &self.assets
            && !assets
                .iter()
                .any(|whitelisted_asset| output.assets.iter().any(|a| whitelisted_asset == a))
        {
            return Ok(false);
        }

        self.utxos.put(wtxn, pointer, output)?;
        self.by_address.put(wtxn, &output.address, pointer)?;
        for asset in output.assets.iter() {
            self.by_asset.put(wtxn, &asset.into(), pointer)?;
        }
        Ok(true)
    }

    fn consume_input(&self, wtxn: &mut RwTxn, input: &TxOutputPointer) -> Result<bool> {
        let Some(utxo) = self.utxos.get(wtxn, input)? else {
            return Ok(false);
        };
        let utxo = rkyv::deserialize::<TxOutput, rkyv::rancor::Error>(utxo)?;

        self.utxos.delete(wtxn, input)?;
        self.by_address
            .delete_one_duplicate(wtxn, &utxo.address, input)?;
        for asset in utxo.assets.iter() {
            self.by_asset
                .delete_one_duplicate(wtxn, &asset.into(), input)?;
        }
        Ok(true)
    }
}

impl Indexer for UtxoIndexer {
    fn insert_tx(&self, _: &Db, wtxn: &mut RwTxn, tx: &Tx) -> anyhow::Result<bool> {
        let mut added_some = false;

        // Mark consumed UTxOs as spent
        for input in tx.spent() {
            added_some |= self.consume_input(wtxn, input)?;
        }

        // Add UTxOs
        for (index, output) in tx.unspent().enumerate() {
            let pointer = TxOutputPointer::new(tx.hash.clone(), index);
            added_some |= self.insert_output(wtxn, &pointer, output)?;
        }

        Ok(added_some)
    }

    fn delete_tx(&self, db: &Db, wtxn: &mut RwTxn, tx: &Tx) -> anyhow::Result<()> {
        // Restore consumed UTxOs
        for input in tx.spent() {
            let volatile_tx_output = db
                .get_volatile_tx_output(wtxn, input)?
                .context("missing tx output in volatile db")?;
            self.insert_output(wtxn, input, &volatile_tx_output)?;
        }

        // Remove UTxOs
        for (index, _) in tx.unspent().enumerate() {
            let pointer = TxOutputPointer::new(tx.hash.clone(), index);
            self.consume_input(wtxn, &pointer)?;
        }

        Ok(())
    }

    fn clear(&self, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        self.utxos.clear(wtxn)?;
        self.by_address.clear(wtxn)?;
        self.by_asset.clear(wtxn)?;
        Ok(())
    }
}
