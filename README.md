<p align="center">
  <h2 align="center">Hydrant</h2>
</p>

<p align="center">
	Embeddable & configurable chain-indexer for Cardano
</p>

**Hydrant** is an embeddable, extensible and fast chain-indexer for Cardano, designed to distill the chain to only the data needed for your usecase.

- High-level API for defining your own indexer
- Rkyv + LMDB for zero-copy memory-mapped storage
- Atomic block writes
- Node-to-Node (N2N) protocol, no Cardano Node required
- Fast syncing (~30 minutes with localhost node)

## Usage

Hydrant handles finding the intersection of its DB to Cardano Node and periodically requests roll forward and roll backward events from the chain-sync protocol (with block fetch batching). The blocks are decoded and stored in a volatile block DB via LMDB. The consumer defines a struct implementing the `Indexer` trait, which receives a handle to the write transaction, to enable atomic writes across both the volatile DB and consumer's DB.

```rust
pub trait Indexer {
    fn insert_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<bool>;
    fn delete_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<()>;

    fn insert_datum(&self, wtxn: &mut heed::RwTxn, datum_hash: &DatumHash, datum: &Datum) -> Result<bool>;
    fn delete_datum(&self, wtxn: &mut heed::RwTxn, datum_hash: &DatumHash) -> Result<()>;

    fn clear(&self, wtxn: &mut heed::RwTxn) -> Result<()>;
}
```

Returning `true` from `insert_tx`/`insert_datum` tells the volatile DB to track the tx/datum, sending a `delete_*` event when rolled back.

## Development

This project uses the Node-to-Node (N2N) protocol for chainsync and blockfetch, such that you don't need a local node, just a relay. But for the purposes of development, we use a local node to speed-up sync times. If `nix develop` takes a long time to build, ensure that you have accepted the flake config such that the IOG cache will be used for downloading `cardano-node`.

Currently, the cardano-node runs against mainnet (please feel free to PR supporting preview) which uses ~250GBs of disk space and ~12GB of RAM.

```bash
nix develop --accept-flake-config
start-node # downloads configuration and starts the node

# in another shell
nix develop --accept-flake-config
cargo run
```
