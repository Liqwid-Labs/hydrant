<p align="center">
  <h2 align="center">Hydrant</h2>
</p>

<p align="center">
	Embeddable & configurable chain-indexer for Cardano
</p>

**Hydrant** is an embeddable, configurable and extremely fast chain-indexer for Cardano, designed to distill the chain to only the data needed for the Liqwid protocol.

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
