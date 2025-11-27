<p align="center">
  <h2 align="center">Hydrant</h2>
</p>

<p align="center">
	Embeddable & configurable chain-indexer for Cardano
</p>

**Hydrant** is an embeddable, configurable and extremely fast chain-indexer for Cardano, designed to distill the chain to only the data needed for the Liqwid protocol.

## Development

This project depends on a local cardano-node to use the Node-to-Client (N2C) protocol for chain-sync. If `nix develop` takes a long time to build, ensure that you have accepted the flake config such that the IOG cache will be used for downloading `cardano-node`.

Currently, the cardano-node runs against mainnet (please feel free to PR supporting preview) which uses ~250GBs of disk space and ~12GB of RAM.

```bash
nix develop --accept-flake-config
start-node # downloads configuration and starts the node

# in another shell
nix develop --accept-flake-config
cargo run
```
