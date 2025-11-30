{
  nixConfig = {
    extra-substituters = [ "https://cache.iog.io" ];
    extra-trusted-public-keys = [ "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" ];
  };

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    cardano-node.url = "github:intersectmbo/cardano-node/10.5.3";
  };

  outputs = { self, nixpkgs, crane, flake-utils, cardano-node, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        craneLib = crane.mkLib pkgs;
        commonArgs = {
          src = ./.;
          strictDeps = true;
        };
        hydrant = craneLib.buildPackage (
          commonArgs
          // {
            pname = "hydrant";
            cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          }
        );
      in
      {
        checks.hydrant = hydrant;
        packages.default = hydrant;

        devShells.default = let
          node = cardano-node.outputs.packages.${system}.default;
          githubRawUrl = "https://raw.githubusercontent.com/IntersectMBO/cardano-node/refs/tags/10.5.3/configuration/cardano/";
          configFiles = [
            "mainnet-topology.json" "mainnet-config.yaml" "mainnet-checkpoints.json" "mainnet-alonzo-genesis.json"
            "mainnet-byron-genesis.json" "mainnet-conway-genesis.json" "mainnet-shelley-genesis.json"
          ];
          startNode = pkgs.writeShellScriptBin "start-node" ''
            set -euo pipefail
            mkdir -p db/node
            ${pkgs.lib.concatMapStringsSep "\n" (
                filename: "${pkgs.curl}/bin/curl -L -s -o db/node/${filename} ${githubRawUrl}/${filename}"
              ) configFiles
            }
            ${node}/bin/cardano-node run \
              --database-path db/node/data \
              --config db/node/mainnet-config.yaml \
              --topology db/node/mainnet-topology.json \
              --host-addr 0.0.0.0 \
              --port 3001
          '';
          clearNode = pkgs.writeShellScriptBin "clear-node" ''
            set -euo pipefail
            rm -rf db/node
          '';
        in craneLib.devShell {
          checks = self.checks.${system};
          packages = with pkgs; [
            rust-analyzer
            cargo-watch
            curl
            node
            startNode
            clearNode
          ];
        };
      }
    );
}
