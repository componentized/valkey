#!/usr/bin/env bash

# Setup script run once for new devcontainers to init the environment.

set -euo pipefail

# valkey-cli, to inspect the Valkey service the dev container runs with
sudo apt-get update && sudo apt-get install -y valkey-tools

curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

cargo check

echo "export \"PATH=$(make -s tools-path):${PATH}\"" >> ~/.bashrc
make tools
