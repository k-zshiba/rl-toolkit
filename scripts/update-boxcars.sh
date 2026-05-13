#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "updating boxcars to the latest compatible crates.io version"
cargo update --manifest-path "${ROOT_DIR}/Cargo.toml" -p boxcars
