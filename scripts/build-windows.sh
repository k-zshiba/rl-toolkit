#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-x86_64-pc-windows-gnu}"
PROFILE_FLAG="${2:---release}"

if ! command -v cross >/dev/null 2>&1; then
  echo "error: cross is not installed" >&2
  echo "install with: cargo install cross --git https://github.com/cross-rs/cross --locked" >&2
  exit 1
fi

cross build ${PROFILE_FLAG} \
  --target "${TARGET}" \
  -p rl-common \
  -p rl-replay-harvester \
  -p rl-replay2json

if [[ "${PROFILE_FLAG}" == "--release" ]]; then
  PROFILE_DIR="release"
else
  PROFILE_DIR="debug"
fi

echo "built artifacts:"
echo "  target/${TARGET}/${PROFILE_DIR}/rl-common.exe"
echo "  target/${TARGET}/${PROFILE_DIR}/rl-replay-harvester.exe"
echo "  target/${TARGET}/${PROFILE_DIR}/rl-replay2json.exe"
