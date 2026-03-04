#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "error: scripts/build-macos.sh must be run on macOS." >&2
  echo "hint: use GitHub Actions Release workflow to build macOS binaries from Linux/Windows." >&2
  exit 1
fi

detect_default_target() {
  local arch
  arch="$(uname -m)"
  case "${arch}" in
    arm64|aarch64) echo "aarch64-apple-darwin" ;;
    x86_64) echo "x86_64-apple-darwin" ;;
    *)
      echo "error: unsupported macOS architecture: ${arch}" >&2
      exit 1
      ;;
  esac
}

TARGET="${1:-$(detect_default_target)}"
PROFILE_FLAG="${2:---release}"

rustup target add "${TARGET}" >/dev/null

cargo build ${PROFILE_FLAG} \
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
echo "  target/${TARGET}/${PROFILE_DIR}/rl-toolkit"
echo "  target/${TARGET}/${PROFILE_DIR}/rl-replay-harvester"
echo "  target/${TARGET}/${PROFILE_DIR}/rl-replay2json"
