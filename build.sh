#!/usr/bin/env bash
# Cross-compile a static x86_64 Linux (musl) binary with Docker, then zip it.
#
# The image tag follows rust-toolchain.toml so the container builds with the pinned toolchain.
# Override with MUSLRUST_IMAGE=... if that tag isn't published yet.
set -euo pipefail

cd "$(dirname "$0")"

toolchain=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)
image=${MUSLRUST_IMAGE:-clux/muslrust:${toolchain}-stable}
target=x86_64-unknown-linux-musl
binary=target/${target}/release/amicleaner

# RUSTUP_TOOLCHAIN makes rustup use the image's toolchain instead of trying to install the one
# named in rust-toolchain.toml (which wouldn't have the musl target).
docker run --rm \
    -e RUSTUP_TOOLCHAIN=stable \
    -v "$(pwd)":/volume \
    -w /volume \
    "$image" \
    cargo build --release --target "$target"

# Stripping happens in the release profile (strip = "symbols"), so no host strip binary is needed.
ls -lh "$binary"
file "$binary"
zip -j amicleaner.zip "$binary"
