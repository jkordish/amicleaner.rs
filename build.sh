#!/bin/bash

###
# using local musl cross you can do the following
###
    # brew install FiloSottile/musl-cross/musl-cross
    # brew tap SergioBenitez/osxct
    # brew install x86_64-unknown-linux-gnu

#docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:nightly cargo build --release
docker run --rm -it -v "$(pwd)":/volume clux/muslrust:nightly cargo build --release

sleep 3

/usr/local/bin/x86_64-unknown-linux-gnu-strip ./target/x86_64-unknown-linux-musl/release/amicleaner

ls -lh ./target/x86_64-unknown-linux-musl/release/amicleaner
file ./target/x86_64-unknown-linux-musl/release/amicleaner
chmod +x ./target/x86_64-unknown-linux-musl/release/amicleaner

zip -j amicleaner target/x86_64-unknown-linux-musl/release/amicleaner