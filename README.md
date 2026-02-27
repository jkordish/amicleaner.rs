# amicleaner.rs

**NOTE: This project is no longer maintained. Use at your own risk.**

Find and terminate old AMIs

## building

This project targets:

- Rust edition `2024`
- MSRV `1.93.1`

### Building on Linux/OSX


```shell
# binary will be ./target/release/amicleaner
cargo build --release
# might want to strip it since build process doesn't do that
strip ./target/release/amicleaner
```

### Cross compiling for Linux on OSX

Must have Docker installed and running. Checkout the [build.sh](build.sh) for what happens.

```shell
./build.sh
```

### help

```shell
$ amicleaner -h
Find and terminate AMIs

Usage: amicleaner [OPTIONS]

Options:
  -i, --input <input>         Input string(s) to match against. Can pass comma separated list.
  -d, --older-than <days>     Anything older than days should be terminated [default: 7]
  -k, --keep-previous <keep>  Number of previous to keep
  -r, --aws-region <region>   Region(s) to check. Can pass comma separated list. [default: us-west-2]
  -f, --force-delete          Skip confirmation
  -o, --check-orphans         Process orphan EBS volumes/snapshots
  -m, --manifest <manifest>   Exclude Manifest(s). Can pass comma separated list [default: 22.04]
  -h, --help                  Print help
  -V, --version               Print version
```
