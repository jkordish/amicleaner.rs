# amicleaner.rs

**NOTE: This project is no longer maintained. Use at your own risk.**

Find and terminate old AMIs

## building

### Building on Linux/OSX


```shell
# binary will be ./targett/release/amicleaner
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
Joseph Kordish <joe@kordish.io>
Find and terminate AMIs

USAGE:
    amicleaner [OPTIONS] --manifest <manifest>

OPTIONS:
    -d, --older-than <days>       Anything older than days should be terminated [default: 7]
    -f, --force-delete            Skip confirmation
    -h, --help                    Print help information
    -i, --input <input>           Input string(s) to match against. Can pass comma separated list.
    -k, --keep-previous <keep>    Number of previous to keep
    -m, --manifest <manifest>     Exclude Manifest(s). Can pass comma separated list [default: 22.04]
    -o, --check-orphans           Process orphan EBS volumes/snapshots
    -r, --aws-region <region>     Region(s) to check. Can pass comma separated list. [default: us-west-2]
    -V, --version                 Print version information
```