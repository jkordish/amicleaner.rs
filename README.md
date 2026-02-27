# amicleaner.rs

Conservative EC2 AMI + EBS snapshot cleanup using a mark-and-sweep strategy:

- Mark AMIs referenced by instances, launch templates, and Auto Scaling resources.
- Mark snapshots referenced by AMIs that will remain registered.
- Deregister only old, unreferenced, unprotected AMIs.
- Delete only old, unreferenced, unprotected snapshots.
- Optionally delete AWS Backup recovery points for backup-managed snapshots.
- Default behavior is **plan-only**; pass `--execute` to apply changes.

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
Conservative EC2 AMI + EBS snapshot mark-and-sweep cleanup

Usage: amicleaner [OPTIONS]

Options:
      --execute                                Execute deletions. Without this flag, the tool only prints a plan
      --dry-run                                Use AWS API dry-run checks when executing
      --execute-backup-recovery-point-deletes  Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots
      --backup-delete-tag <BACKUP_DELETE_TAG>  Tag key required for AWS Backup recovery point deletion unless allow-untagged flag set [default: backup-cleanup]
      --backup-delete-tag-value <VALUE>        Expected value for backup delete tag; empty string = any value [default: true]
      --allow-untagged-backup-recovery-points  Permit deletion of untagged AWS Backup recovery points
      --regions <REGIONS>                      Comma-separated list of regions. If omitted, all enabled account regions are scanned
      --min-age-days <MIN_AGE_DAYS>            Keep resources newer than this age in days [default: 14]
      --ami-recent-launch-days <DAYS>          Keep AMIs launched within the last N days [default: 90]
      --protect-tag <PROTECT_TAG>              Tag key that protects resources from deletion [default: DoNotDelete]
      --protect-tag-value <VALUE>              Expected value for protect tag; empty string = key presence [default: true]
      --delete-tag <DELETE_TAG>                Tag key required for deletion unless --allow-untagged [default: cleanup]
      --delete-tag-value <VALUE>               Expected value for delete tag; empty string = any value [default: true]
      --allow-untagged                         Permit deletion of untagged resources
      --keep-latest-per-family <COUNT>         Keep newest N AMIs per family [default: 0]
      --max-retries <MAX_RETRIES>              Max retry attempts for mutating API operations [default: 3]
      --retry-delay-ms <RETRY_DELAY_MS>        Base delay in milliseconds between retries [default: 1000]
  -h, --help                  Print help
  -V, --version               Print version
```
