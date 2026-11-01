# amicleaner.rs

Conservative EC2 AMI + EBS snapshot cleanup using a mark-and-sweep strategy:

- Mark AMIs referenced by instances, launch templates, Auto Scaling groups and launch configurations, EC2 Fleets, Spot Fleets and open Spot requests (including `resolve:ssm:` image parameters).
- Mark snapshots referenced by AMIs that will remain registered.
- Deregister only old, unreferenced, unprotected, tagged AMIs that aren't shared with other accounts.
- Delete only old, unreferenced, unprotected, tagged snapshots — including the snapshots of AMIs it deregisters.
- Optionally delete AWS Backup recovery points for backup-managed snapshots.
- Default behavior is **plan-only**; pass `--execute` to apply changes.

## safety model

Anything the tool can't determine blocks the deletions it affects:

- An AMI reference that can't be resolved (for example an SSM parameter that doesn't hold an AMI id) cancels all AMI deregistrations in that region. Snapshot cleanup continues.
- If the AWS Backup inventory can't be read, or a snapshot looks AWS Backup-created but has no matching recovery point, those snapshot deletions are cancelled.
- AMIs with launch permissions for other accounts (or a failed launch-permission lookup), AMIs created by AWS Backup, and AMIs with missing or unparseable dates are kept.
- With `--execute`, every planned item is re-checked against live state right before it is deleted.

Tags:

- The protect tag (`DoNotDelete` by default) protects on key presence alone, whatever its value, and the key matches case-insensitively. Set `--protect-tag-value` to require a specific value.
- The delete tag (`cleanup=true` by default) is required unless `--allow-untagged`.
- Snapshots freed by deregistering their AMI inherit the AMI's delete authorization, but their own protect tag still wins.
- For AWS Backup, an EBS recovery point is the snapshot itself, so `--backup-delete-tag` is read from the snapshot's tags.

`--dry-run` (with `--execute`) sends EC2 calls with `DryRun=true`; the summary counts requests that passed the check. AWS Backup has no dry-run mode, so recovery points are only checked, never deleted, in a dry run.

### exit codes

| code | meaning |
|---|---|
| 0 | success |
| 1 | errors (a region failed, or a deletion failed) |
| 2 | invalid arguments |
| 3 | no errors, but safety checks cancelled planned deletions (see `blocked_by_safety_checks` in the plan output) |

### IAM permissions

Read (plan mode): `ec2:DescribeRegions`, `ec2:DescribeImages`, `ec2:DescribeImageAttribute`, `ec2:DescribeSnapshots`, `ec2:DescribeInstances`, `ec2:DescribeLaunchTemplates`, `ec2:DescribeLaunchTemplateVersions`, `ec2:DescribeFleets`, `ec2:DescribeSpotFleetRequests`, `ec2:DescribeSpotInstanceRequests`, `autoscaling:DescribeAutoScalingGroups`, `autoscaling:DescribeLaunchConfigurations`, `ssm:GetParameter`, `backup:ListBackupVaults`, `backup:ListRecoveryPointsByBackupVault`.

Write (`--execute`): `ec2:DeregisterImage`, `ec2:DeleteSnapshot`, and `backup:DeleteRecoveryPoint` for `--execute-backup-recovery-point-deletes`.

A missing read permission fails closed: AMI deregistrations or snapshot deletions in that region are cancelled, not attempted blind.

Set `RUST_LOG=debug|info|warn|error` to change log verbosity.

## building

This project targets:

- Rust edition `2024`
- MSRV `1.93.1` (pinned in `rust-toolchain.toml`)

### Building on Linux/OSX

```shell
# binary will be ./target/release/amicleaner (symbols are stripped by the release profile)
cargo build --release
```

### Testing

```shell
cargo test                    # unit tests plus AWS calls mocked with aws-smithy-mocks
cargo test protect_tag        # tests whose name contains "protect_tag"
```

### Cross compiling for Linux on OSX

Must have Docker installed and running. Checkout the [build.sh](build.sh) for what happens.

```shell
./build.sh
```

### help

```shell
$ amicleaner --help
Conservative EC2 AMI + EBS snapshot mark-and-sweep cleanup

Usage: amicleaner [OPTIONS]

Options:
      --execute
          Execute deletions. Without this flag, the tool only prints a plan
      --dry-run
          Use AWS API dry-run checks when executing (requires --execute)
      --execute-backup-recovery-point-deletes
          Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots (requires --execute)
      --backup-delete-tag <BACKUP_DELETE_TAG>
          Snapshot tag key required before its AWS Backup recovery point may be deleted [default: backup-cleanup]
      --backup-delete-tag-value <BACKUP_DELETE_TAG_VALUE>
          Expected value for backup delete tag. Use empty string to allow any value [default: true]
      --allow-untagged-backup-recovery-points
          Permit deletion of AWS Backup recovery points whose snapshot lacks the backup delete tag
      --regions <REGIONS>
          Comma-separated list of regions. If omitted, all enabled account regions are scanned
      --min-age-days <MIN_AGE_DAYS>
          Keep resources newer than this age in days [default: 14]
      --ami-recent-launch-days <AMI_RECENT_LAUNCH_DAYS>
          Keep AMIs launched within the last N days [default: 90]
      --protect-tag <PROTECT_TAG>
          Tag key that protects resources from deletion (matched case-insensitively) [default: DoNotDelete]
      --protect-tag-value <PROTECT_TAG_VALUE>
          Value the protect tag must have. Empty (the default) protects on key presence alone [default: ""]
      --delete-tag <DELETE_TAG>
          Tag key required for deletion (unless --allow-untagged) [default: cleanup]
      --delete-tag-value <DELETE_TAG_VALUE>
          Expected value for delete tag. Use empty string to allow any value [default: true]
      --allow-untagged
          Permit deletion of untagged resources
      --keep-latest-per-family <KEEP_LATEST_PER_FAMILY>
          Keep newest N AMIs per ImageFamily tag. When N > 0, AMIs without that tag are always kept [default: 0]
      --max-retries <MAX_RETRIES>
          Retries after the first attempt when a mutating call hits throttling or a transient error [default: 2]
      --retry-delay-ms <RETRY_DELAY_MS>
          Base delay in milliseconds between retries (doubles on each retry) [default: 1000]
  -h, --help
          Print help
  -V, --version
          Print version
```
