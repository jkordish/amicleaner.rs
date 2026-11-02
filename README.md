# amicleaner.rs

Conservative EC2 AMI + EBS snapshot cleanup using a mark-and-sweep strategy:

- Mark AMIs referenced by instances, launch templates, Auto Scaling groups and launch configurations, EC2 Fleets, Spot Fleets and open Spot requests (including `resolve:ssm:` image parameters).
- Also mark AMIs used by EC2 Image Builder (recipe parent images, including every build of this account's own parent images, and container recipe build instances), CloudFormation (AMI ids and `{{resolve:ssm:...}}` references in stack templates and parameters), AWS Batch compute environments, and active EMR clusters. These are on by default; `--skip-reference-sources` turns individual ones off.
- Mark snapshots referenced by AMIs that will remain registered.
- Deregister only old, unreferenced, unprotected, tagged AMIs that aren't shared with other accounts.
- Delete only old, unreferenced, unprotected, tagged snapshots that aren't shared with other accounts — including the snapshots of AMIs it deregisters.
- Optionally delete AWS Backup recovery points for backup-managed snapshots.
- Default behavior is **plan-only**; pass `--execute` to apply changes.

## safety model

Anything the tool can't determine blocks the deletions it affects:

- An AMI reference that can't be resolved (for example an SSM parameter that doesn't hold an AMI id) cancels all AMI deregistrations in that region. Snapshot cleanup continues.
- If the AWS Backup inventory can't be read, or a snapshot looks AWS Backup-created but has no matching recovery point, those snapshot deletions are cancelled.
- Anything shared is kept. That means an AMI with any launch permission, or a snapshot with any create-volume permission, granted to another account, an Organization or OU, or the public. If the permission lookup fails, the resource is kept and the region exits with code 3. This protects the build-account pattern, where AMIs are built in one account and shared to the accounts that run them. A shared AMI also keeps its snapshots, because they stay referenced. The plan prints how many candidates were kept this way as `kept_shared_amis` and `kept_shared_snapshots`.
- AMIs created by AWS Backup and AMIs with missing or unparseable dates are kept.
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
| 3 | no errors, but safety checks cancelled planned deletions: `blocked_by_safety_checks` in the plan output, or `execution_blocked` during `--execute` |

### IAM permissions

Read (plan mode): `ec2:DescribeRegions`, `ec2:DescribeImages`, `ec2:DescribeImageAttribute`, `ec2:DescribeSnapshots`, `ec2:DescribeSnapshotAttribute`, `ec2:DescribeInstances`, `ec2:DescribeLaunchTemplates`, `ec2:DescribeLaunchTemplateVersions`, `ec2:DescribeFleets`, `ec2:DescribeSpotFleetRequests`, `ec2:DescribeSpotInstanceRequests`, `autoscaling:DescribeAutoScalingGroups`, `autoscaling:DescribeLaunchConfigurations`, `ssm:GetParameter`, `sts:GetCallerIdentity` (names the launch template cache; without it the cache is off), `backup:ListBackupVaults`, `backup:ListRecoveryPointsByBackupVault`.

Read, per reference source (drop the ones you pass to `--skip-reference-sources`):

| source | permissions |
|---|---|
| `image-builder` | `imagebuilder:ListImageRecipes`, `imagebuilder:ListContainerRecipes`, `imagebuilder:GetContainerRecipe`, `imagebuilder:ListImages`, `imagebuilder:ListImageBuildVersions` |
| `cloudformation` | `cloudformation:DescribeStacks`, `cloudformation:GetTemplate` |
| `batch` | `batch:DescribeComputeEnvironments` |
| `emr` | `elasticmapreduce:ListClusters`, `elasticmapreduce:DescribeCluster`, `elasticmapreduce:ListInstanceGroups`, `elasticmapreduce:ListInstanceFleets` |

Write (`--execute`): `ec2:DeregisterImage`, `ec2:DeleteSnapshot`, and `backup:DeleteRecoveryPoint` for `--execute-backup-recovery-point-deletes`.

A missing read permission fails closed: AMI deregistrations or snapshot deletions in that region are cancelled, not attempted blind.

## output and reports

While it runs, each phase gets a live progress line on stderr that turns into a summary when it's done:

```text
✓ us-west-2 · inventory  330 AMIs · 767 snapshots · 2.1s
✓ us-west-2 · launch templates  86,849 references · 1m04s
– us-west-2 · emr  skipped (--skip-reference-sources)
✓ us-west-2 · snapshot create-volume permissions  3 shared · 4.0s
```

Progress lines are only drawn on a terminal. In CI or when piped, each finished phase is logged as a plain line instead. Logs go to stderr too (`RUST_LOG=error|warn|info|debug|trace`, default `info`), so stdout carries only the summary: per region, what would be deregistered or deleted, why everything else is kept (`kept: 312 referenced · 18 no delete tag`), anything blocked, and the estimated savings.

For the full picture, write a report. The format follows the extension, and the flag can be repeated:

```shell
amicleaner --regions us-west-2 --report plan.md --report plan.json
```

Both formats list every AMI and snapshot with its decision (`deregister`, `delete`, `delete_recovery_point`, `blocked`, `keep`), a stable `reason_code` (`referenced`, `shared`, `no_delete_tag`, `too_new`, …), a readable reason, and supporting details such as which instance or launch template version references an AMI. The Markdown version is for people: a summary table, then per-region actions, blocks and a kept-by-reason table, with the full lists in collapsible sections. The JSON version is for tooling and carries a `schema_version`.

### estimated savings

The plan estimates the monthly snapshot storage that deleting would free, as **an upper bound**. EBS only bills the blocks a snapshot doesn't share with other snapshots of the same volume, and AWS doesn't expose that per snapshot. So the estimate uses each snapshot's full size (falling back to its volume size) times `--snapshot-price-per-gb-month` (default `0.05`) or, for archived snapshots, `--archive-price-per-gb-month` (default `0.0125`). Set both to your region's prices. AMIs themselves cost nothing beyond their snapshots.

### timeouts and concurrency

Every AWS request attempt has a deadline (`--request-timeout-secs`, default 60). Proxies that drop connections without closing them would otherwise leave a request waiting forever. With the deadline, the SDK retries it instead.

Read-heavy steps run up to `--max-concurrency` requests at a time (default 8): launch template versions (a build account can have tens of thousands), CloudFormation templates, and the AMI/snapshot sharing lookups. Lower it if the account gets throttled. Mutations (deregister, delete) always run one at a time. A launch template deleted while the scan runs is skipped, since it can no longer launch anything.

### launch template cache

Reading every launch template version is usually the slowest step: a build account can have tens of thousands of versions, and EC2 can't filter them by image. A version never changes once it's created and version numbers are never reused, so amicleaner caches each version's image reference in `~/.cache/amicleaner/launch-templates-<account>-<region>.json` (or under `$XDG_CACHE_HOME`, or `--cache-dir`). Later runs fetch only versions newer than the cache, so after the first run this step takes seconds instead of minutes.

The cache can only protect more AMIs, never fewer:

- Every cached version still counts as a reference. Nothing in the cache removes one.
- The raw image reference is stored, so `resolve:ssm:` parameters are resolved fresh on every run.
- A version deleted since it was cached keeps its AMI until that template's next full rescan. That happens after `--cache-max-age-days` (default 7; 0 rescans every run).
- Templates that no longer exist are dropped. A missing, corrupt or mismatched cache file only means a full scan, and a cache that can't be written is a warning.

`--no-cache` reads everything and doesn't touch the cache.

## building

This project targets:

- Rust edition `2024`
- MSRV `1.99.1` (pinned in `rust-toolchain.toml`)

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
Conservative EC2 AMI + EBS snapshot mark-and-sweep cleanup

Usage: amicleaner-plan [OPTIONS]

Options:
      --execute
          Execute deletions. Without this flag, the tool only prints a plan

      --dry-run
          Use AWS API dry-run checks when executing (requires --execute)

      --execute-backup-recovery-point-deletes
          Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots (requires --execute)

      --backup-delete-tag <BACKUP_DELETE_TAG>
          Snapshot tag key required before its AWS Backup recovery point may be deleted
          
          [default: backup-cleanup]

      --backup-delete-tag-value <BACKUP_DELETE_TAG_VALUE>
          Expected value for backup delete tag. Use empty string to allow any value
          
          [default: true]

      --allow-untagged-backup-recovery-points
          Permit deletion of AWS Backup recovery points whose snapshot lacks the backup delete tag

      --regions <REGIONS>
          Comma-separated list of regions. If omitted, all enabled account regions are scanned

      --min-age-days <MIN_AGE_DAYS>
          Keep resources newer than this age in days
          
          [default: 14]

      --ami-recent-launch-days <AMI_RECENT_LAUNCH_DAYS>
          Keep AMIs launched within the last N days
          
          [default: 90]

      --protect-tag <PROTECT_TAG>
          Tag key that protects resources from deletion (matched case-insensitively)
          
          [default: DoNotDelete]

      --protect-tag-value <PROTECT_TAG_VALUE>
          Value the protect tag must have. Empty (the default) protects on key presence alone
          
          [default: ""]

      --delete-tag <DELETE_TAG>
          Tag key required for deletion (unless --allow-untagged)
          
          [default: cleanup]

      --delete-tag-value <DELETE_TAG_VALUE>
          Expected value for delete tag. Use empty string to allow any value
          
          [default: true]

      --allow-untagged
          Permit deletion of untagged resources

      --keep-latest-per-family <KEEP_LATEST_PER_FAMILY>
          Keep newest N AMIs per ImageFamily tag. When N > 0, AMIs without that tag are always kept
          
          [default: 0]

      --skip-reference-sources <SKIP_REFERENCE_SOURCES>
          Comma-separated AMI reference sources to skip: image-builder, cloudformation, batch, emr. All are scanned by default; a source you can't read (e.g. missing IAM permission) blocks AMI deregistration rather than being ignored

          Possible values:
          - image-builder:  Image recipe parent images and container recipe build-instance AMIs
          - cloudformation: AMI ids (and SSM dynamic references to AMIs) in stack templates and parameters
          - batch:          Compute environment image ids and EC2 configuration overrides
          - emr:            Custom AMIs of active clusters, instance groups and instance fleets

      --report <FILE>
          Write the full report (every AMI and snapshot with its decision and reason) to a file. The format follows the extension: .md/.markdown or .json. Repeat for several files

      --snapshot-price-per-gb-month <USD>
          Snapshot storage price in USD per GB-month, for the savings estimate (EBS Snapshots standard tier; check your region's price)
          
          [default: 0.05]

      --archive-price-per-gb-month <USD>
          EBS Snapshots Archive tier price in USD per GB-month, for the savings estimate
          
          [default: 0.0125]

      --request-timeout-secs <SECONDS>
          Seconds before a single AWS request attempt is abandoned and retried. Protects against connections that a proxy drops without closing
          
          [default: 60]

      --cache-dir <DIR>
          Directory for the launch template version cache. Defaults to XDG_CACHE_HOME/amicleaner, else ~/.cache/amicleaner

      --no-cache
          Read every launch template version instead of only those newer than the local cache, and don't read or write the cache

      --cache-max-age-days <DAYS>
          Days a cached launch template stays trusted before all its versions are read again, which also forgets versions deleted since. 0 reads everything on every run
          
          [default: 7]

      --max-concurrency <MAX_CONCURRENCY>
          Maximum concurrent read requests per region (launch template versions, CloudFormation templates, permission lookups). Lower it if AWS throttles the account
          
          [default: 8]

      --max-retries <MAX_RETRIES>
          Retries after the first attempt when a mutating call hits throttling or a transient error
          
          [default: 2]

      --retry-delay-ms <RETRY_DELAY_MS>
          Base delay in milliseconds between retries (doubles on each retry)
          
          [default: 1000]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```
