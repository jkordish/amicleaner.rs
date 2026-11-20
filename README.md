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
- Anything shared is kept. That means an AMI with any launch permission, or a snapshot with any create-volume permission, granted to another account, an Organization or OU, or the public. If the permission lookup fails, the resource is kept and the region exits with code 3. This protects the build-account pattern, where AMIs are built in one account and shared to the accounts that run them. A shared AMI also keeps its snapshots, because they stay referenced. The plan prints how many candidates were kept this way as `kept_shared_amis` and `kept_shared_snapshots`. With `--check-accounts`, amicleaner looks inside the accounts you share with, and only treats something as unshared when none of them use it (see below).
- A snapshot that a launch template, launch configuration, Spot Fleet or Spot request, Image Builder recipe, or CloudFormation stack in the account names is kept (`snapshot_referenced`), since it can still create volumes. If the reference scan fails, snapshot deletions in that region are cancelled (exit code 3).
- AMIs created by AWS Backup and AMIs with missing or unparseable dates are kept.
- Snapshots and AMIs created by Amazon Data Lifecycle Manager (an `aws:dlm:` tag, or a "Created for policy: policy-…" description) are always kept, even with the delete tag. DLM deletes them on its own retention schedule. The exception is snapshots DLM will never delete, which `--delete-orphaned-dlm-snapshots` can clean up (see below).
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
| `image-builder` | `imagebuilder:ListImageRecipes`, `imagebuilder:GetImageRecipe`, `imagebuilder:ListContainerRecipes`, `imagebuilder:GetContainerRecipe`, `imagebuilder:ListImages`, `imagebuilder:ListImageBuildVersions` |
| `cloudformation` | `cloudformation:DescribeStacks`, `cloudformation:GetTemplate` |
| `batch` | `batch:DescribeComputeEnvironments` |
| `emr` | `elasticmapreduce:ListClusters`, `elasticmapreduce:DescribeCluster`, `elasticmapreduce:ListInstanceGroups`, `elasticmapreduce:ListInstanceFleets` |
| `eks` (only with `--prune-launch-template-versions`) | `eks:ListClusters`, `eks:ListNodegroups`, `eks:DescribeNodegroup` |

With `--delete-orphaned-dlm-snapshots`: `dlm:GetLifecyclePolicy` and `ec2:DescribeVolumes`.

With `--check-accounts`: `sts:GetCallerIdentity` for every listed profile. In each checked account, the read permissions above (minus AWS Backup), plus `ec2:DescribeVolumes`.

Recycle Bin coverage (whenever snapshots are planned for deletion): `rbin:ListRules` and `rbin:GetRule`. Without them the plan reports coverage as unknown. With `--require-recycle-bin`, missing permissions cancel the region's snapshot deletes.

Write (`--execute`): `ec2:DeregisterImage`, `ec2:DeleteSnapshot`, `backup:DeleteRecoveryPoint` for `--execute-backup-recovery-point-deletes`, and `ec2:DeleteLaunchTemplateVersions` for `--prune-launch-template-versions`.

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

### pruning launch template versions

Launch templates are free, but every old version that names an AMI keeps that AMI and its snapshots "referenced" forever. A template also stops accepting new versions at 10,000. `--prune-launch-template-versions` deletes old versions as well (only with `--execute`; plan mode just reports them). A version is kept if any of these hold:

- it's among the newest `--keep-launch-template-versions` (default 3), or it's the latest
- it's the default version (`$Default`, and launches that name no version)
- an Auto Scaling group (including mixed-instances overrides), EC2 Fleet, Spot Fleet, Batch compute environment or EKS managed node group launches from it by number. Terraform's `latest_version` pins a number, so this is common.
- it's newer than `--min-age-days`, or its creation time is unknown

Templates managed by EKS, Karpenter, Batch or CloudFormation (by tag), templates with the protect tag, and templates pinned to a version string amicleaner doesn't recognize are left alone. If any reference source failed or left unresolved references, or Batch or EKS is skipped, pruning is cancelled for the region (exit code 3). Execute re-reads everything and only deletes versions that both the plan and the re-check agree on, after the AMI and snapshot phases. AMIs that only the deleted versions referenced are reported as freed. They're considered on the *next* run, under every AMI rule. Needs `ec2:DeleteLaunchTemplateVersions`, plus the `eks` permissions above.

Things amicleaner can't see, like scripts or pipelines that launch a specific version number by hand, aren't protected. Raise `--keep-launch-template-versions` if you have those.

### checking the accounts you share with

AMIs built in one account and shared to others are kept forever by default, because amicleaner can't see whether the other accounts still use them. If you have profiles for those accounts, pass them:

```shell
AWS_PROFILE=delta0 amicleaner --regions us-west-2 --check-accounts charlie1,charlie11,india0
```

amicleaner looks up each profile's account id. For every shared candidate, it reads who it's shared with, then scans those accounts in the same region with the same reference scan it runs on its own account. A shared AMI or snapshot is no longer kept just for being shared only when all of these hold:

- **Only specific accounts.** Every grant is to an account. Public, organization and OU shares can't be enumerated, so they're always kept (`shared_publicly`, `shared_with_organization`).
- **Every account checked.** Each of those accounts is in `--check-accounts` (otherwise `shared_with_unchecked_account`), and its scan finished cleanly with no unresolved references (otherwise `account_check_failed`, exit code 3).
- **Nobody uses it.** None of those accounts uses it (otherwise `used_in_other_account`, naming the account and what uses it).
  - For an AMI, "uses" means the full reference scan: instances, launch templates, Auto Scaling groups, launch configurations, fleets, Spot requests, Image Builder, CloudFormation, Batch and EMR. An Image Builder recipe there whose parent image is one of this account's Image Builder images uses every matching build's AMI, listed with this account's Image Builder. If that can't be done (Image Builder skipped, the parent in another region, or this account's id unknown), the account check fails.
  - For a snapshot, it means block devices and CloudFormation that name it, plus any volume created from it.

Everything else still applies: age, last launch, references in this account, tags, DLM, AWS Backup and the Recycle Bin. Execute re-reads the grants and re-scans the accounts before deleting, so something newly shared or newly used is skipped. Deregistering an AMI doesn't affect instances already running from it in other accounts, but nothing new can launch from it.

Each checked account needs the same read permissions as the account being cleaned, plus `ec2:DescribeVolumes`. Its launch template cache is kept separately, by account. Only the region being cleaned is checked, since an AMI or snapshot can only be used in its own region (copies are independent).

### Recycle Bin

An EBS [Recycle Bin](https://docs.aws.amazon.com/ebs/latest/userguide/recycle-bin.html) retention rule turns snapshot deletes into an undo window. A deleted snapshot stays restorable for the rule's period, billed at the normal snapshot rate, and is then gone for good.

Whenever a plan deletes snapshots, amicleaner reads the region's EBS snapshot rules and reports:

- **Summary line:** for example, "Recycle Bin: all 1241 snapshot deletes restorable for 30 days", or "no retention rule covers these snapshots; deletes are permanent".
- **Per snapshot:** each snapshot to delete carries a "restorable for N days" or "permanent" detail.

Matching follows AWS's rules:

- only `available` rules count
- a tag-level rule matches snapshots with one of its tags (key and, if set, value)
- a Region-level rule's exclusion tags opt a snapshot out
- when several rules match, the longest retention period applies

`--require-recycle-bin` turns the report into a gate. Snapshots no rule would retain are kept (`no_recycle_bin`). If the rules can't be read, the region's snapshot deletes are cancelled (exit code 3). Execute re-reads the rules before deleting, so a rule removed after planning stops the deletes. AWS says a rule also retains snapshots that AWS Backup or Data Lifecycle Manager delete, so the gate and the coverage report include recovery-point deletes (`--execute-backup-recovery-point-deletes`), matched on the snapshot's own tags.

While a Region-level rule exists, it also holds every snapshot that DLM retires on schedule, and those are billed for the retention period too. AWS notes that the first rule for a resource type in a Region may take a while to start retaining, even once it's `available`, so leave time between creating it and a delete run.

[`examples/recycle-bin-rule.yaml`](examples/recycle-bin-rule.yaml) is a small CloudFormation template for a Region-level rule, with a 30-day default and an optional exclusion tag. It's unlocked, so it can be removed after the cleanup:

```shell
aws cloudformation deploy --stack-name snapshot-recycle-bin \
  --template-file examples/recycle-bin-rule.yaml --parameter-overrides RetentionDays=30
```

### orphaned DLM snapshots

Data Lifecycle Manager stops retiring its snapshots in three cases, and leaves them forever:

- a **count-based** schedule ("keep the newest N") whose source volume was deleted: DLM only deletes an old snapshot when it takes a new one of the same volume
- the **policy was deleted**: its snapshots aren't deleted with it
- the **schedule was renamed or removed** from the policy: snapshots made under the old name are no longer managed

`--delete-orphaned-dlm-snapshots` treats those snapshots like any other snapshot instead of keeping them as lifecycle managed. They still need everything else: the delete tag (or `--allow-untagged`), no protect tag, not used by a remaining AMI, not shared, and not in AWS Backup. Plan mode just reports them.

To stay safe:

- **Grace period.** The snapshots of one volume under one schedule only count once the newest of them is older than `--min-age-days`. A volume or policy deleted last week keeps its whole set.
- **Narrow scope.** Only snapshots with the `aws:dlm:lifecycle-policy-id` tag and a real source volume are considered. Cross-region and cross-account copies (`vol-ffffffff`) are skipped, because their policy lives elsewhere. So are snapshots with a pending `aws:dlm:expirationTime`, age-based schedules (DLM keeps expiring those), AMI policies, event-based policies and default policies.
- **Fails closed.** If a policy or volume lookup fails, the affected snapshots stay kept, and the region exits with code 3.
- **Re-checked at execute.** Execute re-reads the policies and volumes. A snapshot whose volume came back, or whose schedule was restored, is skipped.

Reports give each one's cause (`dlm_volume_deleted`, `dlm_policy_deleted`, `dlm_schedule_removed`) and its policy, schedule and source volume.

The last snapshots of a deleted volume may be someone's only copy of it. Check with the policy's owner first, and run with `--require-recycle-bin` behind a Recycle Bin rule (see above), so deletes can be undone for a while.

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

Usage: amicleaner [OPTIONS]

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

      --check-accounts <PROFILE>
          Comma-separated AWS profiles of the accounts AMIs and snapshots are shared with. Each one is scanned (same region) for what it uses; a shared AMI or snapshot is no longer kept just for being shared when every account it's shared with is listed, was scanned cleanly, and doesn't use it. Public, organization and OU shares are always kept

      --skip-reference-sources <SKIP_REFERENCE_SOURCES>
          Comma-separated AMI reference sources to skip: image-builder, cloudformation, batch, emr, eks. All are scanned by default; a source you can't read (e.g. missing IAM permission) blocks AMI deregistration rather than being ignored

          Possible values:
          - image-builder:  Image recipe parent images and container recipe build-instance AMIs
          - cloudformation: AMI ids (and SSM dynamic references to AMIs) in stack templates and parameters
          - batch:          Compute environment image ids and EC2 configuration overrides
          - emr:            Custom AMIs of active clusters, instance groups and instance fleets
          - eks:            Launch template versions pinned by EKS managed node groups (only needed to prune versions)

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

      --require-recycle-bin
          Keep snapshots that no Recycle Bin retention rule would retain after deletion, so each snapshot delete can be undone until the rule's period runs out. If the rules can't be read, no snapshots are deleted in that region. Covers AWS Backup recovery-point deletes too. Without this flag the plan still reports whether deletes would be recoverable

      --delete-orphaned-dlm-snapshots
          Also delete Data Lifecycle Manager snapshots that DLM will never retire: those whose policy was deleted, whose schedule was removed, or whose count-based policy lost its source volume. Only once the newest snapshot of that volume and schedule is older than --min-age-days. The normal rules still apply, including the delete tag (or --allow-untagged), the protect tag, sharing and AWS Backup

      --prune-launch-template-versions
          Also delete old launch template versions: those older than --min-age-days that aren't among the newest --keep-launch-template-versions, the default version, or launched by number from an Auto Scaling group, fleet, Batch environment or EKS node group. Templates managed by EKS, Karpenter, Batch or CloudFormation are left alone. Frees the AMIs only old versions referenced on the next run

      --keep-launch-template-versions <COUNT>
          Newest versions of each launch template kept by --prune-launch-template-versions, in addition to the latest and default versions
          
          [default: 3]

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
