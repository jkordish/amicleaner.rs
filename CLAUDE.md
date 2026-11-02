# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Toolchain is pinned to Rust 1.99.1 (edition 2024) via `rust-toolchain.toml` (`profile = "minimal"` so a rustup on the `complete` profile doesn't fail on unpublished components). Don't go back to 1.93.x: its rustc overflowed its stack and segfaulted intermittently on the generated AWS SDK code.

```shell
cargo build                      # debug build
cargo build --release            # ./target/release/amicleaner (fat LTO, symbols stripped)
cargo clippy --all-targets
cargo fmt                        # config in .rustfmt.toml
cargo test                       # src/tests.rs: pure unit tests + AWS calls mocked with aws-smithy-mocks
cargo test protect_tag           # run tests whose name contains a substring
./build.sh                       # Docker musl cross-compile (image tag follows rust-toolchain.toml)
```

`rust-version` in `Cargo.toml` matches the toolchain pin, so `cargo update` only picks dependency versions that build on it. The AWS SDK crates are one family and should move together.

Running against AWS uses the default credential chain. Region auto-discovery (no `--regions`) needs a default region configured. Without `--execute` the binary is read-only and only prints a plan. Exit codes: 0 ok, 1 failures, 2 bad args, 3 safety checks blocked planned work.

## Architecture

All logic is in `src/main.rs`; tests are in `src/tests.rs` (`#[cfg(test)] mod tests;`). It is a conservative mark-and-sweep cleaner for **account-owned** AMIs, EBS snapshots, and AWS Backup EBS recovery points. Regions are processed sequentially; AWS calls within a region are sequential awaits.

Per region, two phases:

1. **Plan**: `build_region_plan()` does the I/O, and `plan_region()` makes every decision as a pure function:
   - **Inventory:** `list_owned_amis()` / `list_owned_snapshots()`.
   - **Mark:** `collect_referenced_ami_ids()` drives `ReferenceCollector`. It covers instances, launch-template versions, ASGs (including mixed-instances overrides), launch configurations, EC2 Fleets, Spot Fleets and Spot requests. It also covers four optional sources, all on by default and turned off with `--skip-reference-sources`: Image Builder (`classify_parent_image`; an own-account parent image ARN expands to every matching build's AMIs via `own_image_builder_amis`), CloudFormation (`find_ami_ids` and `find_ssm_dynamic_references` over templates and parameters), Batch, and EMR. Their clients live in `ReferenceClients` (`None` = skipped). Every image reference goes through `ReferenceCollector::record` → `resolve_image_reference`, which accepts only strict `ami-` ids or `resolve:ssm:` parameters that resolve to one. Anything else becomes "unresolved".
   - **Sweep:** `should_deregister_ami()` and `snapshot_is_deletable()` hold the eligibility rules. Sharing (`ExternalUse`) is looked up per candidate: launch permissions for AMIs (passed into `plan_region`) and create-volume permissions for snapshots (`apply_snapshot_external_use`, before the Backup classification). Shared resources are always kept, because AMIs are built in one account and shared to the accounts that run them. Deregistration does **not** delete snapshots; an AMI's snapshots are planned through the normal snapshot rules and recorded in `snapshots_released_by_ami`, so they inherit the AMI's delete authorization via `snapshot_deletion_authorized()`.
   - **Backup:** `list_ebs_recovery_points()` lists recovery points (I/O), then `classify_backup_snapshots()` decides (pure). It splits candidates into direct EC2 deletes, recovery-point deletes, and blocked snapshots. For EBS, the snapshot is the `RecoveryPointArn` (`ResourceArn` is the volume) and recovery-point tags are the snapshot's own EC2 tags, so there are no `ListTags` calls.
2. **Execute**: `execute_region_plan()` runs only with `--execute`:
   - **AMIs:** `execute_ami_deregistrations()` re-scans references, re-lists AMIs, recomputes the keep-latest set, re-checks launch permissions, then deregisters. It returns the AMIs it released.
   - **Snapshots:** `execute_snapshot_deletions()` re-lists AMIs and snapshots and re-applies `snapshot_is_deletable()` and the create-volume-permission check (`snapshot_still_unshared`) to every direct delete and every recovery-point delete.
   - **Sending:** all mutations go through `send_with_retries()`. It retries only throttling and transient errors (`is_retryable`), and under `--dry-run` it treats the `DryRunOperation` *error code* as success.

### Safety invariants to preserve

- **Unknown blocks only the deletion it affects.** Unresolved references cancel AMI deregistrations, not snapshot cleanup. A failed or unmappable Backup inventory cancels snapshot deletions. A failed launch- or create-volume-permission lookup keeps that one AMI or snapshot. Never let "couldn't determine" fall through to the "not referenced / not backed up" path. Uncertain blocks go into `RegionPlan::blocked` at plan time and `ExecutionSummary::blocked` at execute time (both mean exit code 3). API and inventory errors go into `failures` (exit code 1).
- **Execute re-checks with the same functions plan used** (`should_deregister_ami`, `snapshot_is_deletable`, `backup_recovery_point_deletion_tag_allows`). Add new eligibility rules to those functions rather than to one phase only.
- **Read error codes, never `Display`.** `SdkError`'s `Display` is just "service error". Use `error.code()` to classify and `describe_sdk_error()` / `.aws_context("Op")` / `error_chain()` to build messages.
- New reference sources belong in `ReferenceCollector`, must use SDK paginators (`into_paginator()`), and need a mock test plus a README IAM-permission entry, since a missing permission fails closed.

### Testing pattern

AWS calls are tested with `aws-smithy-mocks` (`mock!` / `mock_client!`, `RuleMode::MatchAny`) through the dev-dependency `test-util` features. Disable SDK-level retries in mock clients (`no_sdk_retries`) so tests count only our own retries. `src/tests.rs` contains helpers for building realistic mocked inventories (`image`, `ec2_snapshot`, `empty_reference_scan_rules`, `ec2_with`). `empty_reference_scan_rules` includes a default "not shared" snapshot-attribute rule; in `MatchAny` mode the first matching rule wins, so put overriding rules (e.g. `shared_snapshot_rule`) before it.

## Repo notes

- The `amicleaner --help` block in `README.md` is pasted from the binary; regenerate it when flags change. Flag definitions and defaults are in the clap `Args` struct at the top of `src/main.rs`.
- `.clippy.toml` sets `cognitive-complexity-threshold`, but that lint isn't enabled by default, so it has no effect unless explicitly turned on.
- `*.yml` is gitignored, which would block adding GitHub Actions workflows.
