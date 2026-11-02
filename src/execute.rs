//! The execute phase: re-checks every planned item against live state, then mutates.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration as StdDuration;

use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use chrono::Utc;
use ec2::error::{ProvideErrorMetadata, SdkError};

use crate::cli::Args;
use crate::errors::{describe_sdk_error, error_chain};
use crate::inventory::{
    list_owned_amis, list_owned_snapshots, lookup_ami_external_use, lookup_snapshot_external_use,
};
use crate::model::{
    AmiInfo, BackupRecoveryPoint, Cutoffs, ExecutionSummary, ExternalUse, RegionPlan, RetryPolicy,
    SnapshotInfo,
};
use crate::policy::{
    backup_recovery_point_deletion_tag_allows, looks_backup_created,
    select_latest_ami_ids_by_family, should_deregister_ami, snapshot_is_deletable,
};
use crate::references::{ReferenceClients, collect_referenced_ami_ids};
use crate::ui::Phase;

/// EC2 returns this error code when a `DryRun=true` request would have succeeded.
const DRY_RUN_OPERATION: &str = "DryRunOperation";

/// Error codes worth retrying: throttling and transient server-side failures.
const RETRYABLE_ERROR_CODES: &[&str] = &[
    "RequestLimitExceeded",
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottled",
    "RequestThrottledException",
    "TooManyRequestsException",
    "EC2ThrottledException",
    "PriorRequestNotComplete",
    "SlowDown",
    "RequestTimeout",
    "RequestTimeoutException",
    "InternalError",
    "InternalFailure",
    "InternalServerError",
    "ServiceUnavailable",
    "ServiceUnavailableException",
    "Unavailable",
];

pub(crate) async fn execute_region_plan(
    ec2_client: &ec2::Client,
    backup_client: &backup::Client,
    reference_clients: &ReferenceClients,
    plan: &RegionPlan,
    args: &Args,
) -> ExecutionSummary {
    let mut summary = ExecutionSummary { dry_run: args.dry_run, ..ExecutionSummary::default() };
    let cutoffs = Cutoffs::new(args, Utc::now());

    let released_amis = execute_ami_deregistrations(
        ec2_client,
        reference_clients,
        plan,
        args,
        cutoffs,
        &mut summary,
    )
    .await;

    execute_snapshot_deletions(
        ec2_client,
        backup_client,
        plan,
        args,
        cutoffs,
        &released_amis,
        &mut summary,
    )
    .await;

    summary
}

/// Re-validates every planned AMI against live state and deregisters it. Returns the AMIs that
/// were deregistered (or passed the dry-run check), whose snapshots the snapshot phase may free.
async fn execute_ami_deregistrations(
    ec2_client: &ec2::Client,
    reference_clients: &ReferenceClients,
    plan: &RegionPlan,
    args: &Args,
    cutoffs: Cutoffs,
    summary: &mut ExecutionSummary,
) -> HashSet<String> {
    let mut released_amis = HashSet::new();
    if plan.deregister_amis.is_empty() {
        return released_amis;
    }

    let fail = |summary: &mut ExecutionSummary, reason: String| {
        summary.skipped_amis += plan.deregister_amis.len();
        summary.failures.push(reason);
    };
    // Same outcome as `fail`, but reported as a safety block (exit code 3), matching the plan.
    let block = |summary: &mut ExecutionSummary, reason: String| {
        summary.skipped_amis += plan.deregister_amis.len();
        summary.blocked.push(reason);
    };

    let references = match collect_referenced_ami_ids(ec2_client, reference_clients).await {
        Ok(scan) if scan.unresolved_references.is_empty() => scan.referenced_ami_ids,
        Ok(scan) => {
            block(
                summary,
                format!(
                    "skipped AMI deregistration: {} unresolved AMI reference(s) at execution time",
                    scan.unresolved_references.len()
                ),
            );
            return released_amis;
        }
        Err(error) => {
            block(
                summary,
                format!(
                    "skipped AMI deregistration: AMI reference scan failed at execution time: {}",
                    error_chain(error.as_ref())
                ),
            );
            return released_amis;
        }
    };

    let current_amis = match list_owned_amis(ec2_client).await {
        Ok(amis) => amis,
        Err(error) => {
            fail(
                summary,
                format!(
                    "skipped AMI deregistration: failed to refresh AMI inventory: {}",
                    error_chain(error.as_ref())
                ),
            );
            return released_amis;
        }
    };

    let keep_latest = select_latest_ami_ids_by_family(&current_amis, args.keep_latest_per_family);
    let current_by_id = current_amis
        .iter()
        .map(|ami| (ami.ami_id.as_str(), ami))
        .collect::<HashMap<&str, &AmiInfo>>();
    let retry_policy = RetryPolicy::from_args(args);

    let mut eligible = Vec::new();
    for ami in &plan.deregister_amis {
        let still_eligible = current_by_id.get(ami.ami_id.as_str()).is_some_and(|current| {
            should_deregister_ami(current, &references, &keep_latest, cutoffs, args)
        });
        if still_eligible {
            eligible.push(ami);
        } else {
            summary.skipped_amis += 1;
        }
    }

    // Prefetched right before the deregistration loop, concurrently; the mutations stay sequential.
    let eligible_ids = eligible.iter().map(|ami| ami.ami_id.as_str()).collect::<Vec<&str>>();
    let external_use =
        lookup_ami_external_use(ec2_client, &eligible_ids, usize::from(args.max_concurrency), None)
            .await;

    let verb = if args.dry_run { "dry-run deregister" } else { "deregister" };
    let phase = Phase::with_total(format!("{} · {verb} AMIs", plan.region), eligible.len());
    for ami in eligible {
        phase.inc();
        match external_use.get(&ami.ami_id) {
            None => {}
            Some(ExternalUse::Shared) => {
                summary.skipped_amis += 1;
                continue;
            }
            Some(ExternalUse::Unknown(reason)) => {
                summary.skipped_amis += 1;
                summary.blocked.push(format!(
                    "skipped {}: launch permission check failed: {}",
                    ami.ami_id, reason
                ));
                continue;
            }
        }

        let result = send_with_retries(retry_policy, args.dry_run, || {
            ec2_client.deregister_image().image_id(&ami.ami_id).dry_run(args.dry_run).send()
        })
        .await;

        match result {
            Ok(()) => {
                summary.deregistered_amis += 1;
                released_amis.insert(ami.ami_id.clone());
            }
            Err(error) => {
                summary.failures.push(format!("deregister {} failed: {}", ami.ami_id, error));
            }
        }
    }

    finish_execution_phase(
        phase,
        summary.deregistered_amis,
        "deregistered",
        summary.failures.len(),
    );
    released_amis
}

/// Re-validates planned snapshots (direct and AWS Backup) against fresh inventory, then deletes.
async fn execute_snapshot_deletions(
    ec2_client: &ec2::Client,
    backup_client: &backup::Client,
    plan: &RegionPlan,
    args: &Args,
    cutoffs: Cutoffs,
    released_amis: &HashSet<String>,
    summary: &mut ExecutionSummary,
) {
    if plan.delete_snapshots.is_empty() && plan.delete_backup_recovery_points.is_empty() {
        return;
    }

    let block = |summary: &mut ExecutionSummary, reason: String| {
        summary.skipped_snapshots += plan.delete_snapshots.len();
        summary.skipped_backup_recovery_points += plan.delete_backup_recovery_points.len();
        summary.failures.push(reason);
    };

    let current_amis = match list_owned_amis(ec2_client).await {
        Ok(amis) => amis,
        Err(error) => {
            block(
                summary,
                format!(
                    "skipped snapshot deletion: failed to refresh AMI snapshot references: {}",
                    error_chain(error.as_ref())
                ),
            );
            return;
        }
    };

    // AMIs this run deregistered no longer hold their snapshots, even if DescribeImages still
    // lists them (eventual consistency, or a dry run that didn't really deregister).
    let references = current_amis
        .iter()
        .filter(|ami| !released_amis.contains(&ami.ami_id))
        .flat_map(|ami| ami.snapshot_ids.iter().cloned())
        .collect::<HashSet<String>>();

    let current_snapshots = match list_owned_snapshots(ec2_client).await {
        Ok(snapshots) => snapshots
            .into_iter()
            .map(|snapshot| (snapshot.snapshot_id.clone(), snapshot))
            .collect::<HashMap<String, SnapshotInfo>>(),
        Err(error) => {
            block(
                summary,
                format!(
                    "skipped snapshot deletion: failed to refresh snapshot inventory: {}",
                    error_chain(error.as_ref())
                ),
            );
            return;
        }
    };

    let still_deletable = |snapshot_id: &str| -> Option<&SnapshotInfo> {
        let current = current_snapshots.get(snapshot_id)?;
        let released = plan
            .snapshots_released_by_ami
            .get(snapshot_id)
            .is_some_and(|ami_id| released_amis.contains(ami_id));
        snapshot_is_deletable(current, released, &references, cutoffs.min_age, args)
            .then_some(current)
    };
    let retry_policy = RetryPolicy::from_args(args);

    let direct_deletes = plan
        .delete_snapshots
        .iter()
        .filter(|snapshot| {
            still_deletable(&snapshot.snapshot_id).is_some_and(|current| {
                !looks_backup_created(&current.tags, current.description.as_deref())
            })
        })
        .collect::<Vec<&SnapshotInfo>>();
    summary.skipped_snapshots += plan.delete_snapshots.len() - direct_deletes.len();

    // The recovery point *is* the snapshot, so it gets the same live checks as a direct delete
    // plus the backup delete tag (read from the snapshot's current tags).
    let recovery_point_deletes = if args.execute_backup_recovery_point_deletes {
        plan.delete_backup_recovery_points
            .iter()
            .filter(|recovery_point| {
                still_deletable(&recovery_point.snapshot_id).is_some_and(|current| {
                    backup_recovery_point_deletion_tag_allows(&current.tags, args)
                })
            })
            .collect::<Vec<&BackupRecoveryPoint>>()
    } else {
        Vec::new()
    };
    summary.skipped_backup_recovery_points +=
        plan.delete_backup_recovery_points.len() - recovery_point_deletes.len();

    // Sharing is prefetched right before the deletion loops, concurrently; deletes stay sequential.
    let snapshot_ids = direct_deletes
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .chain(recovery_point_deletes.iter().map(|point| point.snapshot_id.as_str()))
        .collect::<Vec<&str>>();
    let external_use = lookup_snapshot_external_use(
        ec2_client,
        &snapshot_ids,
        usize::from(args.max_concurrency),
        None,
    )
    .await;

    let failures_before = summary.failures.len();
    let verb = if args.dry_run { "dry-run delete" } else { "delete" };
    let phase = Phase::with_total(
        format!("{} · {verb} snapshots", plan.region),
        direct_deletes.len() + recovery_point_deletes.len(),
    );
    for snapshot in direct_deletes {
        phase.inc();
        if !snapshot_unshared(&external_use, &snapshot.snapshot_id, summary) {
            summary.skipped_snapshots += 1;
            continue;
        }

        let result = send_with_retries(retry_policy, args.dry_run, || {
            ec2_client
                .delete_snapshot()
                .snapshot_id(&snapshot.snapshot_id)
                .dry_run(args.dry_run)
                .send()
        })
        .await;

        match result {
            Ok(()) => summary.deleted_snapshots += 1,
            Err(error) => summary
                .failures
                .push(format!("delete snapshot {} failed: {}", snapshot.snapshot_id, error)),
        }
    }

    for recovery_point in recovery_point_deletes {
        phase.inc();
        // AWS Backup has no dry-run mode, so dry runs stop after the checks.
        if !snapshot_unshared(&external_use, &recovery_point.snapshot_id, summary) || args.dry_run {
            summary.skipped_backup_recovery_points += 1;
            continue;
        }

        let result = send_with_retries(retry_policy, false, || {
            backup_client
                .delete_recovery_point()
                .backup_vault_name(&recovery_point.backup_vault_name)
                .recovery_point_arn(&recovery_point.recovery_point_arn)
                .send()
        })
        .await;

        match result {
            Ok(()) => summary.deleted_backup_recovery_points += 1,
            Err(error) => summary.failures.push(format!(
                "delete recovery point {} in vault {} failed: {}",
                recovery_point.recovery_point_arn, recovery_point.backup_vault_name, error
            )),
        }
    }
    finish_execution_phase(
        phase,
        summary.deleted_snapshots + summary.deleted_backup_recovery_points,
        "deleted",
        summary.failures.len() - failures_before,
    );
}

fn finish_execution_phase(phase: Phase, done: usize, verb: &str, failures: usize) {
    if failures > 0 {
        phase.fail(format!("{done} {verb}, {failures} failed"));
    } else {
        phase.finish(format!("{done} {verb}"));
    }
}

/// Whether the prefetched sharing lookup says a snapshot is unshared. A failed lookup keeps the
/// snapshot and is recorded as a safety block.
fn snapshot_unshared(
    external_use: &HashMap<String, ExternalUse>,
    snapshot_id: &str,
    summary: &mut ExecutionSummary,
) -> bool {
    match external_use.get(snapshot_id) {
        None => true,
        Some(ExternalUse::Shared) => false,
        Some(ExternalUse::Unknown(reason)) => {
            summary.blocked.push(format!(
                "skipped {snapshot_id}: create-volume permission check failed: {reason}"
            ));
            false
        }
    }
}

/// Sends a mutating request, retrying only throttling and transient failures. Under `dry_run`, the
/// `DryRunOperation` error means the request would have succeeded.
pub(crate) async fn send_with_retries<T, E, R, F, Fut>(
    policy: RetryPolicy,
    dry_run: bool,
    mut send: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SdkError<E, R>>>,
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let Err(error) = send().await else {
            return Ok(());
        };

        if dry_run && error.code() == Some(DRY_RUN_OPERATION) {
            return Ok(());
        }

        if attempt > policy.max_retries || !is_retryable(&error) {
            return Err(describe_sdk_error(&error));
        }

        tokio::time::sleep(retry_delay_for_attempt(policy.base_delay_ms, attempt)).await;
    }
}

fn is_retryable<E: ProvideErrorMetadata, R>(error: &SdkError<E, R>) -> bool {
    match error {
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => true,
        SdkError::DispatchFailure(failure) => failure.is_io() || failure.is_timeout(),
        SdkError::ServiceError(_) => error.code().is_some_and(is_retryable_code),
        _ => false,
    }
}

pub(crate) fn is_retryable_code(code: &str) -> bool {
    RETRYABLE_ERROR_CODES.contains(&code)
}

pub(crate) fn retry_delay_for_attempt(base_delay_ms: u64, attempt: u32) -> StdDuration {
    let capped_attempt = u64::from(attempt.saturating_sub(1)).min(10);
    let multiplier = 1u64 << capped_attempt;
    StdDuration::from_millis(base_delay_ms.saturating_mul(multiplier))
}
