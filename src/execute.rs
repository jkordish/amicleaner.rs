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
    ami_has_external_launch_permissions, list_owned_amis, list_owned_snapshots,
    snapshot_has_external_create_volume_permissions,
};
use crate::model::{AmiInfo, Cutoffs, ExecutionSummary, RegionPlan, RetryPolicy, SnapshotInfo};
use crate::policy::{
    backup_recovery_point_deletion_tag_allows, looks_backup_created,
    select_latest_ami_ids_by_family, should_deregister_ami, snapshot_is_deletable,
};
use crate::references::{ReferenceClients, collect_referenced_ami_ids};

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

    for ami in &plan.deregister_amis {
        let still_eligible = current_by_id.get(ami.ami_id.as_str()).is_some_and(|current| {
            should_deregister_ami(current, &references, &keep_latest, cutoffs, args)
        });
        if !still_eligible {
            summary.skipped_amis += 1;
            continue;
        }

        match ami_has_external_launch_permissions(ec2_client, &ami.ami_id).await {
            Ok(false) => {}
            Ok(true) => {
                summary.skipped_amis += 1;
                continue;
            }
            Err(error) => {
                summary.skipped_amis += 1;
                summary.blocked.push(format!(
                    "skipped {}: launch permission check failed: {}",
                    ami.ami_id,
                    error_chain(error.as_ref())
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

    for snapshot in &plan.delete_snapshots {
        let deletable = still_deletable(&snapshot.snapshot_id).is_some_and(|current| {
            !looks_backup_created(&current.tags, current.description.as_deref())
        });
        if !deletable || !snapshot_still_unshared(ec2_client, &snapshot.snapshot_id, summary).await
        {
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

    if !args.execute_backup_recovery_point_deletes {
        summary.skipped_backup_recovery_points += plan.delete_backup_recovery_points.len();
        return;
    }

    for recovery_point in &plan.delete_backup_recovery_points {
        // The recovery point *is* the snapshot, so it gets the same live checks as a direct delete
        // plus the backup delete tag (read from the snapshot's current tags).
        let deletable = still_deletable(&recovery_point.snapshot_id)
            .is_some_and(|current| backup_recovery_point_deletion_tag_allows(&current.tags, args))
            && snapshot_still_unshared(ec2_client, &recovery_point.snapshot_id, summary).await;

        // AWS Backup has no dry-run mode, so dry runs stop after the checks.
        if !deletable || args.dry_run {
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
}

/// Execution-time re-check of snapshot sharing. A failed lookup keeps the snapshot and is recorded
/// as a safety block.
async fn snapshot_still_unshared(
    ec2_client: &ec2::Client,
    snapshot_id: &str,
    summary: &mut ExecutionSummary,
) -> bool {
    match snapshot_has_external_create_volume_permissions(ec2_client, snapshot_id).await {
        Ok(shared) => !shared,
        Err(error) => {
            summary.blocked.push(format!(
                "skipped {snapshot_id}: create-volume permission check failed: {}",
                error_chain(error.as_ref())
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

pub(crate) fn print_execution_summary(region: &str, summary: &ExecutionSummary) {
    println!(
        "execution region={} mode={} deregistered_amis={} skipped_amis={} deleted_snapshots={} skipped_snapshots={} deleted_backup_recovery_points={} skipped_backup_recovery_points={} blocked={} failures={}",
        region,
        if summary.dry_run { "dry-run" } else { "execute" },
        summary.deregistered_amis,
        summary.skipped_amis,
        summary.deleted_snapshots,
        summary.skipped_snapshots,
        summary.deleted_backup_recovery_points,
        summary.skipped_backup_recovery_points,
        summary.blocked.len(),
        summary.failures.len(),
    );
    if summary.dry_run {
        println!(
            "  (dry-run: deregistered/deleted counts are requests that passed the AWS dry-run check)"
        );
    }

    if !summary.blocked.is_empty() {
        println!("execution_blocked:");
        for reason in &summary.blocked {
            println!("  {reason}");
        }
    }

    if !summary.failures.is_empty() {
        println!("execution_failures:");
        for failure in &summary.failures {
            println!("  {failure}");
        }
    }
}
