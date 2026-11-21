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

use crate::accounts::{
    AccountScans, SharedKind, accounts_to_scan, resolve_external_use, scan_accounts,
};
use crate::aws_backup::{classify_backup_snapshots, list_ebs_recovery_points};
use crate::cli::Args;
use crate::dlm::{load_dlm_inventory, orphaned_dlm_snapshots};
use crate::errors::{describe_sdk_error, error_chain};
use crate::inventory::{
    list_owned_amis, list_owned_snapshots, lookup_ami_external_use, lookup_snapshot_external_use,
};
use crate::lt_prune::execute_launch_template_pruning;
use crate::model::{
    AmiInfo, BackupClassification, BackupRecoveryPoint, Cutoffs, ExecutionSummary, ExternalUse,
    RegionPlan, RetryPolicy, SnapshotInfo,
};
use crate::plan::owner;
use crate::policy::{
    select_latest_ami_ids_by_family, should_deregister_ami, snapshot_is_deletable,
    snapshot_reference_keep_reason,
};
use crate::recycle_bin::{list_snapshot_retention_rules, recycle_bin_days};
use crate::references::{AmiReferenceScan, ReferenceClients, collect_referenced_ami_ids};
use crate::ui::Phase;

/// EC2 returns this error code when a `DryRun=true` request would have succeeded.
const DRY_RUN_OPERATION: &str = "DryRunOperation";

/// Error codes meaning the thing to delete no longer exists.
const AMI_GONE: &[&str] = &["InvalidAMIID.NotFound", "InvalidAMIID.Unavailable"];
const SNAPSHOT_GONE: &[&str] = &["InvalidSnapshot.NotFound"];
const RECOVERY_POINT_GONE: &[&str] = &["ResourceNotFoundException"];

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

    // A fresh reference scan for the AMI phase. The snapshot phase reuses it unless the AMI
    // phase did work (which can take a while: retries, other accounts' scans), in which case it
    // gets its own, so a snapshot named meanwhile isn't deleted.
    let snapshot_work =
        !(plan.delete_snapshots.is_empty() && plan.delete_backup_recovery_points.is_empty());
    let scan = || async {
        collect_referenced_ami_ids(ec2_client, reference_clients)
            .await
            .map_err(|error| error_chain(error.as_ref()))
    };
    let reference_scan =
        if plan.deregister_amis.is_empty() && !snapshot_work { None } else { Some(scan().await) };

    let released_amis = execute_ami_deregistrations(
        ec2_client,
        reference_clients,
        plan,
        args,
        (cutoffs, reference_scan.as_ref()),
        &mut summary,
    )
    .await;

    let reference_scan = if snapshot_work && !plan.deregister_amis.is_empty() {
        Some(scan().await)
    } else {
        reference_scan
    };
    let snapshot_references = match &reference_scan {
        Some(Ok(scan)) if let Some(failure) = &scan.failure => {
            Err(format!("snapshot references unknown at execution time: {failure}"))
        }
        Some(Ok(scan)) => Ok(&scan.snapshot_references),
        Some(Err(error)) => Err(format!("reference scan failed at execution time: {error}")),
        None => Err("no reference scan at execution time".to_string()),
    };
    execute_snapshot_deletions(
        ec2_client,
        backup_client,
        reference_clients,
        plan,
        args,
        LiveReferences { released_amis: &released_amis, snapshot_references },
        &mut summary,
    )
    .await;

    // Last, so AMIs a pruned version referenced are only reconsidered on the next run.
    execute_launch_template_pruning(ec2_client, reference_clients, plan, args, &mut summary).await;

    summary
}

/// What the snapshot phase needs from earlier in the execution.
struct LiveReferences<'a> {
    /// AMIs this run deregistered (or that passed the dry-run check).
    released_amis: &'a HashSet<String>,
    /// Snapshot references from the execution-time scan, or why they're unknown.
    snapshot_references: Result<&'a HashMap<String, Vec<String>>, String>,
}

/// Re-validates every planned AMI against live state and deregisters it. Returns the AMIs that
/// were deregistered (or passed the dry-run check), whose snapshots the snapshot phase may free.
async fn execute_ami_deregistrations(
    ec2_client: &ec2::Client,
    reference_clients: &ReferenceClients,
    plan: &RegionPlan,
    args: &Args,
    (cutoffs, reference_scan): (Cutoffs, Option<&Result<AmiReferenceScan, String>>),
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

    let references = match reference_scan {
        Some(Ok(scan)) if scan.unresolved_references.is_empty() => scan.referenced_ami_ids.clone(),
        Some(Ok(scan)) => {
            block(
                summary,
                format!(
                    "skipped AMI deregistration: {} unresolved AMI reference(s) at execution time",
                    scan.unresolved_references.len()
                ),
            );
            return released_amis;
        }
        Some(Err(error)) => {
            block(
                summary,
                format!(
                    "skipped AMI deregistration: AMI reference scan failed at execution time: {error}"
                ),
            );
            return released_amis;
        }
        None => return released_amis,
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
    let mut external_use =
        lookup_ami_external_use(ec2_client, &eligible_ids, usize::from(args.max_concurrency), None)
            .await;
    if let Some(check) = &reference_clients.check_accounts {
        let mut scans = AccountScans::new();
        let accounts = accounts_to_scan(&external_use, check);
        let owner = owner(check, &plan.region, reference_clients);
        scan_accounts(args, check, owner, &accounts, &[], &mut scans).await;
        resolve_external_use(&mut external_use, SharedKind::Ami, check, &scans);
    }

    let verb = if args.dry_run { "dry-run deregister" } else { "deregister" };
    let phase = Phase::with_total(format!("{} · {verb} AMIs", plan.region), eligible.len());
    for ami in eligible {
        phase.inc();
        match external_use.get(&ami.ami_id) {
            None => {}
            Some(ExternalUse::Shared(_)) => {
                summary.skipped_amis += 1;
                continue;
            }
            Some(ExternalUse::Kept(reason)) => {
                summary.skipped_amis += 1;
                if reason.uncertain() {
                    summary.blocked.push(format!("skipped {}: {reason}", ami.ami_id));
                }
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

        let result = send_mutation(retry_policy, args.dry_run, AMI_GONE, || {
            ec2_client.deregister_image().image_id(&ami.ami_id).dry_run(args.dry_run).send()
        })
        .await;

        match result {
            Ok(Sent::Done(_)) => {
                summary.deregistered_amis += 1;
                released_amis.insert(ami.ami_id.clone());
            }
            Ok(Sent::Gone(code)) => {
                // Gone either way, so it no longer holds its snapshots.
                log::warn!("{} was already deregistered ({code})", ami.ami_id);
                summary.skipped_amis += 1;
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
    reference_clients: &ReferenceClients,
    plan: &RegionPlan,
    args: &Args,
    live: LiveReferences<'_>,
    summary: &mut ExecutionSummary,
) {
    let released_amis = live.released_amis;
    // Taken now rather than at the start of the run: a later cutoff can only keep more.
    let cutoffs = Cutoffs::new(args, Utc::now());
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

    let mut current_snapshots = match list_owned_snapshots(ec2_client).await {
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

    // Orphaned DLM snapshots are re-derived from the live policies, volumes and snapshots with the
    // same function the plan used; one that DLM manages again (volume back, schedule restored)
    // falls back to the lifecycle-managed keep.
    let planned_orphans =
        plan.delete_snapshots.iter().any(|snapshot| snapshot.dlm_orphan.is_some());
    if let Some(dlm_client) = reference_clients.dlm.as_ref().filter(|_| planned_orphans) {
        let snapshots = current_snapshots.values().cloned().collect::<Vec<_>>();
        let now = Utc::now();
        let inventory = load_dlm_inventory(
            ec2_client,
            dlm_client,
            &snapshots,
            now,
            usize::from(args.max_concurrency),
        )
        .await;
        let scan = orphaned_dlm_snapshots(&snapshots, &inventory, now, cutoffs.min_age);
        summary.blocked.extend(
            scan.undecided.into_iter().map(|reason| format!("{reason} (at execution time)")),
        );
        for (snapshot_id, snapshot) in &mut current_snapshots {
            snapshot.dlm_orphan = scan.orphans.get(snapshot_id).copied();
        }
    }

    // Snapshots something in the account names stay, and unknown references keep them all.
    let snapshot_references = live.snapshot_references.as_deref().map_err(String::as_str);
    if let Err(error) = &snapshot_references {
        summary.blocked.push(format!("snapshot deletions cancelled: references unknown ({error})"));
    }
    let still_deletable = |snapshot_id: &str| -> Option<&SnapshotInfo> {
        let current = current_snapshots.get(snapshot_id)?;
        let released = plan
            .snapshots_released_by_ami
            .get(snapshot_id)
            .is_some_and(|amis| amis.iter().all(|ami_id| released_amis.contains(ami_id)));
        (snapshot_is_deletable(current, released, &references, cutoffs.min_age, args)
            && snapshot_reference_keep_reason(snapshot_id, snapshot_references).is_none())
        .then_some(current)
    };
    let retry_policy = RetryPolicy::from_args(args);

    // --require-recycle-bin: re-read the rules, since one may have been deleted or changed since
    // planning, and only delete snapshots a rule still retains.
    let recycle_bin_rules = if args.require_recycle_bin
        && !(plan.delete_snapshots.is_empty() && plan.delete_backup_recovery_points.is_empty())
    {
        match list_snapshot_retention_rules(&reference_clients.rbin).await {
            Ok(rules) => Some(rules),
            Err(error) => {
                summary.blocked.push(format!(
                    "snapshot deletions cancelled: Recycle Bin rules couldn't be read at execution time: {error}"
                ));
                Some(Vec::new())
            }
        }
    } else {
        None
    };
    let recoverable = |current: &SnapshotInfo| {
        recycle_bin_rules
            .as_ref()
            .is_none_or(|rules| recycle_bin_days(&current.tags, rules).is_some())
    };

    // Candidates that still pass the live snapshot rules (and the Recycle Bin requirement).
    let recovery_points_wanted = args.execute_backup_recovery_point_deletes;
    let candidates = plan
        .delete_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .chain(
            plan.delete_backup_recovery_points
                .iter()
                .filter(|_| recovery_points_wanted)
                .map(|point| point.snapshot_id.as_str()),
        )
        .filter_map(&still_deletable)
        .filter(|current| recoverable(current))
        .cloned()
        .collect::<Vec<SnapshotInfo>>();

    // AWS Backup is re-read and the candidates re-classified with the plan's function: a
    // snapshot that became a recovery point, or a recovery point that changed status, vault lock
    // or vault since planning, is no longer deleted the planned way.
    let classification = if candidates.is_empty() {
        BackupClassification::default()
    } else {
        match list_ebs_recovery_points(backup_client).await {
            Ok(records) => classify_backup_snapshots(&candidates, &records, args),
            Err(error) => {
                summary.blocked.push(format!(
                    "snapshot deletions cancelled: AWS Backup inventory unavailable at execution time: {}",
                    error_chain(error.as_ref())
                ));
                BackupClassification::default()
            }
        }
    };
    let uncertain =
        classification.blocked.iter().filter(|blocked| blocked.uncertain).collect::<Vec<_>>();
    if let Some(example) = uncertain.first() {
        summary.blocked.push(format!(
            "{} snapshot deletion(s) skipped at execution time: AWS Backup ownership unknown (e.g. {}: {})",
            uncertain.len(),
            example.snapshot.snapshot_id,
            example.reason
        ));
    }
    let direct_ok = classification
        .direct_delete
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .collect::<HashSet<&str>>();
    let recovery_points_ok = classification
        .recovery_points_to_delete
        .iter()
        .map(|point| (point.snapshot_id.as_str(), point.recovery_point_arn.as_str()))
        .collect::<HashMap<&str, &str>>();

    let direct_deletes = plan
        .delete_snapshots
        .iter()
        .filter(|snapshot| direct_ok.contains(snapshot.snapshot_id.as_str()))
        .collect::<Vec<&SnapshotInfo>>();
    summary.skipped_snapshots += plan.delete_snapshots.len() - direct_deletes.len();

    // The recovery point *is* the snapshot, so it gets the same live checks as a direct delete,
    // plus the backup delete tag and recovery-point rules (inside the classification).
    let recovery_point_deletes = plan
        .delete_backup_recovery_points
        .iter()
        .filter(|point| {
            recovery_points_wanted
                && recovery_points_ok.get(point.snapshot_id.as_str())
                    == Some(&point.recovery_point_arn.as_str())
        })
        .collect::<Vec<&BackupRecoveryPoint>>();
    summary.skipped_backup_recovery_points +=
        plan.delete_backup_recovery_points.len() - recovery_point_deletes.len();

    // Sharing is prefetched right before the deletion loops, concurrently; deletes stay sequential.
    let snapshot_ids = direct_deletes
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .chain(recovery_point_deletes.iter().map(|point| point.snapshot_id.as_str()))
        .collect::<Vec<&str>>();
    let mut external_use = lookup_snapshot_external_use(
        ec2_client,
        &snapshot_ids,
        usize::from(args.max_concurrency),
        None,
    )
    .await;
    if let Some(check) = &reference_clients.check_accounts {
        let mut scans = AccountScans::new();
        let accounts = accounts_to_scan(&external_use, check);
        let candidates = external_use
            .iter()
            .filter(|(_, usage)| matches!(usage, ExternalUse::Shared(_)))
            .map(|(snapshot_id, _)| snapshot_id.as_str())
            .collect::<Vec<_>>();
        let owner = owner(check, &plan.region, reference_clients);
        scan_accounts(args, check, owner, &accounts, &candidates, &mut scans).await;
        resolve_external_use(&mut external_use, SharedKind::Snapshot, check, &scans);
    }

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

        let result = send_mutation(retry_policy, args.dry_run, SNAPSHOT_GONE, || {
            ec2_client
                .delete_snapshot()
                .snapshot_id(&snapshot.snapshot_id)
                .dry_run(args.dry_run)
                .send()
        })
        .await;

        match result {
            Ok(Sent::Done(_)) => summary.deleted_snapshots += 1,
            Ok(Sent::Gone(code)) => {
                log::warn!("{} was already deleted ({code})", snapshot.snapshot_id);
                summary.skipped_snapshots += 1;
            }
            Err(error) => summary
                .failures
                .push(format!("delete snapshot {} failed: {}", snapshot.snapshot_id, error)),
        }
    }

    for recovery_point in recovery_point_deletes {
        phase.inc();
        if !snapshot_unshared(&external_use, &recovery_point.snapshot_id, summary) {
            summary.skipped_backup_recovery_points += 1;
            continue;
        }
        // AWS Backup has no dry-run mode, so a dry run stops after the checks, which passed.
        if args.dry_run {
            summary.deleted_backup_recovery_points += 1;
            continue;
        }

        let result = send_mutation(retry_policy, false, RECOVERY_POINT_GONE, || {
            backup_client
                .delete_recovery_point()
                .backup_vault_name(&recovery_point.backup_vault_name)
                .recovery_point_arn(&recovery_point.recovery_point_arn)
                .send()
        })
        .await;

        match result {
            Ok(Sent::Done(_)) => summary.deleted_backup_recovery_points += 1,
            Ok(Sent::Gone(code)) => {
                log::warn!("{} was already deleted ({code})", recovery_point.recovery_point_arn);
                summary.skipped_backup_recovery_points += 1;
            }
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
        Some(ExternalUse::Shared(_)) => false,
        Some(ExternalUse::Kept(reason)) => {
            if reason.uncertain() {
                summary.blocked.push(format!("skipped {snapshot_id}: {reason}"));
            }
            false
        }
        Some(ExternalUse::Unknown(reason)) => {
            summary.blocked.push(format!(
                "skipped {snapshot_id}: create-volume permission check failed: {reason}"
            ));
            false
        }
    }
}

/// Sends a mutating request, retrying only throttling and transient failures. Under `dry_run`, the
/// `DryRunOperation` error means the request would have succeeded. (The execute loops use
/// `send_mutation`, which also recognizes resources that are already gone.)
#[cfg(test)]
pub(crate) async fn send_with_retries<T, E, R, F, Fut>(
    policy: RetryPolicy,
    dry_run: bool,
    send: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SdkError<E, R>>>,
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    send_with_retries_output(policy, dry_run, send).await.map(|_| ())
}

/// Like `send_with_retries`, but returns the response (`None` when a dry run passed).
#[cfg(test)]
pub(crate) async fn send_with_retries_output<T, E, R, F, Fut>(
    policy: RetryPolicy,
    dry_run: bool,
    send: F,
) -> Result<Option<T>, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SdkError<E, R>>>,
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    match send_mutation(policy, dry_run, &[], send).await? {
        Sent::Done(output) => Ok(output),
        Sent::Gone(code) => Err(format!("{code} (unexpected)")),
    }
}

/// What a mutation did.
#[derive(Debug)]
pub(crate) enum Sent<T> {
    /// It succeeded (`None`: a dry run that would have).
    Done(Option<T>),
    /// The resource was already gone (this error code): deleted by an earlier attempt whose
    /// response was lost, or by someone else since the live re-check. Not a failure.
    Gone(String),
}

/// `send_with_retries_output`, where an error code in `gone_codes` means the resource no longer
/// exists.
pub(crate) async fn send_mutation<T, E, R, F, Fut>(
    policy: RetryPolicy,
    dry_run: bool,
    gone_codes: &[&str],
    mut send: F,
) -> Result<Sent<T>, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SdkError<E, R>>>,
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let error = match send().await {
            Ok(output) => return Ok(Sent::Done(Some(output))),
            Err(error) => error,
        };

        if dry_run && error.code() == Some(DRY_RUN_OPERATION) {
            return Ok(Sent::Done(None));
        }
        if let Some(code) = error.code().filter(|code| gone_codes.contains(code)) {
            return Ok(Sent::Gone(code.to_string()));
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
