//! Builds a region plan: inventory and references (I/O), then decisions (pure `plan_region`).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use chrono::{DateTime, Utc};
use log::{info, warn};

use crate::aws_backup::{
    apply_backup_classification, block_all_snapshot_deletions, classify_backup_snapshots,
    list_ebs_recovery_points, prune_released_snapshots,
};
use crate::cli::Args;
use crate::errors::{AppResult, error_chain};
use crate::inventory::{
    list_owned_amis, list_owned_snapshots, lookup_ami_external_use, lookup_snapshot_external_use,
};
use crate::model::{AmiInfo, Cutoffs, ExternalUse, LastLaunched, RegionPlan, SnapshotInfo};
use crate::policy::{
    ami_family, select_latest_ami_ids_by_family, should_deregister_ami, snapshot_is_deletable,
};
use crate::references::{AmiReferenceScan, ReferenceClients, collect_referenced_ami_ids};

pub(crate) async fn build_region_plan(
    args: &Args,
    region: &str,
    ec2_client: &ec2::Client,
    backup_client: &backup::Client,
    reference_clients: &ReferenceClients,
) -> AppResult<RegionPlan> {
    let now = Utc::now();
    let owned_amis = list_owned_amis(ec2_client).await?;
    let owned_snapshots = list_owned_snapshots(ec2_client).await?;
    info!(
        "region {region}: {} owned AMIs, {} owned snapshots; scanning AMI references",
        owned_amis.len(),
        owned_snapshots.len()
    );

    // A failed reference scan only blocks AMI deregistration; snapshot cleanup doesn't depend on it.
    let started = Instant::now();
    let reference_scan = collect_referenced_ami_ids(ec2_client, reference_clients)
        .await
        .unwrap_or_else(|error| AmiReferenceScan {
            referenced_ami_ids: HashSet::new(),
            unresolved_references: HashSet::from([format!(
                "AMI reference scan failed: {}",
                error_chain(error.as_ref())
            )]),
        });

    info!(
        "region {region}: reference scan found {} referenced AMIs, {} unresolved references in {:.0?}",
        reference_scan.referenced_ami_ids.len(),
        reference_scan.unresolved_references.len(),
        started.elapsed()
    );

    // Launch permissions are looked up per candidate, so only bother when deregistration can happen.
    let concurrency = usize::from(args.max_concurrency);
    let mut external_use = HashMap::new();
    if reference_scan.unresolved_references.is_empty() {
        let cutoffs = Cutoffs::new(args, now);
        let keep_latest = select_latest_ami_ids_by_family(&owned_amis, args.keep_latest_per_family);
        let candidates = owned_amis
            .iter()
            .filter(|ami| {
                should_deregister_ami(
                    ami,
                    &reference_scan.referenced_ami_ids,
                    &keep_latest,
                    cutoffs,
                    args,
                )
            })
            .map(|ami| ami.ami_id.as_str())
            .collect::<Vec<&str>>();
        info!(
            "region {region}: checking launch permissions of {} AMI candidates",
            candidates.len()
        );
        external_use = lookup_ami_external_use(ec2_client, &candidates, concurrency).await;
    }

    let mut plan = plan_region(
        args,
        region,
        now,
        &owned_amis,
        &owned_snapshots,
        &reference_scan,
        &external_use,
    );

    // Snapshots shared to other accounts are kept, like shared AMIs. Checked before the Backup
    // classification so shared recovery-point snapshots are kept too.
    let snapshot_ids = plan
        .delete_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .collect::<Vec<_>>();
    info!(
        "region {region}: checking create-volume permissions of {} snapshot candidates",
        snapshot_ids.len()
    );
    let snapshot_external_use =
        lookup_snapshot_external_use(ec2_client, &snapshot_ids, concurrency).await;
    apply_snapshot_external_use(&mut plan, &snapshot_external_use);

    if !plan.delete_snapshots.is_empty() {
        match list_ebs_recovery_points(backup_client).await {
            Ok(records) => {
                let classification =
                    classify_backup_snapshots(&plan.delete_snapshots, &records, args);
                apply_backup_classification(&mut plan, classification);
            }
            Err(error) => {
                let reason =
                    format!("AWS Backup inventory unavailable: {}", error_chain(error.as_ref()));
                warn!("region {region}: {reason}");
                block_all_snapshot_deletions(&mut plan, &reason);
            }
        }
    }

    Ok(plan)
}

pub(crate) fn plan_region(
    args: &Args,
    region: &str,
    now: DateTime<Utc>,
    owned_amis: &[AmiInfo],
    owned_snapshots: &[SnapshotInfo],
    reference_scan: &AmiReferenceScan,
    external_use: &HashMap<String, ExternalUse>,
) -> RegionPlan {
    let cutoffs = Cutoffs::new(args, now);
    let referenced_ami_ids = &reference_scan.referenced_ami_ids;
    let keep_latest_ami_ids =
        select_latest_ami_ids_by_family(owned_amis, args.keep_latest_per_family);

    let mut blocked = Vec::new();
    let mut deregister_amis = Vec::new();
    let mut skipped_amis = 0usize;
    let mut kept_shared_amis = 0usize;

    for ami in owned_amis {
        if !should_deregister_ami(ami, referenced_ami_ids, &keep_latest_ami_ids, cutoffs, args) {
            skipped_amis += 1;
            continue;
        }

        match external_use.get(&ami.ami_id) {
            None => deregister_amis.push(ami.clone()),
            Some(ExternalUse::Shared) => {
                skipped_amis += 1;
                kept_shared_amis += 1;
            }
            Some(ExternalUse::Unknown(reason)) => {
                skipped_amis += 1;
                blocked.push(format!(
                    "AMI {} kept: launch permission lookup failed: {}",
                    ami.ami_id, reason
                ));
            }
        }
    }

    let mut unresolved_ami_references =
        reference_scan.unresolved_references.iter().cloned().collect::<Vec<String>>();
    unresolved_ami_references.sort_unstable();

    if !unresolved_ami_references.is_empty() && !deregister_amis.is_empty() {
        blocked.push(format!(
            "{} AMI deregistration(s) cancelled: {} unresolved AMI reference(s)",
            deregister_amis.len(),
            unresolved_ami_references.len()
        ));
        skipped_amis += deregister_amis.len();
        deregister_amis.clear();
    }

    let kept_untagged_family_amis = if args.keep_latest_per_family > 0 {
        owned_amis.iter().filter(|ami| ami_family(ami).is_none()).count()
    } else {
        0
    };

    let deregister_ami_ids =
        deregister_amis.iter().map(|ami| ami.ami_id.clone()).collect::<HashSet<String>>();

    let remaining_ami_snapshot_refs = owned_amis
        .iter()
        .filter(|ami| !deregister_ami_ids.contains(&ami.ami_id))
        .flat_map(|ami| ami.snapshot_ids.iter().cloned())
        .collect::<HashSet<String>>();

    let mut snapshots_released_by_ami = HashMap::new();
    for ami in &deregister_amis {
        for snapshot_id in &ami.snapshot_ids {
            if !remaining_ami_snapshot_refs.contains(snapshot_id) {
                snapshots_released_by_ami.insert(snapshot_id.clone(), ami.ami_id.clone());
            }
        }
    }

    let mut delete_snapshots = Vec::new();
    let mut skipped_snapshots = 0usize;

    for snapshot in owned_snapshots {
        let released = snapshots_released_by_ami.contains_key(&snapshot.snapshot_id);
        if snapshot_is_deletable(
            snapshot,
            released,
            &remaining_ami_snapshot_refs,
            cutoffs.min_age,
            args,
        ) {
            delete_snapshots.push(snapshot.clone());
        } else {
            skipped_snapshots += 1;
        }
    }

    let planned_snapshot_ids = delete_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .collect::<HashSet<&str>>();
    snapshots_released_by_ami
        .retain(|snapshot_id, _| planned_snapshot_ids.contains(snapshot_id.as_str()));

    RegionPlan {
        region: region.to_string(),
        owned_ami_count: owned_amis.len(),
        owned_snapshot_count: owned_snapshots.len(),
        referenced_ami_count: referenced_ami_ids.len(),
        unresolved_ami_references,
        referenced_snapshot_count_after_ami_keeps: remaining_ami_snapshot_refs.len(),
        deregister_amis,
        snapshots_released_by_ami,
        delete_snapshots,
        backup_managed_snapshots: Vec::new(),
        delete_backup_recovery_points: Vec::new(),
        blocked_backup_snapshots: Vec::new(),
        tag_filtered_backup_recovery_points: 0,
        kept_untagged_family_amis,
        kept_shared_amis,
        kept_shared_snapshots: 0,
        skipped_amis,
        skipped_snapshots,
        blocked,
    }
}

/// Drops shared snapshots from the deletion plan. A failed lookup also keeps the snapshot and
/// counts as a safety block, since "couldn't tell" must never mean "not shared".
pub(crate) fn apply_snapshot_external_use(
    plan: &mut RegionPlan,
    external_use: &HashMap<String, ExternalUse>,
) {
    let mut kept = 0usize;
    let mut shared = 0usize;
    let mut blocked = Vec::new();
    plan.delete_snapshots.retain(|snapshot| match external_use.get(&snapshot.snapshot_id) {
        None => true,
        Some(ExternalUse::Shared) => {
            kept += 1;
            shared += 1;
            false
        }
        Some(ExternalUse::Unknown(reason)) => {
            kept += 1;
            blocked.push(format!(
                "snapshot {} kept: create-volume permission lookup failed: {}",
                snapshot.snapshot_id, reason
            ));
            false
        }
    });
    plan.skipped_snapshots += kept;
    plan.kept_shared_snapshots += shared;
    plan.blocked.extend(blocked);
    prune_released_snapshots(plan);
}

pub(crate) fn print_region_plan(plan: &RegionPlan) {
    println!("\n=== Region {} ===", plan.region);
    println!(
        "owned_amis={} referenced_amis={} unresolved_ami_references={} candidate_amis={} skipped_amis={} kept_shared_amis={} kept_untagged_family_amis={}",
        plan.owned_ami_count,
        plan.referenced_ami_count,
        plan.unresolved_ami_references.len(),
        plan.deregister_amis.len(),
        plan.skipped_amis,
        plan.kept_shared_amis,
        plan.kept_untagged_family_amis,
    );
    println!(
        "owned_snapshots={} referenced_snapshots_after_ami_keeps={} candidate_snapshots={} released_by_ami_snapshots={} backup_managed_candidates={} backup_recovery_point_candidates={} tag_filtered_backup_recovery_points={} blocked_snapshots={} kept_shared_snapshots={} skipped_snapshots={}",
        plan.owned_snapshot_count,
        plan.referenced_snapshot_count_after_ami_keeps,
        plan.delete_snapshots.len(),
        plan.snapshots_released_by_ami.len(),
        plan.backup_managed_snapshots.len(),
        plan.delete_backup_recovery_points.len(),
        plan.tag_filtered_backup_recovery_points,
        plan.blocked_backup_snapshots.len(),
        plan.kept_shared_snapshots,
        plan.skipped_snapshots,
    );

    if !plan.deregister_amis.is_empty() {
        println!("deregister_amis:");
        for ami in &plan.deregister_amis {
            println!(
                "  {} {} created={} last_launched={}",
                ami.ami_id,
                ami.name.as_deref().unwrap_or("<unnamed>"),
                ami.creation_date.map_or_else(|| "unknown".to_string(), |value| value.to_rfc3339()),
                match ami.last_launched {
                    LastLaunched::Never => "never".to_string(),
                    LastLaunched::At(value) => value.to_rfc3339(),
                    LastLaunched::Unknown => "unknown".to_string(),
                }
            );
        }
    }

    if !plan.unresolved_ami_references.is_empty() {
        println!("unresolved_ami_references:");
        for unresolved in plan.unresolved_ami_references.iter().take(20) {
            println!("  {unresolved}");
        }
        if plan.unresolved_ami_references.len() > 20 {
            println!(
                "  ... {} additional unresolved references omitted",
                plan.unresolved_ami_references.len() - 20
            );
        }
    }

    if !plan.delete_snapshots.is_empty() {
        println!("delete_snapshots:");
        for snapshot in &plan.delete_snapshots {
            match plan.snapshots_released_by_ami.get(&snapshot.snapshot_id) {
                Some(ami_id) => println!(
                    "  {} created={} released_by={}",
                    snapshot.snapshot_id,
                    snapshot.start_time.to_rfc3339(),
                    ami_id
                ),
                None => println!(
                    "  {} created={}",
                    snapshot.snapshot_id,
                    snapshot.start_time.to_rfc3339()
                ),
            }
        }
    }

    if !plan.delete_backup_recovery_points.is_empty() {
        println!("delete_backup_recovery_points:");
        for recovery_point in &plan.delete_backup_recovery_points {
            println!(
                "  snapshot={} vault={} recovery_point={}",
                recovery_point.snapshot_id,
                recovery_point.backup_vault_name,
                recovery_point.recovery_point_arn
            );
        }
    }

    if !plan.blocked_backup_snapshots.is_empty() {
        println!("blocked_snapshots:");
        for blocked in &plan.blocked_backup_snapshots {
            println!(
                "  {} reason={} description={}",
                blocked.snapshot.snapshot_id,
                blocked.reason,
                blocked.snapshot.description.as_deref().unwrap_or("<none>")
            );
        }
    }

    if !plan.blocked.is_empty() {
        println!("blocked_by_safety_checks:");
        for reason in &plan.blocked {
            println!("  {reason}");
        }
    }
}
