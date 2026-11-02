//! Builds a region plan: inventory and references (I/O), then decisions (pure `plan_region`).

use std::collections::{HashMap, HashSet};

use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use chrono::{DateTime, Utc};
use log::warn;

use crate::aws_backup::{
    apply_backup_classification, block_all_snapshot_deletions, classify_backup_snapshots,
    list_ebs_recovery_points, prune_released_snapshots,
};
use crate::cli::Args;
use crate::errors::{AppResult, error_chain};
use crate::inventory::{
    list_owned_amis, list_owned_snapshots, lookup_ami_external_use, lookup_snapshot_external_use,
};
use crate::model::{AmiInfo, Cutoffs, ExternalUse, KeepReason, Kept, RegionPlan, SnapshotInfo};
use crate::policy::{
    ami_keep_reason, select_latest_ami_ids_by_family, should_deregister_ami, snapshot_keep_reason,
};
use crate::references::{AmiReferenceScan, ReferenceClients, collect_referenced_ami_ids};
use crate::ui::{Phase, thousands};

pub(crate) async fn build_region_plan(
    args: &Args,
    region: &str,
    ec2_client: &ec2::Client,
    backup_client: &backup::Client,
    reference_clients: &ReferenceClients,
) -> AppResult<RegionPlan> {
    let now = Utc::now();
    let phase = Phase::start(format!("{region} · inventory"));
    let inventory = async {
        let amis = list_owned_amis(ec2_client).await?;
        phase.message(format!("{} AMIs", thousands(amis.len())));
        let snapshots = list_owned_snapshots(ec2_client).await?;
        AppResult::Ok((amis, snapshots))
    }
    .await;
    let (owned_amis, owned_snapshots) = match inventory {
        Ok(inventory) => inventory,
        Err(error) => {
            phase.fail("failed");
            return Err(error);
        }
    };
    phase.finish(format!(
        "{} AMIs · {} snapshots",
        thousands(owned_amis.len()),
        thousands(owned_snapshots.len())
    ));

    // A failed reference scan only blocks AMI deregistration; snapshot cleanup doesn't depend on it.
    let reference_scan = collect_referenced_ami_ids(ec2_client, reference_clients)
        .await
        .unwrap_or_else(|error| AmiReferenceScan {
            unresolved_references: HashSet::from([format!(
                "AMI reference scan failed: {}",
                error_chain(error.as_ref())
            )]),
            ..AmiReferenceScan::default()
        });

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
        if !candidates.is_empty() {
            let phase =
                Phase::with_total(format!("{region} · AMI launch permissions"), candidates.len());
            external_use =
                lookup_ami_external_use(ec2_client, &candidates, concurrency, Some(&phase)).await;
            finish_sharing_phase(phase, &external_use);
        }
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
    let mut snapshot_external_use = HashMap::new();
    if !snapshot_ids.is_empty() {
        let phase = Phase::with_total(
            format!("{region} · snapshot create-volume permissions"),
            snapshot_ids.len(),
        );
        snapshot_external_use =
            lookup_snapshot_external_use(ec2_client, &snapshot_ids, concurrency, Some(&phase))
                .await;
        finish_sharing_phase(phase, &snapshot_external_use);
    }
    apply_snapshot_external_use(&mut plan, &snapshot_external_use);

    if !plan.delete_snapshots.is_empty() {
        let phase = Phase::start(format!("{region} · AWS Backup"));
        match list_ebs_recovery_points(backup_client).await {
            Ok(records) => {
                phase.finish(format!("{} EBS recovery points", thousands(records.len())));
                let classification =
                    classify_backup_snapshots(&plan.delete_snapshots, &records, args);
                apply_backup_classification(&mut plan, classification);
            }
            Err(error) => {
                let reason =
                    format!("AWS Backup inventory unavailable: {}", error_chain(error.as_ref()));
                phase.warn("inventory unavailable; snapshot deletions blocked");
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
    let mut kept_amis = Vec::new();

    for ami in owned_amis {
        let reason = ami_keep_reason(ami, referenced_ami_ids, &keep_latest_ami_ids, cutoffs, args)
            .or_else(|| match external_use.get(&ami.ami_id) {
                None => None,
                Some(ExternalUse::Shared) => Some(KeepReason::Shared),
                Some(ExternalUse::Unknown(error)) => {
                    blocked.push(format!(
                        "AMI {} kept: launch permission lookup failed: {}",
                        ami.ami_id, error
                    ));
                    Some(KeepReason::SharingUnknown(error.clone()))
                }
            });
        match reason {
            Some(reason) => kept_amis.push(Kept { item: ami.clone(), reason }),
            None => deregister_amis.push(ami.clone()),
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
        kept_amis.extend(
            deregister_amis
                .drain(..)
                .map(|ami| Kept { item: ami, reason: KeepReason::UnresolvedReferences }),
        );
    }

    let ami_reference_sources = kept_amis
        .iter()
        .filter(|kept| kept.reason == KeepReason::Referenced)
        .filter_map(|kept| {
            reference_scan
                .sources
                .get(&kept.item.ami_id)
                .map(|sources| (kept.item.ami_id.clone(), sources.clone()))
        })
        .collect::<HashMap<String, Vec<String>>>();

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
    let mut kept_snapshots = Vec::new();

    for snapshot in owned_snapshots {
        let released = snapshots_released_by_ami.contains_key(&snapshot.snapshot_id);
        match snapshot_keep_reason(
            snapshot,
            released,
            &remaining_ami_snapshot_refs,
            cutoffs.min_age,
            args,
        ) {
            None => delete_snapshots.push(snapshot.clone()),
            Some(reason) => kept_snapshots.push(Kept { item: snapshot.clone(), reason }),
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
        // Owned AMIs only: the scan also sees AWS-owned and other accounts' AMIs.
        referenced_ami_count: owned_amis
            .iter()
            .filter(|ami| referenced_ami_ids.contains(&ami.ami_id))
            .count(),
        unresolved_ami_references,
        deregister_amis,
        snapshots_released_by_ami,
        delete_snapshots,
        backup_managed_snapshots: Vec::new(),
        delete_backup_recovery_points: Vec::new(),
        blocked_backup_snapshots: Vec::new(),
        kept_amis,
        kept_snapshots,
        ami_reference_sources,
        blocked,
    }
}

fn finish_sharing_phase(phase: Phase, external_use: &HashMap<String, ExternalUse>) {
    let shared = external_use.values().filter(|usage| **usage == ExternalUse::Shared).count();
    let unknown = external_use.len() - shared;
    if unknown > 0 {
        phase.warn(format!("{shared} shared, {unknown} lookups failed"));
    } else {
        phase.finish(format!("{shared} shared"));
    }
}

/// Drops shared snapshots from the deletion plan. A failed lookup also keeps the snapshot and
/// counts as a safety block, since "couldn't tell" must never mean "not shared".
pub(crate) fn apply_snapshot_external_use(
    plan: &mut RegionPlan,
    external_use: &HashMap<String, ExternalUse>,
) {
    let mut kept = Vec::new();
    let mut blocked = Vec::new();
    plan.delete_snapshots.retain(|snapshot| {
        let reason = match external_use.get(&snapshot.snapshot_id) {
            None => return true,
            Some(ExternalUse::Shared) => KeepReason::Shared,
            Some(ExternalUse::Unknown(error)) => {
                blocked.push(format!(
                    "snapshot {} kept: create-volume permission lookup failed: {}",
                    snapshot.snapshot_id, error
                ));
                KeepReason::SharingUnknown(error.clone())
            }
        };
        kept.push(Kept { item: snapshot.clone(), reason });
        false
    });
    plan.kept_snapshots.extend(kept);
    plan.blocked.extend(blocked);
    prune_released_snapshots(plan);
}
