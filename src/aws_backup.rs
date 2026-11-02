//! AWS Backup: lists EBS recovery points and classifies snapshot candidates against them.

use std::collections::{HashMap, HashSet};

use aws_sdk_backup as backup;

use crate::cli::Args;
use crate::errors::{AppResult, AwsResultExt};
use crate::inventory::is_resource_id;
use crate::model::{
    BackupClassification, BackupRecoveryPoint, BlockedSnapshot, RecoveryPointRecord, RegionPlan,
    SnapshotInfo,
};
use crate::policy::{backup_recovery_point_deletion_tag_allows, looks_backup_created};

pub(crate) async fn list_ebs_recovery_points(
    backup_client: &backup::Client,
) -> AppResult<Vec<RecoveryPointRecord>> {
    let mut records = Vec::new();

    let mut vault_pages = backup_client.list_backup_vaults().into_paginator().page_size(100).send();
    while let Some(vault_page) = vault_pages.next().await {
        let vault_output = vault_page.aws_context("ListBackupVaults")?;
        for vault in vault_output.backup_vault_list() {
            let Some(vault_name) = vault.backup_vault_name() else {
                return Err("ListBackupVaults returned a vault without a name".into());
            };

            let mut recovery_point_pages = backup_client
                .list_recovery_points_by_backup_vault()
                .backup_vault_name(vault_name)
                .by_resource_type("EBS")
                .into_paginator()
                .page_size(100)
                .send();

            while let Some(recovery_point_page) = recovery_point_pages.next().await {
                let recovery_point_output = recovery_point_page
                    .aws_context(&format!("ListRecoveryPointsByBackupVault({vault_name})"))?;
                for recovery_point in recovery_point_output.recovery_points() {
                    records.push(RecoveryPointRecord {
                        backup_vault_name: vault_name.to_string(),
                        vault_locked: vault.locked().unwrap_or(false),
                        vault_type: vault
                            .vault_type()
                            .map(|vault_type| vault_type.as_str().to_string()),
                        recovery_point_arn: recovery_point
                            .recovery_point_arn()
                            .map(ToString::to_string),
                        resource_arn: recovery_point.resource_arn().map(ToString::to_string),
                        status: recovery_point.status().map(|status| status.as_str().to_string()),
                        is_parent: recovery_point.is_parent(),
                        parent_recovery_point_arn: recovery_point
                            .parent_recovery_point_arn()
                            .map(ToString::to_string),
                    });
                }
            }
        }
    }

    Ok(records)
}

/// Splits snapshot deletion candidates into direct EC2 deletes, AWS Backup recovery-point deletes,
/// and snapshots that must be kept. Pure, so the mapping rules are unit-testable.
///
/// EBS recovery points *are* snapshots: the snapshot ARN is the recovery point ARN, and the
/// recovery point's tags are the snapshot's EC2 tags. `ResourceArn` is the source volume.
pub(crate) fn classify_backup_snapshots(
    candidates: &[SnapshotInfo],
    records: &[RecoveryPointRecord],
    args: &Args,
) -> BackupClassification {
    let mut classification = BackupClassification::default();
    let mut records_by_snapshot: HashMap<&str, Vec<&RecoveryPointRecord>> = HashMap::new();
    let mut unmappable = Vec::new();

    for record in records {
        match recovery_point_snapshot_id(record) {
            Some(snapshot_id) => records_by_snapshot.entry(snapshot_id).or_default().push(record),
            None => unmappable.push(record),
        }
    }

    if let Some(example) = unmappable.first() {
        // We can't tell which snapshots these recovery points protect, so none may be deleted.
        let reason = format!(
            "{} EBS recovery point(s) could not be mapped to a snapshot (e.g. {} in vault {})",
            unmappable.len(),
            example.recovery_point_arn.as_deref().unwrap_or("<no ARN>"),
            example.backup_vault_name
        );
        classification.blocked = candidates
            .iter()
            .map(|snapshot| BlockedSnapshot {
                snapshot: snapshot.clone(),
                reason: reason.clone(),
                uncertain: true,
            })
            .collect();
        return classification;
    }

    for snapshot in candidates {
        let Some(snapshot_records) = records_by_snapshot.get(snapshot.snapshot_id.as_str()) else {
            if looks_backup_created(&snapshot.tags, snapshot.description.as_deref()) {
                classification.blocked.push(BlockedSnapshot {
                    snapshot: snapshot.clone(),
                    reason: "looks AWS Backup-created but no matching recovery point was found"
                        .to_string(),
                    uncertain: true,
                });
            } else {
                classification.direct_delete.push(snapshot.clone());
            }
            continue;
        };

        classification.backup_managed.push(snapshot.clone());

        if let Some(reason) = recovery_point_block_reason(snapshot_records) {
            classification.blocked.push(BlockedSnapshot {
                snapshot: snapshot.clone(),
                reason,
                uncertain: false,
            });
            continue;
        }

        if !backup_recovery_point_deletion_tag_allows(&snapshot.tags, args) {
            classification.tag_filtered += 1;
            continue;
        }

        let record = snapshot_records[0];
        if let Some(recovery_point_arn) = record.recovery_point_arn.clone() {
            classification.recovery_points_to_delete.push(BackupRecoveryPoint {
                snapshot_id: snapshot.snapshot_id.clone(),
                backup_vault_name: record.backup_vault_name.clone(),
                recovery_point_arn,
            });
        }
    }

    classification
}

fn recovery_point_snapshot_id(record: &RecoveryPointRecord) -> Option<&str> {
    record
        .recovery_point_arn
        .as_deref()
        .and_then(snapshot_id_from_arn)
        .or_else(|| record.resource_arn.as_deref().and_then(snapshot_id_from_arn))
}

/// Extracts `snap-…` from an EC2 snapshot ARN (`arn:aws:ec2:<region>::snapshot/snap-…`).
pub(crate) fn snapshot_id_from_arn(arn: &str) -> Option<&str> {
    let resource = arn.rsplit(':').next()?;
    let snapshot_id = resource.strip_prefix("snapshot/")?;
    is_resource_id(snapshot_id, "snap-").then_some(snapshot_id)
}

fn recovery_point_block_reason(records: &[&RecoveryPointRecord]) -> Option<String> {
    let [record] = records else {
        return Some(format!("{} recovery points reference this snapshot", records.len()));
    };

    if record.recovery_point_arn.is_none() {
        return Some("recovery point has no ARN".to_string());
    }
    if record.status.as_deref() != Some("COMPLETED") {
        return Some(format!(
            "recovery point status is {}",
            record.status.as_deref().unwrap_or("unknown")
        ));
    }
    if record.vault_locked {
        return Some(format!("vault {} has Vault Lock enabled", record.backup_vault_name));
    }
    if let Some(vault_type) = record.vault_type.as_deref()
        && vault_type != "BACKUP_VAULT"
    {
        return Some(format!("vault {} is a {} vault", record.backup_vault_name, vault_type));
    }
    if record.is_parent || record.parent_recovery_point_arn.is_some() {
        return Some("recovery point is part of a composite backup".to_string());
    }

    None
}

pub(crate) fn apply_backup_classification(
    plan: &mut RegionPlan,
    classification: BackupClassification,
) {
    let uncertain = classification.blocked.iter().filter(|blocked| blocked.uncertain).count();
    if uncertain > 0 {
        plan.blocked.push(format!(
            "{uncertain} snapshot deletion(s) cancelled: AWS Backup ownership could not be determined"
        ));
    }

    plan.delete_snapshots = classification.direct_delete;
    plan.backup_managed_snapshots = classification.backup_managed;
    plan.delete_backup_recovery_points = classification.recovery_points_to_delete;
    plan.tag_filtered_backup_recovery_points = classification.tag_filtered;
    plan.blocked_backup_snapshots = classification.blocked;
    prune_released_snapshots(plan);
}

pub(crate) fn block_all_snapshot_deletions(plan: &mut RegionPlan, reason: &str) {
    plan.blocked.push(format!(
        "{} snapshot deletion(s) cancelled: {}",
        plan.delete_snapshots.len(),
        reason
    ));
    plan.blocked_backup_snapshots = plan
        .delete_snapshots
        .drain(..)
        .map(|snapshot| BlockedSnapshot { snapshot, reason: reason.to_string(), uncertain: true })
        .collect();
    plan.backup_managed_snapshots.clear();
    plan.delete_backup_recovery_points.clear();
    plan.tag_filtered_backup_recovery_points = 0;
    prune_released_snapshots(plan);
}

/// Keeps `snapshots_released_by_ami` limited to snapshots that are still planned for deletion.
pub(crate) fn prune_released_snapshots(plan: &mut RegionPlan) {
    let planned = plan
        .delete_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .chain(plan.delete_backup_recovery_points.iter().map(|point| point.snapshot_id.as_str()))
        .map(ToString::to_string)
        .collect::<HashSet<String>>();
    plan.snapshots_released_by_ami.retain(|snapshot_id, _| planned.contains(snapshot_id));
}
