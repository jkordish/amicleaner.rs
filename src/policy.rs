//! Pure eligibility rules: tags, protection, keep-latest, and the deregister/delete predicates.
//! Plan and execute both call these, so a rule change applies to both phases.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::cli::Args;
use crate::model::{AmiInfo, Cutoffs, KeepReason, LastLaunched, SnapshotInfo};

/// Tag that groups AMIs for `--keep-latest-per-family`.
const FAMILY_TAG: &str = "ImageFamily";

/// Prefix of the tags AWS Backup puts on the AMIs and snapshots it creates.
const AWS_BACKUP_TAG_PREFIX: &str = "aws:backup:";

pub(crate) fn should_deregister_ami(
    ami: &AmiInfo,
    referenced_ami_ids: &HashSet<String>,
    keep_latest_ami_ids: &HashSet<String>,
    cutoffs: Cutoffs,
    args: &Args,
) -> bool {
    ami_keep_reason(ami, referenced_ami_ids, keep_latest_ami_ids, cutoffs, args).is_none()
}

/// The first rule that keeps an AMI, or `None` when it may be deregistered (subject to the
/// sharing and unresolved-reference checks done by the planner).
pub(crate) fn ami_keep_reason(
    ami: &AmiInfo,
    referenced_ami_ids: &HashSet<String>,
    keep_latest_ami_ids: &HashSet<String>,
    cutoffs: Cutoffs,
    args: &Args,
) -> Option<KeepReason> {
    let Some(creation_date) = ami.creation_date else {
        return Some(KeepReason::NoCreationDate);
    };
    if creation_date >= cutoffs.min_age {
        return Some(KeepReason::TooNew);
    }
    if referenced_ami_ids.contains(&ami.ami_id) {
        return Some(KeepReason::Referenced);
    }
    if keep_latest_ami_ids.contains(&ami.ami_id) {
        return Some(KeepReason::LatestInFamily);
    }
    if args.keep_latest_per_family > 0 && ami_family(ami).is_none() {
        return Some(KeepReason::NoFamilyTag);
    }
    if is_protected(&ami.tags, args) {
        return Some(KeepReason::Protected);
    }
    if looks_backup_created(&ami.tags, ami.description.as_deref()) {
        return Some(KeepReason::BackupCreated);
    }
    if !deletion_tag_allows(&ami.tags, args) {
        return Some(KeepReason::NoDeleteTag);
    }
    match ami.last_launched {
        LastLaunched::Never => None,
        LastLaunched::At(last_launched) if last_launched < cutoffs.recent_launch => None,
        LastLaunched::At(_) => Some(KeepReason::RecentlyLaunched),
        LastLaunched::Unknown => Some(KeepReason::LaunchTimeUnknown),
    }
}

pub(crate) fn snapshot_is_deletable(
    snapshot: &SnapshotInfo,
    released_by_deregistered_ami: bool,
    referenced_snapshot_ids: &HashSet<String>,
    min_age_cutoff: DateTime<Utc>,
    args: &Args,
) -> bool {
    snapshot_keep_reason(
        snapshot,
        released_by_deregistered_ami,
        referenced_snapshot_ids,
        min_age_cutoff,
        args,
    )
    .is_none()
}

/// The first rule that keeps a snapshot, or `None` when it may be deleted (subject to the
/// sharing and AWS Backup checks done by the planner).
pub(crate) fn snapshot_keep_reason(
    snapshot: &SnapshotInfo,
    released_by_deregistered_ami: bool,
    referenced_snapshot_ids: &HashSet<String>,
    min_age_cutoff: DateTime<Utc>,
    args: &Args,
) -> Option<KeepReason> {
    if snapshot.start_time >= min_age_cutoff {
        return Some(KeepReason::TooNew);
    }
    if referenced_snapshot_ids.contains(&snapshot.snapshot_id) {
        return Some(KeepReason::UsedByRemainingAmi);
    }
    if is_protected(&snapshot.tags, args) {
        return Some(KeepReason::Protected);
    }
    if !snapshot_deletion_authorized(&snapshot.tags, released_by_deregistered_ami, args) {
        return Some(KeepReason::NoDeleteTag);
    }
    None
}

/// Whether a snapshot is authorized for deletion on delete-tag grounds. Age, references and the
/// protect tag are checked separately by `snapshot_is_deletable` and can't be overridden here.
///
/// A snapshot freed by deregistering its AMI inherits that AMI's authorization (the AMI already
/// carried the delete tag), so AMI snapshots don't leak just because only the AMI was tagged.
fn snapshot_deletion_authorized(
    snapshot_tags: &HashMap<String, String>,
    released_by_deregistered_ami: bool,
    args: &Args,
) -> bool {
    released_by_deregistered_ami || deletion_tag_allows(snapshot_tags, args)
}

pub(crate) fn optional_expected_value(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

pub(crate) fn has_expected_tag(
    tags: &HashMap<String, String>,
    key: &str,
    expected_value: Option<&str>,
) -> bool {
    let Some(actual_value) = tags.get(key) else {
        return false;
    };

    expected_value.is_none_or(|expected| actual_value.trim().eq_ignore_ascii_case(expected))
}

/// Protection fails closed: the key matches case-insensitively, and unless a protect value is
/// configured, any value (even empty or "false") protects.
pub(crate) fn is_protected(tags: &HashMap<String, String>, args: &Args) -> bool {
    let expected_value = optional_expected_value(&args.protect_tag_value);
    tags.iter().any(|(key, value)| {
        key.trim().eq_ignore_ascii_case(&args.protect_tag)
            && expected_value.is_none_or(|expected| value.trim().eq_ignore_ascii_case(expected))
    })
}

fn deletion_tag_allows(tags: &HashMap<String, String>, args: &Args) -> bool {
    if args.allow_untagged {
        return true;
    }

    has_expected_tag(tags, &args.delete_tag, optional_expected_value(&args.delete_tag_value))
}

pub(crate) fn backup_recovery_point_deletion_tag_allows(
    tags: &HashMap<String, String>,
    args: &Args,
) -> bool {
    if args.allow_untagged_backup_recovery_points {
        return true;
    }

    has_expected_tag(
        tags,
        &args.backup_delete_tag,
        optional_expected_value(&args.backup_delete_tag_value),
    )
}

/// Heuristic for AMIs/snapshots created by AWS Backup. A false positive only keeps a resource.
pub(crate) fn looks_backup_created(
    tags: &HashMap<String, String>,
    description: Option<&str>,
) -> bool {
    tags.keys().any(|key| key.starts_with(AWS_BACKUP_TAG_PREFIX))
        || description.is_some_and(|description| description.contains("AWS Backup"))
}

pub(crate) fn ami_family(ami: &AmiInfo) -> Option<&str> {
    ami.tags.get(FAMILY_TAG).map(|family| family.trim()).filter(|family| !family.is_empty())
}

pub(crate) fn select_latest_ami_ids_by_family(
    amis: &[AmiInfo],
    keep_count: usize,
) -> HashSet<String> {
    if keep_count == 0 {
        return HashSet::new();
    }

    let mut families: HashMap<&str, Vec<&AmiInfo>> = HashMap::new();

    for ami in amis {
        if let Some(family) = ami_family(ami) {
            families.entry(family).or_default().push(ami);
        }
    }

    let mut keep_ids = HashSet::new();

    for (_family, mut family_amis) in families {
        // Newest first; AMIs without a creation date sort last so they don't take a keep slot
        // (they're never deregistered anyway). The id breaks ties deterministically.
        family_amis.sort_by(|a, b| {
            b.creation_date.cmp(&a.creation_date).then_with(|| b.ami_id.cmp(&a.ami_id))
        });
        for ami in family_amis.into_iter().take(keep_count) {
            keep_ids.insert(ami.ami_id.clone());
        }
    }

    keep_ids
}
