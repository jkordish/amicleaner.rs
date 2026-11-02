//! Plain data types shared by planning and execution.

use std::collections::HashMap;
use std::fmt;
use std::process::ExitCode;

use chrono::{DateTime, Duration, Utc};

use crate::cli::Args;

/// Exit code when nothing failed but safety checks cancelled planned deletions.
pub(crate) const EXIT_BLOCKED: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LastLaunched {
    Never,
    At(DateTime<Utc>),
    /// AWS sent a timestamp we couldn't parse; treated as a recent launch.
    Unknown,
}

#[derive(Clone, Debug)]
pub(crate) struct AmiInfo {
    pub(crate) ami_id: String,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    /// `None` when AWS omitted the creation date or sent one we couldn't parse. Such AMIs are
    /// never deregistered, but stay in inventory so their snapshots remain referenced.
    pub(crate) creation_date: Option<DateTime<Utc>>,
    pub(crate) last_launched: LastLaunched,
    pub(crate) snapshot_ids: Vec<String>,
    pub(crate) tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotInfo {
    pub(crate) snapshot_id: String,
    /// Bytes written to the source volume when the snapshot was taken (falls back to the volume
    /// size). Not the incremental size AWS bills, so cost estimates are upper bounds.
    pub(crate) size_bytes: Option<u64>,
    /// In the EBS Snapshots Archive tier (cheaper storage).
    pub(crate) archived: bool,
    pub(crate) start_time: DateTime<Utc>,
    pub(crate) description: Option<String>,
    pub(crate) tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct BackupRecoveryPoint {
    pub(crate) snapshot_id: String,
    pub(crate) backup_vault_name: String,
    pub(crate) recovery_point_arn: String,
}

/// An EBS recovery point as listed by AWS Backup, along with the vault properties that decide
/// whether it may be deleted.
#[derive(Clone, Debug, Default)]
pub(crate) struct RecoveryPointRecord {
    pub(crate) backup_vault_name: String,
    pub(crate) vault_locked: bool,
    pub(crate) vault_type: Option<String>,
    pub(crate) recovery_point_arn: Option<String>,
    pub(crate) resource_arn: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) is_parent: bool,
    pub(crate) parent_recovery_point_arn: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct BlockedSnapshot {
    pub(crate) snapshot: SnapshotInfo,
    pub(crate) reason: String,
    /// True when the block comes from not knowing something (vs. a definite rule like Vault Lock).
    pub(crate) uncertain: bool,
}

#[derive(Debug, Default)]
pub(crate) struct BackupClassification {
    pub(crate) direct_delete: Vec<SnapshotInfo>,
    pub(crate) backup_managed: Vec<SnapshotInfo>,
    pub(crate) recovery_points_to_delete: Vec<BackupRecoveryPoint>,
    /// Backup-managed snapshots whose recovery point lacks the backup delete tag.
    pub(crate) tag_filtered: Vec<SnapshotInfo>,
    pub(crate) blocked: Vec<BlockedSnapshot>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RegionPlan {
    pub(crate) region: String,
    pub(crate) owned_ami_count: usize,
    pub(crate) owned_snapshot_count: usize,
    /// Owned AMIs that something references.
    pub(crate) referenced_ami_count: usize,
    pub(crate) unresolved_ami_references: Vec<String>,
    pub(crate) deregister_amis: Vec<AmiInfo>,
    /// Snapshot id -> id of the planned AMI whose deregistration leaves the snapshot unreferenced.
    pub(crate) snapshots_released_by_ami: HashMap<String, String>,
    pub(crate) delete_snapshots: Vec<SnapshotInfo>,
    pub(crate) backup_managed_snapshots: Vec<SnapshotInfo>,
    pub(crate) delete_backup_recovery_points: Vec<BackupRecoveryPoint>,
    pub(crate) blocked_backup_snapshots: Vec<BlockedSnapshot>,
    /// Every owned AMI that isn't deregistered, with the rule that kept it.
    pub(crate) kept_amis: Vec<Kept<AmiInfo>>,
    /// Every owned snapshot that isn't deleted or blocked, with the rule that kept it.
    pub(crate) kept_snapshots: Vec<Kept<SnapshotInfo>>,
    /// What references each kept AMI (first few sources), for the report.
    pub(crate) ami_reference_sources: HashMap<String, Vec<String>>,
    /// Why safety checks cancelled planned deletions. Non-empty means exit code 3.
    pub(crate) blocked: Vec<String>,
}

// Count helpers used by the tests; reports derive their numbers from the lists directly.
#[cfg(test)]
impl RegionPlan {
    pub(crate) fn skipped_amis(&self) -> usize {
        self.kept_amis.len()
    }

    pub(crate) fn skipped_snapshots(&self) -> usize {
        self.kept_snapshots.len()
    }

    pub(crate) fn kept_shared_amis(&self) -> usize {
        self.kept_amis.iter().filter(|kept| kept.reason == KeepReason::Shared).count()
    }

    pub(crate) fn kept_shared_snapshots(&self) -> usize {
        self.kept_snapshots.iter().filter(|kept| kept.reason == KeepReason::Shared).count()
    }
}

/// A resource the plan keeps, and the first rule that kept it.
#[derive(Clone, Debug)]
pub(crate) struct Kept<T> {
    pub(crate) item: T,
    pub(crate) reason: KeepReason,
}

/// Why an AMI or snapshot is kept. Policy rules return the first one that applies; later stages
/// (sharing, references, AWS Backup) add their own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeepReason {
    NoCreationDate,
    TooNew,
    Referenced,
    LatestInFamily,
    NoFamilyTag,
    Protected,
    BackupCreated,
    NoDeleteTag,
    RecentlyLaunched,
    LaunchTimeUnknown,
    UsedByRemainingAmi,
    Shared,
    SharingUnknown(String),
    UnresolvedReferences,
    BackupDeleteTagMissing,
}

impl KeepReason {
    /// Stable identifier for machine-readable reports.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::NoCreationDate => "no_creation_date",
            Self::TooNew => "too_new",
            Self::Referenced => "referenced",
            Self::LatestInFamily => "latest_in_family",
            Self::NoFamilyTag => "no_family_tag",
            Self::Protected => "protected",
            Self::BackupCreated => "backup_created",
            Self::NoDeleteTag => "no_delete_tag",
            Self::RecentlyLaunched => "recently_launched",
            Self::LaunchTimeUnknown => "launch_time_unknown",
            Self::UsedByRemainingAmi => "used_by_remaining_ami",
            Self::Shared => "shared",
            Self::SharingUnknown(_) => "sharing_unknown",
            Self::UnresolvedReferences => "unresolved_references",
            Self::BackupDeleteTagMissing => "backup_delete_tag_missing",
        }
    }

    /// Kept because something couldn't be determined (a safety block), not because of a rule.
    pub(crate) fn uncertain(&self) -> bool {
        matches!(self, Self::SharingUnknown(_) | Self::UnresolvedReferences)
    }
}

impl fmt::Display for KeepReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCreationDate => f.write_str("creation date missing or unparseable"),
            Self::TooNew => f.write_str("newer than --min-age-days"),
            Self::Referenced => f.write_str("referenced"),
            Self::LatestInFamily => f.write_str("among the newest of its ImageFamily"),
            Self::NoFamilyTag => {
                f.write_str("no ImageFamily tag (--keep-latest-per-family is set)")
            }
            Self::Protected => f.write_str("protect tag"),
            Self::BackupCreated => f.write_str("created by AWS Backup"),
            Self::NoDeleteTag => f.write_str("no delete tag"),
            Self::RecentlyLaunched => f.write_str("launched within --ami-recent-launch-days"),
            Self::LaunchTimeUnknown => f.write_str("last launch time unparseable"),
            Self::UsedByRemainingAmi => f.write_str("used by an AMI that stays registered"),
            Self::Shared => f.write_str("shared with other accounts"),
            Self::SharingUnknown(error) => write!(f, "sharing lookup failed: {error}"),
            Self::UnresolvedReferences => {
                f.write_str("AMI deregistration cancelled: unresolved AMI references")
            }
            Self::BackupDeleteTagMissing => {
                f.write_str("AWS Backup recovery point without the backup delete tag")
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionSummary {
    pub(crate) dry_run: bool,
    pub(crate) deregistered_amis: usize,
    pub(crate) skipped_amis: usize,
    pub(crate) deleted_snapshots: usize,
    pub(crate) skipped_snapshots: usize,
    pub(crate) deleted_backup_recovery_points: usize,
    pub(crate) skipped_backup_recovery_points: usize,
    /// Safety checks that cancelled work at execution time. Non-empty means exit code 3.
    pub(crate) blocked: Vec<String>,
    pub(crate) failures: Vec<String>,
}

/// Why an AMI or snapshot candidate is kept because other accounts may use it. AMIs built in one
/// account and shared to others are the main case: deleting them breaks launches elsewhere.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExternalUse {
    /// Launch (AMI) or create-volume (snapshot) permissions are granted to other accounts,
    /// organizations, OUs or the public.
    Shared,
    /// The permission lookup failed.
    Unknown(String),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Cutoffs {
    pub(crate) min_age: DateTime<Utc>,
    pub(crate) recent_launch: DateTime<Utc>,
}

impl Cutoffs {
    pub(crate) fn new(args: &Args, now: DateTime<Utc>) -> Self {
        Self {
            min_age: now - Duration::days(args.min_age_days),
            recent_launch: now - Duration::days(args.ami_recent_launch_days),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) base_delay_ms: u64,
}

impl RetryPolicy {
    pub(crate) fn from_args(args: &Args) -> Self {
        Self { max_retries: args.max_retries, base_delay_ms: args.retry_delay_ms }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RunOutcome {
    pub(crate) failed_regions: Vec<String>,
    pub(crate) blocked_regions: Vec<String>,
}

impl RunOutcome {
    pub(crate) fn exit_code(&self) -> ExitCode {
        if !self.failed_regions.is_empty() {
            ExitCode::FAILURE
        } else if !self.blocked_regions.is_empty() {
            ExitCode::from(EXIT_BLOCKED)
        } else {
            ExitCode::SUCCESS
        }
    }
}
