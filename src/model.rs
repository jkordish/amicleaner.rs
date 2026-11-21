//! Plain data types shared by planning and execution.

use std::collections::{BTreeMap, HashMap};
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
    /// `DescribeImages` state (`available`, `pending`, `failed`, `disabled`, ...); `None` if AWS
    /// omitted it.
    pub(crate) state: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotInfo {
    pub(crate) snapshot_id: String,
    /// Bytes written to the source volume when the snapshot was taken (falls back to the volume
    /// size). Not the incremental size AWS bills, so cost estimates are upper bounds.
    pub(crate) size_bytes: Option<u64>,
    /// In the EBS Snapshots Archive tier (cheaper storage).
    pub(crate) archived: bool,
    /// `None` when AWS omitted it; such a snapshot is always kept (`NoStartTime`).
    pub(crate) start_time: Option<DateTime<Utc>>,
    /// `DescribeSnapshots` state (`completed`, `pending`, `error`, ...); `None` if omitted.
    pub(crate) state: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) tags: HashMap<String, String>,
    /// Volume the snapshot was taken from (`vol-ffffffff` for copies).
    pub(crate) volume_id: Option<String>,
    /// Set by `dlm::orphaned_dlm_snapshots` for a DLM snapshot DLM will never retire, and only
    /// with `--delete-orphaned-dlm-snapshots`. `None` keeps the lifecycle-managed rule in force.
    pub(crate) dlm_orphan: Option<DlmOrphanCause>,
}

/// What `--check-accounts` found in one region.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountCheckSummary {
    /// "profile (account)" for every account scanned, and whether its scan succeeded.
    pub(crate) accounts: Vec<(String, bool)>,
    /// Shared AMIs / snapshots no checked account uses, so the normal rules decide them.
    pub(crate) unused_amis: usize,
    pub(crate) unused_snapshots: usize,
}

/// Whether planned snapshot deletes would land in the Recycle Bin.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum RecycleBinCoverage {
    /// Nothing to delete, so not looked up.
    #[default]
    NotChecked,
    /// The rules couldn't be read.
    Unknown(String),
    /// Days each planned snapshot delete stays restorable; a planned snapshot without an entry
    /// would be deleted permanently.
    Checked(HashMap<String, u32>),
}

/// Why Data Lifecycle Manager will never retire a snapshot it made.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum DlmOrphanCause {
    /// The policy no longer exists; deleting a policy doesn't delete its snapshots.
    PolicyDeleted,
    /// The schedule that made it was renamed or removed from the policy.
    ScheduleRemoved,
    /// Count-based schedule whose source volume is gone; the policy stops managing those.
    VolumeDeleted,
}

impl DlmOrphanCause {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::PolicyDeleted => "dlm_policy_deleted",
            Self::ScheduleRemoved => "dlm_schedule_removed",
            Self::VolumeDeleted => "dlm_volume_deleted",
        }
    }
}

impl fmt::Display for DlmOrphanCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PolicyDeleted => "orphaned DLM snapshot: its policy was deleted",
            Self::ScheduleRemoved => {
                "orphaned DLM snapshot: its schedule was removed from the policy"
            }
            Self::VolumeDeleted => {
                "orphaned DLM snapshot: source volume deleted (count-based policy)"
            }
        })
    }
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
    /// Snapshot id -> the planned AMIs whose deregistration leaves the snapshot unreferenced (all
    /// of them must be deregistered for it to be freed).
    pub(crate) snapshots_released_by_ami: HashMap<String, Vec<String>>,
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
    /// Launch template versions to delete, with `--prune-launch-template-versions`.
    pub(crate) launch_template_prune: Option<LaunchTemplatePrunePlan>,
    pub(crate) recycle_bin: RecycleBinCoverage,
    /// `--check-accounts`: what checking the other accounts found.
    pub(crate) account_check: Option<AccountCheckSummary>,
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
    /// Not in a state it can be deleted from (pending, transient, or unknown).
    NotReady(String),
    /// The snapshot's start time is missing, so its age is unknown.
    NoStartTime,
    NoCreationDate,
    TooNew,
    Referenced,
    LatestInFamily,
    NoFamilyTag,
    Protected,
    BackupCreated,
    LifecycleManaged,
    NoDeleteTag,
    RecentlyLaunched,
    LaunchTimeUnknown,
    UsedByRemainingAmi,
    Shared,
    SharingUnknown(String),
    UnresolvedReferences,
    BackupDeleteTagMissing,
    /// `--require-recycle-bin`: no available retention rule would keep it after deletion.
    NoRecycleBin,
    /// `--require-recycle-bin`: the Recycle Bin rules couldn't be read.
    RecycleBinUnknown(String),
    /// `--check-accounts`: shared publicly, so the accounts using it can't be known.
    SharedPublicly,
    /// `--check-accounts`: shared with an organization or OU (the ARN), whose accounts can't be
    /// enumerated.
    SharedWithOrganization(String),
    /// `--check-accounts`: shared with an account that isn't in the list (the account id).
    SharedWithUncheckedAccount(String),
    /// `--check-accounts`: an account it's shared with couldn't be checked (account and error).
    AccountCheckFailed(String),
    /// `--check-accounts`: a checked account uses it (account and what uses it).
    UsedInOtherAccount(String),
    /// A launch template, launch configuration, fleet, Image Builder recipe or CloudFormation
    /// stack in this account names the snapshot (what names it).
    SnapshotReferenced(String),
    /// The reference scan failed, so snapshot references in this account aren't known.
    SnapshotReferencesUnknown(String),
}

impl KeepReason {
    /// Stable identifier for machine-readable reports.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::NotReady(_) => "not_ready",
            Self::NoStartTime => "no_start_time",
            Self::NoCreationDate => "no_creation_date",
            Self::TooNew => "too_new",
            Self::Referenced => "referenced",
            Self::LatestInFamily => "latest_in_family",
            Self::NoFamilyTag => "no_family_tag",
            Self::Protected => "protected",
            Self::BackupCreated => "backup_created",
            Self::LifecycleManaged => "lifecycle_managed",
            Self::NoDeleteTag => "no_delete_tag",
            Self::RecentlyLaunched => "recently_launched",
            Self::LaunchTimeUnknown => "launch_time_unknown",
            Self::UsedByRemainingAmi => "used_by_remaining_ami",
            Self::Shared => "shared",
            Self::SharingUnknown(_) => "sharing_unknown",
            Self::UnresolvedReferences => "unresolved_references",
            Self::BackupDeleteTagMissing => "backup_delete_tag_missing",
            Self::NoRecycleBin => "no_recycle_bin",
            Self::RecycleBinUnknown(_) => "recycle_bin_unknown",
            Self::SharedPublicly => "shared_publicly",
            Self::SharedWithOrganization(_) => "shared_with_organization",
            Self::SharedWithUncheckedAccount(_) => "shared_with_unchecked_account",
            Self::AccountCheckFailed(_) => "account_check_failed",
            Self::UsedInOtherAccount(_) => "used_in_other_account",
            Self::SnapshotReferenced(_) => "snapshot_referenced",
            Self::SnapshotReferencesUnknown(_) => "snapshot_references_unknown",
        }
    }

    /// Kept because something couldn't be determined (a safety block), not because of a rule.
    pub(crate) fn uncertain(&self) -> bool {
        matches!(
            self,
            Self::SharingUnknown(_)
                | Self::UnresolvedReferences
                | Self::RecycleBinUnknown(_)
                | Self::AccountCheckFailed(_)
                | Self::SnapshotReferencesUnknown(_)
        )
    }
}

impl fmt::Display for KeepReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady(state) => write!(f, "state is {state}"),
            Self::NoStartTime => f.write_str("start time missing or unparseable"),
            Self::NoCreationDate => f.write_str("creation date missing or unparseable"),
            Self::TooNew => f.write_str("newer than --min-age-days"),
            Self::Referenced => f.write_str("referenced"),
            Self::LatestInFamily => f.write_str("among the newest of its ImageFamily"),
            Self::NoFamilyTag => {
                f.write_str("no ImageFamily tag (--keep-latest-per-family is set)")
            }
            Self::Protected => f.write_str("protect tag"),
            Self::BackupCreated => f.write_str("created by AWS Backup"),
            Self::LifecycleManaged => {
                f.write_str("managed by Data Lifecycle Manager (it enforces its own retention)")
            }
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
            Self::NoRecycleBin => f.write_str(
                "no Recycle Bin retention rule covers it, so deleting it would be permanent \
                 (--require-recycle-bin)",
            ),
            Self::RecycleBinUnknown(error) => {
                write!(f, "Recycle Bin rules couldn't be read: {error} (--require-recycle-bin)")
            }
            Self::SharedPublicly => f.write_str("shared publicly"),
            Self::SharedWithOrganization(arn) => write!(f, "shared with {arn}"),
            Self::SharedWithUncheckedAccount(account) => {
                write!(f, "shared with account {account}, which isn't in --check-accounts")
            }
            Self::AccountCheckFailed(detail) => write!(f, "couldn't check {detail}"),
            Self::UsedInOtherAccount(detail) => write!(f, "used in {detail}"),
            Self::SnapshotReferenced(source) => write!(f, "referenced by {source}"),
            Self::SnapshotReferencesUnknown(error) => {
                write!(f, "snapshot references unknown: {error}")
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
    pub(crate) deleted_launch_template_versions: usize,
    pub(crate) skipped_launch_template_versions: usize,
    /// Safety checks that cancelled work at execution time. Non-empty means exit code 3.
    pub(crate) blocked: Vec<String>,
    pub(crate) failures: Vec<String>,
}

/// Why a launch template version survives `--prune-launch-template-versions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VersionKeepReason {
    /// Among the newest `--keep-launch-template-versions` (plus the latest).
    Recent,
    /// The template's default version (`$Default`, and launches that name no version).
    Default,
    /// An ASG, fleet, Batch environment or EKS node group launches from it by number.
    Pinned,
    /// Newer than `--min-age-days`.
    TooNew,
    CreateTimeUnknown,
    /// The template belongs to a service that manages its versions (tag key).
    ManagedTemplate(String),
    /// The template carries the protect tag.
    Protected,
    /// Something pins a version string we can't interpret, so the whole template is kept.
    UnrecognizedPin(String),
    /// A Batch compute environment names `$Default`/`$Latest` (or no version). Batch resolves it
    /// at creation and at infrastructure updates, so the version in use may be any older one.
    BatchSymbolicPin(String),
}

impl VersionKeepReason {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::Default => "default_version",
            Self::Pinned => "pinned",
            Self::TooNew => "too_new",
            Self::CreateTimeUnknown => "create_time_unknown",
            Self::ManagedTemplate(_) => "managed_template",
            Self::Protected => "protected",
            Self::UnrecognizedPin(_) => "unrecognized_pin",
            Self::BatchSymbolicPin(_) => "batch_symbolic_pin",
        }
    }
}

impl fmt::Display for VersionKeepReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recent => f.write_str("among the newest versions"),
            Self::Default => f.write_str("default version"),
            Self::Pinned => f.write_str("launched from by number"),
            Self::TooNew => f.write_str("newer than --min-age-days"),
            Self::CreateTimeUnknown => f.write_str("creation time unknown"),
            Self::ManagedTemplate(tag) => write!(f, "template managed by another service ({tag})"),
            Self::Protected => f.write_str("template has the protect tag"),
            Self::BatchSymbolicPin(version) => write!(
                f,
                "a Batch compute environment uses {version}, resolved at its last infrastructure \
                 update, so any version may be in use"
            ),
            Self::UnrecognizedPin(version) => {
                write!(f, "pinned to unrecognized version {version:?}")
            }
        }
    }
}

/// Planned launch template version deletions for one template.
#[derive(Clone, Debug)]
pub(crate) struct TemplatePrune {
    pub(crate) template_id: String,
    pub(crate) template_name: Option<String>,
    pub(crate) delete_versions: Vec<i64>,
}

/// The launch template version part of a region plan (only with `--prune-launch-template-versions`).
#[derive(Clone, Debug, Default)]
pub(crate) struct LaunchTemplatePrunePlan {
    pub(crate) templates: Vec<TemplatePrune>,
    /// Kept versions per reason code.
    pub(crate) kept_by_reason: BTreeMap<&'static str, usize>,
    /// Owned AMIs that only the versions to delete reference: candidates for the *next* run,
    /// where they still go through every AMI rule.
    pub(crate) amis_freed: Vec<String>,
}

impl LaunchTemplatePrunePlan {
    pub(crate) fn version_count(&self) -> usize {
        self.templates.iter().map(|template| template.delete_versions.len()).sum()
    }
}

/// Why an AMI or snapshot candidate is kept because other accounts may use it. AMIs built in one
/// account and shared to others are the main case: deleting them breaks launches elsewhere.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExternalUse {
    /// Launch (AMI) or create-volume (snapshot) permissions are granted to these.
    Shared(Vec<Grant>),
    /// The permission lookup failed.
    Unknown(String),
    /// Shared, and checking the accounts it's shared with (`--check-accounts`) found this reason
    /// to keep it. Shared items that every checked account leaves unused drop out of the map.
    Kept(KeepReason),
}

/// Who a launch or create-volume permission is granted to.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Grant {
    Account(String),
    Public,
    Organization(String),
    OrganizationalUnit(String),
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
