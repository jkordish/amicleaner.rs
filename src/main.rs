use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::process::ExitCode;
use std::time::Duration as StdDuration;

use aws_config::BehaviorVersion;
use aws_sdk_autoscaling as autoscaling;
use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use aws_sdk_ssm as ssm;
use aws_types::region::Region;
use chrono::{DateTime, Duration, Utc};
use clap::{ArgAction, Parser};
use ec2::error::{ProvideErrorMetadata, SdkError};
use log::{error, info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;
type AppResult<T> = Result<T, DynError>;

/// Upper bound for day-count flags; keeps chrono date arithmetic far away from overflow.
const MAX_DAYS: i64 = 36_500;

/// Exit code when nothing failed but safety checks cancelled planned deletions.
const EXIT_BLOCKED: u8 = 3;

/// Tag that groups AMIs for `--keep-latest-per-family`.
const FAMILY_TAG: &str = "ImageFamily";

/// Prefix of the tags AWS Backup puts on the AMIs and snapshots it creates.
const AWS_BACKUP_TAG_PREFIX: &str = "aws:backup:";

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

#[derive(Debug, Parser)]
#[command(
    name = "amicleaner",
    version,
    about = "Conservative EC2 AMI + EBS snapshot mark-and-sweep cleanup"
)]
struct Args {
    /// Execute deletions. Without this flag, the tool only prints a plan.
    #[arg(long, action = ArgAction::SetTrue)]
    execute: bool,

    /// Use AWS API dry-run checks when executing (requires --execute).
    #[arg(long, action = ArgAction::SetTrue)]
    dry_run: bool,

    /// Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots (requires --execute).
    #[arg(long, action = ArgAction::SetTrue)]
    execute_backup_recovery_point_deletes: bool,

    /// Snapshot tag key required before its AWS Backup recovery point may be deleted.
    #[arg(long, default_value = "backup-cleanup", value_parser = parse_tag_key)]
    backup_delete_tag: String,

    /// Expected value for backup delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    backup_delete_tag_value: String,

    /// Permit deletion of AWS Backup recovery points whose snapshot lacks the backup delete tag.
    #[arg(long, action = ArgAction::SetTrue)]
    allow_untagged_backup_recovery_points: bool,

    /// Comma-separated list of regions. If omitted, all enabled account regions are scanned.
    #[arg(long, value_delimiter = ',', value_parser = parse_region)]
    regions: Vec<String>,

    /// Keep resources newer than this age in days.
    #[arg(
        long,
        default_value_t = 14,
        allow_negative_numbers = true,
        value_parser = clap::value_parser!(i64).range(0..=MAX_DAYS)
    )]
    min_age_days: i64,

    /// Keep AMIs launched within the last N days.
    #[arg(
        long,
        default_value_t = 90,
        allow_negative_numbers = true,
        value_parser = clap::value_parser!(i64).range(0..=MAX_DAYS)
    )]
    ami_recent_launch_days: i64,

    /// Tag key that protects resources from deletion (matched case-insensitively).
    #[arg(long, default_value = "DoNotDelete", value_parser = parse_tag_key)]
    protect_tag: String,

    /// Value the protect tag must have. Empty (the default) protects on key presence alone.
    #[arg(long, default_value = "")]
    protect_tag_value: String,

    /// Tag key required for deletion (unless --allow-untagged).
    #[arg(long, default_value = "cleanup", value_parser = parse_tag_key)]
    delete_tag: String,

    /// Expected value for delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    delete_tag_value: String,

    /// Permit deletion of untagged resources.
    #[arg(long, action = ArgAction::SetTrue)]
    allow_untagged: bool,

    /// Keep newest N AMIs per ImageFamily tag. When N > 0, AMIs without that tag are always kept.
    #[arg(long, default_value_t = 0)]
    keep_latest_per_family: usize,

    /// Retries after the first attempt when a mutating call hits throttling or a transient error.
    #[arg(long, default_value_t = 2)]
    max_retries: u32,

    /// Base delay in milliseconds between retries (doubles on each retry).
    #[arg(long, default_value_t = 1_000)]
    retry_delay_ms: u64,
}

fn parse_tag_key(raw: &str) -> Result<String, String> {
    let key = raw.trim();
    if key.is_empty() {
        return Err("tag key must not be empty".to_string());
    }
    Ok(key.to_string())
}

fn parse_region(raw: &str) -> Result<String, String> {
    let region = raw.trim().to_ascii_lowercase();
    let segments = region.split('-').collect::<Vec<&str>>();
    let valid = segments.len() >= 3
        && segments.iter().all(|segment| {
            !segment.is_empty()
                && segment.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
        && segments[0].bytes().all(|b| b.is_ascii_lowercase())
        && segments.last().is_some_and(|segment| segment.bytes().all(|b| b.is_ascii_digit()));

    if valid { Ok(region) } else { Err(format!("'{}' is not a valid AWS region name", raw.trim())) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LastLaunched {
    Never,
    At(DateTime<Utc>),
    /// AWS sent a timestamp we couldn't parse; treated as a recent launch.
    Unknown,
}

#[derive(Clone, Debug)]
struct AmiInfo {
    ami_id: String,
    name: Option<String>,
    description: Option<String>,
    /// `None` when AWS omitted the creation date or sent one we couldn't parse. Such AMIs are
    /// never deregistered, but stay in inventory so their snapshots remain referenced.
    creation_date: Option<DateTime<Utc>>,
    last_launched: LastLaunched,
    snapshot_ids: Vec<String>,
    tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct SnapshotInfo {
    snapshot_id: String,
    start_time: DateTime<Utc>,
    description: Option<String>,
    tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct BackupRecoveryPoint {
    snapshot_id: String,
    backup_vault_name: String,
    recovery_point_arn: String,
}

/// An EBS recovery point as listed by AWS Backup, along with the vault properties that decide
/// whether it may be deleted.
#[derive(Clone, Debug, Default)]
struct RecoveryPointRecord {
    backup_vault_name: String,
    vault_locked: bool,
    vault_type: Option<String>,
    recovery_point_arn: Option<String>,
    resource_arn: Option<String>,
    status: Option<String>,
    is_parent: bool,
    parent_recovery_point_arn: Option<String>,
}

#[derive(Clone, Debug)]
struct BlockedSnapshot {
    snapshot: SnapshotInfo,
    reason: String,
    /// True when the block comes from not knowing something (vs. a definite rule like Vault Lock).
    uncertain: bool,
}

#[derive(Debug, Default)]
struct BackupClassification {
    direct_delete: Vec<SnapshotInfo>,
    backup_managed: Vec<SnapshotInfo>,
    recovery_points_to_delete: Vec<BackupRecoveryPoint>,
    tag_filtered: usize,
    blocked: Vec<BlockedSnapshot>,
}

#[derive(Clone, Debug, Default)]
struct RegionPlan {
    region: String,
    owned_ami_count: usize,
    owned_snapshot_count: usize,
    referenced_ami_count: usize,
    unresolved_ami_references: Vec<String>,
    referenced_snapshot_count_after_ami_keeps: usize,
    deregister_amis: Vec<AmiInfo>,
    /// Snapshot id -> id of the planned AMI whose deregistration leaves the snapshot unreferenced.
    snapshots_released_by_ami: HashMap<String, String>,
    delete_snapshots: Vec<SnapshotInfo>,
    backup_managed_snapshots: Vec<SnapshotInfo>,
    delete_backup_recovery_points: Vec<BackupRecoveryPoint>,
    blocked_backup_snapshots: Vec<BlockedSnapshot>,
    tag_filtered_backup_recovery_points: usize,
    kept_untagged_family_amis: usize,
    skipped_amis: usize,
    skipped_snapshots: usize,
    /// Why safety checks cancelled planned deletions. Non-empty means exit code 3.
    blocked: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct ExecutionSummary {
    dry_run: bool,
    deregistered_amis: usize,
    skipped_amis: usize,
    deleted_snapshots: usize,
    skipped_snapshots: usize,
    deleted_backup_recovery_points: usize,
    skipped_backup_recovery_points: usize,
    failures: Vec<String>,
}

#[derive(Clone, Debug)]
struct LaunchTemplateRef {
    launch_template_id: Option<String>,
    launch_template_name: Option<String>,
    version: Option<String>,
}

#[derive(Debug, Default)]
struct AmiReferenceScan {
    referenced_ami_ids: HashSet<String>,
    unresolved_references: HashSet<String>,
}

#[derive(Debug)]
enum ImageReferenceResolution {
    Ami(String),
    Unresolved(String),
}

/// Why an AMI candidate is kept because of who else can launch it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ExternalUse {
    /// Launch permissions are granted to other accounts, organizations or the public.
    Shared,
    /// The launch-permission lookup failed.
    Unknown(String),
}

#[derive(Clone, Copy, Debug)]
struct Cutoffs {
    min_age: DateTime<Utc>,
    recent_launch: DateTime<Utc>,
}

impl Cutoffs {
    fn new(args: &Args, now: DateTime<Utc>) -> Self {
        Self {
            min_age: now - Duration::days(args.min_age_days),
            recent_launch: now - Duration::days(args.ami_recent_launch_days),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RetryPolicy {
    max_retries: u32,
    base_delay_ms: u64,
}

impl RetryPolicy {
    fn from_args(args: &Args) -> Self {
        Self { max_retries: args.max_retries, base_delay_ms: args.retry_delay_ms }
    }
}

#[derive(Debug, Default)]
struct RunOutcome {
    failed_regions: Vec<String>,
    blocked_regions: Vec<String>,
}

impl RunOutcome {
    fn exit_code(&self) -> ExitCode {
        if !self.failed_regions.is_empty() {
            ExitCode::FAILURE
        } else if !self.blocked_regions.is_empty() {
            ExitCode::from(EXIT_BLOCKED)
        } else {
            ExitCode::SUCCESS
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) =
        simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).env().init()
    {
        eprintln!("failed to initialize logging: {error}");
        return ExitCode::FAILURE;
    }

    let args = Args::parse();

    match run(&args).await {
        Ok(outcome) => outcome.exit_code(),
        Err(error) => {
            error!("{}", error_chain(error.as_ref()));
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> AppResult<RunOutcome> {
    for warning in inert_flag_warnings(args) {
        warn!("{warning}");
    }

    let shared_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let home_ec2 = ec2::Client::new(&shared_config);

    let regions = resolve_regions(args, &home_ec2).await?;
    if regions.is_empty() {
        return Err("No AWS regions resolved".into());
    }

    info!(
        "mode={} dry_run={} regions={}",
        if args.execute { "execute" } else { "plan" },
        args.dry_run,
        regions.join(",")
    );

    let mut outcome = RunOutcome::default();

    for region in regions {
        info!("scanning region={region}");

        let region_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;

        let ec2_client = ec2::Client::new(&region_config);
        let asg_client = autoscaling::Client::new(&region_config);
        let backup_client = backup::Client::new(&region_config);
        let ssm_client = ssm::Client::new(&region_config);

        match build_region_plan(
            args,
            &region,
            &ec2_client,
            &asg_client,
            &backup_client,
            &ssm_client,
        )
        .await
        {
            Ok(plan) => {
                print_region_plan(&plan);
                if !plan.blocked.is_empty() {
                    outcome.blocked_regions.push(region.clone());
                }
                if args.execute {
                    let summary = execute_region_plan(
                        &ec2_client,
                        &asg_client,
                        &backup_client,
                        &ssm_client,
                        &plan,
                        args,
                    )
                    .await;
                    print_execution_summary(&region, &summary);
                    if !summary.failures.is_empty() {
                        outcome.failed_regions.push(region.clone());
                    }
                }
            }
            Err(error) => {
                outcome.failed_regions.push(region.clone());
                eprintln!("region={region} failed: {}", error_chain(error.as_ref()));
            }
        }
    }

    if !outcome.failed_regions.is_empty() {
        error!("regions with failures: {}", outcome.failed_regions.join(","));
    }
    if !outcome.blocked_regions.is_empty() {
        warn!(
            "safety checks cancelled planned deletions in regions: {}",
            outcome.blocked_regions.join(",")
        );
    }

    Ok(outcome)
}

fn inert_flag_warnings(args: &Args) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    if args.execute {
        return warnings;
    }
    if args.dry_run {
        warnings.push("--dry-run has no effect without --execute; printing the plan only");
    }
    if args.execute_backup_recovery_point_deletes {
        warnings.push(
            "--execute-backup-recovery-point-deletes has no effect without --execute; printing the plan only",
        );
    }
    warnings
}

async fn resolve_regions(args: &Args, ec2_client: &ec2::Client) -> AppResult<Vec<String>> {
    if !args.regions.is_empty() {
        let mut regions = args.regions.clone();
        regions.sort_unstable();
        regions.dedup();
        return Ok(regions);
    }

    let response = ec2_client
        .describe_regions()
        .all_regions(true)
        .send()
        .await
        .aws_context("DescribeRegions")?;
    let mut regions = response
        .regions()
        .iter()
        .filter(|region| is_enabled_region(region.opt_in_status()))
        .filter_map(|region| region.region_name().map(ToString::to_string))
        .collect::<Vec<String>>();

    regions.sort_unstable();
    regions.dedup();

    Ok(regions)
}

fn is_enabled_region(opt_in_status: Option<&str>) -> bool {
    match opt_in_status {
        Some("opted-in" | "opt-in-not-required") | None => true,
        Some(_) => false,
    }
}

async fn build_region_plan(
    args: &Args,
    region: &str,
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    backup_client: &backup::Client,
    ssm_client: &ssm::Client,
) -> AppResult<RegionPlan> {
    let now = Utc::now();
    let owned_amis = list_owned_amis(ec2_client).await?;
    let owned_snapshots = list_owned_snapshots(ec2_client).await?;

    // A failed reference scan only blocks AMI deregistration; snapshot cleanup doesn't depend on it.
    let reference_scan = collect_referenced_ami_ids(ec2_client, asg_client, ssm_client)
        .await
        .unwrap_or_else(|error| AmiReferenceScan {
            referenced_ami_ids: HashSet::new(),
            unresolved_references: HashSet::from([format!(
                "AMI reference scan failed: {}",
                error_chain(error.as_ref())
            )]),
        });

    // Launch permissions are looked up per candidate, so only bother when deregistration can happen.
    let mut external_use = HashMap::new();
    if reference_scan.unresolved_references.is_empty() {
        let cutoffs = Cutoffs::new(args, now);
        let keep_latest = select_latest_ami_ids_by_family(&owned_amis, args.keep_latest_per_family);
        for ami in owned_amis.iter().filter(|ami| {
            should_deregister_ami(
                ami,
                &reference_scan.referenced_ami_ids,
                &keep_latest,
                cutoffs,
                args,
            )
        }) {
            match ami_has_external_launch_permissions(ec2_client, &ami.ami_id).await {
                Ok(false) => {}
                Ok(true) => {
                    external_use.insert(ami.ami_id.clone(), ExternalUse::Shared);
                }
                Err(error) => {
                    external_use.insert(
                        ami.ami_id.clone(),
                        ExternalUse::Unknown(error_chain(error.as_ref())),
                    );
                }
            }
        }
    }

    let mut plan =
        plan_region(args, region, now, owned_amis, owned_snapshots, &reference_scan, &external_use);

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

fn plan_region(
    args: &Args,
    region: &str,
    now: DateTime<Utc>,
    owned_amis: Vec<AmiInfo>,
    owned_snapshots: Vec<SnapshotInfo>,
    reference_scan: &AmiReferenceScan,
    external_use: &HashMap<String, ExternalUse>,
) -> RegionPlan {
    let cutoffs = Cutoffs::new(args, now);
    let referenced_ami_ids = &reference_scan.referenced_ami_ids;
    let keep_latest_ami_ids =
        select_latest_ami_ids_by_family(&owned_amis, args.keep_latest_per_family);

    let mut blocked = Vec::new();
    let mut deregister_amis = Vec::new();
    let mut skipped_amis = 0usize;

    for ami in &owned_amis {
        if !should_deregister_ami(ami, referenced_ami_ids, &keep_latest_ami_ids, cutoffs, args) {
            skipped_amis += 1;
            continue;
        }

        match external_use.get(&ami.ami_id) {
            None => deregister_amis.push(ami.clone()),
            Some(ExternalUse::Shared) => skipped_amis += 1,
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

    for snapshot in &owned_snapshots {
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
        skipped_amis,
        skipped_snapshots,
        blocked,
    }
}

fn should_deregister_ami(
    ami: &AmiInfo,
    referenced_ami_ids: &HashSet<String>,
    keep_latest_ami_ids: &HashSet<String>,
    cutoffs: Cutoffs,
    args: &Args,
) -> bool {
    let Some(creation_date) = ami.creation_date else {
        return false;
    };

    if creation_date >= cutoffs.min_age {
        return false;
    }

    if referenced_ami_ids.contains(&ami.ami_id) {
        return false;
    }

    if keep_latest_ami_ids.contains(&ami.ami_id) {
        return false;
    }

    if args.keep_latest_per_family > 0 && ami_family(ami).is_none() {
        return false;
    }

    if is_protected(&ami.tags, args) {
        return false;
    }

    if looks_backup_created(&ami.tags, ami.description.as_deref()) {
        return false;
    }

    if !deletion_tag_allows(&ami.tags, args) {
        return false;
    }

    match ami.last_launched {
        LastLaunched::Never => true,
        LastLaunched::At(last_launched) => last_launched < cutoffs.recent_launch,
        LastLaunched::Unknown => false,
    }
}

fn snapshot_is_deletable(
    snapshot: &SnapshotInfo,
    released_by_deregistered_ami: bool,
    referenced_snapshot_ids: &HashSet<String>,
    min_age_cutoff: DateTime<Utc>,
    args: &Args,
) -> bool {
    snapshot.start_time < min_age_cutoff
        && !referenced_snapshot_ids.contains(&snapshot.snapshot_id)
        && !is_protected(&snapshot.tags, args)
        && snapshot_deletion_authorized(&snapshot.tags, released_by_deregistered_ami, args)
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

fn optional_expected_value(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

fn has_expected_tag(
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
fn is_protected(tags: &HashMap<String, String>, args: &Args) -> bool {
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

fn backup_recovery_point_deletion_tag_allows(tags: &HashMap<String, String>, args: &Args) -> bool {
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
fn looks_backup_created(tags: &HashMap<String, String>, description: Option<&str>) -> bool {
    tags.keys().any(|key| key.starts_with(AWS_BACKUP_TAG_PREFIX))
        || description.is_some_and(|description| description.contains("AWS Backup"))
}

fn ami_family(ami: &AmiInfo) -> Option<&str> {
    ami.tags.get(FAMILY_TAG).map(|family| family.trim()).filter(|family| !family.is_empty())
}

fn select_latest_ami_ids_by_family(amis: &[AmiInfo], keep_count: usize) -> HashSet<String> {
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

async fn list_owned_amis(ec2_client: &ec2::Client) -> AppResult<Vec<AmiInfo>> {
    let mut amis = Vec::new();

    let mut pages = ec2_client
        .describe_images()
        .owners("self")
        .include_deprecated(true)
        .include_disabled(true)
        .into_paginator()
        .page_size(100)
        .send();

    while let Some(page) = pages.next().await {
        let output = page.aws_context("DescribeImages")?;
        for image in output.images() {
            let Some(ami_id) = image.image_id() else {
                continue;
            };

            let creation_date = image.creation_date().and_then(parse_aws_timestamp);
            if creation_date.is_none() {
                warn!(
                    "AMI {} has a missing or unparseable creation date ({:?}); it will be kept",
                    ami_id,
                    image.creation_date()
                );
            }

            let last_launched = match image.last_launched_time() {
                None => LastLaunched::Never,
                Some(raw) => parse_aws_timestamp(raw).map_or_else(
                    || {
                        warn!(
                            "AMI {ami_id} has an unparseable last launched time ({raw}); it will be kept"
                        );
                        LastLaunched::Unknown
                    },
                    LastLaunched::At,
                ),
            };

            let snapshot_ids = image
                .block_device_mappings()
                .iter()
                .filter_map(|mapping| mapping.ebs().and_then(|ebs| ebs.snapshot_id()))
                .map(ToString::to_string)
                .collect::<Vec<String>>();

            amis.push(AmiInfo {
                ami_id: ami_id.to_string(),
                name: image.name().map(ToString::to_string),
                description: image.description().map(ToString::to_string),
                creation_date,
                last_launched,
                snapshot_ids,
                tags: tags_to_map(image.tags()),
            });
        }
    }

    Ok(amis)
}

async fn list_owned_snapshots(ec2_client: &ec2::Client) -> AppResult<Vec<SnapshotInfo>> {
    let mut snapshots = Vec::new();

    let mut pages =
        ec2_client.describe_snapshots().owner_ids("self").into_paginator().page_size(100).send();

    while let Some(page) = pages.next().await {
        let output = page.aws_context("DescribeSnapshots")?;
        for snapshot in output.snapshots() {
            let Some(snapshot_id) = snapshot.snapshot_id() else {
                continue;
            };
            // Without a start time the age check can't pass, so leaving it out keeps it.
            let Some(start_time) = snapshot.start_time().and_then(smithy_datetime_to_utc) else {
                continue;
            };

            snapshots.push(SnapshotInfo {
                snapshot_id: snapshot_id.to_string(),
                start_time,
                description: snapshot.description().map(ToString::to_string),
                tags: tags_to_map(snapshot.tags()),
            });
        }
    }

    Ok(snapshots)
}

fn tags_to_map(tags: &[ec2::types::Tag]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for tag in tags {
        if let (Some(key), Some(value)) = (tag.key(), tag.value()) {
            map.insert(key.to_string(), value.to_string());
        }
    }
    map
}

fn parse_aws_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw).ok().map(|timestamp| timestamp.with_timezone(&Utc))
}

fn smithy_datetime_to_utc(value: &ec2::primitives::DateTime) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value.secs(), 0)
}

fn is_resource_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|hex| {
        matches!(hex.len(), 8 | 17)
            && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn is_ami_id(value: &str) -> bool {
    is_resource_id(value, "ami-")
}

async fn ami_has_external_launch_permissions(
    ec2_client: &ec2::Client,
    ami_id: &str,
) -> AppResult<bool> {
    let output = ec2_client
        .describe_image_attribute()
        .image_id(ami_id)
        .attribute(ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .aws_context("DescribeImageAttribute")?;
    Ok(!output.launch_permissions().is_empty())
}

async fn list_ebs_recovery_points(
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
fn classify_backup_snapshots(
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
fn snapshot_id_from_arn(arn: &str) -> Option<&str> {
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

fn apply_backup_classification(plan: &mut RegionPlan, classification: BackupClassification) {
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

fn block_all_snapshot_deletions(plan: &mut RegionPlan, reason: &str) {
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
fn prune_released_snapshots(plan: &mut RegionPlan) {
    let planned = plan
        .delete_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .chain(plan.delete_backup_recovery_points.iter().map(|point| point.snapshot_id.as_str()))
        .map(ToString::to_string)
        .collect::<HashSet<String>>();
    plan.snapshots_released_by_ami.retain(|snapshot_id, _| planned.contains(snapshot_id));
}

async fn collect_referenced_ami_ids(
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    ssm_client: &ssm::Client,
) -> AppResult<AmiReferenceScan> {
    let mut collector = ReferenceCollector {
        ec2: ec2_client,
        asg: asg_client,
        ssm: ssm_client,
        ssm_cache: HashMap::new(),
        scan: AmiReferenceScan::default(),
    };

    collector.collect_instances().await?;
    collector.collect_launch_templates().await?;
    collector.collect_auto_scaling_groups().await?;
    collector.collect_launch_configurations().await?;
    collector.collect_ec2_fleets().await?;
    collector.collect_spot_fleets().await?;
    collector.collect_spot_instance_requests().await?;

    Ok(collector.scan)
}

/// Gathers every AMI that something in the region could launch. Any reference that can't be
/// resolved to a concrete AMI id is recorded as unresolved, which blocks AMI deregistration.
struct ReferenceCollector<'a> {
    ec2: &'a ec2::Client,
    asg: &'a autoscaling::Client,
    ssm: &'a ssm::Client,
    ssm_cache: HashMap<String, Option<String>>,
    scan: AmiReferenceScan,
}

impl ReferenceCollector<'_> {
    async fn record(&mut self, image_reference: &str) {
        match resolve_image_reference(image_reference, self.ssm, &mut self.ssm_cache).await {
            ImageReferenceResolution::Ami(ami_id) => {
                self.scan.referenced_ami_ids.insert(ami_id);
            }
            ImageReferenceResolution::Unresolved(reason) => {
                self.scan.unresolved_references.insert(reason);
            }
        }
    }

    async fn collect_instances(&mut self) -> AppResult<()> {
        let state_filter = ec2::types::Filter::builder()
            .name("instance-state-name")
            .values("pending")
            .values("running")
            .values("stopping")
            .values("stopped")
            .values("shutting-down")
            .build();

        let mut pages = self
            .ec2
            .describe_instances()
            .filters(state_filter)
            .into_paginator()
            .page_size(100)
            .send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeInstances")?;
            for reservation in output.reservations() {
                for instance in reservation.instances() {
                    if let Some(image_id) = instance.image_id() {
                        self.record(image_id).await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn collect_launch_templates(&mut self) -> AppResult<()> {
        let mut template_pages =
            self.ec2.describe_launch_templates().into_paginator().page_size(100).send();

        while let Some(template_page) = template_pages.next().await {
            let output = template_page.aws_context("DescribeLaunchTemplates")?;
            for launch_template in output.launch_templates() {
                let mut request = self.ec2.describe_launch_template_versions();

                if let Some(launch_template_id) = launch_template.launch_template_id() {
                    request = request.launch_template_id(launch_template_id);
                } else if let Some(launch_template_name) = launch_template.launch_template_name() {
                    request = request.launch_template_name(launch_template_name);
                } else {
                    continue;
                }

                let mut version_pages = request.into_paginator().page_size(100).send();
                while let Some(version_page) = version_pages.next().await {
                    let version_output =
                        version_page.aws_context("DescribeLaunchTemplateVersions")?;
                    for version in version_output.launch_template_versions() {
                        if let Some(image_reference) = version
                            .launch_template_data()
                            .and_then(|launch_template_data| launch_template_data.image_id())
                        {
                            self.record(image_reference).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn collect_auto_scaling_groups(&mut self) -> AppResult<()> {
        let mut group_pages =
            self.asg.describe_auto_scaling_groups().into_paginator().page_size(100).send();

        while let Some(group_page) = group_pages.next().await {
            let output = group_page.aws_context("DescribeAutoScalingGroups")?;
            for group in output.auto_scaling_groups() {
                let mut template_refs = Vec::new();

                if let Some(spec) = group.launch_template() {
                    template_refs.push(LaunchTemplateRef {
                        launch_template_id: spec.launch_template_id().map(ToString::to_string),
                        launch_template_name: spec.launch_template_name().map(ToString::to_string),
                        version: spec.version().map(ToString::to_string),
                    });
                }

                if let Some(mixed_instances_policy) = group.mixed_instances_policy()
                    && let Some(launch_template) = mixed_instances_policy.launch_template()
                {
                    let override_specs = launch_template
                        .overrides()
                        .iter()
                        .filter_map(|override_spec| override_spec.launch_template_specification());
                    for spec in launch_template
                        .launch_template_specification()
                        .into_iter()
                        .chain(override_specs)
                    {
                        template_refs.push(LaunchTemplateRef {
                            launch_template_id: spec.launch_template_id().map(ToString::to_string),
                            launch_template_name: spec
                                .launch_template_name()
                                .map(ToString::to_string),
                            version: spec.version().map(ToString::to_string),
                        });
                    }
                }

                for template_ref in &template_refs {
                    self.collect_launch_template_ref(group.auto_scaling_group_name(), template_ref)
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Resolves the image of one launch template an ASG points at. Every existing template version
    /// is already covered by `collect_launch_templates`; this catches templates that couldn't be
    /// listed. A template that no longer exists can't launch anything, so it's skipped; any other
    /// failure is recorded as unresolved rather than aborting the whole scan.
    async fn collect_launch_template_ref(
        &mut self,
        group_name: Option<&str>,
        launch_template_ref: &LaunchTemplateRef,
    ) {
        let mut request = self.ec2.describe_launch_template_versions();
        let label =
            if let Some(launch_template_id) = launch_template_ref.launch_template_id.as_deref() {
                request = request.launch_template_id(launch_template_id);
                launch_template_id.to_string()
            } else if let Some(launch_template_name) =
                launch_template_ref.launch_template_name.as_deref()
            {
                request = request.launch_template_name(launch_template_name);
                launch_template_name.to_string()
            } else {
                return;
            };

        // No version means "all versions" here, which is the conservative reading.
        if let Some(version) = launch_template_ref.version.as_deref() {
            request = request.versions(version);
        }

        let mut image_references = Vec::new();
        let mut pages = request.into_paginator().page_size(100).send();
        while let Some(page) = pages.next().await {
            match page {
                Ok(output) => image_references.extend(
                    output
                        .launch_template_versions()
                        .iter()
                        .filter_map(|version| {
                            version.launch_template_data().and_then(|data| data.image_id())
                        })
                        .map(ToString::to_string),
                ),
                Err(error) if is_launch_template_not_found(error.code()) => {
                    warn!(
                        "Auto Scaling group {} references launch template {} which no longer exists ({}); ignoring",
                        group_name.unwrap_or("<unnamed>"),
                        label,
                        describe_sdk_error(&error)
                    );
                    return;
                }
                Err(error) => {
                    self.scan.unresolved_references.insert(format!(
                        "failed to describe launch template {} for Auto Scaling group {}: {}",
                        label,
                        group_name.unwrap_or("<unnamed>"),
                        describe_sdk_error(&error)
                    ));
                    return;
                }
            }
        }

        for image_reference in image_references {
            self.record(&image_reference).await;
        }
    }

    async fn collect_launch_configurations(&mut self) -> AppResult<()> {
        let mut pages =
            self.asg.describe_launch_configurations().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeLaunchConfigurations")?;
            for launch_configuration in output.launch_configurations() {
                if let Some(image_id) = launch_configuration.image_id() {
                    self.record(image_id).await;
                }
            }
        }

        Ok(())
    }

    /// EC2 Fleet overrides can name an AMI directly. Their launch templates are already covered.
    async fn collect_ec2_fleets(&mut self) -> AppResult<()> {
        let mut pages = self.ec2.describe_fleets().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeFleets")?;
            for fleet in output.fleets() {
                let can_launch = fleet.fleet_state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::FleetStateCode::Submitted
                            | ec2::types::FleetStateCode::Active
                            | ec2::types::FleetStateCode::Modifying
                    )
                });
                if !can_launch {
                    continue;
                }

                for config in fleet.launch_template_configs() {
                    for override_spec in config.overrides() {
                        if let Some(image_reference) = override_spec.image_id() {
                            self.record(image_reference).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Spot Fleet launch specifications name an AMI directly. Launch templates are already covered.
    async fn collect_spot_fleets(&mut self) -> AppResult<()> {
        let mut pages =
            self.ec2.describe_spot_fleet_requests().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeSpotFleetRequests")?;
            for request in output.spot_fleet_request_configs() {
                let can_launch = request.spot_fleet_request_state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::BatchState::Submitted
                            | ec2::types::BatchState::Active
                            | ec2::types::BatchState::Modifying
                    )
                });
                if !can_launch {
                    continue;
                }

                let Some(config) = request.spot_fleet_request_config() else {
                    continue;
                };
                for spec in config.launch_specifications() {
                    if let Some(image_reference) = spec.image_id() {
                        self.record(image_reference).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Open or persistent Spot requests can launch (or relaunch) their AMI later.
    async fn collect_spot_instance_requests(&mut self) -> AppResult<()> {
        let mut pages =
            self.ec2.describe_spot_instance_requests().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeSpotInstanceRequests")?;
            for request in output.spot_instance_requests() {
                let can_launch = request.state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::SpotInstanceState::Open
                            | ec2::types::SpotInstanceState::Active
                            | ec2::types::SpotInstanceState::Disabled
                    )
                });
                if !can_launch {
                    continue;
                }

                if let Some(image_reference) =
                    request.launch_specification().and_then(|spec| spec.image_id())
                {
                    self.record(image_reference).await;
                }
            }
        }

        Ok(())
    }
}

fn is_launch_template_not_found(code: Option<&str>) -> bool {
    code.is_some_and(|code| code.starts_with("InvalidLaunchTemplate") && code.contains("NotFound"))
}

async fn resolve_image_reference(
    image_reference: &str,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
) -> ImageReferenceResolution {
    if is_ami_id(image_reference) {
        return ImageReferenceResolution::Ami(image_reference.to_string());
    }

    let Some(parameter_name) = extract_ssm_parameter_name(image_reference) else {
        let reason = format!("unresolved image reference '{}'", image_reference.escape_debug());
        warn!("{}", reason);
        return ImageReferenceResolution::Unresolved(reason);
    };

    let resolved_ami = match ssm_cache.get(&parameter_name) {
        Some(cached) => cached.clone(),
        None => {
            let response = ssm_client.get_parameter().name(parameter_name.clone()).send().await;
            let resolved_ami = match response {
                Ok(output) => {
                    let value = output.parameter().and_then(|parameter| parameter.value());
                    let ami_id = value.filter(|value| is_ami_id(value)).map(ToString::to_string);
                    if ami_id.is_none() {
                        warn!(
                            "SSM image parameter '{}' does not hold a single AMI id (value: {:?})",
                            parameter_name, value
                        );
                    }
                    ami_id
                }
                Err(error) => {
                    warn!(
                        "Failed to resolve SSM image parameter '{}': {}",
                        parameter_name,
                        describe_sdk_error(&error)
                    );
                    None
                }
            };
            ssm_cache.insert(parameter_name.clone(), resolved_ami.clone());
            resolved_ami
        }
    };

    match resolved_ami {
        Some(ami_id) => ImageReferenceResolution::Ami(ami_id),
        None => ImageReferenceResolution::Unresolved(format!(
            "failed to resolve SSM image parameter '{}'",
            parameter_name
        )),
    }
}

fn extract_ssm_parameter_name(image_reference: &str) -> Option<String> {
    if let Some(raw) = image_reference.strip_prefix("resolve:ssm:") {
        return Some(raw.to_string());
    }

    if image_reference.starts_with("{{resolve:ssm:") && image_reference.ends_with("}}") {
        return Some(
            image_reference.trim_start_matches("{{resolve:ssm:").trim_end_matches("}}").to_string(),
        );
    }

    None
}

fn print_region_plan(plan: &RegionPlan) {
    println!("\n=== Region {} ===", plan.region);
    println!(
        "owned_amis={} referenced_amis={} unresolved_ami_references={} candidate_amis={} skipped_amis={} kept_untagged_family_amis={}",
        plan.owned_ami_count,
        plan.referenced_ami_count,
        plan.unresolved_ami_references.len(),
        plan.deregister_amis.len(),
        plan.skipped_amis,
        plan.kept_untagged_family_amis,
    );
    println!(
        "owned_snapshots={} referenced_snapshots_after_ami_keeps={} candidate_snapshots={} released_by_ami_snapshots={} backup_managed_candidates={} backup_recovery_point_candidates={} tag_filtered_backup_recovery_points={} blocked_snapshots={} skipped_snapshots={}",
        plan.owned_snapshot_count,
        plan.referenced_snapshot_count_after_ami_keeps,
        plan.delete_snapshots.len(),
        plan.snapshots_released_by_ami.len(),
        plan.backup_managed_snapshots.len(),
        plan.delete_backup_recovery_points.len(),
        plan.tag_filtered_backup_recovery_points,
        plan.blocked_backup_snapshots.len(),
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
            println!("  {}", unresolved);
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

async fn execute_region_plan(
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    backup_client: &backup::Client,
    ssm_client: &ssm::Client,
    plan: &RegionPlan,
    args: &Args,
) -> ExecutionSummary {
    let mut summary = ExecutionSummary { dry_run: args.dry_run, ..ExecutionSummary::default() };
    let cutoffs = Cutoffs::new(args, Utc::now());

    let released_amis = execute_ami_deregistrations(
        ec2_client,
        asg_client,
        ssm_client,
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
    asg_client: &autoscaling::Client,
    ssm_client: &ssm::Client,
    plan: &RegionPlan,
    args: &Args,
    cutoffs: Cutoffs,
    summary: &mut ExecutionSummary,
) -> HashSet<String> {
    let mut released_amis = HashSet::new();
    if plan.deregister_amis.is_empty() {
        return released_amis;
    }

    let block = |summary: &mut ExecutionSummary, reason: String| {
        summary.skipped_amis += plan.deregister_amis.len();
        summary.failures.push(reason);
    };

    let references = match collect_referenced_ami_ids(ec2_client, asg_client, ssm_client).await {
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
            block(
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
                summary.failures.push(format!(
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
        if !deletable {
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
            .is_some_and(|current| backup_recovery_point_deletion_tag_allows(&current.tags, args));

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

/// Sends a mutating request, retrying only throttling and transient failures. Under `dry_run`, the
/// `DryRunOperation` error means the request would have succeeded.
async fn send_with_retries<T, E, R, F, Fut>(
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
        let error = match send().await {
            Ok(_) => return Ok(()),
            Err(error) => error,
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

fn is_retryable_code(code: &str) -> bool {
    RETRYABLE_ERROR_CODES.contains(&code)
}

fn retry_delay_for_attempt(base_delay_ms: u64, attempt: u32) -> StdDuration {
    let capped_attempt = u64::from(attempt.saturating_sub(1)).min(10);
    let multiplier = 1u64 << capped_attempt;
    StdDuration::from_millis(base_delay_ms.saturating_mul(multiplier))
}

/// Renders an error with its whole `source()` chain. `SdkError`'s own `Display` is only
/// "service error", so the useful detail lives further down.
fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = cause.source();
    }
    message
}

/// `Code: message` for service errors, otherwise the error chain (timeouts, dispatch failures).
fn describe_sdk_error<E, R>(error: &SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    if let SdkError::ServiceError(_) = error {
        match (error.code(), error.message()) {
            (Some(code), Some(message)) => return format!("{code}: {message}"),
            (Some(code), None) => return code.to_string(),
            _ => {}
        }
    }
    error_chain(error)
}

trait AwsResultExt<T> {
    /// Converts an SDK error into an app error that names the operation and keeps the error code.
    fn aws_context(self, operation: &str) -> AppResult<T>;
}

impl<T, E, R> AwsResultExt<T> for Result<T, SdkError<E, R>>
where
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    fn aws_context(self, operation: &str) -> AppResult<T> {
        self.map_err(|error| format!("{operation} failed: {}", describe_sdk_error(&error)).into())
    }
}

fn print_execution_summary(region: &str, summary: &ExecutionSummary) {
    println!(
        "execution region={} mode={} deregistered_amis={} skipped_amis={} deleted_snapshots={} skipped_snapshots={} deleted_backup_recovery_points={} skipped_backup_recovery_points={} failures={}",
        region,
        if summary.dry_run { "dry-run" } else { "execute" },
        summary.deregistered_amis,
        summary.skipped_amis,
        summary.deleted_snapshots,
        summary.skipped_snapshots,
        summary.deleted_backup_recovery_points,
        summary.skipped_backup_recovery_points,
        summary.failures.len(),
    );
    if summary.dry_run {
        println!(
            "  (dry-run: deregistered/deleted counts are requests that passed the AWS dry-run check)"
        );
    }

    if !summary.failures.is_empty() {
        println!("execution_failures:");
        for failure in &summary.failures {
            println!("  {failure}");
        }
    }
}

#[cfg(test)]
mod tests;
