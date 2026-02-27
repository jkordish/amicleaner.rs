use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::Duration as StdDuration;

use aws_config::BehaviorVersion;
use aws_sdk_autoscaling as autoscaling;
use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use aws_sdk_ssm as ssm;
use aws_types::region::Region;
use chrono::{DateTime, Duration, Utc};
use clap::{ArgAction, Parser};
use log::{info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;
type AppResult<T> = Result<T, DynError>;

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

    /// Use AWS API dry-run checks when executing.
    #[arg(long, action = ArgAction::SetTrue)]
    dry_run: bool,

    /// Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots.
    #[arg(long, action = ArgAction::SetTrue)]
    execute_backup_recovery_point_deletes: bool,

    /// Tag key required for AWS Backup recovery point deletion (unless allow-untagged flag set).
    #[arg(long, default_value = "backup-cleanup")]
    backup_delete_tag: String,

    /// Expected value for backup delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    backup_delete_tag_value: String,

    /// Permit deletion of untagged AWS Backup recovery points.
    #[arg(long, action = ArgAction::SetTrue)]
    allow_untagged_backup_recovery_points: bool,

    /// Comma-separated list of regions. If omitted, all enabled account regions are scanned.
    #[arg(long, value_delimiter = ',')]
    regions: Vec<String>,

    /// Keep resources newer than this age in days.
    #[arg(long, default_value_t = 14)]
    min_age_days: i64,

    /// Keep AMIs launched within the last N days.
    #[arg(long, default_value_t = 90)]
    ami_recent_launch_days: i64,

    /// Tag key that protects resources from deletion.
    #[arg(long, default_value = "DoNotDelete")]
    protect_tag: String,

    /// Expected value for protect tag. Use empty string to protect by key presence only.
    #[arg(long, default_value = "true")]
    protect_tag_value: String,

    /// Tag key required for deletion (unless --allow-untagged).
    #[arg(long, default_value = "cleanup")]
    delete_tag: String,

    /// Expected value for delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    delete_tag_value: String,

    /// Permit deletion of untagged resources.
    #[arg(long, action = ArgAction::SetTrue)]
    allow_untagged: bool,

    /// Keep newest N AMIs per family (ImageFamily tag, otherwise AMI name).
    #[arg(long, default_value_t = 0)]
    keep_latest_per_family: usize,

    /// Max retry attempts for mutating API operations.
    #[arg(long, default_value_t = 3)]
    max_retries: u32,

    /// Base delay in milliseconds between retries.
    #[arg(long, default_value_t = 1_000)]
    retry_delay_ms: u64,
}

#[derive(Clone, Debug)]
struct AmiInfo {
    ami_id: String,
    name: Option<String>,
    creation_date: DateTime<Utc>,
    last_launched_time: Option<DateTime<Utc>>,
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
    tags: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct RegionPlan {
    region: String,
    owned_ami_count: usize,
    owned_snapshot_count: usize,
    referenced_ami_count: usize,
    referenced_snapshot_count_after_ami_keeps: usize,
    deregister_amis: Vec<AmiInfo>,
    delete_snapshots: Vec<SnapshotInfo>,
    backup_managed_snapshots: Vec<SnapshotInfo>,
    delete_backup_recovery_points: Vec<BackupRecoveryPoint>,
    unresolved_backup_snapshots: Vec<SnapshotInfo>,
    tag_filtered_backup_recovery_points: usize,
    skipped_amis: usize,
    skipped_snapshots: usize,
}

#[derive(Clone, Debug, Default)]
struct ExecutionSummary {
    deregistered_amis: usize,
    deleted_snapshots: usize,
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

#[tokio::main]
async fn main() -> AppResult<()> {
    simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).init()?;

    let args = Args::parse();

    if args.min_age_days < 0 {
        return Err("--min-age-days cannot be negative".into());
    }
    if args.ami_recent_launch_days < 0 {
        return Err("--ami-recent-launch-days cannot be negative".into());
    }

    let shared_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let home_ec2 = ec2::Client::new(&shared_config);

    let regions = resolve_regions(&args, &home_ec2).await?;
    if regions.is_empty() {
        return Err("No AWS regions resolved".into());
    }

    info!(
        "mode={} dry_run={} regions={}",
        if args.execute { "execute" } else { "plan" },
        args.dry_run,
        regions.join(",")
    );

    let mut had_failures = false;

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
            &args,
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
                if args.execute {
                    let summary =
                        execute_region_plan(&ec2_client, &backup_client, &plan, &args).await;
                    print_execution_summary(&region, &summary);
                    if !summary.failures.is_empty() {
                        had_failures = true;
                    }
                }
            }
            Err(error) => {
                had_failures = true;
                eprintln!("region={region} failed: {error}");
            }
        }
    }

    if had_failures {
        return Err("one or more regions failed".into());
    }

    Ok(())
}

async fn resolve_regions(args: &Args, ec2_client: &ec2::Client) -> AppResult<Vec<String>> {
    if !args.regions.is_empty() {
        let mut regions = args
            .regions
            .iter()
            .map(|region| region.trim())
            .filter(|region| !region.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<String>>();
        regions.sort_unstable();
        regions.dedup();
        return Ok(regions);
    }

    let response = ec2_client.describe_regions().all_regions(true).send().await?;
    let mut regions = response
        .regions()
        .iter()
        .filter_map(|region| region.region_name().map(ToString::to_string))
        .collect::<Vec<String>>();

    regions.sort_unstable();
    regions.dedup();

    Ok(regions)
}

async fn build_region_plan(
    args: &Args,
    region: &str,
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    backup_client: &backup::Client,
    ssm_client: &ssm::Client,
) -> AppResult<RegionPlan> {
    let owned_amis = list_owned_amis(ec2_client).await?;
    let owned_snapshots = list_owned_snapshots(ec2_client).await?;
    let referenced_ami_ids = collect_referenced_ami_ids(ec2_client, asg_client, ssm_client).await?;

    let mut plan = plan_region(args, region, owned_amis, owned_snapshots, referenced_ami_ids);

    match discover_backup_recovery_points(backup_client, &plan.backup_managed_snapshots).await {
        Ok((mut resolved, unresolved)) => {
            let mut tag_filtered = 0usize;
            resolved.retain(|recovery_point| {
                let allowed = backup_recovery_point_deletion_tag_allows(&recovery_point.tags, args);
                if !allowed {
                    tag_filtered += 1;
                }
                allowed
            });
            plan.tag_filtered_backup_recovery_points = tag_filtered;
            plan.delete_backup_recovery_points = resolved;
            plan.unresolved_backup_snapshots = unresolved;
        }
        Err(error) => {
            warn!(
                "Failed to map backup-managed snapshots to recovery points in region {}: {}",
                region, error
            );
            plan.unresolved_backup_snapshots = plan.backup_managed_snapshots.clone();
        }
    }

    Ok(plan)
}

fn plan_region(
    args: &Args,
    region: &str,
    owned_amis: Vec<AmiInfo>,
    owned_snapshots: Vec<SnapshotInfo>,
    referenced_ami_ids: HashSet<String>,
) -> RegionPlan {
    let now = Utc::now();
    let min_age_cutoff = now - Duration::days(args.min_age_days);
    let recent_launch_cutoff = now - Duration::days(args.ami_recent_launch_days);

    let keep_latest_ami_ids =
        select_latest_ami_ids_by_family(&owned_amis, args.keep_latest_per_family);

    let mut deregister_amis = Vec::new();
    let mut skipped_amis = 0usize;

    for ami in &owned_amis {
        if should_deregister_ami(
            ami,
            &referenced_ami_ids,
            &keep_latest_ami_ids,
            min_age_cutoff,
            recent_launch_cutoff,
            args,
        ) {
            deregister_amis.push(ami.clone());
        } else {
            skipped_amis += 1;
        }
    }

    let deregister_ami_ids =
        deregister_amis.iter().map(|ami| ami.ami_id.clone()).collect::<HashSet<String>>();

    let remaining_ami_snapshot_refs = owned_amis
        .iter()
        .filter(|ami| !deregister_ami_ids.contains(&ami.ami_id))
        .flat_map(|ami| ami.snapshot_ids.iter().cloned())
        .collect::<HashSet<String>>();

    let snapshots_associated_with_candidate_amis = deregister_amis
        .iter()
        .flat_map(|ami| ami.snapshot_ids.iter().cloned())
        .collect::<HashSet<String>>();

    let mut delete_snapshots = Vec::new();
    let mut backup_managed_snapshots = Vec::new();
    let mut skipped_snapshots = 0usize;

    for snapshot in &owned_snapshots {
        let eligible = snapshot.start_time < min_age_cutoff
            && !remaining_ami_snapshot_refs.contains(&snapshot.snapshot_id)
            && !snapshots_associated_with_candidate_amis.contains(&snapshot.snapshot_id)
            && !has_expected_tag(
                &snapshot.tags,
                &args.protect_tag,
                optional_expected_value(&args.protect_tag_value),
            )
            && deletion_tag_allows(&snapshot.tags, args);

        if !eligible {
            skipped_snapshots += 1;
            continue;
        }

        if looks_like_backup_managed(snapshot) {
            backup_managed_snapshots.push(snapshot.clone());
        } else {
            delete_snapshots.push(snapshot.clone());
        }
    }

    RegionPlan {
        region: region.to_string(),
        owned_ami_count: owned_amis.len(),
        owned_snapshot_count: owned_snapshots.len(),
        referenced_ami_count: referenced_ami_ids.len(),
        referenced_snapshot_count_after_ami_keeps: remaining_ami_snapshot_refs.len(),
        deregister_amis,
        delete_snapshots,
        backup_managed_snapshots,
        delete_backup_recovery_points: Vec::new(),
        unresolved_backup_snapshots: Vec::new(),
        tag_filtered_backup_recovery_points: 0,
        skipped_amis,
        skipped_snapshots,
    }
}

fn should_deregister_ami(
    ami: &AmiInfo,
    referenced_ami_ids: &HashSet<String>,
    keep_latest_ami_ids: &HashSet<String>,
    min_age_cutoff: DateTime<Utc>,
    recent_launch_cutoff: DateTime<Utc>,
    args: &Args,
) -> bool {
    if ami.creation_date >= min_age_cutoff {
        return false;
    }

    if referenced_ami_ids.contains(&ami.ami_id) {
        return false;
    }

    if keep_latest_ami_ids.contains(&ami.ami_id) {
        return false;
    }

    if has_expected_tag(
        &ami.tags,
        &args.protect_tag,
        optional_expected_value(&args.protect_tag_value),
    ) {
        return false;
    }

    if !deletion_tag_allows(&ami.tags, args) {
        return false;
    }

    if let Some(last_launched_time) = ami.last_launched_time
        && last_launched_time >= recent_launch_cutoff
    {
        return false;
    }

    true
}

fn optional_expected_value(value: &str) -> Option<&str> {
    if value.trim().is_empty() { None } else { Some(value) }
}

fn has_expected_tag(
    tags: &HashMap<String, String>,
    key: &str,
    expected_value: Option<&str>,
) -> bool {
    let Some(actual_value) = tags.get(key) else {
        return false;
    };

    match expected_value {
        Some(expected) => actual_value.eq_ignore_ascii_case(expected),
        None => true,
    }
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

fn looks_like_backup_managed(snapshot: &SnapshotInfo) -> bool {
    if snapshot.tags.keys().any(|key| key.starts_with("aws:backup:")) {
        return true;
    }

    snapshot.description.as_deref().is_some_and(|description| {
        let lowered = description.to_ascii_lowercase();
        lowered.contains("aws backup") || lowered.contains("recovery point")
    })
}

fn select_latest_ami_ids_by_family(amis: &[AmiInfo], keep_count: usize) -> HashSet<String> {
    if keep_count == 0 {
        return HashSet::new();
    }

    let mut families: HashMap<String, Vec<&AmiInfo>> = HashMap::new();

    for ami in amis {
        let family = ami
            .tags
            .get("ImageFamily")
            .cloned()
            .or_else(|| ami.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        families.entry(family).or_default().push(ami);
    }

    let mut keep_ids = HashSet::new();

    for (_family, mut family_amis) in families {
        family_amis.sort_by(|a, b| b.creation_date.cmp(&a.creation_date));
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
        let output = page?;
        for image in output.images() {
            let Some(ami_id) = image.image_id() else {
                continue;
            };
            let Some(creation_raw) = image.creation_date() else {
                continue;
            };
            let Some(creation_date) = parse_aws_timestamp(creation_raw) else {
                warn!("Skipping AMI with unparseable creation_date: {}", creation_raw);
                continue;
            };

            let snapshot_ids = image
                .block_device_mappings()
                .iter()
                .filter_map(|mapping| mapping.ebs().and_then(|ebs| ebs.snapshot_id()))
                .map(ToString::to_string)
                .collect::<Vec<String>>();

            let tags = tags_to_map(image.tags());
            let last_launched_time = image.last_launched_time().and_then(parse_aws_timestamp);

            amis.push(AmiInfo {
                ami_id: ami_id.to_string(),
                name: image.name().map(ToString::to_string),
                creation_date,
                last_launched_time,
                snapshot_ids,
                tags,
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
        let output = page?;
        for snapshot in output.snapshots() {
            let Some(snapshot_id) = snapshot.snapshot_id() else {
                continue;
            };
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

async fn discover_backup_recovery_points(
    backup_client: &backup::Client,
    backup_managed_snapshots: &[SnapshotInfo],
) -> AppResult<(Vec<BackupRecoveryPoint>, Vec<SnapshotInfo>)> {
    if backup_managed_snapshots.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let candidate_snapshot_ids = backup_managed_snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.clone())
        .collect::<HashSet<String>>();

    let mut resolved_by_snapshot: HashMap<String, BackupRecoveryPoint> = HashMap::new();

    let mut vault_pages = backup_client.list_backup_vaults().into_paginator().page_size(100).send();
    while let Some(vault_page) = vault_pages.next().await {
        let vault_output = vault_page?;
        for vault in vault_output.backup_vault_list() {
            let Some(vault_name) = vault.backup_vault_name() else {
                continue;
            };

            let mut recovery_point_pages = backup_client
                .list_recovery_points_by_backup_vault()
                .backup_vault_name(vault_name)
                .by_resource_type("EBS")
                .into_paginator()
                .page_size(100)
                .send();

            while let Some(recovery_point_page) = recovery_point_pages.next().await {
                let recovery_point_output = recovery_point_page?;
                for recovery_point in recovery_point_output.recovery_points() {
                    let Some(resource_arn) = recovery_point.resource_arn() else {
                        continue;
                    };
                    let Some(snapshot_id) = snapshot_id_from_resource_arn(resource_arn) else {
                        continue;
                    };
                    if !candidate_snapshot_ids.contains(snapshot_id) {
                        continue;
                    }
                    let Some(recovery_point_arn) = recovery_point.recovery_point_arn() else {
                        continue;
                    };

                    let tags =
                        match list_backup_recovery_point_tags(backup_client, recovery_point_arn)
                            .await
                        {
                            Ok(tags) => tags,
                            Err(error) => {
                                warn!(
                                    "Failed to fetch tags for backup recovery point {}: {}",
                                    recovery_point_arn, error
                                );
                                continue;
                            }
                        };

                    resolved_by_snapshot.entry(snapshot_id.to_string()).or_insert_with(|| {
                        BackupRecoveryPoint {
                            snapshot_id: snapshot_id.to_string(),
                            backup_vault_name: vault_name.to_string(),
                            recovery_point_arn: recovery_point_arn.to_string(),
                            tags,
                        }
                    });
                }
            }
        }
    }

    let mut resolved = resolved_by_snapshot.into_values().collect::<Vec<BackupRecoveryPoint>>();
    resolved.sort_by(|left, right| left.snapshot_id.cmp(&right.snapshot_id));

    let resolved_snapshot_ids =
        resolved.iter().map(|entry| entry.snapshot_id.clone()).collect::<HashSet<String>>();

    let unresolved = backup_managed_snapshots
        .iter()
        .filter(|snapshot| !resolved_snapshot_ids.contains(&snapshot.snapshot_id))
        .cloned()
        .collect::<Vec<SnapshotInfo>>();

    Ok((resolved, unresolved))
}

fn snapshot_id_from_resource_arn(resource_arn: &str) -> Option<&str> {
    let snapshot_id = resource_arn.split('/').next_back()?;
    snapshot_id.starts_with("snap-").then_some(snapshot_id)
}

async fn list_backup_recovery_point_tags(
    backup_client: &backup::Client,
    recovery_point_arn: &str,
) -> AppResult<HashMap<String, String>> {
    let response = backup_client.list_tags().resource_arn(recovery_point_arn).send().await?;
    let mut tags = HashMap::new();

    if let Some(response_tags) = response.tags() {
        for (key, value) in response_tags {
            tags.insert(key.to_string(), value.to_string());
        }
    }

    Ok(tags)
}

async fn collect_referenced_ami_ids(
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    ssm_client: &ssm::Client,
) -> AppResult<HashSet<String>> {
    let mut references = HashSet::new();
    let mut ssm_cache = HashMap::new();

    references.extend(collect_instance_ami_ids(ec2_client).await?);
    references
        .extend(collect_launch_template_ami_ids(ec2_client, ssm_client, &mut ssm_cache).await?);
    references
        .extend(collect_asg_ami_ids(ec2_client, asg_client, ssm_client, &mut ssm_cache).await?);

    Ok(references)
}

async fn collect_instance_ami_ids(ec2_client: &ec2::Client) -> AppResult<HashSet<String>> {
    let state_filter = ec2::types::Filter::builder()
        .name("instance-state-name")
        .values("pending")
        .values("running")
        .values("stopping")
        .values("stopped")
        .values("shutting-down")
        .build();

    let mut references = HashSet::new();

    let mut pages = ec2_client
        .describe_instances()
        .filters(state_filter)
        .into_paginator()
        .page_size(100)
        .send();

    while let Some(page) = pages.next().await {
        let output = page?;
        for reservation in output.reservations() {
            for instance in reservation.instances() {
                if let Some(image_id) = instance.image_id()
                    && image_id.starts_with("ami-")
                {
                    references.insert(image_id.to_string());
                }
            }
        }
    }

    Ok(references)
}

async fn collect_launch_template_ami_ids(
    ec2_client: &ec2::Client,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
) -> AppResult<HashSet<String>> {
    let mut references = HashSet::new();

    let mut template_pages =
        ec2_client.describe_launch_templates().into_paginator().page_size(100).send();

    while let Some(template_page) = template_pages.next().await {
        let output = template_page?;
        for launch_template in output.launch_templates() {
            let mut request = ec2_client.describe_launch_template_versions();

            if let Some(launch_template_id) = launch_template.launch_template_id() {
                request = request.launch_template_id(launch_template_id);
            } else if let Some(launch_template_name) = launch_template.launch_template_name() {
                request = request.launch_template_name(launch_template_name);
            } else {
                continue;
            }

            let mut version_pages = request.into_paginator().page_size(100).send();
            while let Some(version_page) = version_pages.next().await {
                let version_output = version_page?;
                for version in version_output.launch_template_versions() {
                    if let Some(image_reference) = version
                        .launch_template_data()
                        .and_then(|launch_template_data| launch_template_data.image_id())
                        && let Some(resolved_ami_id) =
                            resolve_image_reference(image_reference, ssm_client, ssm_cache).await?
                    {
                        references.insert(resolved_ami_id);
                    }
                }
            }
        }
    }

    Ok(references)
}

async fn collect_asg_ami_ids(
    ec2_client: &ec2::Client,
    asg_client: &autoscaling::Client,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
) -> AppResult<HashSet<String>> {
    let mut references = HashSet::new();

    let mut group_pages =
        asg_client.describe_auto_scaling_groups().into_paginator().page_size(100).send();

    while let Some(group_page) = group_pages.next().await {
        let output = group_page?;
        for group in output.auto_scaling_groups() {
            if let Some(launch_template_spec) = group.launch_template() {
                let template_refs = resolve_launch_template_ref_image_ids(
                    ec2_client,
                    ssm_client,
                    ssm_cache,
                    &LaunchTemplateRef {
                        launch_template_id: launch_template_spec
                            .launch_template_id()
                            .map(ToString::to_string),
                        launch_template_name: launch_template_spec
                            .launch_template_name()
                            .map(ToString::to_string),
                        version: launch_template_spec.version().map(ToString::to_string),
                    },
                )
                .await?;

                references.extend(template_refs);
            }

            if let Some(mixed_instances_policy) = group.mixed_instances_policy()
                && let Some(launch_template) = mixed_instances_policy.launch_template()
            {
                if let Some(base_spec) = launch_template.launch_template_specification() {
                    let template_refs = resolve_launch_template_ref_image_ids(
                        ec2_client,
                        ssm_client,
                        ssm_cache,
                        &LaunchTemplateRef {
                            launch_template_id: base_spec
                                .launch_template_id()
                                .map(ToString::to_string),
                            launch_template_name: base_spec
                                .launch_template_name()
                                .map(ToString::to_string),
                            version: base_spec.version().map(ToString::to_string),
                        },
                    )
                    .await?;
                    references.extend(template_refs);
                }

                for override_spec in launch_template.overrides() {
                    if let Some(template_spec) = override_spec.launch_template_specification() {
                        let template_refs = resolve_launch_template_ref_image_ids(
                            ec2_client,
                            ssm_client,
                            ssm_cache,
                            &LaunchTemplateRef {
                                launch_template_id: template_spec
                                    .launch_template_id()
                                    .map(ToString::to_string),
                                launch_template_name: template_spec
                                    .launch_template_name()
                                    .map(ToString::to_string),
                                version: template_spec.version().map(ToString::to_string),
                            },
                        )
                        .await?;
                        references.extend(template_refs);
                    }
                }
            }
        }
    }

    let mut launch_configuration_pages =
        asg_client.describe_launch_configurations().into_paginator().page_size(100).send();

    while let Some(launch_configuration_page) = launch_configuration_pages.next().await {
        let output = launch_configuration_page?;
        for launch_configuration in output.launch_configurations() {
            if let Some(image_id) = launch_configuration.image_id()
                && image_id.starts_with("ami-")
            {
                references.insert(image_id.to_string());
            }
        }
    }

    Ok(references)
}

async fn resolve_launch_template_ref_image_ids(
    ec2_client: &ec2::Client,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
    launch_template_ref: &LaunchTemplateRef,
) -> AppResult<HashSet<String>> {
    let mut request = ec2_client.describe_launch_template_versions();

    if let Some(launch_template_id) = launch_template_ref.launch_template_id.as_deref() {
        request = request.launch_template_id(launch_template_id);
    } else if let Some(launch_template_name) = launch_template_ref.launch_template_name.as_deref() {
        request = request.launch_template_name(launch_template_name);
    } else {
        return Ok(HashSet::new());
    }

    if let Some(version) = launch_template_ref.version.as_deref() {
        request = request.versions(version);
        let output = request.send().await?;

        let mut references = HashSet::new();
        for version in output.launch_template_versions() {
            if let Some(image_reference) = version
                .launch_template_data()
                .and_then(|launch_template_data| launch_template_data.image_id())
                && let Some(resolved_ami_id) =
                    resolve_image_reference(image_reference, ssm_client, ssm_cache).await?
            {
                references.insert(resolved_ami_id);
            }
        }

        return Ok(references);
    }

    // No version provided; keep all versions conservative.
    let mut references = HashSet::new();
    let mut pages = request.into_paginator().page_size(100).send();

    while let Some(page) = pages.next().await {
        let output = page?;
        for version in output.launch_template_versions() {
            if let Some(image_reference) = version
                .launch_template_data()
                .and_then(|launch_template_data| launch_template_data.image_id())
                && let Some(resolved_ami_id) =
                    resolve_image_reference(image_reference, ssm_client, ssm_cache).await?
            {
                references.insert(resolved_ami_id);
            }
        }
    }

    Ok(references)
}

async fn resolve_image_reference(
    image_reference: &str,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
) -> AppResult<Option<String>> {
    if image_reference.starts_with("ami-") {
        return Ok(Some(image_reference.to_string()));
    }

    let Some(parameter_name) = extract_ssm_parameter_name(image_reference) else {
        warn!("Ignoring non-AMI launch-template image reference: {}", image_reference);
        return Ok(None);
    };

    if let Some(cached) = ssm_cache.get(&parameter_name) {
        return Ok(cached.clone());
    }

    let response = ssm_client.get_parameter().name(parameter_name.clone()).send().await;
    let resolved_ami = match response {
        Ok(output) => output
            .parameter()
            .and_then(|parameter| parameter.value())
            .filter(|value| value.starts_with("ami-"))
            .map(ToString::to_string),
        Err(error) => {
            warn!("Failed to resolve SSM image parameter '{}': {}", parameter_name, error);
            None
        }
    };

    ssm_cache.insert(parameter_name, resolved_ami.clone());
    Ok(resolved_ami)
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
        "owned_amis={} referenced_amis={} candidate_amis={} skipped_amis={}",
        plan.owned_ami_count,
        plan.referenced_ami_count,
        plan.deregister_amis.len(),
        plan.skipped_amis,
    );
    println!(
        "owned_snapshots={} referenced_snapshots_after_ami_keeps={} candidate_snapshots={} backup_managed_candidates={} backup_recovery_point_candidates={} tag_filtered_backup_recovery_points={} unresolved_backup_snapshots={} skipped_snapshots={}",
        plan.owned_snapshot_count,
        plan.referenced_snapshot_count_after_ami_keeps,
        plan.delete_snapshots.len(),
        plan.backup_managed_snapshots.len(),
        plan.delete_backup_recovery_points.len(),
        plan.tag_filtered_backup_recovery_points,
        plan.unresolved_backup_snapshots.len(),
        plan.skipped_snapshots,
    );

    if !plan.deregister_amis.is_empty() {
        println!("deregister_amis:");
        for ami in &plan.deregister_amis {
            println!(
                "  {} {} created={} last_launched={}",
                ami.ami_id,
                ami.name.clone().unwrap_or_else(|| "<unnamed>".to_string()),
                ami.creation_date.to_rfc3339(),
                ami.last_launched_time
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "never/unknown".to_string())
            );
        }
    }

    if !plan.delete_snapshots.is_empty() {
        println!("delete_snapshots:");
        for snapshot in &plan.delete_snapshots {
            println!("  {} created={}", snapshot.snapshot_id, snapshot.start_time.to_rfc3339());
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

    if !plan.unresolved_backup_snapshots.is_empty() {
        println!("backup_managed_snapshots_requiring_aws_backup_cleanup:");
        for snapshot in &plan.unresolved_backup_snapshots {
            println!(
                "  {} description={}",
                snapshot.snapshot_id,
                snapshot.description.as_deref().unwrap_or("<none>")
            );
        }
    }
}

async fn execute_region_plan(
    ec2_client: &ec2::Client,
    backup_client: &backup::Client,
    plan: &RegionPlan,
    args: &Args,
) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();

    for ami in &plan.deregister_amis {
        match deregister_ami_with_retries(
            ec2_client,
            &ami.ami_id,
            args.dry_run,
            args.max_retries,
            args.retry_delay_ms,
        )
        .await
        {
            Ok(_) => {
                summary.deregistered_amis += 1;
            }
            Err(error) => {
                summary.failures.push(format!("deregister {} failed: {}", ami.ami_id, error));
            }
        }
    }

    for snapshot in &plan.delete_snapshots {
        match delete_snapshot_with_retries(
            ec2_client,
            &snapshot.snapshot_id,
            args.dry_run,
            args.max_retries,
            args.retry_delay_ms,
        )
        .await
        {
            Ok(_) => {
                summary.deleted_snapshots += 1;
            }
            Err(error) => {
                summary
                    .failures
                    .push(format!("delete snapshot {} failed: {}", snapshot.snapshot_id, error));
            }
        }
    }

    if args.execute_backup_recovery_point_deletes {
        for recovery_point in &plan.delete_backup_recovery_points {
            if args.dry_run {
                summary.deleted_backup_recovery_points += 1;
                continue;
            }

            match delete_recovery_point_with_retries(
                backup_client,
                &recovery_point.backup_vault_name,
                &recovery_point.recovery_point_arn,
                args.max_retries,
                args.retry_delay_ms,
            )
            .await
            {
                Ok(_) => {
                    summary.deleted_backup_recovery_points += 1;
                }
                Err(error) => {
                    summary.failures.push(format!(
                        "delete recovery point {} in vault {} failed: {}",
                        recovery_point.recovery_point_arn, recovery_point.backup_vault_name, error
                    ));
                }
            }
        }
    } else {
        summary.skipped_backup_recovery_points = plan.delete_backup_recovery_points.len();
    }

    summary
}

fn is_dry_run_operation(message: &str) -> bool {
    message.to_ascii_lowercase().contains("dryrunoperation")
}

async fn deregister_ami_with_retries(
    ec2_client: &ec2::Client,
    ami_id: &str,
    dry_run: bool,
    max_retries: u32,
    retry_delay_ms: u64,
) -> Result<(), String> {
    let attempts = max_retries.max(1);

    for attempt in 1..=attempts {
        let mut request =
            ec2_client.deregister_image().image_id(ami_id).delete_associated_snapshots(true);
        if dry_run {
            request = request.dry_run(true);
        }

        match request.send().await {
            Ok(_) => return Ok(()),
            Err(error) if dry_run && is_dry_run_operation(&error.to_string()) => return Ok(()),
            Err(error) => {
                if attempt == attempts {
                    return Err(error.to_string());
                }
                tokio::time::sleep(retry_delay_for_attempt(retry_delay_ms, attempt)).await;
            }
        }
    }

    Err("unexpected retry loop exit".to_string())
}

async fn delete_snapshot_with_retries(
    ec2_client: &ec2::Client,
    snapshot_id: &str,
    dry_run: bool,
    max_retries: u32,
    retry_delay_ms: u64,
) -> Result<(), String> {
    let attempts = max_retries.max(1);

    for attempt in 1..=attempts {
        let mut request = ec2_client.delete_snapshot().snapshot_id(snapshot_id);
        if dry_run {
            request = request.dry_run(true);
        }

        match request.send().await {
            Ok(_) => return Ok(()),
            Err(error) if dry_run && is_dry_run_operation(&error.to_string()) => return Ok(()),
            Err(error) => {
                if attempt == attempts {
                    return Err(error.to_string());
                }
                tokio::time::sleep(retry_delay_for_attempt(retry_delay_ms, attempt)).await;
            }
        }
    }

    Err("unexpected retry loop exit".to_string())
}

async fn delete_recovery_point_with_retries(
    backup_client: &backup::Client,
    backup_vault_name: &str,
    recovery_point_arn: &str,
    max_retries: u32,
    retry_delay_ms: u64,
) -> Result<(), String> {
    let attempts = max_retries.max(1);

    for attempt in 1..=attempts {
        let request = backup_client
            .delete_recovery_point()
            .backup_vault_name(backup_vault_name)
            .recovery_point_arn(recovery_point_arn);

        match request.send().await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if attempt == attempts {
                    return Err(error.to_string());
                }
                tokio::time::sleep(retry_delay_for_attempt(retry_delay_ms, attempt)).await;
            }
        }
    }

    Err("unexpected retry loop exit".to_string())
}

fn retry_delay_for_attempt(base_delay_ms: u64, attempt: u32) -> StdDuration {
    let capped_attempt = u64::from(attempt.saturating_sub(1)).min(10);
    let multiplier = 1u64 << capped_attempt;
    StdDuration::from_millis(base_delay_ms.saturating_mul(multiplier))
}

fn print_execution_summary(region: &str, summary: &ExecutionSummary) {
    println!(
        "execution region={} deregistered_amis={} deleted_snapshots={} deleted_backup_recovery_points={} skipped_backup_recovery_points={} failures={}",
        region,
        summary.deregistered_amis,
        summary.deleted_snapshots,
        summary.deleted_backup_recovery_points,
        summary.skipped_backup_recovery_points,
        summary.failures.len(),
    );

    if !summary.failures.is_empty() {
        println!("execution_failures:");
        for failure in &summary.failures {
            println!("  {failure}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ami(
        ami_id: &str,
        family: Option<&str>,
        name: Option<&str>,
        creation_date: DateTime<Utc>,
        last_launched_time: Option<DateTime<Utc>>,
        tags: HashMap<String, String>,
        snapshot_ids: &[&str],
    ) -> AmiInfo {
        let mut combined_tags = tags;
        if let Some(family) = family {
            combined_tags.insert("ImageFamily".to_string(), family.to_string());
        }

        AmiInfo {
            ami_id: ami_id.to_string(),
            name: name.map(ToString::to_string),
            creation_date,
            last_launched_time,
            snapshot_ids: snapshot_ids.iter().map(|entry| (*entry).to_string()).collect(),
            tags: combined_tags,
        }
    }

    fn make_snapshot(
        snapshot_id: &str,
        start_time: DateTime<Utc>,
        description: Option<&str>,
        tags: HashMap<String, String>,
    ) -> SnapshotInfo {
        SnapshotInfo {
            snapshot_id: snapshot_id.to_string(),
            start_time,
            description: description.map(ToString::to_string),
            tags,
        }
    }

    fn test_args() -> Args {
        Args {
            execute: false,
            dry_run: false,
            execute_backup_recovery_point_deletes: false,
            backup_delete_tag: "backup-cleanup".to_string(),
            backup_delete_tag_value: "true".to_string(),
            allow_untagged_backup_recovery_points: false,
            regions: Vec::new(),
            min_age_days: 30,
            ami_recent_launch_days: 30,
            protect_tag: "DoNotDelete".to_string(),
            protect_tag_value: "true".to_string(),
            delete_tag: "cleanup".to_string(),
            delete_tag_value: "true".to_string(),
            allow_untagged: false,
            keep_latest_per_family: 0,
            max_retries: 3,
            retry_delay_ms: 1_000,
        }
    }

    #[test]
    fn extract_ssm_parameter_name_handles_supported_formats() {
        assert_eq!(
            extract_ssm_parameter_name("resolve:ssm:/images/prod/latest"),
            Some("/images/prod/latest".to_string())
        );
        assert_eq!(
            extract_ssm_parameter_name("{{resolve:ssm:/images/prod/latest}}"),
            Some("/images/prod/latest".to_string())
        );
        assert_eq!(extract_ssm_parameter_name("ami-123"), None);
    }

    #[test]
    fn has_expected_tag_supports_value_or_key_only() {
        let tags = HashMap::from([
            ("cleanup".to_string(), "true".to_string()),
            ("DoNotDelete".to_string(), "yes".to_string()),
        ]);

        assert!(has_expected_tag(&tags, "cleanup", Some("true")));
        assert!(!has_expected_tag(&tags, "cleanup", Some("false")));
        assert!(has_expected_tag(&tags, "DoNotDelete", None));
    }

    #[test]
    fn backup_recovery_point_deletion_tag_allows_respects_dedicated_toggle() {
        let mut args = test_args();
        let tags = HashMap::from([("backup-cleanup".to_string(), "true".to_string())]);
        let no_tags = HashMap::new();

        assert!(backup_recovery_point_deletion_tag_allows(&tags, &args));
        assert!(!backup_recovery_point_deletion_tag_allows(&no_tags, &args));

        args.allow_untagged_backup_recovery_points = true;
        assert!(backup_recovery_point_deletion_tag_allows(&no_tags, &args));
    }

    #[test]
    fn select_latest_ami_ids_by_family_keeps_latest_per_group() {
        let now = Utc::now();

        let ami_old = make_ami(
            "ami-old",
            Some("app"),
            Some("app"),
            now - Duration::days(20),
            None,
            HashMap::new(),
            &[],
        );
        let ami_new = make_ami(
            "ami-new",
            Some("app"),
            Some("app"),
            now - Duration::days(10),
            None,
            HashMap::new(),
            &[],
        );

        let kept = select_latest_ami_ids_by_family(&[ami_old, ami_new], 1);
        assert_eq!(kept.len(), 1);
        assert!(kept.contains("ami-new"));
    }

    #[test]
    fn plan_region_marks_only_unreferenced_old_and_tagged_resources() {
        let mut args = test_args();
        args.keep_latest_per_family = 0;

        let now = Utc::now();

        let deletable_ami = make_ami(
            "ami-delete",
            Some("svc"),
            Some("svc-1"),
            now - Duration::days(90),
            Some(now - Duration::days(40)),
            HashMap::from([("cleanup".to_string(), "true".to_string())]),
            &["snap-ami-delete"],
        );

        let referenced_ami = make_ami(
            "ami-ref",
            Some("svc"),
            Some("svc-2"),
            now - Duration::days(90),
            Some(now - Duration::days(90)),
            HashMap::from([("cleanup".to_string(), "true".to_string())]),
            &["snap-ref"],
        );

        let protected_ami = make_ami(
            "ami-protect",
            Some("svc"),
            Some("svc-3"),
            now - Duration::days(90),
            None,
            HashMap::from([
                ("cleanup".to_string(), "true".to_string()),
                ("DoNotDelete".to_string(), "true".to_string()),
            ]),
            &["snap-protect"],
        );

        let deletable_snapshot = make_snapshot(
            "snap-delete",
            now - Duration::days(90),
            Some("manual"),
            HashMap::from([("cleanup".to_string(), "true".to_string())]),
        );

        let referenced_snapshot = make_snapshot(
            "snap-ref",
            now - Duration::days(90),
            Some("manual"),
            HashMap::from([("cleanup".to_string(), "true".to_string())]),
        );

        let backup_managed_snapshot = make_snapshot(
            "snap-backup",
            now - Duration::days(90),
            Some("AWS Backup recovery point"),
            HashMap::from([("cleanup".to_string(), "true".to_string())]),
        );

        let plan = plan_region(
            &args,
            "us-east-1",
            vec![deletable_ami, referenced_ami, protected_ami],
            vec![deletable_snapshot, referenced_snapshot, backup_managed_snapshot],
            HashSet::from(["ami-ref".to_string()]),
        );

        assert_eq!(plan.deregister_amis.len(), 1);
        assert_eq!(plan.deregister_amis[0].ami_id, "ami-delete");

        assert_eq!(plan.delete_snapshots.len(), 1);
        assert_eq!(plan.delete_snapshots[0].snapshot_id, "snap-delete");

        assert_eq!(plan.backup_managed_snapshots.len(), 1);
        assert_eq!(plan.backup_managed_snapshots[0].snapshot_id, "snap-backup");
    }
}
