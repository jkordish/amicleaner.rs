//! Command-line arguments and their value parsers.

use std::path::PathBuf;

use clap::{ArgAction, Parser};

use crate::references::ReferenceSource;
use crate::report::parse_report_path;

/// Upper bound for day-count flags; keeps chrono date arithmetic far away from overflow.
const MAX_DAYS: i64 = 36_500;

#[derive(Debug, Parser)]
#[command(
    name = "amicleaner",
    version,
    about = "Conservative EC2 AMI + EBS snapshot mark-and-sweep cleanup"
)]
pub(crate) struct Args {
    /// Execute deletions. Without this flag, the tool only prints a plan.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) execute: bool,

    /// Use AWS API dry-run checks when executing (requires --execute).
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) dry_run: bool,

    /// Execute AWS Backup DeleteRecoveryPoint calls for backup-managed snapshots (requires --execute).
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) execute_backup_recovery_point_deletes: bool,

    /// Snapshot tag key required before its AWS Backup recovery point may be deleted.
    #[arg(long, default_value = "backup-cleanup", value_parser = parse_tag_key)]
    pub(crate) backup_delete_tag: String,

    /// Expected value for backup delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    pub(crate) backup_delete_tag_value: String,

    /// Permit deletion of AWS Backup recovery points whose snapshot lacks the backup delete tag.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) allow_untagged_backup_recovery_points: bool,

    /// Comma-separated list of regions. If omitted, all enabled account regions are scanned.
    #[arg(long, value_delimiter = ',', value_parser = parse_region)]
    pub(crate) regions: Vec<String>,

    /// Keep resources newer than this age in days.
    #[arg(
        long,
        default_value_t = 14,
        allow_negative_numbers = true,
        value_parser = clap::value_parser!(i64).range(0..=MAX_DAYS)
    )]
    pub(crate) min_age_days: i64,

    /// Keep AMIs launched within the last N days.
    #[arg(
        long,
        default_value_t = 90,
        allow_negative_numbers = true,
        value_parser = clap::value_parser!(i64).range(0..=MAX_DAYS)
    )]
    pub(crate) ami_recent_launch_days: i64,

    /// Tag key that protects resources from deletion (matched case-insensitively).
    #[arg(long, default_value = "DoNotDelete", value_parser = parse_tag_key)]
    pub(crate) protect_tag: String,

    /// Value the protect tag must have. Empty (the default) protects on key presence alone.
    #[arg(long, default_value = "")]
    pub(crate) protect_tag_value: String,

    /// Tag key required for deletion (unless --allow-untagged).
    #[arg(long, default_value = "cleanup", value_parser = parse_tag_key)]
    pub(crate) delete_tag: String,

    /// Expected value for delete tag. Use empty string to allow any value.
    #[arg(long, default_value = "true")]
    pub(crate) delete_tag_value: String,

    /// Permit deletion of untagged resources.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) allow_untagged: bool,

    /// Keep newest N AMIs per ImageFamily tag. When N > 0, AMIs without that tag are always kept.
    #[arg(long, default_value_t = 0)]
    pub(crate) keep_latest_per_family: usize,

    /// Comma-separated AMI reference sources to skip: image-builder, cloudformation, batch, emr.
    /// All are scanned by default; a source you can't read (e.g. missing IAM permission) blocks
    /// AMI deregistration rather than being ignored.
    #[arg(long, value_enum, value_delimiter = ',')]
    pub(crate) skip_reference_sources: Vec<ReferenceSource>,

    /// Write the full report (every AMI and snapshot with its decision and reason) to a file.
    /// The format follows the extension: .md/.markdown or .json. Repeat for several files.
    #[arg(long, value_name = "FILE", value_parser = parse_report_path)]
    pub(crate) report: Vec<PathBuf>,

    /// Snapshot storage price in USD per GB-month, for the savings estimate (EBS Snapshots
    /// standard tier; check your region's price).
    #[arg(long, value_name = "USD", default_value_t = 0.05, value_parser = parse_price)]
    pub(crate) snapshot_price_per_gb_month: f64,

    /// EBS Snapshots Archive tier price in USD per GB-month, for the savings estimate.
    #[arg(long, value_name = "USD", default_value_t = 0.0125, value_parser = parse_price)]
    pub(crate) archive_price_per_gb_month: f64,

    /// Seconds before a single AWS request attempt is abandoned and retried. Protects against
    /// connections that a proxy drops without closing.
    #[arg(long, value_name = "SECONDS", default_value_t = 60,
          value_parser = clap::value_parser!(u64).range(5..=600))]
    pub(crate) request_timeout_secs: u64,

    /// Directory for the launch template version cache. Defaults to XDG_CACHE_HOME/amicleaner,
    /// else ~/.cache/amicleaner.
    #[arg(long, value_name = "DIR")]
    pub(crate) cache_dir: Option<PathBuf>,

    /// Read every launch template version instead of only those newer than the local cache, and
    /// don't read or write the cache.
    #[arg(long)]
    pub(crate) no_cache: bool,

    /// Days a cached launch template stays trusted before all its versions are read again, which
    /// also forgets versions deleted since. 0 reads everything on every run.
    #[arg(long, value_name = "DAYS", default_value_t = 7,
          value_parser = clap::value_parser!(u16).range(0..=90))]
    pub(crate) cache_max_age_days: u16,

    /// Maximum concurrent read requests per region (launch template versions, CloudFormation
    /// templates, permission lookups). Lower it if AWS throttles the account.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=64))]
    pub(crate) max_concurrency: u16,

    /// Retries after the first attempt when a mutating call hits throttling or a transient error.
    #[arg(long, default_value_t = 2)]
    pub(crate) max_retries: u32,

    /// Base delay in milliseconds between retries (doubles on each retry).
    #[arg(long, default_value_t = 1_000)]
    pub(crate) retry_delay_ms: u64,
}

fn parse_tag_key(raw: &str) -> Result<String, String> {
    let key = raw.trim();
    if key.is_empty() {
        return Err("tag key must not be empty".to_string());
    }
    Ok(key.to_string())
}

fn parse_price(raw: &str) -> Result<f64, String> {
    match raw.trim().parse::<f64>() {
        Ok(price) if price.is_finite() && price >= 0.0 => Ok(price),
        _ => Err(format!("'{raw}' is not a non-negative price")),
    }
}

pub(crate) fn parse_region(raw: &str) -> Result<String, String> {
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
