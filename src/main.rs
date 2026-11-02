//! amicleaner: conservative mark-and-sweep cleanup of account-owned AMIs, EBS snapshots and AWS
//! Backup EBS recovery points. Per region it builds a plan (`plan`), prints it, and only with
//! `--execute` re-checks every item against live state and applies it (`execute`). Anything it
//! can't determine blocks the deletions it affects; see the safety invariants in CLAUDE.md.

mod aws_backup;
mod cli;
mod errors;
mod execute;
mod inventory;
mod lt_prune;
mod model;
mod plan;
mod policy;
mod references;
mod report;
mod ui;

use std::process::ExitCode;

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use aws_sdk_sts as sts;
use aws_types::region::Region;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use log::{error, warn};

use crate::cli::Args;
use crate::errors::{AppResult, AwsResultExt, describe_sdk_error, error_chain};
use crate::execute::execute_region_plan;
use crate::model::RunOutcome;
use crate::plan::build_region_plan;
use crate::references::lt_cache::{LtCacheConfig, default_cache_dir};
use crate::references::{ReferenceClients, ReferenceSource};
use crate::report::{Pricing, RegionReport, Report};

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) = ui::init_logging() {
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
    if let Some(warning) = skipped_reference_sources_warning(&args.skip_reference_sources) {
        warn!("{warning}");
    }

    let started_at = Utc::now();
    let shared_config = aws_config_loader(args).load().await;
    let home_ec2 = ec2::Client::new(&shared_config);

    let regions = resolve_regions(args, &home_ec2).await?;
    if regions.is_empty() {
        return Err("No AWS regions resolved".into());
    }

    let mode = report::run_mode(args);
    ui::print_stdout(&format!(
        "{} {} · {}{} · {}\n",
        console::style("amicleaner").bold(),
        env!("CARGO_PKG_VERSION"),
        mode,
        if mode == "plan" { " (read-only)" } else { "" },
        regions.join(", ")
    ));

    let mut outcome = RunOutcome::default();
    let mut region_reports = Vec::new();
    let mut account_id = None;

    for region in regions {
        let region_config =
            aws_config_loader(args).region(Region::new(region.clone())).load().await;

        let ec2_client = ec2::Client::new(&region_config);
        let backup_client = backup::Client::new(&region_config);
        let reference_clients = ReferenceClients::new(
            &region_config,
            &args.skip_reference_sources,
            usize::from(args.max_concurrency),
        )
        .with_eks(
            &region_config,
            &args.skip_reference_sources,
            args.prune_launch_template_versions,
        )
        .with_lt_cache(lt_cache_config(args, &region_config, &mut account_id).await);

        let region_report =
            match build_region_plan(args, &region, &ec2_client, &backup_client, &reference_clients)
                .await
            {
                Ok(plan) => {
                    if !plan.blocked.is_empty() {
                        outcome.blocked_regions.push(region.clone());
                    }
                    let execution = if args.execute {
                        let summary = execute_region_plan(
                            &ec2_client,
                            &backup_client,
                            &reference_clients,
                            &plan,
                            args,
                        )
                        .await;
                        if !summary.failures.is_empty() {
                            outcome.failed_regions.push(region.clone());
                        }
                        if !summary.blocked.is_empty() && !outcome.blocked_regions.contains(&region)
                        {
                            outcome.blocked_regions.push(region.clone());
                        }
                        Some(summary)
                    } else {
                        None
                    };
                    RegionReport::from_plan(&plan, execution.as_ref(), Pricing::from_args(args))
                }
                Err(error) => {
                    outcome.failed_regions.push(region.clone());
                    RegionReport::failed(&region, error_chain(error.as_ref()))
                }
            };
        ui::print_stdout(&report::render_region_text(&region_report));
        region_reports.push(region_report);
    }

    let report = Report::new(args, started_at, region_reports);
    for path in &args.report {
        report::write_report(&report, path)?;
    }
    ui::print_stdout(&report::render_footer(&report, &args.report));

    Ok(outcome)
}

/// Attempts per AWS request (first try included) in the SDK's own retry layer.
const SDK_MAX_ATTEMPTS: u32 = 6;

/// SDK config with a deadline per request attempt. Without one, a request whose connection is
/// dropped silently (e.g. by a TLS-inspecting proxy) waits forever instead of being retried by the
/// SDK's standard retry strategy; a delta0 plan run sat idle for 20 minutes that way.
fn aws_config_loader(args: &Args) -> aws_config::ConfigLoader {
    let timeouts = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(10))
        .operation_attempt_timeout(Duration::from_secs(args.request_timeout_secs))
        .build();
    // Adaptive retry backs off client-side when AWS throttles (a throttled CloudFormation read
    // failed a whole delta0 reference scan with the default 3 attempts).
    let retries = RetryConfig::adaptive().with_max_attempts(SDK_MAX_ATTEMPTS);
    aws_config::defaults(BehaviorVersion::latest()).timeout_config(timeouts).retry_config(retries)
}

/// Launch template cache settings, or `None` (with a warning) when the cache can't be used. The
/// cache is keyed by account, so the account id is looked up once (STS) and reused.
async fn lt_cache_config(
    args: &Args,
    region_config: &aws_types::SdkConfig,
    account_id: &mut Option<String>,
) -> Option<LtCacheConfig> {
    if args.no_cache {
        return None;
    }
    let dir = args.cache_dir.clone().or_else(|| {
        default_cache_dir(
            std::env::var("XDG_CACHE_HOME").ok().as_deref(),
            std::env::var("HOME").ok().as_deref(),
        )
    });
    let Some(dir) = dir else {
        warn!(
            "no cache directory (set --cache-dir or HOME); reading every launch template version"
        );
        return None;
    };
    if account_id.is_none() {
        match sts::Client::new(region_config).get_caller_identity().send().await {
            Ok(identity) => *account_id = identity.account().map(ToString::to_string),
            Err(error) => {
                warn!(
                    "launch template cache disabled: GetCallerIdentity failed: {}",
                    describe_sdk_error(&error)
                );
                return None;
            }
        }
    }
    Some(LtCacheConfig {
        dir,
        account_id: account_id.clone()?,
        max_age: chrono::Duration::days(i64::from(args.cache_max_age_days)),
    })
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

/// Skipping a reference source means AMIs it uses aren't protected, so say so on every run.
fn skipped_reference_sources_warning(skipped: &[ReferenceSource]) -> Option<String> {
    if skipped.is_empty() {
        return None;
    }
    let mut names = skipped
        .iter()
        .filter_map(|source| source.to_possible_value().map(|value| value.get_name().to_string()))
        .collect::<Vec<String>>();
    names.sort_unstable();
    names.dedup();
    Some(format!(
        "not scanning {} for AMI references (--skip-reference-sources); AMIs used only there are not protected",
        names.join(", ")
    ))
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

#[cfg(test)]
mod tests;
