//! amicleaner: conservative mark-and-sweep cleanup of account-owned AMIs, EBS snapshots and AWS
//! Backup EBS recovery points. Per region it builds a plan (`plan`), prints it, and only with
//! `--execute` re-checks every item against live state and applies it (`execute`). Anything it
//! can't determine blocks the deletions it affects; see the safety invariants in CLAUDE.md.

mod aws_backup;
mod cli;
mod errors;
mod execute;
mod inventory;
mod model;
mod plan;
mod policy;
mod references;

use std::process::ExitCode;

use aws_config::BehaviorVersion;
use aws_sdk_backup as backup;
use aws_sdk_ec2 as ec2;
use aws_types::region::Region;
use clap::{Parser, ValueEnum};
use log::{error, info, warn};

use crate::cli::Args;
use crate::errors::{AppResult, AwsResultExt, error_chain};
use crate::execute::{execute_region_plan, print_execution_summary};
use crate::model::RunOutcome;
use crate::plan::{build_region_plan, print_region_plan};
use crate::references::{ReferenceClients, ReferenceSource};

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
    if let Some(warning) = skipped_reference_sources_warning(&args.skip_reference_sources) {
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
        let backup_client = backup::Client::new(&region_config);
        let reference_clients = ReferenceClients::new(&region_config, &args.skip_reference_sources);

        match build_region_plan(args, &region, &ec2_client, &backup_client, &reference_clients)
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
                        &backup_client,
                        &reference_clients,
                        &plan,
                        args,
                    )
                    .await;
                    print_execution_summary(&region, &summary);
                    if !summary.failures.is_empty() {
                        outcome.failed_regions.push(region.clone());
                    }
                    if !summary.blocked.is_empty() && !outcome.blocked_regions.contains(&region) {
                        outcome.blocked_regions.push(region.clone());
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
