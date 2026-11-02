//! Opt-in pruning of old launch template versions (`--prune-launch-template-versions`).
//!
//! Versions are free, but every version that names an AMI keeps that AMI (and its snapshots)
//! "referenced" forever, and a template stops accepting new versions at 10,000. A version is
//! deleted only when it's old, not among the newest few, not the default, and nothing (ASG, fleet,
//! Batch, EKS) launches from it by number. Any doubt about who pins what blocks pruning for the
//! region. AMIs freed this way are left for the next run, where every AMI rule still applies.

use std::collections::{BTreeMap, HashMap, HashSet};

use aws_sdk_ec2 as ec2;
use chrono::{DateTime, Utc};

use crate::cli::Args;
use crate::errors::error_chain;
use crate::execute::send_with_retries_output;
use crate::inventory::is_ami_id;
use crate::model::{
    Cutoffs, ExecutionSummary, LaunchTemplatePrunePlan, RegionPlan, RetryPolicy, TemplatePrune,
};
use crate::policy::{
    parse_pinned_version, recent_versions, template_keep_reason, version_keep_reason,
};
use crate::references::lt_cache::LaunchTemplateCache;
use crate::references::{AmiReferenceScan, ReferenceClients, collect_referenced_ami_ids};
use crate::ui::Phase;

/// Most versions `DeleteLaunchTemplateVersions` accepts in one call.
const DELETE_BATCH: usize = 200;

/// Why pruning can't run safely for this scan, if it can't. Pins are only known for the sources
/// that were read, so a skipped or failed source means "unknown", which blocks.
pub(crate) fn prune_block_reason(
    scan: &AmiReferenceScan,
    clients: &ReferenceClients,
) -> Option<String> {
    let prefix = "launch template version pruning cancelled";
    if !scan.unresolved_references.is_empty() {
        return Some(format!(
            "{prefix}: {} unresolved reference(s), so pinned versions aren't fully known",
            scan.unresolved_references.len()
        ));
    }
    if clients.batch.is_none() {
        return Some(format!("{prefix}: Batch isn't scanned (--skip-reference-sources batch)"));
    }
    if clients.eks.is_none() {
        return Some(format!("{prefix}: EKS isn't scanned (--skip-reference-sources eks)"));
    }
    None
}

/// Decides which versions to delete (pure). `owned_ami_ids` is only used for the freed-AMI
/// projection.
pub(crate) fn plan_launch_template_pruning(
    scan: &AmiReferenceScan,
    owned_ami_ids: &HashSet<&str>,
    keep: usize,
    min_age_cutoff: DateTime<Utc>,
    args: &Args,
) -> LaunchTemplatePrunePlan {
    let mut plan = LaunchTemplatePrunePlan::default();
    // AMI id -> references that the versions to delete account for.
    let mut deleted_references: HashMap<&str, usize> = HashMap::new();

    for template in &scan.launch_templates {
        let pins = [Some(&template.id), template.name.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(|key| scan.pinned_launch_template_versions.get(key))
            .flatten()
            .cloned()
            .collect::<HashSet<String>>();

        if let Some(reason) = template_keep_reason(template, &pins, args) {
            *plan.kept_by_reason.entry(reason.code()).or_default() += template.versions.len();
            continue;
        }

        let pinned = pins
            .iter()
            .filter_map(|pin| parse_pinned_version(pin).flatten())
            .collect::<HashSet<_>>();
        let recent = recent_versions(template, keep);
        let mut delete_versions = Vec::new();
        for (number, version) in &template.versions {
            match version_keep_reason(
                *number,
                version.created,
                template,
                &recent,
                &pinned,
                min_age_cutoff,
            ) {
                Some(reason) => *plan.kept_by_reason.entry(reason.code()).or_default() += 1,
                None => {
                    delete_versions.push(*number);
                    if let Some(image) = version.image.as_deref().filter(|image| is_ami_id(image)) {
                        *deleted_references.entry(image).or_default() += 1;
                    }
                }
            }
        }
        if !delete_versions.is_empty() {
            plan.templates.push(TemplatePrune {
                template_id: template.id.clone(),
                template_name: template.name.clone(),
                delete_versions,
            });
        }
    }

    // An AMI is freed when every reference to it came from a version being deleted. SSM-resolved
    // references aren't counted as deleted, so those AMIs never show as freed (conservative).
    plan.amis_freed = deleted_references
        .into_iter()
        .filter(|(ami_id, deleted)| {
            owned_ami_ids.contains(ami_id)
                && scan.reference_counts.get(*ami_id).is_some_and(|total| total == deleted)
        })
        .map(|(ami_id, _)| ami_id.to_string())
        .collect();
    plan.amis_freed.sort_unstable();
    plan
}

/// Re-scans references, recomputes the plan with the same rules, and deletes only versions both
/// plans agree on. Runs after the AMI and snapshot phases.
pub(crate) async fn execute_launch_template_pruning(
    ec2_client: &ec2::Client,
    reference_clients: &ReferenceClients,
    plan: &RegionPlan,
    args: &Args,
    summary: &mut ExecutionSummary,
) {
    let Some(prune) =
        plan.launch_template_prune.as_ref().filter(|prune| !prune.templates.is_empty())
    else {
        return;
    };
    let planned = prune.version_count();
    let block = |summary: &mut ExecutionSummary, reason: String| {
        summary.skipped_launch_template_versions += planned;
        summary.blocked.push(reason);
    };

    let scan = match collect_referenced_ami_ids(ec2_client, reference_clients).await {
        Ok(scan) => scan,
        Err(error) => {
            block(
                summary,
                format!(
                    "launch template version pruning cancelled: reference scan failed at execution time: {}",
                    error_chain(error.as_ref())
                ),
            );
            return;
        }
    };
    if let Some(reason) = prune_block_reason(&scan, reference_clients) {
        block(summary, format!("{reason} (at execution time)"));
        return;
    }
    let min_age_cutoff = Cutoffs::new(args, Utc::now()).min_age;
    let fresh = plan_launch_template_pruning(
        &scan,
        &HashSet::new(),
        usize::from(args.keep_launch_template_versions),
        min_age_cutoff,
        args,
    );
    let still_deletable = fresh
        .templates
        .iter()
        .map(|template| {
            (
                template.template_id.as_str(),
                template.delete_versions.iter().copied().collect(),
            )
        })
        .collect::<HashMap<&str, HashSet<i64>>>();

    let retry_policy = RetryPolicy::from_args(args);
    let verb = if args.dry_run { "dry-run delete" } else { "delete" };
    let phase =
        Phase::with_total(format!("{} · {verb} launch template versions", plan.region), planned);
    let mut deleted: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    let failures_before = summary.failures.len();
    for template in &prune.templates {
        let empty = HashSet::new();
        let allowed = still_deletable.get(template.template_id.as_str()).unwrap_or(&empty);
        let (eligible, changed): (Vec<i64>, Vec<i64>) =
            template.delete_versions.iter().partition(|version| allowed.contains(version));
        summary.skipped_launch_template_versions += changed.len();
        for _ in &changed {
            phase.inc();
        }

        for batch in eligible.chunks(DELETE_BATCH) {
            let versions = batch.iter().map(ToString::to_string).collect::<Vec<_>>();
            let result = send_with_retries_output(retry_policy, args.dry_run, || {
                ec2_client
                    .delete_launch_template_versions()
                    .launch_template_id(&template.template_id)
                    .set_versions(Some(versions.clone()))
                    .dry_run(args.dry_run)
                    .send()
            })
            .await;
            for _ in batch {
                phase.inc();
            }
            match result {
                // Dry run: the request would have succeeded.
                Ok(None) => summary.deleted_launch_template_versions += batch.len(),
                Ok(Some(output)) => {
                    let done = output
                        .successfully_deleted_launch_template_versions()
                        .iter()
                        .filter_map(|item| item.version_number())
                        .collect::<Vec<i64>>();
                    summary.deleted_launch_template_versions += done.len();
                    deleted.entry(template.template_id.as_str()).or_default().extend(done);
                    for item in output.unsuccessfully_deleted_launch_template_versions() {
                        let error = item.response_error();
                        summary.failures.push(format!(
                            "delete {} v{} failed: {}: {}",
                            template.template_id,
                            item.version_number().unwrap_or_default(),
                            error
                                .and_then(|error| error.code())
                                .map_or("unknown", |code| code.as_str()),
                            error.and_then(|error| error.message()).unwrap_or("no message")
                        ));
                    }
                }
                Err(error) => summary
                    .failures
                    .push(format!("delete versions of {} failed: {error}", template.template_id)),
            }
        }
    }
    let failures = summary.failures.len() - failures_before;
    let done = format!("{} deleted", summary.deleted_launch_template_versions);
    if failures > 0 {
        phase.fail(format!("{done}, {failures} failed"));
    } else {
        phase.finish(done);
    }

    // Deleted versions stop protecting their AMIs right away instead of at the next full rescan.
    if let Some(config) = &reference_clients.lt_cache
        && !deleted.is_empty()
    {
        let path = config.path(&reference_clients.region);
        let mut cache =
            LaunchTemplateCache::load(&path, &config.account_id, &reference_clients.region);
        for (template_id, versions) in &deleted {
            cache.remove_versions(template_id, versions);
        }
        if let Err(error) = cache.save(&path) {
            log::warn!("couldn't update launch template cache {}: {error}", path.display());
        }
    }
}
