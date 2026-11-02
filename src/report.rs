//! The run report: one serializable value per run, rendered as the stdout summary, Markdown and
//! JSON. Everything shown to the user comes from here, so the three formats can't disagree.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::Serialize;

use crate::cli::Args;
use crate::errors::AppResult;
use crate::model::{ExecutionSummary, KeepReason, Kept, RegionPlan, SnapshotInfo};

const SAVINGS_NOTE: &str = "Upper bound. EBS bills only the blocks a snapshot doesn't share with \
other snapshots of the same volume; this estimate uses each snapshot's full size (or its volume \
size) times the per-GB-month price. AMIs themselves cost nothing beyond their snapshots.";

/// Snapshot storage prices used for the savings estimate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pricing {
    pub(crate) standard_per_gb_month: f64,
    pub(crate) archive_per_gb_month: f64,
}

impl Pricing {
    pub(crate) fn from_args(args: &Args) -> Self {
        Self {
            standard_per_gb_month: args.snapshot_price_per_gb_month,
            archive_per_gb_month: args.archive_price_per_gb_month,
        }
    }

    fn size_and_cost(self, snapshot: &SnapshotInfo) -> (Option<f64>, Option<f64>) {
        let Some(bytes) = snapshot.size_bytes else {
            return (None, None);
        };
        #[allow(clippy::cast_precision_loss)] // sizes are far below 2^52 bytes
        let gib = bytes as f64 / f64::from(1u32 << 30);
        let price =
            if snapshot.archived { self.archive_per_gb_month } else { self.standard_per_gb_month };
        (Some(round2(gib)), Some(round2(gib * price)))
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// Bumped when the JSON shape changes incompatibly.
const SCHEMA_VERSION: u32 = 1;

/// How many actions per kind the stdout summary lists before pointing at the report.
const TEXT_LIST_LIMIT: usize = 10;

#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub(crate) schema_version: u32,
    pub(crate) tool_version: &'static str,
    pub(crate) generated_at: DateTime<Utc>,
    /// `plan` (read-only), `dry-run` or `execute`.
    pub(crate) mode: &'static str,
    pub(crate) settings: Settings,
    /// How savings are estimated, so readers don't mistake them for a bill.
    pub(crate) savings_note: &'static str,
    pub(crate) totals: Totals,
    pub(crate) regions: Vec<RegionReport>,
}

/// The arguments that shape the plan, so a report can be read on its own.
#[derive(Debug, Serialize)]
pub(crate) struct Settings {
    pub(crate) min_age_days: i64,
    pub(crate) ami_recent_launch_days: i64,
    pub(crate) delete_tag: String,
    pub(crate) delete_tag_value: String,
    pub(crate) allow_untagged: bool,
    pub(crate) protect_tag: String,
    pub(crate) protect_tag_value: String,
    pub(crate) keep_latest_per_family: usize,
    pub(crate) backup_recovery_point_deletes: bool,
    pub(crate) skipped_reference_sources: Vec<String>,
    pub(crate) snapshot_price_per_gb_month: f64,
    pub(crate) archive_price_per_gb_month: f64,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct Totals {
    pub(crate) regions: usize,
    pub(crate) failed_regions: usize,
    pub(crate) blocked_regions: usize,
    pub(crate) deregister_amis: usize,
    pub(crate) delete_snapshots: usize,
    pub(crate) delete_recovery_points: usize,
    pub(crate) blocked_items: usize,
    pub(crate) estimated_monthly_savings_usd: f64,
    pub(crate) estimated_savings_gib: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegionStatus {
    Ok,
    Blocked,
    Failed,
}

#[derive(Debug, Serialize)]
pub(crate) struct RegionReport {
    pub(crate) region: String,
    pub(crate) status: RegionStatus,
    /// Why the region failed before a plan could be built.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    pub(crate) counts: Counts,
    /// Upper bound: full size of every snapshot/recovery point to delete × its tier's price.
    pub(crate) estimated_monthly_savings_usd: f64,
    pub(crate) estimated_savings_gib: f64,
    /// Kept resources per reason code.
    pub(crate) kept_amis_by_reason: BTreeMap<&'static str, usize>,
    pub(crate) kept_snapshots_by_reason: BTreeMap<&'static str, usize>,
    /// Safety checks that cancelled planned work (exit code 3).
    pub(crate) blocked: Vec<String>,
    pub(crate) unresolved_references: Vec<String>,
    pub(crate) amis: Vec<Decision>,
    pub(crate) snapshots: Vec<Decision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) execution: Option<ExecutionReport>,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct Counts {
    pub(crate) owned_amis: usize,
    pub(crate) referenced_amis: usize,
    pub(crate) deregister_amis: usize,
    pub(crate) kept_amis: usize,
    pub(crate) owned_snapshots: usize,
    pub(crate) delete_snapshots: usize,
    pub(crate) delete_recovery_points: usize,
    pub(crate) blocked_snapshots: usize,
    pub(crate) kept_snapshots: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Deregister,
    Delete,
    DeleteRecoveryPoint,
    Blocked,
    Keep,
}

impl Action {
    fn label(self) -> &'static str {
        match self {
            Self::Deregister => "deregister",
            Self::Delete => "delete",
            Self::DeleteRecoveryPoint => "delete recovery point",
            Self::Blocked => "blocked",
            Self::Keep => "keep",
        }
    }
}

/// What the plan does with one AMI or snapshot, and why.
#[derive(Debug, Serialize)]
pub(crate) struct Decision {
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created: Option<DateTime<Utc>>,
    /// Snapshots: full size (written blocks, not the incremental size AWS bills).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size_gib: Option<f64>,
    /// Snapshots: `size_gib` × the tier's price; an upper bound on what deleting it saves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) monthly_cost_usd: Option<f64>,
    pub(crate) action: Action,
    pub(crate) reason_code: &'static str,
    pub(crate) reason: String,
    /// Supporting facts: what references an AMI, which AMI frees a snapshot, the vault, etc.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) details: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExecutionReport {
    pub(crate) dry_run: bool,
    pub(crate) deregistered_amis: usize,
    pub(crate) skipped_amis: usize,
    pub(crate) deleted_snapshots: usize,
    pub(crate) skipped_snapshots: usize,
    pub(crate) deleted_recovery_points: usize,
    pub(crate) skipped_recovery_points: usize,
    pub(crate) blocked: Vec<String>,
    pub(crate) failures: Vec<String>,
}

impl From<&ExecutionSummary> for ExecutionReport {
    fn from(summary: &ExecutionSummary) -> Self {
        Self {
            dry_run: summary.dry_run,
            deregistered_amis: summary.deregistered_amis,
            skipped_amis: summary.skipped_amis,
            deleted_snapshots: summary.deleted_snapshots,
            skipped_snapshots: summary.skipped_snapshots,
            deleted_recovery_points: summary.deleted_backup_recovery_points,
            skipped_recovery_points: summary.skipped_backup_recovery_points,
            blocked: summary.blocked.clone(),
            failures: summary.failures.clone(),
        }
    }
}

impl Settings {
    pub(crate) fn from_args(args: &Args) -> Self {
        let mut skipped = args
            .skip_reference_sources
            .iter()
            .filter_map(|source| {
                source.to_possible_value().map(|value| value.get_name().to_string())
            })
            .collect::<Vec<String>>();
        skipped.sort_unstable();
        skipped.dedup();
        Self {
            min_age_days: args.min_age_days,
            ami_recent_launch_days: args.ami_recent_launch_days,
            delete_tag: args.delete_tag.clone(),
            delete_tag_value: args.delete_tag_value.clone(),
            allow_untagged: args.allow_untagged,
            protect_tag: args.protect_tag.clone(),
            protect_tag_value: args.protect_tag_value.clone(),
            keep_latest_per_family: args.keep_latest_per_family,
            backup_recovery_point_deletes: args.execute_backup_recovery_point_deletes,
            skipped_reference_sources: skipped,
            snapshot_price_per_gb_month: args.snapshot_price_per_gb_month,
            archive_price_per_gb_month: args.archive_price_per_gb_month,
        }
    }
}

pub(crate) fn run_mode(args: &Args) -> &'static str {
    match (args.execute, args.dry_run) {
        (false, _) => "plan",
        (true, true) => "dry-run",
        (true, false) => "execute",
    }
}

impl Report {
    pub(crate) fn new(
        args: &Args,
        generated_at: DateTime<Utc>,
        regions: Vec<RegionReport>,
    ) -> Self {
        let mut totals = Totals { regions: regions.len(), ..Totals::default() };
        for region in &regions {
            match region.status {
                RegionStatus::Failed => totals.failed_regions += 1,
                RegionStatus::Blocked => totals.blocked_regions += 1,
                RegionStatus::Ok => {}
            }
            totals.deregister_amis += region.counts.deregister_amis;
            totals.delete_snapshots += region.counts.delete_snapshots;
            totals.delete_recovery_points += region.counts.delete_recovery_points;
            totals.estimated_monthly_savings_usd += region.estimated_monthly_savings_usd;
            totals.estimated_savings_gib += region.estimated_savings_gib;
            totals.blocked_items += region
                .amis
                .iter()
                .chain(&region.snapshots)
                .filter(|decision| decision.action == Action::Blocked)
                .count();
        }
        totals.estimated_monthly_savings_usd = round2(totals.estimated_monthly_savings_usd);
        totals.estimated_savings_gib = round2(totals.estimated_savings_gib);
        Self {
            schema_version: SCHEMA_VERSION,
            savings_note: SAVINGS_NOTE,
            tool_version: env!("CARGO_PKG_VERSION"),
            generated_at,
            mode: run_mode(args),
            settings: Settings::from_args(args),
            totals,
            regions,
        }
    }
}

impl RegionReport {
    pub(crate) fn failed(region: &str, error: String) -> Self {
        Self {
            region: region.to_string(),
            status: RegionStatus::Failed,
            error: Some(error),
            counts: Counts::default(),
            estimated_monthly_savings_usd: 0.0,
            estimated_savings_gib: 0.0,
            kept_amis_by_reason: BTreeMap::new(),
            kept_snapshots_by_reason: BTreeMap::new(),
            blocked: Vec::new(),
            unresolved_references: Vec::new(),
            amis: Vec::new(),
            snapshots: Vec::new(),
            execution: None,
        }
    }

    pub(crate) fn from_plan(
        plan: &RegionPlan,
        execution: Option<&ExecutionSummary>,
        pricing: Pricing,
    ) -> Self {
        let mut amis = Vec::new();
        for ami in &plan.deregister_amis {
            amis.push(Decision {
                id: ami.ami_id.clone(),
                name: ami.name.clone(),
                created: ami.creation_date,
                size_gib: None,
                monthly_cost_usd: None,
                action: Action::Deregister,
                reason_code: "eligible",
                reason: "old, unreferenced, unshared and tagged for deletion".to_string(),
                details: Vec::new(),
            });
        }
        for kept in &plan.kept_amis {
            let details = if kept.reason == KeepReason::Referenced {
                plan.ami_reference_sources.get(&kept.item.ami_id).cloned().unwrap_or_default()
            } else {
                Vec::new()
            };
            amis.push(Decision {
                id: kept.item.ami_id.clone(),
                name: kept.item.name.clone(),
                created: kept.item.creation_date,
                size_gib: None,
                monthly_cost_usd: None,
                action: keep_action(&kept.reason),
                reason_code: kept.reason.code(),
                reason: kept.reason.to_string(),
                details,
            });
        }

        let snapshot_by_id = plan
            .backup_managed_snapshots
            .iter()
            .map(|snapshot| (snapshot.snapshot_id.as_str(), snapshot))
            .collect::<HashMap<&str, &SnapshotInfo>>();
        let mut snapshots = Vec::new();
        for snapshot in &plan.delete_snapshots {
            let details = plan
                .snapshots_released_by_ami
                .get(&snapshot.snapshot_id)
                .map(|ami_id| vec![format!("freed by deregistering {ami_id}")])
                .unwrap_or_default();
            snapshots.push(snapshot_decision(
                pricing,
                snapshot,
                Action::Delete,
                "eligible",
                "old, unreferenced, unshared and tagged for deletion".to_string(),
                details,
            ));
        }
        for point in &plan.delete_backup_recovery_points {
            let mut details = vec![
                format!("vault {}", point.backup_vault_name),
                format!("recovery point {}", point.recovery_point_arn),
            ];
            if let Some(ami_id) = plan.snapshots_released_by_ami.get(&point.snapshot_id) {
                details.push(format!("freed by deregistering {ami_id}"));
            }
            let snapshot = snapshot_by_id.get(point.snapshot_id.as_str());
            let (size_gib, monthly_cost_usd) =
                snapshot.map_or((None, None), |snapshot| pricing.size_and_cost(snapshot));
            snapshots.push(Decision {
                id: point.snapshot_id.clone(),
                name: None,
                created: snapshot.map(|snapshot| snapshot.start_time),
                size_gib,
                monthly_cost_usd,
                action: Action::DeleteRecoveryPoint,
                reason_code: "eligible_recovery_point",
                reason: "AWS Backup recovery point tagged for deletion".to_string(),
                details,
            });
        }
        for blocked in &plan.blocked_backup_snapshots {
            let (action, code) = if blocked.uncertain {
                (Action::Blocked, "backup_unknown")
            } else {
                (Action::Keep, "backup_rule")
            };
            snapshots.push(snapshot_decision(
                pricing,
                &blocked.snapshot,
                action,
                code,
                blocked.reason.clone(),
                Vec::new(),
            ));
        }
        for kept in &plan.kept_snapshots {
            snapshots.push(kept_snapshot_decision(pricing, kept));
        }

        sort_decisions(&mut amis);
        sort_decisions(&mut snapshots);

        let deleted = snapshots.iter().filter(|decision| {
            matches!(decision.action, Action::Delete | Action::DeleteRecoveryPoint)
        });
        let (savings_usd, savings_gib) = deleted.fold((0.0, 0.0), |(usd, gib), decision| {
            (
                usd + decision.monthly_cost_usd.unwrap_or(0.0),
                gib + decision.size_gib.unwrap_or(0.0),
            )
        });

        let execution = execution.map(ExecutionReport::from);
        let failed = execution.as_ref().is_some_and(|execution| !execution.failures.is_empty());
        let blocked = !plan.blocked.is_empty()
            || execution.as_ref().is_some_and(|execution| !execution.blocked.is_empty());
        let status = if failed {
            RegionStatus::Failed
        } else if blocked {
            RegionStatus::Blocked
        } else {
            RegionStatus::Ok
        };

        Self {
            region: plan.region.clone(),
            status,
            error: None,
            counts: Counts {
                owned_amis: plan.owned_ami_count,
                referenced_amis: plan.referenced_ami_count,
                deregister_amis: plan.deregister_amis.len(),
                kept_amis: plan.kept_amis.len(),
                owned_snapshots: plan.owned_snapshot_count,
                delete_snapshots: plan.delete_snapshots.len(),
                delete_recovery_points: plan.delete_backup_recovery_points.len(),
                blocked_snapshots: plan.blocked_backup_snapshots.len(),
                kept_snapshots: plan.kept_snapshots.len(),
            },
            estimated_monthly_savings_usd: round2(savings_usd),
            estimated_savings_gib: round2(savings_gib),
            kept_amis_by_reason: count_reasons(plan.kept_amis.iter().map(|kept| &kept.reason)),
            kept_snapshots_by_reason: count_reasons(
                plan.kept_snapshots.iter().map(|kept| &kept.reason),
            ),
            blocked: plan.blocked.clone(),
            unresolved_references: plan.unresolved_ami_references.clone(),
            amis,
            snapshots,
            execution,
        }
    }
}

fn keep_action(reason: &KeepReason) -> Action {
    if reason.uncertain() { Action::Blocked } else { Action::Keep }
}

fn snapshot_decision(
    pricing: Pricing,
    snapshot: &SnapshotInfo,
    action: Action,
    reason_code: &'static str,
    reason: String,
    details: Vec<String>,
) -> Decision {
    let (size_gib, monthly_cost_usd) = pricing.size_and_cost(snapshot);
    Decision {
        id: snapshot.snapshot_id.clone(),
        name: snapshot.description.clone().filter(|description| !description.is_empty()),
        created: Some(snapshot.start_time),
        size_gib,
        monthly_cost_usd,
        action,
        reason_code,
        reason,
        details,
    }
}

fn kept_snapshot_decision(pricing: Pricing, kept: &Kept<SnapshotInfo>) -> Decision {
    snapshot_decision(
        pricing,
        &kept.item,
        keep_action(&kept.reason),
        kept.reason.code(),
        kept.reason.to_string(),
        Vec::new(),
    )
}

fn sort_decisions(decisions: &mut [Decision]) {
    decisions.sort_by(|a, b| a.action.cmp(&b.action).then_with(|| a.id.cmp(&b.id)));
}

fn count_reasons<'a>(
    reasons: impl Iterator<Item = &'a KeepReason>,
) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for reason in reasons {
        *counts.entry(reason.code()).or_insert(0) += 1;
    }
    counts
}

// -------------------------------------------------------------------------------------------------
// Report files
// -------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReportFormat {
    Markdown,
    Json,
}

/// Clap value parser for `--report`: the format comes from the file extension.
pub(crate) fn parse_report_path(raw: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(raw);
    report_format(&path).map(|_| path).ok_or_else(|| {
        format!("unsupported report file '{raw}': use a .md, .markdown or .json extension")
    })
}

pub(crate) fn report_format(path: &Path) -> Option<ReportFormat> {
    match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "md" | "markdown" => Some(ReportFormat::Markdown),
        "json" => Some(ReportFormat::Json),
        _ => None,
    }
}

pub(crate) fn write_report(report: &Report, path: &Path) -> AppResult<()> {
    let content = match report_format(path) {
        Some(ReportFormat::Markdown) => render_markdown(report),
        Some(ReportFormat::Json) => render_json(report)?,
        None => return Err(format!("unsupported report file {}", path.display()).into()),
    };
    std::fs::write(path, content)
        .map_err(|error| format!("failed to write report {}: {error}", path.display()).into())
}

pub(crate) fn render_json(report: &Report) -> AppResult<String> {
    let mut json = serde_json::to_string_pretty(report)?;
    json.push('\n');
    Ok(json)
}

// -------------------------------------------------------------------------------------------------
// Markdown
// -------------------------------------------------------------------------------------------------

pub(crate) fn render_markdown(report: &Report) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# amicleaner {}", mode_title(report.mode));
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Generated {} by amicleaner {} · mode **{}**{}",
        report.generated_at.format("%Y-%m-%d %H:%M UTC"),
        report.tool_version,
        report.mode,
        if report.mode == "plan" { " (read-only, nothing was changed)" } else { "" }
    );
    let _ = writeln!(out);
    write_settings_markdown(&mut out, &report.settings);

    if report.totals.estimated_monthly_savings_usd > 0.0 {
        let _ = writeln!(
            out,
            "**Estimated savings: up to {}/month** ({} GiB of snapshots)",
            usd(report.totals.estimated_monthly_savings_usd),
            report.totals.estimated_savings_gib
        );
        let _ = writeln!(out);
        let _ = writeln!(out, "> {}", report.savings_note);
        let _ = writeln!(out);
    }

    let _ = writeln!(out, "## Summary");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Region | Status | Deregister AMIs | Kept AMIs | Delete snapshots | Recovery points | Kept snapshots | Blocked | Est. savings/mo |"
    );
    let _ = writeln!(out, "|---|---|--:|--:|--:|--:|--:|--:|--:|");
    for region in &report.regions {
        let blocked_items = region
            .amis
            .iter()
            .chain(&region.snapshots)
            .filter(|decision| decision.action == Action::Blocked)
            .count();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            region.region,
            status_label(region.status),
            region.counts.deregister_amis,
            region.counts.kept_amis,
            region.counts.delete_snapshots,
            region.counts.delete_recovery_points,
            region.counts.kept_snapshots,
            blocked_items,
            usd(region.estimated_monthly_savings_usd)
        );
    }
    let _ = writeln!(out);

    for region in &report.regions {
        write_region_markdown(&mut out, region);
    }
    out
}

fn mode_title(mode: &str) -> &'static str {
    match mode {
        "plan" => "plan",
        "dry-run" => "dry run",
        _ => "execution report",
    }
}

fn status_label(status: RegionStatus) -> &'static str {
    match status {
        RegionStatus::Ok => "✅ ok",
        RegionStatus::Blocked => "⚠️ blocked",
        RegionStatus::Failed => "❌ failed",
    }
}

fn write_settings_markdown(out: &mut String, settings: &Settings) {
    let delete_tag = if settings.allow_untagged {
        "not required (--allow-untagged)".to_string()
    } else {
        tag_label(&settings.delete_tag, &settings.delete_tag_value)
    };
    let skipped = if settings.skipped_reference_sources.is_empty() {
        "none".to_string()
    } else {
        settings.skipped_reference_sources.join(", ")
    };
    let _ = writeln!(out, "<details><summary>Settings</summary>");
    let _ = writeln!(out);
    let _ = writeln!(out, "| Setting | Value |");
    let _ = writeln!(out, "|---|---|");
    let _ = writeln!(out, "| Minimum age | {} days |", settings.min_age_days);
    let _ = writeln!(out, "| Recent launch window | {} days |", settings.ami_recent_launch_days);
    let _ = writeln!(out, "| Delete tag | {} |", cell(&delete_tag));
    let _ = writeln!(
        out,
        "| Protect tag | {} |",
        cell(&tag_label(&settings.protect_tag, &settings.protect_tag_value))
    );
    let _ = writeln!(out, "| Keep latest per family | {} |", settings.keep_latest_per_family);
    let _ = writeln!(
        out,
        "| Recovery point deletes | {} |",
        if settings.backup_recovery_point_deletes { "enabled" } else { "disabled" }
    );
    let _ = writeln!(out, "| Skipped reference sources | {} |", cell(&skipped));
    let _ = writeln!(out);
    let _ = writeln!(out, "</details>");
    let _ = writeln!(out);
}

fn tag_label(key: &str, value: &str) -> String {
    if value.trim().is_empty() {
        format!("`{key}` (any value)")
    } else {
        format!("`{key}={value}`")
    }
}

fn write_region_markdown(out: &mut String, region: &RegionReport) {
    let _ = writeln!(out, "## {} — {}", region.region, status_label(region.status));
    let _ = writeln!(out);

    if let Some(error) = &region.error {
        let _ = writeln!(out, "> **Region failed:** {}", cell(error));
        let _ = writeln!(out);
        return;
    }

    let counts = &region.counts;
    let _ = writeln!(
        out,
        "{} owned AMIs ({} referenced) · {} owned snapshots",
        counts.owned_amis, counts.referenced_amis, counts.owned_snapshots
    );
    let _ = writeln!(out);
    if region.estimated_monthly_savings_usd > 0.0 {
        let _ = writeln!(
            out,
            "Estimated savings: up to **{}/month** ({} GiB)",
            usd(region.estimated_monthly_savings_usd),
            region.estimated_savings_gib
        );
        let _ = writeln!(out);
    }

    if !region.blocked.is_empty() {
        let _ = writeln!(out, "> [!WARNING]");
        let _ = writeln!(out, "> Safety checks cancelled planned work:");
        for reason in &region.blocked {
            let _ = writeln!(out, "> - {}", cell(reason));
        }
        let _ = writeln!(out);
    }
    if !region.unresolved_references.is_empty() {
        let _ = writeln!(out, "**Unresolved AMI references** (AMI deregistration is blocked):");
        let _ = writeln!(out);
        for reference in &region.unresolved_references {
            let _ = writeln!(out, "- {}", cell(reference));
        }
        let _ = writeln!(out);
    }

    write_decision_table(out, "AMIs to deregister", &actions(&region.amis, Action::Deregister));
    write_decision_table(out, "Snapshots to delete", &actions(&region.snapshots, Action::Delete));
    write_decision_table(
        out,
        "AWS Backup recovery points to delete",
        &actions(&region.snapshots, Action::DeleteRecoveryPoint),
    );
    let blocked = region
        .amis
        .iter()
        .chain(&region.snapshots)
        .filter(|decision| decision.action == Action::Blocked)
        .collect::<Vec<_>>();
    write_decision_table(out, "Blocked", &blocked);

    if region
        .amis
        .iter()
        .chain(&region.snapshots)
        .all(|decision| matches!(decision.action, Action::Keep | Action::Blocked))
    {
        let _ = writeln!(out, "Nothing to delete in this region.");
        let _ = writeln!(out);
    }

    let _ = writeln!(out, "### Kept");
    let _ = writeln!(out);
    let mut reasons = BTreeMap::<&str, (usize, usize, String)>::new();
    for decision in region.amis.iter().filter(|decision| decision.action == Action::Keep) {
        reasons.entry(decision.reason_code).or_insert((0, 0, decision.reason.clone())).0 += 1;
    }
    for decision in region.snapshots.iter().filter(|decision| decision.action == Action::Keep) {
        reasons.entry(decision.reason_code).or_insert((0, 0, decision.reason.clone())).1 += 1;
    }
    if reasons.is_empty() {
        let _ = writeln!(out, "Nothing kept.");
    } else {
        let _ = writeln!(out, "| Reason | AMIs | Snapshots |");
        let _ = writeln!(out, "|---|--:|--:|");
        let mut rows = reasons.into_iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| (b.1.0 + b.1.1).cmp(&(a.1.0 + a.1.1)).then(a.0.cmp(b.0)));
        for (_, (amis, snapshots, reason)) in rows {
            let _ = writeln!(out, "| {} | {amis} | {snapshots} |", cell(&reason));
        }
    }
    let _ = writeln!(out);

    write_details_table(out, "All AMIs", &region.amis);
    write_details_table(out, "All snapshots", &region.snapshots);

    if let Some(execution) = &region.execution {
        write_execution_markdown(out, execution);
    }
}

fn write_decision_table(out: &mut String, title: &str, decisions: &[&Decision]) {
    if decisions.is_empty() {
        return;
    }
    let _ = writeln!(out, "### {title} ({})", decisions.len());
    let _ = writeln!(out);
    let _ = writeln!(out, "| ID | Name | Created | Size | Est. $/mo | Reason | Details |");
    let _ = writeln!(out, "|---|---|---|--:|--:|---|---|");
    for decision in decisions {
        write_decision_row(out, decision, false);
    }
    let _ = writeln!(out);
}

fn write_details_table(out: &mut String, title: &str, decisions: &[Decision]) {
    if decisions.is_empty() {
        return;
    }
    let _ = writeln!(out, "<details><summary>{title} ({})</summary>", decisions.len());
    let _ = writeln!(out);
    let _ = writeln!(out, "| ID | Name | Created | Size | Est. $/mo | Action | Reason | Details |");
    let _ = writeln!(out, "|---|---|---|--:|--:|---|---|---|");
    for decision in decisions {
        write_decision_row(out, decision, true);
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "</details>");
    let _ = writeln!(out);
}

fn write_decision_row(out: &mut String, decision: &Decision, with_action: bool) {
    let created = decision
        .created
        .map_or_else(|| "unknown".to_string(), |created| created.format("%Y-%m-%d").to_string());
    let name = decision.name.as_deref().map_or_else(String::new, cell);
    let details =
        decision.details.iter().map(|detail| cell(detail)).collect::<Vec<_>>().join("<br>");
    let size = decision.size_gib.map_or_else(String::new, |gib| format!("{gib} GiB"));
    let cost = decision.monthly_cost_usd.map_or_else(String::new, usd);
    if with_action {
        let _ = writeln!(
            out,
            "| `{}` | {name} | {created} | {size} | {cost} | {} | {} | {details} |",
            decision.id,
            decision.action.label(),
            cell(&decision.reason)
        );
    } else {
        let _ = writeln!(
            out,
            "| `{}` | {name} | {created} | {size} | {cost} | {} | {details} |",
            decision.id,
            cell(&decision.reason)
        );
    }
}

fn write_execution_markdown(out: &mut String, execution: &ExecutionReport) {
    let _ = writeln!(
        out,
        "### Execution{}",
        if execution.dry_run {
            " (dry run: counts are requests that passed the AWS check)"
        } else {
            ""
        }
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "| | Done | Skipped |");
    let _ = writeln!(out, "|---|--:|--:|");
    let _ = writeln!(
        out,
        "| AMIs deregistered | {} | {} |",
        execution.deregistered_amis, execution.skipped_amis
    );
    let _ = writeln!(
        out,
        "| Snapshots deleted | {} | {} |",
        execution.deleted_snapshots, execution.skipped_snapshots
    );
    let _ = writeln!(
        out,
        "| Recovery points deleted | {} | {} |",
        execution.deleted_recovery_points, execution.skipped_recovery_points
    );
    let _ = writeln!(out);
    for (title, items) in
        [("Blocked at execution", &execution.blocked), ("Failures", &execution.failures)]
    {
        if !items.is_empty() {
            let _ = writeln!(out, "**{title}:**");
            let _ = writeln!(out);
            for item in items {
                let _ = writeln!(out, "- {}", cell(item));
            }
            let _ = writeln!(out);
        }
    }
}

fn actions(decisions: &[Decision], action: Action) -> Vec<&Decision> {
    decisions.iter().filter(|decision| decision.action == action).collect()
}

fn usd(value: f64) -> String {
    format!("${value:.2}")
}

/// Makes text safe inside a Markdown table cell.
fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace(['\n', '\r'], " ")
}

// -------------------------------------------------------------------------------------------------
// Terminal summary
// -------------------------------------------------------------------------------------------------

/// The per-region block printed to stdout as each region finishes.
pub(crate) fn render_region_text(region: &RegionReport) -> String {
    let mut out = String::new();
    let status = match region.status {
        RegionStatus::Ok => console::style("ok").green(),
        RegionStatus::Blocked => console::style("blocked").yellow(),
        RegionStatus::Failed => console::style("failed").red(),
    };
    let _ = writeln!(out, "\n{}  {status}", console::style(&region.region).bold());

    if let Some(error) = &region.error {
        let _ = writeln!(out, "  {}", console::style(error).red());
        return out;
    }

    let counts = &region.counts;
    let _ = writeln!(
        out,
        "  AMIs       {:>6} owned  {:>6} to deregister  {:>6} kept",
        counts.owned_amis, counts.deregister_amis, counts.kept_amis
    );
    write_reason_line(&mut out, &region.kept_amis_by_reason);
    let _ = writeln!(
        out,
        "  Snapshots  {:>6} owned  {:>6} to delete      {:>6} kept",
        counts.owned_snapshots,
        counts.delete_snapshots + counts.delete_recovery_points,
        counts.kept_snapshots
    );
    write_reason_line(&mut out, &region.kept_snapshots_by_reason);
    if region.estimated_monthly_savings_usd > 0.0 {
        let _ = writeln!(
            out,
            "  Savings    up to {}/month ({} GiB, upper bound)",
            console::style(usd(region.estimated_monthly_savings_usd)).green(),
            region.estimated_savings_gib
        );
    }

    for reason in &region.blocked {
        let _ = writeln!(out, "  {} {reason}", console::style("blocked:").yellow());
    }

    for (title, action, decisions) in [
        ("deregister", Action::Deregister, &region.amis),
        ("delete", Action::Delete, &region.snapshots),
        ("delete recovery point", Action::DeleteRecoveryPoint, &region.snapshots),
    ] {
        let items =
            decisions.iter().filter(|decision| decision.action == action).collect::<Vec<_>>();
        for decision in items.iter().take(TEXT_LIST_LIMIT) {
            let _ = writeln!(
                out,
                "  {} {} {}",
                console::style(format!("{title}:")).cyan(),
                decision.id,
                decision.name.as_deref().unwrap_or("")
            );
        }
        if items.len() > TEXT_LIST_LIMIT {
            let _ = writeln!(
                out,
                "  … {} more to {title} (see --report for the full list)",
                items.len() - TEXT_LIST_LIMIT
            );
        }
    }

    if let Some(execution) = &region.execution {
        let _ = writeln!(
            out,
            "  {}  {} AMIs deregistered · {} snapshots deleted · {} recovery points deleted{}",
            console::style(if execution.dry_run { "dry run" } else { "executed" }).bold(),
            execution.deregistered_amis,
            execution.deleted_snapshots,
            execution.deleted_recovery_points,
            if execution.dry_run { " (requests that passed the AWS dry-run check)" } else { "" }
        );
        for reason in &execution.blocked {
            let _ = writeln!(out, "  {} {reason}", console::style("blocked:").yellow());
        }
        for failure in &execution.failures {
            let _ = writeln!(out, "  {} {failure}", console::style("failed:").red());
        }
    }
    out
}

fn write_reason_line(out: &mut String, reasons: &BTreeMap<&'static str, usize>) {
    if reasons.is_empty() {
        return;
    }
    let mut sorted = reasons.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    let line = sorted
        .iter()
        .map(|(code, count)| format!("{count} {}", code.replace('_', " ")))
        .collect::<Vec<_>>()
        .join(" · ");
    let _ = writeln!(out, "             {}", console::style(format!("kept: {line}")).dim());
}

/// The closing lines after every region has been printed.
pub(crate) fn render_footer(report: &Report, report_paths: &[PathBuf]) -> String {
    let mut out = String::new();
    let totals = &report.totals;
    let work = totals.deregister_amis + totals.delete_snapshots + totals.delete_recovery_points;
    let _ = writeln!(out);
    if report.mode == "plan" {
        if work == 0 {
            let _ = writeln!(out, "Nothing to clean up.");
        } else {
            let _ = writeln!(
                out,
                "Plan: {} AMIs to deregister, {} snapshots and {} recovery points to delete \
                 (up to {}/month). Run with --execute to apply.",
                totals.deregister_amis,
                totals.delete_snapshots,
                totals.delete_recovery_points,
                usd(totals.estimated_monthly_savings_usd)
            );
        }
    }
    if totals.blocked_regions > 0 || totals.failed_regions > 0 {
        let _ = writeln!(
            out,
            "{} of {} regions blocked by safety checks, {} failed.",
            totals.blocked_regions, totals.regions, totals.failed_regions
        );
    }
    for path in report_paths {
        let _ = writeln!(out, "Report written to {}", path.display());
    }
    out
}
