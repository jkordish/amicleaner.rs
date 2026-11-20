//! Orphaned Data Lifecycle Manager snapshots (`--delete-orphaned-dlm-snapshots`).
//!
//! DLM snapshots are normally kept because DLM retires them itself. It stops doing so when:
//! - a count-based schedule's source volume is deleted (the policy no longer manages them; only
//!   for policies that target volumes: instance policies count snapshot sets per instance);
//! - the policy is deleted (its snapshots aren't deleted with it);
//! - the schedule that made them is renamed or removed.
//!
//! Snapshots left in any of those states are never cleaned up by DLM. This module finds them; the
//! normal snapshot rules (age, protect tag, delete authorization, sharing, AWS Backup) still decide
//! whether each one is deleted. Anything that can't be determined keeps the snapshot.

use std::collections::{HashMap, HashSet};

use aws_sdk_dlm as dlm;
use aws_sdk_dlm::types::{PolicyTypeValues, ResourceTypeValues};
use aws_sdk_ec2 as ec2;
use aws_sdk_ec2::error::ProvideErrorMetadata;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};

use crate::errors::describe_sdk_error;
use crate::model::{DlmOrphanCause, SnapshotInfo};

pub(crate) const DLM_POLICY_TAG: &str = "aws:dlm:lifecycle-policy-id";
pub(crate) const DLM_SCHEDULE_TAG: &str = "aws:dlm:lifecycle-schedule-name";
/// DLM writes this one as `aws:dlm:expirationTime`; matched case-insensitively.
const DLM_EXPIRATION_TAG: &str = "aws:dlm:expirationtime";
/// Volume id EBS gives copied snapshots (cross-region and cross-account copies).
const COPY_PLACEHOLDER_VOLUME: &str = "vol-ffffffff";
/// Values per `volume-id` filter in one DescribeVolumes call.
const VOLUME_FILTER_CHUNK: usize = 200;

/// How a schedule retains snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Retention {
    /// Keep the newest N: stops when the source volume is gone.
    Count,
    /// Keep for a time period, or anything else: DLM keeps expiring these.
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PolicyState {
    /// GetLifecyclePolicy returned ResourceNotFoundException.
    Deleted,
    /// The lookup failed; nothing about this policy's snapshots can be decided.
    Unknown(String),
    /// A custom EBS snapshot policy, with its schedules by name.
    SnapshotPolicy { schedules: HashMap<String, Retention> },
    /// Any other policy (AMI, event-based copy, default policy): never treated as orphaning.
    OtherPolicy,
}

#[derive(Clone, Debug)]
pub(crate) struct DlmInventory {
    pub(crate) policies: HashMap<String, PolicyState>,
    /// Source volumes that still exist, or the error that stopped the lookup.
    pub(crate) live_volumes: Result<HashSet<String>, String>,
}

impl Default for DlmInventory {
    fn default() -> Self {
        Self { policies: HashMap::new(), live_volumes: Ok(HashSet::new()) }
    }
}

/// Snapshots found orphaned, plus why any DLM snapshots couldn't be decided.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DlmOrphanScan {
    pub(crate) orphans: HashMap<String, DlmOrphanCause>,
    pub(crate) undecided: Vec<String>,
}

fn tag<'a>(snapshot: &'a SnapshotInfo, key: &str) -> Option<&'a str> {
    snapshot
        .tags
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
}

/// The source volume, unless it's missing or the snapshot is a copy. A copy's volume id is
/// arbitrary (`vol-ffffffff` in practice), and a cross-region copy carries the source region's
/// policy id, which looks deleted here; so copies are recognized by description too.
fn source_volume(snapshot: &SnapshotInfo) -> Option<&str> {
    let copied = snapshot.description.as_deref().is_some_and(|description| {
        description.contains("Copied snap-") || description.contains("Copied for")
    });
    if copied {
        return None;
    }
    snapshot.volume_id.as_deref().filter(|volume| {
        volume.starts_with("vol-") && !volume.eq_ignore_ascii_case(COPY_PLACEHOLDER_VOLUME)
    })
}

/// Snapshots that may be orphaned at all: DLM-tagged, from a real volume, with no pending expiry.
fn candidates(
    snapshots: &[SnapshotInfo],
    now: DateTime<Utc>,
) -> impl Iterator<Item = &SnapshotInfo> {
    snapshots.iter().filter(move |snapshot| {
        if tag(snapshot, DLM_POLICY_TAG).is_none() || source_volume(snapshot).is_none() {
            return false;
        }
        // An expiry DLM (or the archive tier) still acts on means it isn't orphaned. An expiry
        // that can't be read is treated as pending.
        match tag(snapshot, DLM_EXPIRATION_TAG) {
            None => true,
            Some(value) => DateTime::parse_from_rfc3339(value)
                .is_ok_and(|expires| expires.with_timezone(&Utc) <= now),
        }
    })
}

/// Reads the policies and volumes the candidates depend on. Never fails as a whole: errors are
/// recorded per policy, or for the volume lookup, and keep the affected snapshots.
/// `now` must be the same instant later given to `orphaned_dlm_snapshots`, so both pick the same
/// candidates (an expiry passing in between would leave a candidate whose policy wasn't read).
pub(crate) async fn load_dlm_inventory(
    ec2_client: &ec2::Client,
    dlm_client: &dlm::Client,
    snapshots: &[SnapshotInfo],
    now: DateTime<Utc>,
    concurrency: usize,
) -> DlmInventory {
    let mut policy_ids = HashSet::new();
    let mut volume_ids = HashSet::new();
    for snapshot in candidates(snapshots, now) {
        policy_ids.extend(tag(snapshot, DLM_POLICY_TAG).map(str::to_string));
        volume_ids.extend(source_volume(snapshot).map(str::to_string));
    }

    let policies = stream::iter(policy_ids)
        .map(|policy_id| async move {
            let state = match dlm_client.get_lifecycle_policy().policy_id(&policy_id).send().await {
                Ok(output) => match output.policy() {
                    Some(policy) => policy_state(policy),
                    None => PolicyState::Unknown("GetLifecyclePolicy returned no policy".into()),
                },
                Err(error) if error.code() == Some("ResourceNotFoundException") => {
                    PolicyState::Deleted
                }
                Err(error) => PolicyState::Unknown(format!(
                    "GetLifecyclePolicy failed: {}",
                    describe_sdk_error(&error)
                )),
            };
            (policy_id, state)
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<HashMap<_, _>>()
        .await;

    let live_volumes = list_live_volumes(ec2_client, volume_ids.into_iter().collect()).await;
    DlmInventory { policies, live_volumes }
}

fn policy_state(policy: &dlm::types::LifecyclePolicy) -> PolicyState {
    let Some(details) = policy.policy_details() else {
        return PolicyState::Unknown("policy has no details".to_string());
    };
    // Default policies retire snapshots their own way (and can extend deletion); leave them be.
    let custom_snapshot_policy = policy.default_policy() != Some(true)
        && details.policy_type() == Some(&PolicyTypeValues::EbsSnapshotManagement);
    if !custom_snapshot_policy {
        return PolicyState::OtherPolicy;
    }
    // An instance policy's count applies to snapshot sets per instance: a detached and deleted
    // volume's snapshots still rotate out with their sets, so they're never orphaned by volume.
    let targets_volumes =
        !details.resource_types().iter().any(|resource| resource == &ResourceTypeValues::Instance);
    let schedules = details
        .schedules()
        .iter()
        .filter_map(|schedule| {
            let retention = match schedule.retain_rule().and_then(|rule| rule.count()) {
                Some(_) if targets_volumes => Retention::Count,
                _ => Retention::Other,
            };
            schedule.name().map(|name| (name.to_string(), retention))
        })
        .collect();
    PolicyState::SnapshotPolicy { schedules }
}

/// Volumes among `volume_ids` that still exist. A `volume-id` filter returns only existing
/// volumes, without the NotFound error that listing ids directly gives.
async fn list_live_volumes(
    ec2_client: &ec2::Client,
    volume_ids: Vec<String>,
) -> Result<HashSet<String>, String> {
    let mut live = HashSet::new();
    for chunk in volume_ids.chunks(VOLUME_FILTER_CHUNK) {
        let filter = ec2::types::Filter::builder()
            .name("volume-id")
            .set_values(Some(chunk.to_vec()))
            .build();
        let mut pages = ec2_client.describe_volumes().filters(filter).into_paginator().send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| {
                format!("DescribeVolumes failed: {}", describe_sdk_error(&error))
            })?;
            live.extend(
                page.volumes().iter().filter_map(|volume| volume.volume_id().map(str::to_string)),
            );
        }
    }
    Ok(live)
}

/// (policy id, schedule name, source volume): one DLM schedule's snapshots of one volume.
type GroupKey<'a> = (&'a str, Option<&'a str>, &'a str);

fn key(snapshot: &SnapshotInfo) -> Option<GroupKey<'_>> {
    Some((
        tag(snapshot, DLM_POLICY_TAG)?,
        tag(snapshot, DLM_SCHEDULE_TAG),
        source_volume(snapshot)?,
    ))
}

/// Decides which DLM snapshots are orphaned (pure). A group (policy, schedule, volume) counts only
/// once its newest snapshot is older than `min_age_cutoff`: that's when DLM last acted on it. It
/// isn't when the volume or policy was deleted, which nothing here can see, so a schedule that ran
/// rarely (monthly, say) can lose its volume today and be orphaned on the next run.
pub(crate) fn orphaned_dlm_snapshots(
    snapshots: &[SnapshotInfo],
    inventory: &DlmInventory,
    now: DateTime<Utc>,
    min_age_cutoff: DateTime<Utc>,
) -> DlmOrphanScan {
    // `None`: some snapshot in the group has no start time, so its newest is unknown.
    let mut newest: HashMap<GroupKey, Option<DateTime<Utc>>> = HashMap::new();
    for snapshot in snapshots {
        if let Some(group) = key(snapshot) {
            let entry = newest.entry(group).or_insert(snapshot.start_time);
            *entry = entry.zip(snapshot.start_time).map(|(a, b)| a.max(b));
        }
    }

    let mut scan = DlmOrphanScan::default();
    let mut undecided: HashMap<String, usize> = HashMap::new();
    for snapshot in candidates(snapshots, now) {
        let Some(group @ (policy_id, schedule, volume)) = key(snapshot) else { continue };
        let cause = match inventory.policies.get(policy_id) {
            Some(PolicyState::Deleted) => Some(DlmOrphanCause::PolicyDeleted),
            Some(PolicyState::OtherPolicy) => None,
            Some(PolicyState::Unknown(error)) => {
                *undecided.entry(format!("policy {policy_id}: {error}")).or_default() += 1;
                continue;
            }
            None => {
                *undecided.entry(format!("policy {policy_id}: not looked up")).or_default() += 1;
                continue;
            }
            Some(PolicyState::SnapshotPolicy { schedules }) => match schedule {
                // Without the schedule tag it's unclear which rule applies; keep it.
                None => None,
                Some(name) => match schedules.get(name) {
                    None => Some(DlmOrphanCause::ScheduleRemoved),
                    Some(Retention::Other) => None,
                    Some(Retention::Count) => match &inventory.live_volumes {
                        Ok(live) if live.contains(volume) => None,
                        Ok(_) => Some(DlmOrphanCause::VolumeDeleted),
                        Err(error) => {
                            *undecided.entry(error.clone()).or_default() += 1;
                            continue;
                        }
                    },
                },
            },
        };
        let Some(cause) = cause else { continue };
        if newest.get(&group).copied().flatten().is_some_and(|newest| newest < min_age_cutoff) {
            scan.orphans.insert(snapshot.snapshot_id.clone(), cause);
        }
    }

    let mut undecided = undecided.into_iter().collect::<Vec<_>>();
    undecided.sort();
    scan.undecided = undecided
        .into_iter()
        .map(|(reason, count)| {
            format!("orphaned DLM snapshot check: {reason}; {count} snapshot(s) stay kept")
        })
        .collect();
    scan
}
