//! Lists account-owned AMIs and snapshots and looks up who else can use them.

use std::collections::HashMap;
use std::future::Future;

use aws_sdk_ec2 as ec2;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use log::warn;

use crate::errors::{AppResult, AwsResultExt, error_chain};
use crate::model::{AmiInfo, ExternalUse, Grant, LastLaunched, SnapshotInfo};
use crate::ui::Phase;

pub(crate) async fn list_owned_amis(ec2_client: &ec2::Client) -> AppResult<Vec<AmiInfo>> {
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
            // Skipping it would drop its snapshot references and make them look deletable.
            let Some(ami_id) = image.image_id() else {
                return Err("DescribeImages returned an AMI without an image id".into());
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
                state: image.state().map(|state| state.as_str().to_string()),
            });
        }
    }

    Ok(amis)
}

pub(crate) async fn list_owned_snapshots(ec2_client: &ec2::Client) -> AppResult<Vec<SnapshotInfo>> {
    let mut snapshots = Vec::new();

    let mut pages =
        ec2_client.describe_snapshots().owner_ids("self").into_paginator().page_size(100).send();

    while let Some(page) = pages.next().await {
        let output = page.aws_context("DescribeSnapshots")?;
        for snapshot in output.snapshots() {
            // Without an id there's nothing to delete or report; AMIs' references to it still count.
            let Some(snapshot_id) = snapshot.snapshot_id() else {
                warn!("DescribeSnapshots returned a snapshot without an id; ignoring it");
                continue;
            };
            let start_time = snapshot.start_time().and_then(smithy_datetime_to_utc);
            if start_time.is_none() {
                warn!("snapshot {snapshot_id} has no start time; it will be kept");
            }

            let size_bytes = snapshot
                .full_snapshot_size_in_bytes()
                .and_then(|bytes| u64::try_from(bytes).ok())
                .or_else(|| {
                    snapshot
                        .volume_size()
                        .and_then(|gib| u64::try_from(gib).ok())
                        .map(|gib| gib << 30)
                });
            snapshots.push(SnapshotInfo {
                snapshot_id: snapshot_id.to_string(),
                size_bytes,
                archived: snapshot.storage_tier() == Some(&ec2::types::StorageTier::Archive),
                start_time,
                description: snapshot.description().map(ToString::to_string),
                tags: tags_to_map(snapshot.tags()),
                volume_id: snapshot.volume_id().map(ToString::to_string),
                dlm_orphan: None,
                state: snapshot.state().map(|state| state.as_str().to_string()),
            });
        }
    }

    Ok(snapshots)
}

/// A tag without a value counts as an empty value: the protect tag protects with any value.
fn tags_to_map(tags: &[ec2::types::Tag]) -> HashMap<String, String> {
    tags.iter()
        .filter_map(|tag| Some((tag.key()?.to_string(), tag.value().unwrap_or("").to_string())))
        .collect()
}

fn parse_aws_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw).ok().map(|timestamp| timestamp.with_timezone(&Utc))
}

fn smithy_datetime_to_utc(value: &ec2::primitives::DateTime) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value.secs(), 0)
}

pub(crate) fn is_resource_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|hex| {
        matches!(hex.len(), 8 | 17)
            && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

pub(crate) fn is_ami_id(value: &str) -> bool {
    is_resource_id(value, "ami-")
}

pub(crate) fn is_snapshot_id(value: &str) -> bool {
    is_resource_id(value, "snap-")
}

pub(crate) async fn ami_launch_grants(
    ec2_client: &ec2::Client,
    ami_id: &str,
) -> AppResult<Vec<Grant>> {
    let output = ec2_client
        .describe_image_attribute()
        .image_id(ami_id)
        .attribute(ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .aws_context("DescribeImageAttribute")?;
    Ok(output
        .launch_permissions()
        .iter()
        .map(|permission| {
            grant(
                permission.user_id(),
                permission.group(),
                permission.organization_arn(),
                permission.organizational_unit_arn(),
            )
        })
        .collect())
}

/// Who a snapshot grants create-volume permissions to (other accounts or the public). The owner
/// never needs one, so any entry means someone else can use it.
pub(crate) async fn snapshot_volume_grants(
    ec2_client: &ec2::Client,
    snapshot_id: &str,
) -> AppResult<Vec<Grant>> {
    let output = ec2_client
        .describe_snapshot_attribute()
        .snapshot_id(snapshot_id)
        .attribute(ec2::types::SnapshotAttributeName::CreateVolumePermission)
        .send()
        .await
        .aws_context("DescribeSnapshotAttribute")?;
    Ok(output
        .create_volume_permissions()
        .iter()
        .map(|permission| grant(permission.user_id(), permission.group(), None, None))
        .collect())
}

/// One permission entry as a grant. An entry with nothing recognizable is treated as public,
/// the widest reading, so it can never make a shared item look less shared.
fn grant(
    user_id: Option<&str>,
    group: Option<&ec2::types::PermissionGroup>,
    organization_arn: Option<&str>,
    organizational_unit_arn: Option<&str>,
) -> Grant {
    fn non_empty(value: Option<&str>) -> Option<&str> {
        value.map(str::trim).filter(|value| !value.is_empty())
    }
    if group.is_some() {
        Grant::Public
    } else if let Some(arn) = non_empty(organizational_unit_arn) {
        Grant::OrganizationalUnit(arn.to_string())
    } else if let Some(arn) = non_empty(organization_arn) {
        Grant::Organization(arn.to_string())
    } else if let Some(account) = non_empty(user_id) {
        Grant::Account(account.to_string())
    } else {
        Grant::Public
    }
}

/// Looks up, `concurrency` at a time, which AMIs grant launch permissions to anyone else.
/// Unshared AMIs are absent from the result; a failed lookup is `ExternalUse::Unknown`.
pub(crate) async fn lookup_ami_external_use(
    ec2_client: &ec2::Client,
    ami_ids: &[&str],
    concurrency: usize,
    phase: Option<&Phase>,
) -> HashMap<String, ExternalUse> {
    lookup_external_use(ami_ids, concurrency, phase, |ami_id| ami_launch_grants(ec2_client, ami_id))
        .await
}

/// Looks up, `concurrency` at a time, which snapshots grant create-volume permissions to anyone
/// else. Unshared snapshots are absent from the result; a failed lookup is `ExternalUse::Unknown`.
pub(crate) async fn lookup_snapshot_external_use(
    ec2_client: &ec2::Client,
    snapshot_ids: &[&str],
    concurrency: usize,
    phase: Option<&Phase>,
) -> HashMap<String, ExternalUse> {
    lookup_external_use(snapshot_ids, concurrency, phase, |snapshot_id| {
        snapshot_volume_grants(ec2_client, snapshot_id)
    })
    .await
}

async fn lookup_external_use<'a, F, Fut>(
    ids: &[&'a str],
    concurrency: usize,
    phase: Option<&Phase>,
    is_shared: F,
) -> HashMap<String, ExternalUse>
where
    F: Fn(&'a str) -> Fut,
    Fut: Future<Output = AppResult<Vec<Grant>>>,
{
    stream::iter(ids.iter().copied())
        .map(|id| {
            let lookup = is_shared(id);
            async move { (id, lookup.await) }
        })
        .buffered(concurrency.max(1))
        .inspect(|_| {
            if let Some(phase) = phase {
                phase.inc();
            }
        })
        .filter_map(|(id, result)| async move {
            match result {
                Ok(grants) if grants.is_empty() => None,
                Ok(grants) => Some((id.to_string(), ExternalUse::Shared(grants))),
                Err(error) => {
                    Some((id.to_string(), ExternalUse::Unknown(error_chain(error.as_ref()))))
                }
            }
        })
        .collect()
        .await
}
