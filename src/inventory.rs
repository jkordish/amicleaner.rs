//! Lists account-owned AMIs and snapshots and looks up who else can use them.

use std::collections::HashMap;
use std::future::Future;

use aws_sdk_ec2 as ec2;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use log::warn;

use crate::errors::{AppResult, AwsResultExt, error_chain};
use crate::model::{AmiInfo, ExternalUse, LastLaunched, SnapshotInfo};

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

pub(crate) fn is_resource_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|hex| {
        matches!(hex.len(), 8 | 17)
            && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

pub(crate) fn is_ami_id(value: &str) -> bool {
    is_resource_id(value, "ami-")
}

pub(crate) async fn ami_has_external_launch_permissions(
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

/// Whether a snapshot grants create-volume permissions to anyone (other accounts, organizations,
/// OUs, or the public). The owner never needs one, so any entry means someone else can use it.
pub(crate) async fn snapshot_has_external_create_volume_permissions(
    ec2_client: &ec2::Client,
    snapshot_id: &str,
) -> AppResult<bool> {
    let output = ec2_client
        .describe_snapshot_attribute()
        .snapshot_id(snapshot_id)
        .attribute(ec2::types::SnapshotAttributeName::CreateVolumePermission)
        .send()
        .await
        .aws_context("DescribeSnapshotAttribute")?;
    Ok(!output.create_volume_permissions().is_empty())
}

/// Looks up, `concurrency` at a time, which AMIs grant launch permissions to anyone else.
/// Unshared AMIs are absent from the result; a failed lookup is `ExternalUse::Unknown`.
pub(crate) async fn lookup_ami_external_use(
    ec2_client: &ec2::Client,
    ami_ids: &[&str],
    concurrency: usize,
) -> HashMap<String, ExternalUse> {
    lookup_external_use(ami_ids, concurrency, |ami_id| {
        ami_has_external_launch_permissions(ec2_client, ami_id)
    })
    .await
}

/// Looks up, `concurrency` at a time, which snapshots grant create-volume permissions to anyone
/// else. Unshared snapshots are absent from the result; a failed lookup is `ExternalUse::Unknown`.
pub(crate) async fn lookup_snapshot_external_use(
    ec2_client: &ec2::Client,
    snapshot_ids: &[&str],
    concurrency: usize,
) -> HashMap<String, ExternalUse> {
    lookup_external_use(snapshot_ids, concurrency, |snapshot_id| {
        snapshot_has_external_create_volume_permissions(ec2_client, snapshot_id)
    })
    .await
}

async fn lookup_external_use<'a, F, Fut>(
    ids: &[&'a str],
    concurrency: usize,
    is_shared: F,
) -> HashMap<String, ExternalUse>
where
    F: Fn(&'a str) -> Fut,
    Fut: Future<Output = AppResult<bool>>,
{
    stream::iter(ids.iter().copied())
        .map(|id| {
            let lookup = is_shared(id);
            async move { (id, lookup.await) }
        })
        .buffered(concurrency.max(1))
        .filter_map(|(id, result)| async move {
            match result {
                Ok(false) => None,
                Ok(true) => Some((id.to_string(), ExternalUse::Shared)),
                Err(error) => {
                    Some((id.to_string(), ExternalUse::Unknown(error_chain(error.as_ref()))))
                }
            }
        })
        .collect()
        .await
}
