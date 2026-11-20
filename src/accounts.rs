//! `--check-accounts`: shared AMIs and snapshots that no other account uses.
//!
//! Anything shared is kept by default, because the accounts it's shared with can't be seen from
//! here. With `--check-accounts`, those accounts are scanned (same region, their own profile) with
//! the same reference collector, and a shared item stops being kept only when every grant is to a
//! listed account, every one of those accounts was scanned cleanly, and none of them uses it.
//! Public, organization and OU grants can't be enumerated, so they always keep the item.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use aws_sdk_ec2 as ec2;
use aws_sdk_sts as sts;
use aws_types::region::Region;
use log::warn;

use crate::cli::Args;
use crate::errors::{describe_sdk_error, error_chain};
use crate::model::{ExternalUse, Grant, KeepReason};
use crate::references::{ReferenceClients, collect_referenced_ami_ids};

/// Values per `snapshot-id` filter in one DescribeVolumes call.
const SNAPSHOT_FILTER_CHUNK: usize = 200;

/// The accounts `--check-accounts` resolved to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CheckAccounts {
    /// Account id -> the profile that reaches it.
    pub(crate) profiles: BTreeMap<String, String>,
    /// The account being cleaned. A launch permission for it grants nothing (the owner can always
    /// use its own AMIs), so it's ignored; Image Builder distributions often add one.
    pub(crate) owner: Option<String>,
}

impl CheckAccounts {
    /// Grants to anyone other than the owner account.
    fn external<'a>(&'a self, grants: &'a [Grant]) -> impl Iterator<Item = &'a Grant> {
        grants.iter().filter(move |grant| {
            !matches!(grant, Grant::Account(account) if self.owner.as_deref() == Some(account.as_str()))
        })
    }
}

/// What one checked account uses, by id, with what uses it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountUse {
    pub(crate) amis: HashMap<String, Vec<String>>,
    pub(crate) snapshots: HashMap<String, Vec<String>>,
}

/// Account id -> what it uses, or why it couldn't be checked.
pub(crate) type AccountScans = HashMap<String, Result<AccountUse, String>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SharedKind {
    Ami,
    Snapshot,
}

/// Looks up each profile's account (STS). A profile that fails, duplicates another, or is the
/// owner account is left out with a warning; anything shared with it then stays kept.
pub(crate) async fn resolve_check_accounts(
    args: &Args,
    owner_account: Option<&str>,
) -> Option<CheckAccounts> {
    if args.check_accounts.is_empty() {
        return None;
    }
    let mut profiles = BTreeMap::new();
    for profile in &args.check_accounts {
        let config = crate::aws_config_loader(args).profile_name(profile).load().await;
        match sts::Client::new(&config).get_caller_identity().send().await {
            Ok(identity) => match identity.account() {
                Some(account) if Some(account) == owner_account => {
                    warn!(
                        "--check-accounts {profile} is the account being cleaned ({account}); ignoring it"
                    );
                }
                Some(account) => {
                    if let Some(previous) = profiles.insert(account.to_string(), profile.clone()) {
                        warn!(
                            "--check-accounts {previous} and {profile} are both account {account}; using {profile}"
                        );
                    }
                }
                None => warn!(
                    "--check-accounts {profile}: GetCallerIdentity returned no account; anything shared with it stays kept"
                ),
            },
            Err(error) => warn!(
                "--check-accounts {profile}: GetCallerIdentity failed: {}; anything shared with it stays kept",
                describe_sdk_error(&error)
            ),
        }
    }
    Some(CheckAccounts { profiles, owner: owner_account.map(ToString::to_string) })
}

/// Accounts worth scanning for these sharing results: only items shared exclusively with listed
/// accounts can become deletable, so other items don't cost a scan.
pub(crate) fn accounts_to_scan(
    external_use: &HashMap<String, ExternalUse>,
    check: &CheckAccounts,
) -> BTreeSet<String> {
    external_use
        .values()
        .filter_map(|usage| match usage {
            ExternalUse::Shared(grants) => Some(grants),
            _ => None,
        })
        .filter(|grants| {
            check.external(grants).all(
                |grant| matches!(grant, Grant::Account(account) if check.profiles.contains_key(account)),
            )
        })
        .flat_map(|grants| check.external(grants))
        .filter_map(|grant| match grant {
            Grant::Account(account) => Some(account.clone()),
            _ => None,
        })
        .collect()
}

/// Scans `accounts` in `region` that aren't in `scans` yet, then checks every listed account for
/// volumes created from `snapshot_candidates`. Every failure is recorded per account, never
/// skipped: a failed account keeps whatever is shared with it.
pub(crate) async fn scan_accounts(
    args: &Args,
    check: &CheckAccounts,
    region: &str,
    accounts: &BTreeSet<String>,
    snapshot_candidates: &[&str],
    scans: &mut AccountScans,
) {
    for account in accounts {
        let Some(profile) = check.profiles.get(account) else {
            continue;
        };
        if scans.contains_key(account) && snapshot_candidates.is_empty() {
            continue;
        }
        let config = crate::aws_config_loader(args)
            .profile_name(profile)
            .region(Region::new(region.to_string()))
            .load()
            .await;
        let clients = ReferenceClients::new(
            &config,
            &args.skip_reference_sources,
            usize::from(args.max_concurrency),
        )
        .with_label(format!("{profile} · {region}"))
        .with_lt_cache(crate::lt_cache_for_account(args, account));
        let previous = scans.remove(account);
        let result =
            check_account(&ec2::Client::new(&config), &clients, previous, snapshot_candidates)
                .await;
        scans.insert(account.clone(), result);
    }
}

/// What one account uses: its reference scan (or `previous`, when already scanned), plus volumes
/// created from `snapshot_candidates`. Unresolved references make the whole account unknown.
pub(crate) async fn check_account(
    ec2_client: &ec2::Client,
    clients: &ReferenceClients,
    previous: Option<Result<AccountUse, String>>,
    snapshot_candidates: &[&str],
) -> Result<AccountUse, String> {
    let mut usage = match previous {
        Some(previous) => previous?,
        None => match collect_referenced_ami_ids(ec2_client, clients).await {
            Ok(scan) if scan.unresolved_references.is_empty() => AccountUse {
                amis: scan
                    .referenced_ami_ids
                    .iter()
                    .map(|ami_id| {
                        (ami_id.clone(), scan.sources.get(ami_id).cloned().unwrap_or_default())
                    })
                    .collect(),
                snapshots: scan.snapshot_references,
            },
            Ok(scan) => {
                return Err(format!(
                    "{} unresolved AMI reference(s), so its references aren't fully known",
                    scan.unresolved_references.len()
                ));
            }
            Err(error) => {
                return Err(format!("reference scan failed: {}", error_chain(error.as_ref())));
            }
        },
    };
    if !snapshot_candidates.is_empty() {
        for (snapshot_id, volume_id) in
            volumes_from_snapshots(ec2_client, snapshot_candidates).await?
        {
            let sources = usage.snapshots.entry(snapshot_id).or_default();
            let source = format!("volume {volume_id}");
            if !sources.contains(&source) {
                sources.push(source);
            }
        }
    }
    Ok(usage)
}

/// (snapshot id, volume id) for volumes in the account created from one of `snapshot_ids`. A
/// volume doesn't need its source snapshot, but it shows someone restored from it.
async fn volumes_from_snapshots(
    ec2_client: &ec2::Client,
    snapshot_ids: &[&str],
) -> Result<Vec<(String, String)>, String> {
    let mut found = Vec::new();
    for chunk in snapshot_ids.chunks(SNAPSHOT_FILTER_CHUNK) {
        let filter = ec2::types::Filter::builder()
            .name("snapshot-id")
            .set_values(Some(chunk.iter().map(ToString::to_string).collect()))
            .build();
        let mut pages = ec2_client.describe_volumes().filters(filter).into_paginator().send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| {
                format!("DescribeVolumes failed: {}", describe_sdk_error(&error))
            })?;
            found.extend(page.volumes().iter().filter_map(|volume| {
                Some((volume.snapshot_id()?.to_string(), volume.volume_id()?.to_string()))
            }));
        }
    }
    Ok(found)
}

/// Why a shared item must still be kept after checking the accounts (pure), or `None` when every
/// account it's shared with was checked and none uses it.
pub(crate) fn shared_keep_reason(
    grants: &[Grant],
    id: &str,
    kind: SharedKind,
    check: &CheckAccounts,
    scans: &AccountScans,
) -> Option<KeepReason> {
    let mut grants = check.external(grants).cloned().collect::<Vec<_>>();
    grants.sort();
    if grants.contains(&Grant::Public) {
        return Some(KeepReason::SharedPublicly);
    }
    if let Some(arn) = grants.iter().find_map(|grant| match grant {
        Grant::Organization(arn) | Grant::OrganizationalUnit(arn) => Some(arn),
        _ => None,
    }) {
        return Some(KeepReason::SharedWithOrganization(arn.clone()));
    }
    let accounts = grants
        .iter()
        .filter_map(|grant| match grant {
            Grant::Account(account) => Some(account),
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(account) = accounts.iter().find(|account| !check.profiles.contains_key(**account)) {
        return Some(KeepReason::SharedWithUncheckedAccount((*account).clone()));
    }
    for account in &accounts {
        let profile = &check.profiles[*account];
        match scans.get(*account) {
            None => {
                return Some(KeepReason::AccountCheckFailed(format!(
                    "{profile} ({account}): not scanned"
                )));
            }
            Some(Err(error)) => {
                return Some(KeepReason::AccountCheckFailed(format!(
                    "{profile} ({account}): {error}"
                )));
            }
            Some(Ok(_)) => {}
        }
    }
    for account in accounts {
        let profile = &check.profiles[account];
        let Some(Ok(usage)) = scans.get(account) else { continue };
        let used = match kind {
            SharedKind::Ami => usage.amis.get(id),
            SharedKind::Snapshot => usage.snapshots.get(id),
        };
        if let Some(sources) = used {
            let what =
                if sources.is_empty() { "referenced".to_string() } else { sources.join("; ") };
            return Some(KeepReason::UsedInOtherAccount(format!("{profile} ({account}): {what}")));
        }
    }
    None
}

/// Applies the account checks to sharing results: a shared item that must still be kept becomes
/// `Kept(reason)`; one no checked account uses is removed, so the normal rules decide it.
pub(crate) fn resolve_external_use(
    external_use: &mut HashMap<String, ExternalUse>,
    kind: SharedKind,
    check: &CheckAccounts,
    scans: &AccountScans,
) {
    external_use.retain(|id, usage| {
        let ExternalUse::Shared(grants) = usage else {
            return true;
        };
        match shared_keep_reason(grants, id, kind, check, scans) {
            Some(reason) => {
                *usage = ExternalUse::Kept(reason);
                true
            }
            None => false,
        }
    });
}
