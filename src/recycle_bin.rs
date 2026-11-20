//! Recycle Bin coverage for snapshot deletes. With a retention rule for EBS snapshots, a deleted
//! snapshot is kept in the Recycle Bin and can be restored until the rule's period runs out.
//! Every plan reports whether its deletes would be recoverable; `--require-recycle-bin` keeps the
//! snapshots that wouldn't be.

use std::collections::HashMap;

use aws_sdk_rbin as rbin;
use aws_sdk_rbin::types::{ResourceType, RetentionPeriodUnit, RuleStatus};

use crate::errors::describe_sdk_error;

/// A tag a rule matches on: the key, and the value unless any value matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuleTag {
    pub(crate) key: String,
    pub(crate) value: Option<String>,
}

/// One EBS snapshot retention rule, as far as coverage is concerned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetentionRule {
    pub(crate) id: String,
    /// Only `available` rules retain anything.
    pub(crate) available: bool,
    /// Retention in days; `None` if the period is in a unit this tool doesn't know.
    pub(crate) days: Option<u32>,
    /// Tag-level rule: retains snapshots with any of these tags. Empty for a Region-level rule.
    pub(crate) resource_tags: Vec<RuleTag>,
    /// Region-level rule: snapshots with any of these tags aren't retained.
    pub(crate) exclude_tags: Vec<RuleTag>,
}

fn rule_tags(tags: &[rbin::types::ResourceTag]) -> Vec<RuleTag> {
    tags.iter()
        .map(|tag| RuleTag {
            key: tag.resource_tag_key().to_string(),
            value: tag.resource_tag_value().filter(|value| !value.is_empty()).map(str::to_string),
        })
        .collect()
}

/// Every EBS snapshot retention rule in the client's region.
pub(crate) async fn list_snapshot_retention_rules(
    client: &rbin::Client,
) -> Result<Vec<RetentionRule>, String> {
    let mut ids = Vec::new();
    let mut pages =
        client.list_rules().resource_type(ResourceType::EbsSnapshot).into_paginator().send();
    while let Some(page) = pages.next().await {
        let page =
            page.map_err(|error| format!("ListRules failed: {}", describe_sdk_error(&error)))?;
        ids.extend(page.rules().iter().filter_map(|rule| rule.identifier().map(str::to_string)));
    }

    // The summaries carry neither status nor tags, so each rule is read in full.
    let mut rules = Vec::new();
    for id in ids {
        let rule = client
            .get_rule()
            .identifier(&id)
            .send()
            .await
            .map_err(|error| format!("GetRule {id} failed: {}", describe_sdk_error(&error)))?;
        let days = rule.retention_period().and_then(|period| {
            (period.retention_period_unit() == &RetentionPeriodUnit::Days)
                .then(|| u32::try_from(period.retention_period_value()).ok())
                .flatten()
        });
        rules.push(RetentionRule {
            id,
            available: rule.status() == Some(&RuleStatus::Available),
            days,
            resource_tags: rule_tags(rule.resource_tags()),
            exclude_tags: rule_tags(rule.exclude_resource_tags()),
        });
    }
    Ok(rules)
}

fn has_tag(tags: &HashMap<String, String>, rule_tag: &RuleTag) -> bool {
    tags.get(&rule_tag.key)
        .is_some_and(|value| rule_tag.value.as_deref().is_none_or(|expected| value == expected))
}

/// Days a deleted snapshot with these tags would stay restorable, or `None` if no available rule
/// retains it (pure). Matching tag-level rules take precedence over Region-level rules; among
/// several, the longest retention applies. A rule whose period can't be read (a unit other than
/// days) doesn't count, so the answer errs towards "not recoverable".
pub(crate) fn recycle_bin_days(
    tags: &HashMap<String, String>,
    rules: &[RetentionRule],
) -> Option<u32> {
    let available = rules.iter().filter(|rule| rule.available);
    let tag_level = available
        .clone()
        .filter(|rule| !rule.resource_tags.is_empty())
        .filter(|rule| rule.resource_tags.iter().any(|tag| has_tag(tags, tag)))
        .collect::<Vec<_>>();
    let matching = if tag_level.is_empty() {
        available
            .filter(|rule| rule.resource_tags.is_empty())
            .filter(|rule| !rule.exclude_tags.iter().any(|tag| has_tag(tags, tag)))
            .collect()
    } else {
        tag_level
    };
    matching.iter().filter_map(|rule| rule.days).max()
}
