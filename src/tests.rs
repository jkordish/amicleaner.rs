use std::collections::{HashMap, HashSet};
use std::process::ExitCode;
use std::time::Duration as StdDuration;

use aws_config::BehaviorVersion;
use aws_sdk_autoscaling as autoscaling;
use aws_sdk_backup as backup;
use aws_sdk_batch as batch;
use aws_sdk_cloudformation as cloudformation;
use aws_sdk_ec2 as ec2;
use aws_sdk_emr as emr;
use aws_sdk_imagebuilder as imagebuilder;
use aws_sdk_ssm as ssm;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use aws_types::SdkConfig;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use ec2::error::ErrorMetadata;

use super::{inert_flag_warnings, is_enabled_region, skipped_reference_sources_warning};
use crate::aws_backup::{
    apply_backup_classification, block_all_snapshot_deletions, classify_backup_snapshots,
    snapshot_id_from_arn,
};
use crate::cli::{Args, parse_region};
use crate::errors::error_chain;
use crate::execute::{
    execute_region_plan, is_retryable_code, retry_delay_for_attempt, send_with_retries,
};
use crate::inventory::{
    ami_has_external_launch_permissions, is_ami_id, list_owned_amis, lookup_snapshot_external_use,
};
use crate::model::{
    AmiInfo, EXIT_BLOCKED, ExecutionSummary, ExternalUse, LastLaunched, RecoveryPointRecord,
    RegionPlan, RetryPolicy, RunOutcome, SnapshotInfo,
};
use crate::plan::{apply_snapshot_external_use, plan_region};
use crate::policy::{
    backup_recovery_point_deletion_tag_allows, has_expected_tag, is_protected,
    looks_backup_created, optional_expected_value, select_latest_ami_ids_by_family,
};
use crate::references::services::{
    OwnImageBuilderImage, ParentImage, classify_parent_image, find_ami_ids,
    find_ssm_dynamic_references, image_version_matches,
};
use crate::references::{
    AmiReferenceScan, ImageReferenceResolution, LaunchTemplateRef, ReferenceClients,
    ReferenceCollector, ReferenceSource, collect_referenced_ami_ids, extract_ssm_parameter_name,
    resolve_image_reference,
};

const AMI_A: &str = "ami-0123456789abcdef0";
const AMI_B: &str = "ami-0123456789abcdef1";
const AMI_C: &str = "ami-0123456789abcdef2";
const AMI_D: &str = "ami-0123456789abcdef3";
const ACCOUNT: &str = "123456789012";
const SNAP_A: &str = "snap-0123456789abcdef0";
const SNAP_B: &str = "snap-0123456789abcdef1";
const SNAP_C: &str = "snap-0123456789abcdef2";

fn test_args() -> Args {
    let mut args = Args::try_parse_from(["amicleaner"]).expect("defaults parse");
    args.min_age_days = 30;
    args.ami_recent_launch_days = 30;
    args.retry_delay_ms = 0;
    args
}

fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(key, value)| ((*key).to_string(), (*value).to_string())).collect()
}

fn make_ami(
    ami_id: &str,
    creation_date: DateTime<Utc>,
    tag_pairs: &[(&str, &str)],
    snapshot_ids: &[&str],
) -> AmiInfo {
    AmiInfo {
        ami_id: ami_id.to_string(),
        name: Some(format!("{ami_id}-name")),
        description: None,
        creation_date: Some(creation_date),
        last_launched: LastLaunched::Never,
        snapshot_ids: snapshot_ids.iter().map(|entry| (*entry).to_string()).collect(),
        tags: tags(tag_pairs),
    }
}

fn make_snapshot(
    snapshot_id: &str,
    start_time: DateTime<Utc>,
    tag_pairs: &[(&str, &str)],
) -> SnapshotInfo {
    SnapshotInfo {
        snapshot_id: snapshot_id.to_string(),
        start_time,
        description: None,
        tags: tags(tag_pairs),
    }
}

fn scan(referenced: &[&str], unresolved: &[&str]) -> AmiReferenceScan {
    AmiReferenceScan {
        referenced_ami_ids: referenced.iter().map(|entry| (*entry).to_string()).collect(),
        unresolved_references: unresolved.iter().map(|entry| (*entry).to_string()).collect(),
    }
}

fn plan(
    args: &Args,
    amis: Vec<AmiInfo>,
    snapshots: Vec<SnapshotInfo>,
    scan: &AmiReferenceScan,
) -> RegionPlan {
    plan_region(args, "us-east-1", Utc::now(), &amis, &snapshots, scan, &HashMap::new())
}

fn ec2_error(code: &str, message: &str) -> ErrorMetadata {
    ErrorMetadata::builder().code(code).message(message).build()
}

fn policy(max_retries: u32) -> RetryPolicy {
    RetryPolicy { max_retries, base_delay_ms: 0 }
}

// ---------------------------------------------------------------------------------------------
// CLI parsing (#6, #17, #18)
// ---------------------------------------------------------------------------------------------

#[test]
fn defaults_protect_on_key_presence_and_retry_twice() {
    let args = Args::try_parse_from(["amicleaner"]).unwrap();
    assert_eq!(args.protect_tag_value, "");
    assert_eq!(args.max_retries, 2);
}

#[test]
fn day_flags_are_bounded_and_accept_negative_syntax_for_a_clear_error() {
    assert!(Args::try_parse_from(["amicleaner", "--min-age-days", "0"]).is_ok());
    assert!(Args::try_parse_from(["amicleaner", "--min-age-days", "36500"]).is_ok());
    assert!(Args::try_parse_from(["amicleaner", "--min-age-days", "100000000"]).is_err());
    assert!(
        Args::try_parse_from(["amicleaner", "--ami-recent-launch-days", "99999999999"]).is_err()
    );

    let error = Args::try_parse_from(["amicleaner", "--min-age-days", "-5"]).unwrap_err();
    assert!(error.to_string().contains("-5"), "{error}");
    assert!(!error.to_string().contains("unexpected argument"), "{error}");
}

#[test]
fn empty_tag_keys_are_rejected() {
    for flag in ["--protect-tag", "--delete-tag", "--backup-delete-tag"] {
        assert!(Args::try_parse_from(["amicleaner", flag, ""]).is_err(), "{flag}");
        assert!(Args::try_parse_from(["amicleaner", flag, "   "]).is_err(), "{flag}");
    }
    let args = Args::try_parse_from(["amicleaner", "--protect-tag", " Keep "]).unwrap();
    assert_eq!(args.protect_tag, "Keep");
}

#[test]
fn regions_are_validated_and_normalized() {
    for valid in ["us-east-1", "ap-southeast-4", "us-gov-west-1", "cn-north-1", "il-central-1"] {
        assert_eq!(parse_region(valid).as_deref(), Ok(valid));
    }
    assert_eq!(parse_region(" US-EAST-1 ").as_deref(), Ok("us-east-1"));
    for invalid in ["", "us-east", "useast1", "us-east-1a", "us_east_1", "us--1", "1-east-1"] {
        assert!(parse_region(invalid).is_err(), "{invalid:?}");
    }

    let args = Args::try_parse_from(["amicleaner", "--regions", "us-east-1,eu-west-1"]).unwrap();
    assert_eq!(args.regions, vec!["us-east-1", "eu-west-1"]);
    assert!(Args::try_parse_from(["amicleaner", "--regions", "us-east-1,"]).is_err());
    assert!(Args::try_parse_from(["amicleaner", "--regions", "nope"]).is_err());
}

#[test]
fn inert_flags_warn_only_without_execute() {
    let mut args = test_args();
    args.dry_run = true;
    args.execute_backup_recovery_point_deletes = true;
    assert_eq!(inert_flag_warnings(&args).len(), 2);

    args.execute = true;
    assert!(inert_flag_warnings(&args).is_empty());
}

#[test]
fn is_enabled_region_only_allows_opted_in_or_not_required() {
    assert!(is_enabled_region(Some("opted-in")));
    assert!(is_enabled_region(Some("opt-in-not-required")));
    assert!(is_enabled_region(None));
    assert!(!is_enabled_region(Some("not-opted-in")));
}

// ---------------------------------------------------------------------------------------------
// Tags (#6)
// ---------------------------------------------------------------------------------------------

#[test]
fn has_expected_tag_supports_value_or_key_only() {
    let tags = tags(&[("cleanup", "true"), ("DoNotDelete", "yes"), ("padded", " TRUE ")]);

    assert!(has_expected_tag(&tags, "cleanup", Some("true")));
    assert!(!has_expected_tag(&tags, "cleanup", Some("false")));
    assert!(has_expected_tag(&tags, "DoNotDelete", None));
    assert!(has_expected_tag(&tags, "padded", optional_expected_value(" true ")));
    assert!(!has_expected_tag(&tags, "Cleanup", Some("true")), "delete tag key stays exact");
}

#[test]
fn protect_tag_fails_closed_on_any_value_and_key_case() {
    let args = test_args();
    for value in ["true", "yes", "1", "", "false", " true "] {
        assert!(is_protected(&tags(&[("DoNotDelete", value)]), &args), "value {value:?}");
    }
    assert!(is_protected(&tags(&[("donotdelete", "true")]), &args));
    assert!(is_protected(&tags(&[("DONOTDELETE", "")]), &args));
    assert!(!is_protected(&tags(&[("cleanup", "true")]), &args));
}

#[test]
fn protect_tag_value_can_still_be_required_explicitly() {
    let mut args = test_args();
    args.protect_tag_value = "true".to_string();
    assert!(is_protected(&tags(&[("DoNotDelete", " TRUE ")]), &args));
    assert!(!is_protected(&tags(&[("DoNotDelete", "no")]), &args));
}

#[test]
fn backup_recovery_point_deletion_tag_allows_respects_dedicated_toggle() {
    let mut args = test_args();
    let tagged = tags(&[("backup-cleanup", "true")]);
    let untagged = HashMap::new();

    assert!(backup_recovery_point_deletion_tag_allows(&tagged, &args));
    assert!(!backup_recovery_point_deletion_tag_allows(&untagged, &args));

    args.allow_untagged_backup_recovery_points = true;
    assert!(backup_recovery_point_deletion_tag_allows(&untagged, &args));
}

#[test]
fn backup_created_heuristic() {
    assert!(looks_backup_created(&tags(&[("aws:backup:source-resource", "x")]), None));
    assert!(looks_backup_created(
        &HashMap::new(),
        Some("This snapshot is created by the AWS Backup service.")
    ));
    assert!(!looks_backup_created(&tags(&[("cleanup", "true")]), Some("manual")));
}

// ---------------------------------------------------------------------------------------------
// Planning: AMIs (#7, #16, #19) and snapshots (#5)
// ---------------------------------------------------------------------------------------------

#[test]
fn plan_region_marks_only_unreferenced_old_and_tagged_resources() {
    let args = test_args();
    let now = Utc::now();
    let old = now - Duration::days(90);

    let deletable_ami = make_ami("ami-delete", old, &[("cleanup", "true")], &["snap-ami-delete"]);
    let mut referenced_ami = make_ami("ami-ref", old, &[("cleanup", "true")], &["snap-ref"]);
    referenced_ami.last_launched = LastLaunched::At(old);
    let protected_ami = make_ami(
        "ami-protect",
        old,
        &[("cleanup", "true"), ("DoNotDelete", "true")],
        &["snap-protect"],
    );

    let snapshots = vec![
        make_snapshot("snap-delete", old, &[("cleanup", "true")]),
        make_snapshot("snap-ref", old, &[("cleanup", "true")]),
        make_snapshot("snap-untagged", old, &[]),
    ];

    let plan = plan(
        &args,
        vec![deletable_ami, referenced_ami, protected_ami],
        snapshots,
        &scan(&["ami-ref"], &[]),
    );

    assert_eq!(plan.deregister_amis.len(), 1);
    assert_eq!(plan.deregister_amis[0].ami_id, "ami-delete");

    let planned = plan.delete_snapshots.iter().map(|s| s.snapshot_id.as_str()).collect::<Vec<_>>();
    assert_eq!(planned, vec!["snap-delete"]);
    assert!(plan.blocked.is_empty());
}

#[test]
fn snapshots_freed_by_ami_go_through_snapshot_checks_and_inherit_authorization() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);

    let amis = vec![
        make_ami(
            "ami-delete",
            old,
            &[("cleanup", "true")],
            &["snap-freed", "snap-protected", "snap-shared"],
        ),
        make_ami("ami-keep", old, &[], &["snap-shared"]),
    ];
    let snapshots = vec![
        // Untagged: inherits the AMI's delete authorization.
        make_snapshot("snap-freed", old, &[]),
        // The snapshot's own protect tag still wins (bug #5).
        make_snapshot("snap-protected", old, &[("DoNotDelete", "true")]),
        // Still used by an AMI that stays.
        make_snapshot("snap-shared", old, &[("cleanup", "true")]),
    ];

    let plan = plan(&args, amis, snapshots, &scan(&[], &[]));

    assert_eq!(plan.deregister_amis.len(), 1);
    let planned = plan.delete_snapshots.iter().map(|s| s.snapshot_id.as_str()).collect::<Vec<_>>();
    assert_eq!(planned, vec!["snap-freed"]);
    assert_eq!(
        plan.snapshots_released_by_ami.get("snap-freed").map(String::as_str),
        Some("ami-delete")
    );
    assert_eq!(plan.snapshots_released_by_ami.len(), 1);
}

#[test]
fn freed_snapshots_are_not_planned_when_ami_deregistration_is_cancelled() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let amis = vec![make_ami("ami-delete", old, &[("cleanup", "true")], &["snap-freed"])];
    let snapshots = vec![make_snapshot("snap-freed", old, &[])];

    let plan = plan(
        &args,
        amis,
        snapshots,
        &scan(&[], &["failed to resolve SSM image parameter '/x'"]),
    );

    assert!(plan.deregister_amis.is_empty());
    assert!(plan.delete_snapshots.is_empty());
    assert_eq!(plan.blocked.len(), 1);
}

#[test]
fn unresolved_references_only_block_when_there_was_something_to_delete() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let untagged_ami = make_ami("ami-untagged", old, &[], &[]);

    let plan = plan(
        &args,
        vec![untagged_ami],
        Vec::new(),
        &scan(&[], &["unresolved image reference 'x'"]),
    );

    assert_eq!(plan.unresolved_ami_references.len(), 1);
    assert!(plan.blocked.is_empty());
}

#[test]
fn amis_with_unknown_dates_are_kept_and_keep_their_snapshots_referenced() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);

    let mut no_creation_date = make_ami(AMI_A, old, &[("cleanup", "true")], &[SNAP_A]);
    no_creation_date.creation_date = None;
    let mut unknown_launch = make_ami(AMI_B, old, &[("cleanup", "true")], &[SNAP_B]);
    unknown_launch.last_launched = LastLaunched::Unknown;

    let snapshots = vec![
        make_snapshot(SNAP_A, old, &[("cleanup", "true")]),
        make_snapshot(SNAP_B, old, &[("cleanup", "true")]),
    ];

    let plan = plan(&args, vec![no_creation_date, unknown_launch], snapshots, &scan(&[], &[]));

    assert!(plan.deregister_amis.is_empty());
    assert!(plan.delete_snapshots.is_empty(), "snapshots of kept AMIs must stay referenced");
}

#[test]
fn recent_launch_keeps_ami() {
    let args = test_args();
    let now = Utc::now();
    let mut ami = make_ami(AMI_A, now - Duration::days(90), &[("cleanup", "true")], &[]);
    ami.last_launched = LastLaunched::At(now - Duration::days(1));

    assert!(plan(&args, vec![ami], Vec::new(), &scan(&[], &[])).deregister_amis.is_empty());
}

#[test]
fn backup_created_amis_are_never_deregistered() {
    let mut args = test_args();
    args.allow_untagged = true;
    let old = Utc::now() - Duration::days(90);
    let ami = make_ami(AMI_A, old, &[("aws:backup:source-resource", "i-123")], &[]);

    assert!(plan(&args, vec![ami], Vec::new(), &scan(&[], &[])).deregister_amis.is_empty());
}

#[test]
fn shared_amis_are_kept_and_failed_lookups_block() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let amis = vec![
        make_ami(AMI_A, old, &[("cleanup", "true")], &[]),
        make_ami(AMI_B, old, &[("cleanup", "true")], &[]),
    ];
    let external = HashMap::from([
        (AMI_A.to_string(), ExternalUse::Shared),
        (AMI_B.to_string(), ExternalUse::Unknown("AccessDenied".to_string())),
    ]);

    let plan = plan_region(&args, "us-east-1", Utc::now(), &amis, &[], &scan(&[], &[]), &external);

    assert!(plan.deregister_amis.is_empty());
    assert_eq!(plan.skipped_amis, 2);
    assert_eq!(plan.kept_shared_amis, 1, "the failed lookup is blocked, not counted as shared");
    assert_eq!(plan.blocked.len(), 1, "only the failed lookup is uncertain");
    assert!(plan.blocked[0].contains(AMI_B));
}

#[test]
fn select_latest_ami_ids_by_family_keeps_latest_per_group() {
    let now = Utc::now();
    let ami_old = make_ami("ami-old", now - Duration::days(20), &[("ImageFamily", "app")], &[]);
    let ami_new = make_ami("ami-new", now - Duration::days(10), &[("ImageFamily", "app")], &[]);

    let kept = select_latest_ami_ids_by_family(&[ami_old, ami_new], 1);
    assert_eq!(kept, HashSet::from(["ami-new".to_string()]));
}

#[test]
fn family_ties_are_broken_deterministically() {
    let created = Utc::now() - Duration::days(20);
    let first = make_ami(AMI_A, created, &[("ImageFamily", "app")], &[]);
    let second = make_ami(AMI_B, created, &[("ImageFamily", "app")], &[]);

    let forward = select_latest_ami_ids_by_family(&[first.clone(), second.clone()], 1);
    let reverse = select_latest_ami_ids_by_family(&[second, first], 1);
    assert_eq!(forward, reverse);
}

#[test]
fn keep_latest_per_family_keeps_untagged_amis_explicitly() {
    let mut args = test_args();
    args.keep_latest_per_family = 1;
    let old = Utc::now() - Duration::days(90);
    let amis = vec![
        make_ami(AMI_A, old, &[("cleanup", "true")], &[]),
        make_ami(
            AMI_B,
            old - Duration::days(1),
            &[("cleanup", "true"), ("ImageFamily", "app")],
            &[],
        ),
        make_ami(
            "ami-0123456789abcdef2",
            old - Duration::days(2),
            &[("cleanup", "true"), ("ImageFamily", "app")],
            &[],
        ),
    ];

    let plan = plan(&args, amis, Vec::new(), &scan(&[], &[]));

    let planned = plan.deregister_amis.iter().map(|ami| ami.ami_id.as_str()).collect::<Vec<_>>();
    assert_eq!(
        planned,
        vec!["ami-0123456789abcdef2"],
        "only the older tagged family member goes"
    );
    assert_eq!(plan.kept_untagged_family_amis, 1);
}

// ---------------------------------------------------------------------------------------------
// AWS Backup mapping (#1, #2, #4)
// ---------------------------------------------------------------------------------------------

fn ebs_record(snapshot_id: &str) -> RecoveryPointRecord {
    RecoveryPointRecord {
        backup_vault_name: "Default".to_string(),
        vault_locked: false,
        vault_type: Some("BACKUP_VAULT".to_string()),
        recovery_point_arn: Some(format!("arn:aws:ec2:us-east-1::snapshot/{snapshot_id}")),
        // For EBS the resource ARN is the *volume*, which is what the old code parsed.
        resource_arn: Some(
            "arn:aws:ec2:us-east-1:123456789012:volume/vol-0123456789abcdef0".to_string(),
        ),
        status: Some("COMPLETED".to_string()),
        is_parent: false,
        parent_recovery_point_arn: None,
    }
}

#[test]
fn snapshot_id_is_parsed_from_snapshot_arns_only() {
    assert_eq!(
        snapshot_id_from_arn(&format!("arn:aws:ec2:us-east-1::snapshot/{SNAP_A}")),
        Some(SNAP_A)
    );
    assert_eq!(
        snapshot_id_from_arn("arn:aws:ec2:us-east-1:123:volume/vol-0123456789abcdef0"),
        None
    );
    assert_eq!(snapshot_id_from_arn("arn:aws:ec2:us-east-1::snapshot/snap-nothex!"), None);
    assert_eq!(snapshot_id_from_arn("snap-0123456789abcdef0"), None);
}

#[test]
fn realistic_ebs_recovery_point_maps_to_its_snapshot() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let candidates = vec![
        make_snapshot(SNAP_A, old, &[("cleanup", "true"), ("backup-cleanup", "true")]),
        make_snapshot(SNAP_B, old, &[("cleanup", "true")]),
    ];

    let classification = classify_backup_snapshots(&candidates, &[ebs_record(SNAP_A)], &args);

    assert_eq!(classification.recovery_points_to_delete.len(), 1);
    assert_eq!(classification.recovery_points_to_delete[0].snapshot_id, SNAP_A);
    assert_eq!(
        classification.direct_delete.iter().map(|s| s.snapshot_id.as_str()).collect::<Vec<_>>(),
        vec![SNAP_B],
        "only the snapshot without a recovery point may go through EC2 DeleteSnapshot"
    );
    assert!(classification.blocked.is_empty());
}

#[test]
fn backup_delete_tag_is_read_from_the_snapshot() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let candidates = vec![make_snapshot(SNAP_A, old, &[("cleanup", "true")])];

    let classification = classify_backup_snapshots(&candidates, &[ebs_record(SNAP_A)], &args);

    assert_eq!(classification.tag_filtered, 1);
    assert!(classification.recovery_points_to_delete.is_empty());
    assert!(
        classification.direct_delete.is_empty(),
        "a backup-managed snapshot never goes direct"
    );
    assert_eq!(classification.backup_managed.len(), 1);
}

#[test]
fn undeletable_recovery_points_block_their_snapshot() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let snapshot = make_snapshot(SNAP_A, old, &[("cleanup", "true"), ("backup-cleanup", "true")]);

    let mut creating = ebs_record(SNAP_A);
    creating.status = Some("CREATING".to_string());
    let mut locked = ebs_record(SNAP_A);
    locked.vault_locked = true;
    let mut air_gapped = ebs_record(SNAP_A);
    air_gapped.vault_type = Some("LOGICALLY_AIR_GAPPED_BACKUP_VAULT".to_string());
    let mut composite_child = ebs_record(SNAP_A);
    composite_child.parent_recovery_point_arn = Some("arn:aws:backup:parent".to_string());

    for (record, expected) in [
        (creating, "status is CREATING"),
        (locked, "Vault Lock"),
        (air_gapped, "LOGICALLY_AIR_GAPPED_BACKUP_VAULT"),
        (composite_child, "composite"),
    ] {
        let classification =
            classify_backup_snapshots(std::slice::from_ref(&snapshot), &[record], &args);
        assert!(classification.recovery_points_to_delete.is_empty(), "{expected}");
        assert!(classification.direct_delete.is_empty(), "{expected}");
        assert_eq!(classification.blocked.len(), 1, "{expected}");
        assert!(
            classification.blocked[0].reason.contains(expected),
            "{}",
            classification.blocked[0].reason
        );
        assert!(!classification.blocked[0].uncertain);
    }

    let duplicate =
        classify_backup_snapshots(&[snapshot], &[ebs_record(SNAP_A), ebs_record(SNAP_A)], &args);
    assert_eq!(duplicate.blocked.len(), 1);
}

#[test]
fn unmappable_recovery_points_block_every_candidate() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let candidates = vec![make_snapshot(SNAP_A, old, &[("cleanup", "true")])];
    let mut unmappable = ebs_record(SNAP_B);
    unmappable.recovery_point_arn =
        Some("arn:aws:backup:us-east-1:123:recovery-point:abc".to_string());

    let classification = classify_backup_snapshots(&candidates, &[unmappable], &args);

    assert!(classification.direct_delete.is_empty());
    assert_eq!(classification.blocked.len(), 1);
    assert!(classification.blocked[0].uncertain);
}

#[test]
fn backup_looking_snapshot_without_recovery_point_is_blocked() {
    let args = test_args();
    let old = Utc::now() - Duration::days(90);
    let mut snapshot = make_snapshot(SNAP_A, old, &[("cleanup", "true")]);
    snapshot.description = Some("This snapshot is created by the AWS Backup service.".to_string());

    let classification = classify_backup_snapshots(&[snapshot], &[], &args);

    assert!(classification.direct_delete.is_empty());
    assert!(classification.blocked[0].uncertain);

    let mut plan = RegionPlan::default();
    plan.snapshots_released_by_ami.insert(SNAP_A.to_string(), AMI_A.to_string());
    apply_backup_classification(&mut plan, classification);
    assert_eq!(plan.blocked.len(), 1, "uncertain blocks feed exit code 3");
    assert!(plan.snapshots_released_by_ami.is_empty(), "blocked snapshots aren't released");
}

#[test]
fn backup_inventory_failure_blocks_every_snapshot_deletion() {
    let old = Utc::now() - Duration::days(90);
    let mut plan = RegionPlan {
        delete_snapshots: vec![make_snapshot(SNAP_A, old, &[]), make_snapshot(SNAP_B, old, &[])],
        snapshots_released_by_ami: HashMap::from([(SNAP_A.to_string(), AMI_A.to_string())]),
        ..RegionPlan::default()
    };

    block_all_snapshot_deletions(&mut plan, "AWS Backup inventory unavailable: AccessDenied");

    assert!(plan.delete_snapshots.is_empty());
    assert_eq!(plan.blocked_backup_snapshots.len(), 2);
    assert_eq!(plan.blocked.len(), 1);
    assert!(plan.snapshots_released_by_ami.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Errors, retries and dry-run (#10, #11, #14) against mocked AWS clients
// ---------------------------------------------------------------------------------------------

fn no_sdk_retries(builder: ec2::config::Builder) -> ec2::config::Builder {
    builder.retry_config(ec2::config::retry::RetryConfig::disabled())
}

#[tokio::test]
async fn dry_run_operation_counts_as_success() {
    let rule = mock!(ec2::Client::delete_snapshot).then_error(|| {
        ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
            "DryRunOperation",
            "Request would have succeeded, but DryRun flag is set.",
        ))
    });
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let result = send_with_retries(policy(2), true, || {
        client.delete_snapshot().snapshot_id(SNAP_A).dry_run(true).send()
    })
    .await;

    assert_eq!(result, Ok(()));
    assert_eq!(rule.num_calls(), 1);
}

#[tokio::test]
async fn dry_run_code_is_a_failure_outside_dry_run_and_errors_keep_their_code() {
    let rule = mock!(ec2::Client::delete_snapshot).then_error(|| {
        ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
            "DryRunOperation",
            "Request would have succeeded",
        ))
    });
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let result =
        send_with_retries(policy(2), false, || client.delete_snapshot().snapshot_id(SNAP_A).send())
            .await;

    assert_eq!(result, Err("DryRunOperation: Request would have succeeded".to_string()));
    assert_eq!(rule.num_calls(), 1, "not a retryable code");
}

#[tokio::test]
async fn throttling_is_retried_until_success() {
    let rule = mock!(ec2::Client::delete_snapshot)
        .sequence()
        .error(|| {
            ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
                "RequestLimitExceeded",
                "Request limit exceeded.",
            ))
        })
        .output(|| ec2::operation::delete_snapshot::DeleteSnapshotOutput::builder().build())
        .build();
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let result =
        send_with_retries(policy(2), false, || client.delete_snapshot().snapshot_id(SNAP_A).send())
            .await;

    assert_eq!(result, Ok(()));
    assert_eq!(rule.num_calls(), 2);
}

#[tokio::test]
async fn permanent_errors_are_not_retried() {
    let rule = mock!(ec2::Client::delete_snapshot).then_error(|| {
        ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
            "InvalidSnapshot.InUse",
            "The snapshot is currently in use by ami-0123456789abcdef0",
        ))
    });
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let result =
        send_with_retries(policy(5), false, || client.delete_snapshot().snapshot_id(SNAP_A).send())
            .await;

    assert!(result.unwrap_err().starts_with("InvalidSnapshot.InUse: "));
    assert_eq!(rule.num_calls(), 1);
}

#[tokio::test]
async fn max_retries_counts_retries_not_attempts() {
    for (max_retries, expected_calls) in [(0, 1), (2, 3)] {
        let rule = mock!(ec2::Client::delete_snapshot).then_error(|| {
            ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
                "RequestLimitExceeded",
                "slow down",
            ))
        });
        let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

        let result = send_with_retries(policy(max_retries), false, || {
            client.delete_snapshot().snapshot_id(SNAP_A).send()
        })
        .await;

        assert!(result.is_err());
        assert_eq!(rule.num_calls(), expected_calls, "max_retries={max_retries}");
    }
}

#[test]
fn retry_classification_and_backoff() {
    assert!(is_retryable_code("RequestLimitExceeded"));
    assert!(is_retryable_code("ThrottlingException"));
    assert!(!is_retryable_code("InvalidSnapshot.InUse"));
    assert!(!is_retryable_code("UnauthorizedOperation"));

    assert_eq!(retry_delay_for_attempt(100, 1), StdDuration::from_millis(100));
    assert_eq!(retry_delay_for_attempt(100, 3), StdDuration::from_millis(400));
    assert_eq!(retry_delay_for_attempt(u64::MAX, 20), StdDuration::from_millis(u64::MAX));
}

#[tokio::test]
async fn inventory_errors_name_the_operation_and_code() {
    let rule = mock!(ec2::Client::describe_images).then_error(|| {
        ec2::operation::describe_images::DescribeImagesError::generic(ec2_error(
            "UnauthorizedOperation",
            "You are not authorized to perform this operation.",
        ))
    });
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let error = list_owned_amis(&client).await.unwrap_err();

    assert_eq!(
        error_chain(error.as_ref()),
        "DescribeImages failed: UnauthorizedOperation: You are not authorized to perform this operation."
    );
}

#[tokio::test]
async fn ami_without_an_id_fails_the_inventory_instead_of_dropping_its_snapshot_refs() {
    // Skipping it would leave its snapshots looking unreferenced, i.e. deletable.
    let nameless = ec2::types::Image::builder()
        .creation_date(old_timestamp())
        .block_device_mappings(
            ec2::types::BlockDeviceMapping::builder()
                .ebs(
                    ec2::types::EbsBlockDevice::builder()
                        .snapshot_id("snap-0123456789abcdef0")
                        .build(),
                )
                .build(),
        )
        .build();
    let rule = images_rule(vec![nameless]);
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&rule], no_sdk_retries);

    let error = list_owned_amis(&client).await.unwrap_err();

    assert!(error_chain(error.as_ref()).contains("without an image id"));
}

// ---------------------------------------------------------------------------------------------
// Reference collection (#9, #13, #15, #19)
// ---------------------------------------------------------------------------------------------

/// Reference clients with only the always-on sources; the optional ones are skipped.
fn reference_clients(asg: autoscaling::Client, ssm: ssm::Client) -> ReferenceClients {
    ReferenceClients { asg, ssm, image_builder: None, cloudformation: None, batch: None, emr: None }
}

fn collector<'a>(
    ec2_client: &'a ec2::Client,
    clients: &'a ReferenceClients,
) -> ReferenceCollector<'a> {
    ReferenceCollector {
        ec2: ec2_client,
        clients,
        ssm_cache: HashMap::new(),
        scan: AmiReferenceScan::default(),
    }
}

#[test]
fn ami_ids_must_be_well_formed() {
    assert!(is_ami_id("ami-12345678"));
    assert!(is_ami_id(AMI_A));
    assert!(!is_ami_id("ami-12345678\n"));
    assert!(!is_ami_id("ami-1234567"));
    assert!(!is_ami_id("ami-12345678,ami-87654321"));
    assert!(!is_ami_id("ami-ABCDEF12"));
}

#[test]
fn extract_ssm_parameter_name_handles_supported_formats() {
    assert_eq!(
        extract_ssm_parameter_name("resolve:ssm:/images/prod/latest"),
        Some("/images/prod/latest".to_string())
    );
    assert_eq!(
        extract_ssm_parameter_name("{{resolve:ssm:/images/prod/latest}}"),
        Some("/images/prod/latest".to_string())
    );
    assert_eq!(extract_ssm_parameter_name("ami-123"), None);
}

#[tokio::test]
async fn ssm_values_must_be_a_single_ami_and_failures_are_reported_once() {
    let good = mock!(ssm::Client::get_parameter)
        .match_requests(|request| request.name() == Some("/good"))
        .then_output(|| {
            ssm::operation::get_parameter::GetParameterOutput::builder()
                .parameter(ssm::types::Parameter::builder().value(AMI_A).build())
                .build()
        });
    let padded = mock!(ssm::Client::get_parameter)
        .match_requests(|request| request.name() == Some("/padded"))
        .then_output(|| {
            ssm::operation::get_parameter::GetParameterOutput::builder()
                .parameter(ssm::types::Parameter::builder().value(format!("{AMI_A}\n")).build())
                .build()
        });
    let ssm_client = mock_client!(aws_sdk_ssm, RuleMode::MatchAny, [&good, &padded]);
    let mut cache = HashMap::new();

    assert!(matches!(
        resolve_image_reference("resolve:ssm:/good", &ssm_client, &mut cache).await,
        ImageReferenceResolution::Ami(ami) if ami == AMI_A
    ));

    let first = resolve_image_reference("resolve:ssm:/padded", &ssm_client, &mut cache).await;
    let second = resolve_image_reference("{{resolve:ssm:/padded}}", &ssm_client, &mut cache).await;
    let (ImageReferenceResolution::Unresolved(first), ImageReferenceResolution::Unresolved(second)) =
        (first, second)
    else {
        panic!("padded value must not resolve");
    };
    assert_eq!(first, second, "same parameter, same reason, counted once");
    assert_eq!(padded.num_calls(), 1, "result is cached");

    assert!(matches!(
        resolve_image_reference("ami-12345678\n", &ssm_client, &mut cache).await,
        ImageReferenceResolution::Unresolved(_)
    ));
}

#[tokio::test]
async fn missing_launch_template_is_ignored_but_other_errors_are_unresolved() {
    let not_found = mock!(ec2::Client::describe_launch_template_versions)
        .match_requests(|request| request.launch_template_id() == Some("lt-gone"))
        .then_error(|| {
            ec2::operation::describe_launch_template_versions::DescribeLaunchTemplateVersionsError::generic(
                ec2_error("InvalidLaunchTemplateId.NotFound", "The specified launch template does not exist."),
            )
        });
    let denied = mock!(ec2::Client::describe_launch_template_versions)
        .match_requests(|request| request.launch_template_id() == Some("lt-denied"))
        .then_error(|| {
            ec2::operation::describe_launch_template_versions::DescribeLaunchTemplateVersionsError::generic(
                ec2_error("UnauthorizedOperation", "denied"),
            )
        });
    let ec2_client =
        mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&not_found, &denied], no_sdk_retries);
    let asg_client = mock_client!(
        aws_sdk_autoscaling,
        RuleMode::MatchAny,
        Vec::<&aws_smithy_mocks::Rule>::new()
    );
    let ssm_client =
        mock_client!(aws_sdk_ssm, RuleMode::MatchAny, Vec::<&aws_smithy_mocks::Rule>::new());
    let clients = reference_clients(asg_client, ssm_client);
    let mut collector = collector(&ec2_client, &clients);

    let template = |id: &str| LaunchTemplateRef {
        launch_template_id: Some(id.to_string()),
        launch_template_name: None,
        version: Some("$Latest".to_string()),
    };

    collector.collect_launch_template_ref(Some("asg-a"), &template("lt-gone")).await;
    assert!(collector.scan.unresolved_references.is_empty());

    collector.collect_launch_template_ref(Some("asg-b"), &template("lt-denied")).await;
    assert_eq!(collector.scan.unresolved_references.len(), 1);
    let reason = collector.scan.unresolved_references.iter().next().unwrap();
    assert!(
        reason.contains("lt-denied") && reason.contains("UnauthorizedOperation"),
        "{reason}"
    );
}

#[tokio::test]
async fn fleets_and_spot_requests_reference_their_amis() {
    use ec2::types::{
        BatchState, FleetData, FleetLaunchTemplateConfig, FleetLaunchTemplateOverrides,
        FleetStateCode, LaunchSpecification, SpotFleetLaunchSpecification, SpotFleetRequestConfig,
        SpotFleetRequestConfigData, SpotInstanceRequest, SpotInstanceState,
    };

    let fleets = mock!(ec2::Client::describe_fleets).then_output(|| {
        let fleet = |state: FleetStateCode, image: &str| {
            FleetData::builder()
                .fleet_state(state)
                .launch_template_configs(
                    FleetLaunchTemplateConfig::builder()
                        .overrides(FleetLaunchTemplateOverrides::builder().image_id(image).build())
                        .build(),
                )
                .build()
        };
        ec2::operation::describe_fleets::DescribeFleetsOutput::builder()
            .fleets(fleet(FleetStateCode::Active, AMI_A))
            .fleets(fleet(FleetStateCode::Deleted, "ami-0000000000000000d"))
            .build()
    });
    let spot_fleets = mock!(ec2::Client::describe_spot_fleet_requests).then_output(|| {
        ec2::operation::describe_spot_fleet_requests::DescribeSpotFleetRequestsOutput::builder()
            .spot_fleet_request_configs(
                SpotFleetRequestConfig::builder()
                    .spot_fleet_request_state(BatchState::Active)
                    .spot_fleet_request_config(
                        SpotFleetRequestConfigData::builder()
                            .launch_specifications(
                                SpotFleetLaunchSpecification::builder().image_id(AMI_B).build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .build()
    });
    let spot_requests = mock!(ec2::Client::describe_spot_instance_requests).then_output(|| {
        ec2::operation::describe_spot_instance_requests::DescribeSpotInstanceRequestsOutput::builder()
            .spot_instance_requests(
                SpotInstanceRequest::builder()
                    .state(SpotInstanceState::Open)
                    .launch_specification(
                        LaunchSpecification::builder().image_id("ami-0123456789abcdef2").build(),
                    )
                    .build(),
            )
            .spot_instance_requests(
                SpotInstanceRequest::builder()
                    .state(SpotInstanceState::Closed)
                    .launch_specification(
                        LaunchSpecification::builder().image_id("ami-0000000000000000c").build(),
                    )
                    .build(),
            )
            .build()
    });
    let ec2_client = mock_client!(
        aws_sdk_ec2,
        RuleMode::MatchAny,
        [&fleets, &spot_fleets, &spot_requests],
        no_sdk_retries
    );
    let asg_client = mock_client!(
        aws_sdk_autoscaling,
        RuleMode::MatchAny,
        Vec::<&aws_smithy_mocks::Rule>::new()
    );
    let ssm_client =
        mock_client!(aws_sdk_ssm, RuleMode::MatchAny, Vec::<&aws_smithy_mocks::Rule>::new());
    let clients = reference_clients(asg_client, ssm_client);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_ec2_fleets().await.unwrap();
    collector.collect_spot_fleets().await.unwrap();
    collector.collect_spot_instance_requests().await.unwrap();

    assert_eq!(
        collector.scan.referenced_ami_ids,
        HashSet::from([AMI_A.to_string(), AMI_B.to_string(), "ami-0123456789abcdef2".to_string()]),
        "only fleets/requests that can still launch count"
    );
    assert!(collector.scan.unresolved_references.is_empty());
}

#[tokio::test]
async fn launch_permissions_mark_an_ami_as_shared() {
    let shared = mock!(ec2::Client::describe_image_attribute)
        .match_requests(|request| request.image_id() == Some(AMI_A))
        .then_output(|| {
            ec2::operation::describe_image_attribute::DescribeImageAttributeOutput::builder()
                .launch_permissions(
                    ec2::types::LaunchPermission::builder().user_id("210987654321").build(),
                )
                .build()
        });
    let private = mock!(ec2::Client::describe_image_attribute)
        .match_requests(|request| request.image_id() == Some(AMI_B))
        .then_output(|| {
            ec2::operation::describe_image_attribute::DescribeImageAttributeOutput::builder()
                .build()
        });
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&shared, &private], no_sdk_retries);

    assert!(ami_has_external_launch_permissions(&client, AMI_A).await.unwrap());
    assert!(!ami_has_external_launch_permissions(&client, AMI_B).await.unwrap());
}

// ---------------------------------------------------------------------------------------------
// Execution against mocked AWS (#3, #5, #8, #10, #15)
// ---------------------------------------------------------------------------------------------

fn old_timestamp() -> &'static str {
    "2020-01-01T00:00:00.000Z"
}

fn image(ami_id: &str, tag_pairs: &[(&str, &str)], snapshot_ids: &[&str]) -> ec2::types::Image {
    let mut builder = ec2::types::Image::builder().image_id(ami_id).creation_date(old_timestamp());
    for (key, value) in tag_pairs {
        builder = builder.tags(ec2::types::Tag::builder().key(*key).value(*value).build());
    }
    for snapshot_id in snapshot_ids {
        builder = builder.block_device_mappings(
            ec2::types::BlockDeviceMapping::builder()
                .ebs(ec2::types::EbsBlockDevice::builder().snapshot_id(*snapshot_id).build())
                .build(),
        );
    }
    builder.build()
}

fn ec2_snapshot(snapshot_id: &str, tag_pairs: &[(&str, &str)]) -> ec2::types::Snapshot {
    let mut builder = ec2::types::Snapshot::builder()
        .snapshot_id(snapshot_id)
        .start_time(ec2::primitives::DateTime::from_secs(1_577_836_800));
    for (key, value) in tag_pairs {
        builder = builder.tags(ec2::types::Tag::builder().key(*key).value(*value).build());
    }
    builder.build()
}

/// Rules that make the execution-time reference scan come back empty.
fn empty_reference_scan_rules() -> Vec<aws_smithy_mocks::Rule> {
    vec![
        mock!(ec2::Client::describe_instances)
            .then_output(|| ec2::operation::describe_instances::DescribeInstancesOutput::builder().build()),
        mock!(ec2::Client::describe_launch_templates).then_output(|| {
            ec2::operation::describe_launch_templates::DescribeLaunchTemplatesOutput::builder().build()
        }),
        mock!(ec2::Client::describe_fleets)
            .then_output(|| ec2::operation::describe_fleets::DescribeFleetsOutput::builder().build()),
        mock!(ec2::Client::describe_spot_fleet_requests).then_output(|| {
            ec2::operation::describe_spot_fleet_requests::DescribeSpotFleetRequestsOutput::builder().build()
        }),
        mock!(ec2::Client::describe_spot_instance_requests).then_output(|| {
            ec2::operation::describe_spot_instance_requests::DescribeSpotInstanceRequestsOutput::builder()
                .build()
        }),
        mock!(ec2::Client::describe_image_attribute).then_output(|| {
            ec2::operation::describe_image_attribute::DescribeImageAttributeOutput::builder().build()
        }),
        not_shared_snapshot_rule(),
    ]
}

/// Snapshot has no create-volume permissions (not shared). In `MatchAny` mode the first matching
/// rule wins, so a test overriding this must put its own rule earlier.
fn not_shared_snapshot_rule() -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::describe_snapshot_attribute).then_output(|| {
        ec2::operation::describe_snapshot_attribute::DescribeSnapshotAttributeOutput::builder()
            .build()
    })
}

fn shared_snapshot_rule(snapshot_id: &'static str) -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::describe_snapshot_attribute)
        .match_requests(move |request| request.snapshot_id() == Some(snapshot_id))
        .then_output(|| {
            ec2::operation::describe_snapshot_attribute::DescribeSnapshotAttributeOutput::builder()
                .create_volume_permissions(
                    // e.g. shared from the build account to a consumer account
                    ec2::types::CreateVolumePermission::builder().user_id("210987654321").build(),
                )
                .build()
        })
}

fn snapshot_permission_error_rule() -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::describe_snapshot_attribute).then_error(|| {
        ec2::operation::describe_snapshot_attribute::DescribeSnapshotAttributeError::generic(
            ec2_error("UnauthorizedOperation", "no"),
        )
    })
}

fn empty_asg_client() -> autoscaling::Client {
    let groups = mock!(autoscaling::Client::describe_auto_scaling_groups).then_output(|| {
        autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput::builder()
            .build()
    });
    let launch_configurations = mock!(autoscaling::Client::describe_launch_configurations).then_output(|| {
        autoscaling::operation::describe_launch_configurations::DescribeLaunchConfigurationsOutput::builder()
            .build()
    });
    mock_client!(aws_sdk_autoscaling, RuleMode::MatchAny, [&groups, &launch_configurations])
}

fn images_rule(images: Vec<ec2::types::Image>) -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::describe_images).then_output(move || {
        ec2::operation::describe_images::DescribeImagesOutput::builder()
            .set_images(Some(images.clone()))
            .build()
    })
}

fn snapshots_rule(snapshots: Vec<ec2::types::Snapshot>) -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::describe_snapshots).then_output(move || {
        ec2::operation::describe_snapshots::DescribeSnapshotsOutput::builder()
            .set_snapshots(Some(snapshots.clone()))
            .build()
    })
}

fn deregister_dry_run_rule() -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::deregister_image)
        // The AMI's snapshots go through the snapshot phase instead (bug #5).
        .match_requests(|request| request.delete_associated_snapshots().is_none() && request.dry_run() == Some(true))
        .then_error(|| {
            ec2::operation::deregister_image::DeregisterImageError::generic(ec2_error(
                "DryRunOperation",
                "Request would have succeeded",
            ))
        })
}

fn delete_snapshot_dry_run_rule() -> aws_smithy_mocks::Rule {
    mock!(ec2::Client::delete_snapshot)
        .match_requests(|request| request.dry_run() == Some(true))
        .then_error(|| {
            ec2::operation::delete_snapshot::DeleteSnapshotError::generic(ec2_error(
                "DryRunOperation",
                "Request would have succeeded",
            ))
        })
}

fn ec2_with(
    mut rules: Vec<aws_smithy_mocks::Rule>,
    extra: &[&aws_smithy_mocks::Rule],
) -> ec2::Client {
    rules.extend(extra.iter().map(|rule| (*rule).clone()));
    mock_client!(aws_sdk_ec2, RuleMode::MatchAny, rules.iter(), no_sdk_retries)
}

fn no_backup_client() -> backup::Client {
    mock_client!(aws_sdk_backup, RuleMode::MatchAny, Vec::<&aws_smithy_mocks::Rule>::new())
}

fn no_ssm_client() -> ssm::Client {
    mock_client!(aws_sdk_ssm, RuleMode::MatchAny, Vec::<&aws_smithy_mocks::Rule>::new())
}

#[tokio::test]
async fn dry_run_execution_deregisters_ami_then_frees_its_snapshot() {
    let mut args = test_args();
    args.execute = true;
    args.dry_run = true;
    let old = Utc::now() - Duration::days(365);

    let plan = plan(
        &args,
        vec![make_ami(AMI_A, old, &[("cleanup", "true")], &[SNAP_A])],
        vec![make_snapshot(SNAP_A, old, &[])],
        &scan(&[], &[]),
    );
    assert_eq!(plan.delete_snapshots.len(), 1);

    let images = images_rule(vec![image(AMI_A, &[("cleanup", "true")], &[SNAP_A])]);
    let snapshots = snapshots_rule(vec![ec2_snapshot(SNAP_A, &[])]);
    let deregister = deregister_dry_run_rule();
    let delete_snapshot = delete_snapshot_dry_run_rule();
    let ec2_client = ec2_with(
        empty_reference_scan_rules(),
        &[&images, &snapshots, &deregister, &delete_snapshot],
    );

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!((summary.deregistered_amis, summary.deleted_snapshots), (1, 1));
    assert_eq!((deregister.num_calls(), delete_snapshot.num_calls()), (1, 1));
}

#[tokio::test]
async fn execution_recomputes_keep_latest_from_live_inventory() {
    let mut args = test_args();
    args.execute = true;
    args.dry_run = true;
    args.keep_latest_per_family = 1;
    let old = Utc::now() - Duration::days(365);
    let family = [("cleanup", "true"), ("ImageFamily", "app")];

    let plan = plan(
        &args,
        vec![
            make_ami(AMI_A, old - Duration::days(1), &family, &[]),
            make_ami(AMI_B, old, &family, &[]),
        ],
        Vec::new(),
        &scan(&[], &[]),
    );
    assert_eq!(plan.deregister_amis.len(), 1, "AMI_A is the older one");

    // The newer AMI disappeared after planning, so AMI_A is now the family's latest and must stay.
    let images = images_rule(vec![image(AMI_A, &family, &[])]);
    let deregister = deregister_dry_run_rule();
    let ec2_client = ec2_with(empty_reference_scan_rules(), &[&images, &deregister]);

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert_eq!(summary.skipped_amis, 1);
    assert_eq!(deregister.num_calls(), 0);
}

#[tokio::test]
async fn every_planned_item_is_counted_when_live_inventory_is_empty() {
    let mut args = test_args();
    args.execute = true;
    let old = Utc::now() - Duration::days(365);
    let plan = plan(
        &args,
        vec![
            make_ami(AMI_A, old, &[("cleanup", "true")], &[]),
            make_ami(AMI_B, old, &[("cleanup", "true")], &[]),
        ],
        Vec::new(),
        &scan(&[], &[]),
    );

    let images = images_rule(Vec::new());
    let ec2_client = ec2_with(empty_reference_scan_rules(), &[&images]);

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert_eq!(summary.skipped_amis, 2);
    assert!(summary.failures.is_empty());
}

#[tokio::test]
async fn failed_inventory_refresh_blocks_and_reports_the_error_code() {
    let mut args = test_args();
    args.execute = true;
    let old = Utc::now() - Duration::days(365);
    let plan = plan(
        &args,
        vec![make_ami(AMI_A, old, &[("cleanup", "true")], &[])],
        Vec::new(),
        &scan(&[], &[]),
    );

    let images = mock!(ec2::Client::describe_images).then_error(|| {
        ec2::operation::describe_images::DescribeImagesError::generic(ec2_error(
            "UnauthorizedOperation",
            "no",
        ))
    });
    let ec2_client = ec2_with(empty_reference_scan_rules(), &[&images]);

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert_eq!(summary.skipped_amis, 1);
    assert_eq!(summary.failures.len(), 1);
    assert!(summary.failures[0].contains("UnauthorizedOperation"), "{}", summary.failures[0]);
}

fn recovery_point_plan(args: &Args) -> RegionPlan {
    let old = Utc::now() - Duration::days(365);
    let mut plan = RegionPlan { region: "us-east-1".to_string(), ..RegionPlan::default() };
    let snapshot = make_snapshot(SNAP_A, old, &[("cleanup", "true"), ("backup-cleanup", "true")]);
    let classification = classify_backup_snapshots(&[snapshot], &[ebs_record(SNAP_A)], args);
    apply_backup_classification(&mut plan, classification);
    assert_eq!(plan.delete_backup_recovery_points.len(), 1);
    plan
}

async fn execute_recovery_point_plan(
    live_snapshot_tags: &[(&str, &str)],
) -> (ExecutionSummary, usize) {
    let mut args = test_args();
    args.execute = true;
    args.execute_backup_recovery_point_deletes = true;
    let plan = recovery_point_plan(&args);

    let images = images_rule(Vec::new());
    let snapshots = snapshots_rule(vec![ec2_snapshot(SNAP_A, live_snapshot_tags)]);
    let ec2_client = ec2_with(empty_reference_scan_rules(), &[&images, &snapshots]);
    let delete_recovery_point = mock!(backup::Client::delete_recovery_point).then_output(|| {
        backup::operation::delete_recovery_point::DeleteRecoveryPointOutput::builder().build()
    });
    let backup_client = mock_client!(aws_sdk_backup, RuleMode::MatchAny, [&delete_recovery_point]);

    let summary = execute_region_plan(
        &ec2_client,
        &backup_client,
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;
    (summary, delete_recovery_point.num_calls())
}

#[tokio::test]
async fn recovery_point_is_deleted_when_snapshot_still_qualifies() {
    let (summary, calls) =
        execute_recovery_point_plan(&[("cleanup", "true"), ("backup-cleanup", "true")]).await;

    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!(summary.deleted_backup_recovery_points, 1);
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn recovery_point_deletion_rechecks_the_live_snapshot() {
    for live_tags in [
        // Protected after planning.
        vec![("cleanup", "true"), ("backup-cleanup", "true"), ("DoNotDelete", "true")],
        // Backup delete tag removed after planning.
        vec![("cleanup", "true")],
        // Delete tag removed after planning.
        vec![("backup-cleanup", "true")],
    ] {
        let (summary, calls) = execute_recovery_point_plan(&live_tags).await;
        assert_eq!(summary.skipped_backup_recovery_points, 1, "{live_tags:?}");
        assert_eq!(calls, 0, "{live_tags:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Cross-account sharing: build account shares AMIs/snapshots to consumer accounts
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn create_volume_permissions_mark_a_snapshot_as_shared() {
    let shared = shared_snapshot_rule(SNAP_A);
    let private = not_shared_snapshot_rule();
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&shared, &private], no_sdk_retries);

    let external_use = lookup_snapshot_external_use(
        &client,
        &[make_snapshot(SNAP_A, Utc::now(), &[]), make_snapshot(SNAP_B, Utc::now(), &[])],
    )
    .await;

    assert_eq!(external_use, HashMap::from([(SNAP_A.to_string(), ExternalUse::Shared)]));

    let failing = snapshot_permission_error_rule();
    let client = mock_client!(aws_sdk_ec2, RuleMode::MatchAny, [&failing], no_sdk_retries);
    let external_use =
        lookup_snapshot_external_use(&client, &[make_snapshot(SNAP_A, Utc::now(), &[])]).await;
    assert!(
        matches!(external_use.get(SNAP_A), Some(ExternalUse::Unknown(reason)) if reason.contains("UnauthorizedOperation"))
    );
}

#[test]
fn shared_snapshots_are_kept_and_failed_lookups_block() {
    let args = test_args();
    let old = Utc::now() - Duration::days(365);
    let mut plan = plan(
        &args,
        vec![make_ami(AMI_A, old, &[("cleanup", "true")], &[SNAP_A])],
        vec![
            make_snapshot(SNAP_A, old, &[]),
            make_snapshot(SNAP_B, old, &[("cleanup", "true")]),
            make_snapshot(SNAP_C, old, &[("cleanup", "true")]),
        ],
        &scan(&[], &[]),
    );
    assert_eq!(plan.delete_snapshots.len(), 3);
    assert!(plan.snapshots_released_by_ami.contains_key(SNAP_A));
    let skipped_before = plan.skipped_snapshots;

    apply_snapshot_external_use(
        &mut plan,
        &HashMap::from([
            (SNAP_A.to_string(), ExternalUse::Shared),
            (SNAP_B.to_string(), ExternalUse::Unknown("AccessDenied".to_string())),
        ]),
    );

    let planned = plan.delete_snapshots.iter().map(|s| s.snapshot_id.as_str()).collect::<Vec<_>>();
    assert_eq!(planned, vec![SNAP_C]);
    assert_eq!(plan.skipped_snapshots, skipped_before + 2);
    assert_eq!(plan.kept_shared_snapshots, 1);
    assert_eq!(plan.blocked.len(), 1, "only the failed lookup is a safety block");
    assert!(plan.blocked[0].contains(SNAP_B));
    assert!(plan.snapshots_released_by_ami.is_empty(), "pruned with the shared snapshot");
}

async fn execute_single_snapshot_delete(
    first_rules: Vec<aws_smithy_mocks::Rule>,
) -> (ExecutionSummary, usize) {
    let mut args = test_args();
    args.execute = true;
    args.dry_run = true;
    let old = Utc::now() - Duration::days(365);
    let plan = plan(
        &args,
        Vec::new(),
        vec![make_snapshot(SNAP_A, old, &[("cleanup", "true")])],
        &scan(&[], &[]),
    );
    assert_eq!(plan.delete_snapshots.len(), 1);

    let mut rules = first_rules;
    rules.extend(empty_reference_scan_rules());
    let images = images_rule(Vec::new());
    let snapshots = snapshots_rule(vec![ec2_snapshot(SNAP_A, &[("cleanup", "true")])]);
    let delete_snapshot = delete_snapshot_dry_run_rule();
    let ec2_client = ec2_with(rules, &[&images, &snapshots, &delete_snapshot]);

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;
    (summary, delete_snapshot.num_calls())
}

#[tokio::test]
async fn snapshot_shared_after_planning_is_not_deleted() {
    let (summary, calls) = execute_single_snapshot_delete(vec![shared_snapshot_rule(SNAP_A)]).await;

    assert_eq!(calls, 0);
    assert_eq!(summary.skipped_snapshots, 1);
    assert!(summary.failures.is_empty() && summary.blocked.is_empty());
}

#[tokio::test]
async fn failed_snapshot_permission_recheck_blocks_instead_of_failing() {
    let (summary, calls) =
        execute_single_snapshot_delete(vec![snapshot_permission_error_rule()]).await;

    assert_eq!(calls, 0);
    assert_eq!(summary.skipped_snapshots, 1);
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!(summary.blocked.len(), 1);
    assert!(summary.blocked[0].contains("UnauthorizedOperation"), "{}", summary.blocked[0]);
}

#[tokio::test]
async fn recovery_point_for_a_shared_snapshot_is_not_deleted() {
    let mut args = test_args();
    args.execute = true;
    args.execute_backup_recovery_point_deletes = true;
    let plan = recovery_point_plan(&args);

    let mut rules = vec![shared_snapshot_rule(SNAP_A)];
    rules.extend(empty_reference_scan_rules());
    let images = images_rule(Vec::new());
    let snapshots = snapshots_rule(vec![ec2_snapshot(
        SNAP_A,
        &[("cleanup", "true"), ("backup-cleanup", "true")],
    )]);
    let ec2_client = ec2_with(rules, &[&images, &snapshots]);
    let delete_recovery_point = mock!(backup::Client::delete_recovery_point).then_output(|| {
        backup::operation::delete_recovery_point::DeleteRecoveryPointOutput::builder().build()
    });
    let backup_client = mock_client!(aws_sdk_backup, RuleMode::MatchAny, [&delete_recovery_point]);

    let summary = execute_region_plan(
        &ec2_client,
        &backup_client,
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert_eq!(delete_recovery_point.num_calls(), 0);
    assert_eq!(summary.skipped_backup_recovery_points, 1);
}

#[tokio::test]
async fn execution_time_safety_blocks_are_blocks_not_failures() {
    let mut args = test_args();
    args.execute = true;
    args.dry_run = true;
    let old = Utc::now() - Duration::days(365);
    let plan = plan(
        &args,
        vec![make_ami(AMI_A, old, &[("cleanup", "true")], &[])],
        Vec::new(),
        &scan(&[], &[]),
    );

    // A launch template that now points at something that isn't an AMI id.
    let templates = mock!(ec2::Client::describe_launch_templates).then_output(|| {
        ec2::operation::describe_launch_templates::DescribeLaunchTemplatesOutput::builder()
            .launch_templates(
                ec2::types::LaunchTemplate::builder()
                    .launch_template_id("lt-0123456789abcdef0")
                    .build(),
            )
            .build()
    });
    let versions = mock!(ec2::Client::describe_launch_template_versions).then_output(|| {
        ec2::operation::describe_launch_template_versions::DescribeLaunchTemplateVersionsOutput::builder()
            .launch_template_versions(
                ec2::types::LaunchTemplateVersion::builder()
                    .launch_template_data(
                        ec2::types::ResponseLaunchTemplateData::builder().image_id("not-an-ami").build(),
                    )
                    .build(),
            )
            .build()
    });
    let mut rules = vec![templates, versions];
    rules.extend(empty_reference_scan_rules());
    let deregister = deregister_dry_run_rule();
    let ec2_client = ec2_with(rules, &[&deregister]);

    let summary = execute_region_plan(
        &ec2_client,
        &no_backup_client(),
        &reference_clients(empty_asg_client(), no_ssm_client()),
        &plan,
        &args,
    )
    .await;

    assert_eq!(deregister.num_calls(), 0);
    assert_eq!(summary.skipped_amis, 1);
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!(summary.blocked.len(), 1);
}

// ---------------------------------------------------------------------------------------------
// Exit codes (#12)
// ---------------------------------------------------------------------------------------------

#[test]
fn exit_codes_distinguish_failures_from_safety_blocks() {
    let mut outcome = RunOutcome::default();
    assert_eq!(outcome.exit_code(), ExitCode::SUCCESS);

    outcome.blocked_regions.push("us-east-1".to_string());
    assert_eq!(outcome.exit_code(), ExitCode::from(EXIT_BLOCKED));

    outcome.failed_regions.push("us-east-1".to_string());
    assert_eq!(outcome.exit_code(), ExitCode::FAILURE);
}

// ---------------------------------------------------------------------------------------------
// Extra reference sources: Image Builder, CloudFormation, Batch, EMR (#19)
// ---------------------------------------------------------------------------------------------

fn referenced(collector: &ReferenceCollector<'_>) -> Vec<String> {
    let mut ids = collector.scan.referenced_ami_ids.iter().cloned().collect::<Vec<String>>();
    ids.sort_unstable();
    ids
}

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(ToString::to_string).collect()
}

fn empty_ec2() -> ec2::Client {
    mock_client!(
        aws_sdk_ec2,
        RuleMode::MatchAny,
        Vec::<&aws_smithy_mocks::Rule>::new(),
        no_sdk_retries
    )
}

#[test]
fn reference_sources_are_on_by_default_and_can_be_skipped() {
    let config = SdkConfig::builder().behavior_version(BehaviorVersion::latest()).build();

    let all = ReferenceClients::new(&config, &test_args().skip_reference_sources);
    assert!(all.image_builder.is_some() && all.cloudformation.is_some());
    assert!(all.batch.is_some() && all.emr.is_some());

    let args =
        Args::try_parse_from(["amicleaner", "--skip-reference-sources", "image-builder,emr"])
            .unwrap();
    let some = ReferenceClients::new(&config, &args.skip_reference_sources);
    assert!(some.image_builder.is_none() && some.emr.is_none());
    assert!(some.cloudformation.is_some() && some.batch.is_some());

    assert!(Args::try_parse_from(["amicleaner", "--skip-reference-sources", "ec2"]).is_err());
}

#[test]
fn image_builder_parent_images_are_classified() {
    let own = |version: &str, build: Option<&str>| {
        ParentImage::OwnImage(OwnImageBuilderImage {
            name: "base".to_string(),
            version: version.to_string(),
            build: build.map(ToString::to_string),
        })
    };
    let arn =
        |owner: &str, rest: &str| format!("arn:aws:imagebuilder:us-east-1:{owner}:image/{rest}");

    assert_eq!(
        classify_parent_image(AMI_A, Some(ACCOUNT)),
        ParentImage::Reference(AMI_A.to_string())
    );
    assert_eq!(
        classify_parent_image("ssm:/golden/ami", Some(ACCOUNT)),
        ParentImage::Reference("resolve:ssm:/golden/ami".to_string())
    );
    assert_eq!(classify_parent_image("prod-abc123", Some(ACCOUNT)), ParentImage::NotOwned);
    assert_eq!(
        classify_parent_image(&arn("aws", "amazon-linux-2023-x86/x.x.x"), Some(ACCOUNT)),
        ParentImage::NotOwned
    );
    assert_eq!(
        classify_parent_image(&arn("210987654321", "base/1.0.0"), Some(ACCOUNT)),
        ParentImage::NotOwned
    );
    assert_eq!(
        classify_parent_image(&arn(ACCOUNT, "base/x.x.x"), Some(ACCOUNT)),
        own("x.x.x", None)
    );
    assert_eq!(
        classify_parent_image(&arn(ACCOUNT, "base/1.2.3/4"), Some(ACCOUNT)),
        own("1.2.3", Some("4"))
    );
    // Unknown recipe owner: assume it's ours, which keeps more.
    assert_eq!(classify_parent_image(&arn(ACCOUNT, "base/1.2.3"), None), own("1.2.3", None));
    for weird in [
        "ami-123",
        "arn:aws:ec2:us-east-1:123456789012:image/ami-1",
        &arn(ACCOUNT, "base"),
        "latest",
    ] {
        assert!(
            matches!(classify_parent_image(weird, Some(ACCOUNT)), ParentImage::Unresolved(_)),
            "{weird}"
        );
    }
}

#[test]
fn image_versions_match_x_wildcards() {
    assert!(image_version_matches("x.x.x", "1.2.3"));
    assert!(image_version_matches("1.x.x", "1.9.0/3"));
    assert!(image_version_matches("1.2.3", "1.2.3/1"));
    assert!(!image_version_matches("1.x.x", "2.0.0"));
    assert!(!image_version_matches("1.2.3", "1.2.4"));
    assert!(
        image_version_matches("1.2.3", "weird"),
        "unparseable versions match (keeps more)"
    );
}

#[test]
fn ami_ids_and_ssm_references_are_found_in_templates() {
    let template = r#"{"Mappings": {"R": {"us-east-1": {"Ami": "ami-0123456789ABCDEF0"}}},
        "Image": "ami-12345678", "NotAnAmi": "ami-123", "Long": "ami-0123456789abcdef0123",
        "Ssm": "{{resolve:ssm:/amis/app}}", "Versioned": "{{resolve:ssm:/amis/app:3}}",
        "Secret": "{{resolve:ssm-secure:/db/password}}", "Broken": "{{resolve:ssm:"}"#;

    assert_eq!(find_ami_ids(template), ids(&[AMI_A, "ami-12345678"]));
    assert_eq!(find_ssm_dynamic_references(template), ids(&["/amis/app", "/amis/app:3"]));
}

#[tokio::test]
async fn image_builder_recipes_reference_parents_and_builds_of_own_images() {
    let own_parent = format!("arn:aws:imagebuilder:us-east-1:{ACCOUNT}:image/base/1.x.x");
    let recipes = mock!(imagebuilder::Client::list_image_recipes).then_output(move || {
        imagebuilder::operation::list_image_recipes::ListImageRecipesOutput::builder()
            .image_recipe_summary_list(
                imagebuilder::types::ImageRecipeSummary::builder()
                    .owner(ACCOUNT)
                    .parent_image(AMI_A)
                    .build(),
            )
            .image_recipe_summary_list(
                imagebuilder::types::ImageRecipeSummary::builder()
                    .owner(ACCOUNT)
                    .parent_image(own_parent.clone())
                    .build(),
            )
            .image_recipe_summary_list(
                imagebuilder::types::ImageRecipeSummary::builder()
                    .owner(ACCOUNT)
                    .parent_image(
                        "arn:aws:imagebuilder:us-east-1:aws:image/amazon-linux-2023-x86/x.x.x",
                    )
                    .build(),
            )
            .build()
    });
    let versions = mock!(imagebuilder::Client::list_images).then_output(|| {
        let version = |v: &str| {
            imagebuilder::types::ImageVersion::builder()
                .arn(format!("arn:aws:imagebuilder:us-east-1:{ACCOUNT}:image/base/{v}"))
                .version(v)
                .build()
        };
        imagebuilder::operation::list_images::ListImagesOutput::builder()
            .image_version_list(version("1.0.0"))
            .image_version_list(version("2.0.0"))
            .build()
    });
    let builds = mock!(imagebuilder::Client::list_image_build_versions)
        .match_requests(|request| request.image_version_arn().is_some_and(|arn| arn.ends_with("/1.0.0")))
        .then_output(|| {
            imagebuilder::operation::list_image_build_versions::ListImageBuildVersionsOutput::builder()
                .image_summary_list(
                    imagebuilder::types::ImageSummary::builder()
                        .arn(format!("arn:aws:imagebuilder:us-east-1:{ACCOUNT}:image/base/1.0.0/1"))
                        .output_resources(
                            imagebuilder::types::OutputResources::builder()
                                .amis(imagebuilder::types::Ami::builder().region("us-east-1").image(AMI_B).build())
                                .build(),
                        )
                        .build(),
                )
                .build()
        });
    let container_recipes = mock!(imagebuilder::Client::list_container_recipes).then_output(|| {
        imagebuilder::operation::list_container_recipes::ListContainerRecipesOutput::builder()
            .container_recipe_summary_list(
                imagebuilder::types::ContainerRecipeSummary::builder()
                    .arn("arn:aws:imagebuilder:us-east-1:123456789012:container-recipe/app/1.0.0")
                    .owner(ACCOUNT)
                    .build(),
            )
            .build()
    });
    let container_recipe = mock!(imagebuilder::Client::get_container_recipe).then_output(|| {
        imagebuilder::operation::get_container_recipe::GetContainerRecipeOutput::builder()
            .container_recipe(
                imagebuilder::types::ContainerRecipe::builder()
                    .instance_configuration(
                        imagebuilder::types::InstanceConfiguration::builder().image(AMI_C).build(),
                    )
                    .build(),
            )
            .build()
    });
    let image_builder = mock_client!(
        aws_sdk_imagebuilder,
        RuleMode::MatchAny,
        [&recipes, &versions, &builds, &container_recipes, &container_recipe]
    );
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), no_ssm_client());
    clients.image_builder = Some(image_builder);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_image_builder_recipes().await.unwrap();

    assert_eq!(referenced(&collector), ids(&[AMI_A, AMI_B, AMI_C]), "2.0.0 doesn't match 1.x.x");
    assert!(
        collector.scan.unresolved_references.is_empty(),
        "{:?}",
        collector.scan.unresolved_references
    );
    assert_eq!(builds.num_calls(), 1);
}

#[tokio::test]
async fn unreadable_reference_source_fails_the_scan() {
    let denied = mock!(imagebuilder::Client::list_image_recipes).then_error(|| {
        imagebuilder::operation::list_image_recipes::ListImageRecipesError::generic(ec2_error(
            "AccessDeniedException",
            "not authorized",
        ))
    });
    let image_builder = mock_client!(aws_sdk_imagebuilder, RuleMode::MatchAny, [&denied]);
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), no_ssm_client());
    clients.image_builder = Some(image_builder);
    let mut collector = collector(&ec2_client, &clients);

    let error = collector.collect_image_builder_recipes().await.unwrap_err();

    assert!(error_chain(error.as_ref()).contains("ListImageRecipes failed: AccessDeniedException"));
}

fn ssm_parameter_rule(name: &'static str, value: &'static str) -> aws_smithy_mocks::Rule {
    mock!(ssm::Client::get_parameter)
        .match_requests(move |request| request.name() == Some(name))
        .then_output(move || {
            ssm::operation::get_parameter::GetParameterOutput::builder()
                .parameter(ssm::types::Parameter::builder().name(name).value(value).build())
                .build()
        })
}

#[tokio::test]
async fn cloudformation_templates_and_parameters_reference_amis() {
    let stacks = mock!(cloudformation::Client::describe_stacks).then_output(|| {
        cloudformation::operation::describe_stacks::DescribeStacksOutput::builder()
            .stacks(
                cloudformation::types::Stack::builder()
                    .stack_id("arn:aws:cloudformation:us-east-1:123456789012:stack/app/1")
                    .parameters(
                        cloudformation::types::Parameter::builder().parameter_value(AMI_A).build(),
                    )
                    .parameters(
                        cloudformation::types::Parameter::builder()
                            .parameter_value("/amis/base")
                            .resolved_value(AMI_B)
                            .build(),
                    )
                    .build(),
            )
            .build()
    });
    let template = mock!(cloudformation::Client::get_template).then_output(|| {
        cloudformation::operation::get_template::GetTemplateOutput::builder()
            .template_body(format!(
                "Resources:\n  Lt:\n    ImageId: {AMI_C}\n  Other:\n    ImageId: '{{{{resolve:ssm:/amis/app}}}}'\n    VpcId: '{{{{resolve:ssm:/config/vpc}}}}'\n"
            ))
            .build()
    });
    let cloudformation =
        mock_client!(aws_sdk_cloudformation, RuleMode::MatchAny, [&stacks, &template]);
    let app = ssm_parameter_rule("/amis/app", AMI_D);
    let vpc = ssm_parameter_rule("/config/vpc", "vpc-0123456789abcdef0");
    let ssm_client = mock_client!(aws_sdk_ssm, RuleMode::MatchAny, [&app, &vpc]);
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), ssm_client);
    clients.cloudformation = Some(cloudformation);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_cloudformation_stacks().await.unwrap();

    assert_eq!(referenced(&collector), ids(&[AMI_A, AMI_B, AMI_C, AMI_D]));
    assert!(
        collector.scan.unresolved_references.is_empty(),
        "a non-AMI parameter is fine: {:?}",
        collector.scan.unresolved_references
    );
}

#[tokio::test]
async fn cloudformation_ssm_reference_that_fails_to_resolve_is_unresolved() {
    let stacks = mock!(cloudformation::Client::describe_stacks).then_output(|| {
        cloudformation::operation::describe_stacks::DescribeStacksOutput::builder()
            .stacks(cloudformation::types::Stack::builder().stack_id("stack-1").build())
            .build()
    });
    let template = mock!(cloudformation::Client::get_template).then_output(|| {
        cloudformation::operation::get_template::GetTemplateOutput::builder()
            .template_body("ImageId: '{{resolve:ssm:/amis/gone}}'")
            .build()
    });
    let cloudformation =
        mock_client!(aws_sdk_cloudformation, RuleMode::MatchAny, [&stacks, &template]);
    let gone = mock!(ssm::Client::get_parameter).then_error(|| {
        ssm::operation::get_parameter::GetParameterError::generic(ec2_error(
            "AccessDeniedException",
            "no",
        ))
    });
    let ssm_client = mock_client!(aws_sdk_ssm, RuleMode::MatchAny, [&gone]);
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), ssm_client);
    clients.cloudformation = Some(cloudformation);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_cloudformation_stacks().await.unwrap();

    assert_eq!(collector.scan.unresolved_references.len(), 1);
    assert!(
        collector.scan.unresolved_references.iter().all(|reason| reason.contains("/amis/gone"))
    );
}

#[tokio::test]
#[allow(deprecated)] // ComputeResource::image_id is deprecated but still used by older environments.
async fn batch_compute_environments_reference_image_ids_and_overrides() {
    let environments = mock!(batch::Client::describe_compute_environments).then_output(|| {
        batch::operation::describe_compute_environments::DescribeComputeEnvironmentsOutput::builder(
        )
        .compute_environments(
            batch::types::ComputeEnvironmentDetail::builder()
                .compute_environment_name("legacy")
                .compute_environment_arn(
                    "arn:aws:batch:us-east-1:123456789012:compute-environment/legacy",
                )
                .compute_resources(
                    batch::types::ComputeResource::builder()
                        .image_id(AMI_A)
                        .ec2_configuration(
                            batch::types::Ec2Configuration::builder()
                                .image_type("ECS_AL2")
                                .image_id_override(AMI_B)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .build()
    });
    let batch_client = mock_client!(aws_sdk_batch, RuleMode::MatchAny, [&environments]);
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), no_ssm_client());
    clients.batch = Some(batch_client);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_batch_compute_environments().await.unwrap();

    assert_eq!(referenced(&collector), ids(&[AMI_A, AMI_B]));
}

#[tokio::test]
async fn emr_clusters_reference_cluster_group_and_fleet_amis() {
    let clusters = mock!(emr::Client::list_clusters)
        .match_requests(|request| {
            // Only clusters that can still launch or scale; terminated ones can't.
            request.cluster_states()
                == [
                    emr::types::ClusterState::Starting,
                    emr::types::ClusterState::Bootstrapping,
                    emr::types::ClusterState::Running,
                    emr::types::ClusterState::Waiting,
                ]
        })
        .then_output(|| {
            emr::operation::list_clusters::ListClustersOutput::builder()
                .clusters(emr::types::ClusterSummary::builder().id("j-GROUPS").build())
                .clusters(emr::types::ClusterSummary::builder().id("j-FLEETS").build())
                .build()
        });
    let group_cluster = mock!(emr::Client::describe_cluster)
        .match_requests(|request| request.cluster_id() == Some("j-GROUPS"))
        .then_output(|| {
            emr::operation::describe_cluster::DescribeClusterOutput::builder()
                .cluster(
                    emr::types::Cluster::builder()
                        .custom_ami_id(AMI_A)
                        .instance_collection_type(emr::types::InstanceCollectionType::InstanceGroup)
                        .build(),
                )
                .build()
        });
    let fleet_cluster = mock!(emr::Client::describe_cluster)
        .match_requests(|request| request.cluster_id() == Some("j-FLEETS"))
        .then_output(|| {
            emr::operation::describe_cluster::DescribeClusterOutput::builder()
                .cluster(
                    emr::types::Cluster::builder()
                        .instance_collection_type(emr::types::InstanceCollectionType::InstanceFleet)
                        .build(),
                )
                .build()
        });
    let groups = mock!(emr::Client::list_instance_groups).then_output(|| {
        emr::operation::list_instance_groups::ListInstanceGroupsOutput::builder()
            .instance_groups(emr::types::InstanceGroup::builder().custom_ami_id(AMI_B).build())
            .build()
    });
    let fleets = mock!(emr::Client::list_instance_fleets).then_output(|| {
        emr::operation::list_instance_fleets::ListInstanceFleetsOutput::builder()
            .instance_fleets(
                emr::types::InstanceFleet::builder()
                    .instance_type_specifications(
                        emr::types::InstanceTypeSpecification::builder()
                            .custom_ami_id(AMI_C)
                            .build(),
                    )
                    .build(),
            )
            .build()
    });
    let emr_client = mock_client!(
        aws_sdk_emr,
        RuleMode::MatchAny,
        [&clusters, &group_cluster, &fleet_cluster, &groups, &fleets]
    );
    let ec2_client = empty_ec2();
    let mut clients = reference_clients(empty_asg_client(), no_ssm_client());
    clients.emr = Some(emr_client);
    let mut collector = collector(&ec2_client, &clients);

    collector.collect_emr_clusters().await.unwrap();

    assert_eq!(referenced(&collector), ids(&[AMI_A, AMI_B, AMI_C]));
    assert_eq!(
        (groups.num_calls(), fleets.num_calls()),
        (1, 1),
        "each cluster uses its own listing"
    );
}

#[tokio::test]
async fn full_reference_scan_reads_every_enabled_source() {
    fn denied() -> ErrorMetadata {
        ec2_error("AccessDeniedException", "not authorized")
    }
    let image_builder = mock!(imagebuilder::Client::list_image_recipes).then_error(|| {
        imagebuilder::operation::list_image_recipes::ListImageRecipesError::generic(denied())
    });
    let cloudformation = mock!(cloudformation::Client::describe_stacks).then_error(|| {
        cloudformation::operation::describe_stacks::DescribeStacksError::generic(denied())
    });
    let batch = mock!(batch::Client::describe_compute_environments).then_error(|| {
        batch::operation::describe_compute_environments::DescribeComputeEnvironmentsError::generic(
            denied(),
        )
    });
    let emr = mock!(emr::Client::list_clusters)
        .then_error(|| emr::operation::list_clusters::ListClustersError::generic(denied()));
    let ec2_client = ec2_with(empty_reference_scan_rules(), &[]);

    for (source, expected) in [
        ("image-builder", "ListImageRecipes"),
        ("cloudformation", "DescribeStacks"),
        ("batch", "DescribeComputeEnvironments"),
        ("emr", "ListClusters"),
    ] {
        let mut clients = reference_clients(empty_asg_client(), no_ssm_client());
        match source {
            "image-builder" => {
                clients.image_builder =
                    Some(mock_client!(aws_sdk_imagebuilder, RuleMode::MatchAny, [&image_builder]));
            }
            "cloudformation" => {
                clients.cloudformation = Some(mock_client!(
                    aws_sdk_cloudformation,
                    RuleMode::MatchAny,
                    [&cloudformation]
                ));
            }
            "batch" => {
                clients.batch = Some(mock_client!(aws_sdk_batch, RuleMode::MatchAny, [&batch]));
            }
            _ => clients.emr = Some(mock_client!(aws_sdk_emr, RuleMode::MatchAny, [&emr])),
        }

        let error = collect_referenced_ami_ids(&ec2_client, &clients).await.unwrap_err();
        assert!(
            error_chain(error.as_ref()).contains(expected),
            "{source}: {}",
            error_chain(error.as_ref())
        );
    }

    // With every optional source skipped, the scan only reads EC2 and Auto Scaling.
    let clients = reference_clients(empty_asg_client(), no_ssm_client());
    assert!(collect_referenced_ami_ids(&ec2_client, &clients).await.is_ok());
}

#[test]
fn skipping_reference_sources_is_announced() {
    assert_eq!(skipped_reference_sources_warning(&[]), None);

    let warning = skipped_reference_sources_warning(&[
        ReferenceSource::Emr,
        ReferenceSource::ImageBuilder,
        ReferenceSource::Emr,
    ])
    .unwrap();
    assert!(
        warning.starts_with("not scanning emr, image-builder for AMI references"),
        "{warning}"
    );
}
