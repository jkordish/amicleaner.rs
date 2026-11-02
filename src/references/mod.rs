//! The "mark" phase: finds every AMI something in the region could launch. Anything that can't
//! be resolved to a concrete AMI id is recorded as unresolved, which blocks AMI deregistration.

pub(crate) mod services;

use std::collections::{HashMap, HashSet};

use aws_sdk_autoscaling as autoscaling;
use aws_sdk_batch as batch;
use aws_sdk_cloudformation as cloudformation;
use aws_sdk_ec2 as ec2;
use aws_sdk_emr as emr;
use aws_sdk_imagebuilder as imagebuilder;
use aws_sdk_ssm as ssm;
use aws_types::SdkConfig;
use clap::ValueEnum;
use ec2::error::ProvideErrorMetadata;
use futures_util::{StreamExt, stream};
use log::warn;

use crate::errors::{AppResult, AwsResultExt, describe_sdk_error};
use crate::inventory::is_ami_id;
use crate::ui::{Phase, thousands};

/// Services beyond EC2 and Auto Scaling that can launch an AMI and are scanned for references.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum ReferenceSource {
    /// Image recipe parent images and container recipe build-instance AMIs.
    ImageBuilder,
    /// AMI ids (and SSM dynamic references to AMIs) in stack templates and parameters.
    Cloudformation,
    /// Compute environment image ids and EC2 configuration overrides.
    Batch,
    /// Custom AMIs of active clusters, instance groups and instance fleets.
    Emr,
}

/// Clients the AMI reference scan reads from. An optional client that is `None` is skipped.
#[derive(Clone, Debug)]
pub(crate) struct ReferenceClients {
    pub(crate) asg: autoscaling::Client,
    pub(crate) ssm: ssm::Client,
    pub(crate) image_builder: Option<imagebuilder::Client>,
    pub(crate) cloudformation: Option<cloudformation::Client>,
    pub(crate) batch: Option<batch::Client>,
    pub(crate) emr: Option<emr::Client>,
    /// Maximum concurrent requests for the fan-out lookups (launch template versions,
    /// CloudFormation templates).
    pub(crate) concurrency: usize,
    /// Region name, for progress labels.
    pub(crate) region: String,
}

impl ReferenceClients {
    pub(crate) fn new(config: &SdkConfig, skip: &[ReferenceSource], concurrency: usize) -> Self {
        let enabled = |source| !skip.contains(&source);
        Self {
            asg: autoscaling::Client::new(config),
            ssm: ssm::Client::new(config),
            image_builder: enabled(ReferenceSource::ImageBuilder)
                .then(|| imagebuilder::Client::new(config)),
            cloudformation: enabled(ReferenceSource::Cloudformation)
                .then(|| cloudformation::Client::new(config)),
            batch: enabled(ReferenceSource::Batch).then(|| batch::Client::new(config)),
            emr: enabled(ReferenceSource::Emr).then(|| emr::Client::new(config)),
            concurrency: concurrency.max(1),
            region: config.region().map(ToString::to_string).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LaunchTemplateRef {
    pub(crate) launch_template_id: Option<String>,
    pub(crate) launch_template_name: Option<String>,
    pub(crate) version: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct AmiReferenceScan {
    pub(crate) referenced_ami_ids: HashSet<String>,
    pub(crate) unresolved_references: HashSet<String>,
    /// AMI id -> the first few things found referencing it, for reports.
    pub(crate) sources: HashMap<String, Vec<String>>,
    /// Every reference recorded (an AMI referenced twice counts twice), for progress output.
    pub(crate) references_seen: usize,
}

/// How many referencing sources are kept per AMI for reports.
const MAX_SOURCES_PER_AMI: usize = 3;

impl AmiReferenceScan {
    pub(crate) fn add_reference(&mut self, ami_id: String, source: &str) {
        let sources = self.sources.entry(ami_id.clone()).or_default();
        if sources.len() < MAX_SOURCES_PER_AMI && !sources.iter().any(|known| known == source) {
            sources.push(source.to_string());
        }
        self.referenced_ami_ids.insert(ami_id);
        self.references_seen += 1;
    }
}

#[derive(Debug)]
pub(crate) enum ImageReferenceResolution {
    Ami(String),
    Unresolved(String),
}

/// Runs one reference source under its own progress line, finishing it with what it found. A
/// skipped source still gets a line so the output shows it wasn't scanned.
macro_rules! scan_source {
    ($collector:ident, $name:expr, $enabled:expr, $call:expr) => {{
        let phase = Phase::start(format!("{} · {}", $collector.clients.region, $name));
        if $enabled {
            let seen = $collector.scan.references_seen;
            let unresolved = $collector.scan.unresolved_references.len();
            $collector.phase = Some(phase);
            let result = $call.await;
            let phase = $collector.phase.take().expect("phase set above");
            match result {
                Ok(()) => {
                    let found = $collector.scan.references_seen - seen;
                    let unresolved = $collector.scan.unresolved_references.len() - unresolved;
                    let summary = format!("{} references", thousands(found));
                    if unresolved > 0 {
                        phase.warn(format!("{summary}, {unresolved} unresolved"));
                    } else {
                        phase.finish(summary);
                    }
                }
                Err(error) => {
                    phase.fail("failed");
                    return Err(error);
                }
            }
        } else {
            phase.skip("skipped (--skip-reference-sources)");
        }
    }};
}

pub(crate) async fn collect_referenced_ami_ids(
    ec2_client: &ec2::Client,
    reference_clients: &ReferenceClients,
) -> AppResult<AmiReferenceScan> {
    let mut collector = ReferenceCollector {
        ec2: ec2_client,
        clients: reference_clients,
        ssm_cache: HashMap::new(),
        scanned_launch_templates: HashSet::new(),
        scan: AmiReferenceScan::default(),
        phase: None,
    };

    let clients = reference_clients;
    scan_source!(collector, "instances", true, collector.collect_instances());
    scan_source!(collector, "launch templates", true, collector.collect_launch_templates());
    scan_source!(collector, "auto scaling groups", true, collector.collect_auto_scaling_groups());
    scan_source!(
        collector,
        "launch configurations",
        true,
        collector.collect_launch_configurations()
    );
    scan_source!(collector, "EC2 fleets", true, collector.collect_ec2_fleets());
    scan_source!(collector, "spot fleets", true, collector.collect_spot_fleets());
    scan_source!(collector, "spot requests", true, collector.collect_spot_instance_requests());
    scan_source!(
        collector,
        "image builder",
        clients.image_builder.is_some(),
        collector.collect_image_builder_recipes()
    );
    scan_source!(
        collector,
        "cloudformation",
        clients.cloudformation.is_some(),
        collector.collect_cloudformation_stacks()
    );
    scan_source!(
        collector,
        "batch",
        clients.batch.is_some(),
        collector.collect_batch_compute_environments()
    );
    scan_source!(collector, "emr", clients.emr.is_some(), collector.collect_emr_clusters());

    Ok(collector.scan)
}

/// Gathers every AMI that something in the region could launch. Any reference that can't be
/// resolved to a concrete AMI id is recorded as unresolved, which blocks AMI deregistration.
pub(crate) struct ReferenceCollector<'a> {
    pub(crate) ec2: &'a ec2::Client,
    pub(crate) clients: &'a ReferenceClients,
    pub(crate) ssm_cache: HashMap<String, Option<String>>,
    /// Ids and names of launch templates whose versions were all recorded.
    pub(crate) scanned_launch_templates: HashSet<String>,
    pub(crate) scan: AmiReferenceScan,
    /// Progress line of the source being scanned, if any.
    pub(crate) phase: Option<Phase>,
}

impl ReferenceCollector<'_> {
    /// Records the AMI an image reference resolves to, noting `source` (what referenced it).
    pub(crate) async fn record(&mut self, image_reference: &str, source: &str) {
        match resolve_image_reference(image_reference, &self.clients.ssm, &mut self.ssm_cache).await
        {
            ImageReferenceResolution::Ami(ami_id) => {
                if image_reference.starts_with(['r', '{']) {
                    let via = format!("{source} via {image_reference}");
                    self.scan.add_reference(ami_id, &via);
                } else {
                    self.scan.add_reference(ami_id, source);
                }
            }
            ImageReferenceResolution::Unresolved(reason) => {
                self.scan.unresolved_references.insert(reason);
            }
        }
    }

    pub(crate) async fn collect_instances(&mut self) -> AppResult<()> {
        let state_filter = ec2::types::Filter::builder()
            .name("instance-state-name")
            .values("pending")
            .values("running")
            .values("stopping")
            .values("stopped")
            .values("shutting-down")
            .build();

        let mut pages = self
            .ec2
            .describe_instances()
            .filters(state_filter)
            .into_paginator()
            .page_size(100)
            .send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeInstances")?;
            for reservation in output.reservations() {
                for instance in reservation.instances() {
                    if let Some(image_id) = instance.image_id() {
                        let source =
                            format!("instance {}", instance.instance_id().unwrap_or("<unknown>"));
                        self.record(image_id, &source).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Every version of every launch template, since any version can be launched by number.
    /// Versions are fetched `concurrency` templates at a time (build accounts can have tens of
    /// thousands of versions). Templates scanned here are remembered so the Auto Scaling pass
    /// doesn't fetch them again.
    pub(crate) async fn collect_launch_templates(&mut self) -> AppResult<()> {
        let mut templates = Vec::new();
        let mut template_pages =
            self.ec2.describe_launch_templates().into_paginator().page_size(200).send();
        while let Some(template_page) = template_pages.next().await {
            let output = template_page.aws_context("DescribeLaunchTemplates")?;
            for launch_template in output.launch_templates() {
                let id = launch_template.launch_template_id().map(ToString::to_string);
                let name = launch_template.launch_template_name().map(ToString::to_string);
                if id.is_some() || name.is_some() {
                    let template = LaunchTemplateRef {
                        launch_template_id: id,
                        launch_template_name: name,
                        version: None,
                    };
                    templates.push((template, launch_template.latest_version_number()));
                }
            }
        }

        // A few templates hold most versions (6,500+ in a build account), so each template is
        // split into version ranges that are fetched concurrently with everything else.
        let chunks = templates
            .iter()
            .enumerate()
            .flat_map(|(index, (_, latest))| {
                version_ranges(*latest).into_iter().map(move |range| (index, range))
            })
            .collect::<Vec<_>>();

        let ec2_client = self.ec2;
        let phase = self.phase.as_ref();
        if let Some(phase) = phase {
            phase.set_total(chunks.len());
        }
        let results = stream::iter(&chunks)
            .map(|(index, range)| {
                launch_template_image_references(ec2_client, &templates[*index].0, range)
            })
            .buffered(self.clients.concurrency)
            .inspect(|_| {
                if let Some(phase) = phase {
                    phase.inc();
                }
            })
            .collect::<Vec<_>>()
            .await;

        // Regroup per template: a template is only "scanned" if every range came back.
        let mut per_template: Vec<Option<Vec<(String, String)>>> =
            templates.iter().map(|_| Some(Vec::new())).collect();
        for ((index, _), result) in chunks.iter().zip(results) {
            match result? {
                Some(references) => {
                    if let Some(collected) = &mut per_template[*index] {
                        collected.extend(references);
                    }
                }
                None => per_template[*index] = None,
            }
        }

        for ((template, _), references) in templates.iter().zip(per_template) {
            let Some(references) = references else {
                warn!(
                    "launch template {} was deleted during the scan; ignoring it",
                    template
                        .launch_template_id
                        .as_deref()
                        .or(template.launch_template_name.as_deref())
                        .unwrap_or("<unknown>")
                );
                continue;
            };
            self.scanned_launch_templates.extend(
                template.launch_template_id.iter().chain(&template.launch_template_name).cloned(),
            );
            for (image_reference, source) in references {
                self.record(&image_reference, &source).await;
            }
        }

        Ok(())
    }

    pub(crate) async fn collect_auto_scaling_groups(&mut self) -> AppResult<()> {
        let mut group_pages =
            self.clients.asg.describe_auto_scaling_groups().into_paginator().page_size(100).send();

        while let Some(group_page) = group_pages.next().await {
            let output = group_page.aws_context("DescribeAutoScalingGroups")?;
            for group in output.auto_scaling_groups() {
                let mut template_refs = Vec::new();

                if let Some(spec) = group.launch_template() {
                    template_refs.push(LaunchTemplateRef {
                        launch_template_id: spec.launch_template_id().map(ToString::to_string),
                        launch_template_name: spec.launch_template_name().map(ToString::to_string),
                        version: spec.version().map(ToString::to_string),
                    });
                }

                if let Some(mixed_instances_policy) = group.mixed_instances_policy()
                    && let Some(launch_template) = mixed_instances_policy.launch_template()
                {
                    let override_specs = launch_template
                        .overrides()
                        .iter()
                        .filter_map(|override_spec| override_spec.launch_template_specification());
                    for spec in launch_template
                        .launch_template_specification()
                        .into_iter()
                        .chain(override_specs)
                    {
                        template_refs.push(LaunchTemplateRef {
                            launch_template_id: spec.launch_template_id().map(ToString::to_string),
                            launch_template_name: spec
                                .launch_template_name()
                                .map(ToString::to_string),
                            version: spec.version().map(ToString::to_string),
                        });
                    }
                }

                for template_ref in &template_refs {
                    let already_scanned = template_ref
                        .launch_template_id
                        .iter()
                        .chain(&template_ref.launch_template_name)
                        .any(|key| self.scanned_launch_templates.contains(key));
                    if already_scanned {
                        // collect_launch_templates already recorded every version of it.
                        continue;
                    }
                    self.collect_launch_template_ref(group.auto_scaling_group_name(), template_ref)
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Resolves the image of one launch template an ASG points at. Every existing template version
    /// is already covered by `collect_launch_templates`; this catches templates that couldn't be
    /// listed. A template that no longer exists can't launch anything, so it's skipped; any other
    /// failure is recorded as unresolved rather than aborting the whole scan.
    pub(crate) async fn collect_launch_template_ref(
        &mut self,
        group_name: Option<&str>,
        launch_template_ref: &LaunchTemplateRef,
    ) {
        let mut request = self.ec2.describe_launch_template_versions();
        let label =
            if let Some(launch_template_id) = launch_template_ref.launch_template_id.as_deref() {
                request = request.launch_template_id(launch_template_id);
                launch_template_id.to_string()
            } else if let Some(launch_template_name) =
                launch_template_ref.launch_template_name.as_deref()
            {
                request = request.launch_template_name(launch_template_name);
                launch_template_name.to_string()
            } else {
                return;
            };

        // No version means "all versions" here, which is the conservative reading. EC2 rejects a
        // page size combined with an explicit version list (InvalidParameterCombination), so a
        // pinned version is paginated without one.
        let paginator = match launch_template_ref.version.as_deref() {
            Some(version) => request.versions(version).into_paginator(),
            None => request.into_paginator().page_size(100),
        };

        let mut image_references = Vec::new();
        let mut pages = paginator.send();
        while let Some(page) = pages.next().await {
            match page {
                Ok(output) => image_references.extend(
                    output
                        .launch_template_versions()
                        .iter()
                        .filter_map(|version| {
                            version.launch_template_data().and_then(|data| data.image_id())
                        })
                        .map(ToString::to_string),
                ),
                Err(error) if is_launch_template_not_found(error.code()) => {
                    warn!(
                        "Auto Scaling group {} references launch template {} which no longer exists ({}); ignoring",
                        group_name.unwrap_or("<unnamed>"),
                        label,
                        describe_sdk_error(&error)
                    );
                    return;
                }
                Err(error) => {
                    self.scan.unresolved_references.insert(format!(
                        "failed to describe launch template {} for Auto Scaling group {}: {}",
                        label,
                        group_name.unwrap_or("<unnamed>"),
                        describe_sdk_error(&error)
                    ));
                    return;
                }
            }
        }

        let source = format!(
            "Auto Scaling group {} (launch template {label})",
            group_name.unwrap_or("<unnamed>")
        );
        for image_reference in image_references {
            self.record(&image_reference, &source).await;
        }
    }

    pub(crate) async fn collect_launch_configurations(&mut self) -> AppResult<()> {
        let mut pages = self
            .clients
            .asg
            .describe_launch_configurations()
            .into_paginator()
            .page_size(100)
            .send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeLaunchConfigurations")?;
            for launch_configuration in output.launch_configurations() {
                if let Some(image_id) = launch_configuration.image_id() {
                    let source = format!(
                        "launch configuration {}",
                        launch_configuration.launch_configuration_name().unwrap_or("<unnamed>")
                    );
                    self.record(image_id, &source).await;
                }
            }
        }

        Ok(())
    }

    /// EC2 Fleet overrides can name an AMI directly. Their launch templates are already covered.
    pub(crate) async fn collect_ec2_fleets(&mut self) -> AppResult<()> {
        let mut pages = self.ec2.describe_fleets().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeFleets")?;
            for fleet in output.fleets() {
                let can_launch = fleet.fleet_state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::FleetStateCode::Submitted
                            | ec2::types::FleetStateCode::Active
                            | ec2::types::FleetStateCode::Modifying
                    )
                });
                if !can_launch {
                    continue;
                }

                let source = format!("EC2 Fleet {}", fleet.fleet_id().unwrap_or("<unknown>"));
                for config in fleet.launch_template_configs() {
                    for override_spec in config.overrides() {
                        if let Some(image_reference) = override_spec.image_id() {
                            self.record(image_reference, &source).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Spot Fleet launch specifications name an AMI directly. Launch templates are already covered.
    pub(crate) async fn collect_spot_fleets(&mut self) -> AppResult<()> {
        let mut pages =
            self.ec2.describe_spot_fleet_requests().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeSpotFleetRequests")?;
            for request in output.spot_fleet_request_configs() {
                let can_launch = request.spot_fleet_request_state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::BatchState::Submitted
                            | ec2::types::BatchState::Active
                            | ec2::types::BatchState::Modifying
                    )
                });
                if !can_launch {
                    continue;
                }

                let Some(config) = request.spot_fleet_request_config() else {
                    continue;
                };
                let source = format!(
                    "Spot Fleet {}",
                    request.spot_fleet_request_id().unwrap_or("<unknown>")
                );
                for spec in config.launch_specifications() {
                    if let Some(image_reference) = spec.image_id() {
                        self.record(image_reference, &source).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Open or persistent Spot requests can launch (or relaunch) their AMI later.
    pub(crate) async fn collect_spot_instance_requests(&mut self) -> AppResult<()> {
        let mut pages =
            self.ec2.describe_spot_instance_requests().into_paginator().page_size(100).send();

        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeSpotInstanceRequests")?;
            for request in output.spot_instance_requests() {
                let can_launch = request.state().is_some_and(|state| {
                    matches!(
                        state,
                        ec2::types::SpotInstanceState::Open
                            | ec2::types::SpotInstanceState::Active
                            | ec2::types::SpotInstanceState::Disabled
                    )
                });
                if !can_launch {
                    continue;
                }

                if let Some(image_reference) =
                    request.launch_specification().and_then(|spec| spec.image_id())
                {
                    let source = format!(
                        "Spot request {}",
                        request.spot_instance_request_id().unwrap_or("<unknown>")
                    );
                    self.record(image_reference, &source).await;
                }
            }
        }

        Ok(())
    }
}

/// Versions per range when splitting a launch template; matches the API's maximum page size.
const VERSION_RANGE: i64 = 200;

/// `(min_version, max_version)` ranges covering every version of a template. The last range is
/// open-ended so versions created during the scan are still read; templates with no more than one
/// range's worth of versions (or an unknown latest version) are fetched in one paginated call.
pub(crate) fn version_ranges(latest_version: Option<i64>) -> Vec<(Option<i64>, Option<i64>)> {
    let Some(latest) = latest_version.filter(|latest| *latest > VERSION_RANGE) else {
        return vec![(None, None)];
    };
    let mut ranges = Vec::new();
    let mut start = 1;
    while start + VERSION_RANGE <= latest {
        ranges.push((Some(start), Some(start + VERSION_RANGE - 1)));
        start += VERSION_RANGE;
    }
    ranges.push((Some(start), None));
    ranges
}

/// Image references in one version range of a launch template. `None` when the template was
/// deleted after it was listed: it can't launch anything any more, and build accounts churn
/// templates constantly, so failing the whole scan over it would block AMI cleanup for no reason.
async fn launch_template_image_references(
    ec2_client: &ec2::Client,
    template: &LaunchTemplateRef,
    (min_version, max_version): &(Option<i64>, Option<i64>),
) -> AppResult<Option<Vec<(String, String)>>> {
    let mut request = ec2_client.describe_launch_template_versions();
    if let Some(id) = &template.launch_template_id {
        request = request.launch_template_id(id);
    } else if let Some(name) = &template.launch_template_name {
        request = request.launch_template_name(name);
    }
    if let Some(min_version) = min_version {
        request = request.min_version(min_version.to_string());
    }
    if let Some(max_version) = max_version {
        request = request.max_version(max_version.to_string());
    }

    let mut image_references = Vec::new();
    let mut pages = request.into_paginator().page_size(200).send();
    while let Some(page) = pages.next().await {
        let output = match page {
            Ok(output) => output,
            Err(error) if is_launch_template_not_found(error.code()) => return Ok(None),
            Err(error) => return Err(error).aws_context("DescribeLaunchTemplateVersions"),
        };
        image_references.extend(output.launch_template_versions().iter().filter_map(|version| {
            let image_id = version.launch_template_data().and_then(|data| data.image_id())?;
            let source = format!(
                "launch template {} v{}",
                version
                    .launch_template_id()
                    .or(template.launch_template_id.as_deref())
                    .or(template.launch_template_name.as_deref())
                    .unwrap_or("<unknown>"),
                version.version_number().unwrap_or_default()
            );
            Some((image_id.to_string(), source))
        }));
    }
    Ok(Some(image_references))
}

fn is_launch_template_not_found(code: Option<&str>) -> bool {
    code.is_some_and(|code| code.starts_with("InvalidLaunchTemplate") && code.contains("NotFound"))
}

pub(crate) async fn resolve_image_reference(
    image_reference: &str,
    ssm_client: &ssm::Client,
    ssm_cache: &mut HashMap<String, Option<String>>,
) -> ImageReferenceResolution {
    if is_ami_id(image_reference) {
        return ImageReferenceResolution::Ami(image_reference.to_string());
    }

    let Some(parameter_name) = extract_ssm_parameter_name(image_reference) else {
        let reason = format!("unresolved image reference '{}'", image_reference.escape_debug());
        warn!("{reason}");
        return ImageReferenceResolution::Unresolved(reason);
    };

    let resolved_ami = match ssm_cache.get(&parameter_name) {
        Some(cached) => cached.clone(),
        None => {
            let resolved_ami = fetch_ssm_ami_id(ssm_client, &parameter_name).await;
            ssm_cache.insert(parameter_name.clone(), resolved_ami.clone());
            resolved_ami
        }
    };

    match resolved_ami {
        Some(ami_id) => ImageReferenceResolution::Ami(ami_id),
        None => ImageReferenceResolution::Unresolved(format!(
            "failed to resolve SSM image parameter '{parameter_name}'"
        )),
    }
}

/// Reads an SSM parameter that should hold exactly one AMI id. `None` (with a warning) when it
/// can't be read or holds anything else.
async fn fetch_ssm_ami_id(ssm_client: &ssm::Client, parameter_name: &str) -> Option<String> {
    match ssm_client.get_parameter().name(parameter_name).send().await {
        Ok(output) => {
            let value = output.parameter().and_then(|parameter| parameter.value());
            let ami_id = value.filter(|value| is_ami_id(value)).map(ToString::to_string);
            if ami_id.is_none() {
                warn!(
                    "SSM image parameter '{parameter_name}' does not hold a single AMI id (value: {value:?})"
                );
            }
            ami_id
        }
        Err(error) => {
            warn!(
                "Failed to resolve SSM image parameter '{parameter_name}': {}",
                describe_sdk_error(&error)
            );
            None
        }
    }
}

pub(crate) fn extract_ssm_parameter_name(image_reference: &str) -> Option<String> {
    if let Some(raw) = image_reference.strip_prefix("resolve:ssm:") {
        return Some(raw.to_string());
    }

    if image_reference.starts_with("{{resolve:ssm:") && image_reference.ends_with("}}") {
        return Some(
            image_reference.trim_start_matches("{{resolve:ssm:").trim_end_matches("}}").to_string(),
        );
    }

    None
}
