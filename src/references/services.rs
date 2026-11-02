//! Optional reference sources beyond EC2 and Auto Scaling: Image Builder, CloudFormation, Batch
//! and EMR, plus the text parsers they use.

use aws_sdk_ec2::error::ProvideErrorMetadata;
use aws_sdk_emr as emr;
use aws_sdk_imagebuilder as imagebuilder;
use futures_util::{StreamExt, stream};
use log::warn;

use crate::errors::{AppResult, AwsResultExt, describe_sdk_error};
use crate::inventory::is_ami_id;
use crate::references::ReferenceCollector;

/// Concurrent `GetTemplate` calls; CloudFormation throttles well below EC2's limits.
const CLOUDFORMATION_CONCURRENCY: usize = 2;

impl ReferenceCollector<'_> {
    /// Image Builder can launch an AMI as a recipe's parent image or as a container recipe's
    /// build instance.
    pub(crate) async fn collect_image_builder_recipes(&mut self) -> AppResult<()> {
        let Some(client) = self.clients.image_builder.clone() else {
            return Ok(());
        };
        let own = imagebuilder::types::Ownership::SelfValue;

        let mut parents = Vec::new();
        let mut pages = client.list_image_recipes().owner(own.clone()).into_paginator().send();
        while let Some(page) = pages.next().await {
            let output = page.aws_context("ListImageRecipes")?;
            for recipe in output.image_recipe_summary_list() {
                if let Some(parent) = recipe.parent_image() {
                    let source =
                        format!("Image Builder recipe {}", recipe.name().unwrap_or("<unnamed>"));
                    parents.push((
                        parent.to_string(),
                        recipe.owner().map(ToString::to_string),
                        source,
                    ));
                }
            }
        }

        let mut pages = client.list_container_recipes().owner(own).into_paginator().send();
        while let Some(page) = pages.next().await {
            let output = page.aws_context("ListContainerRecipes")?;
            for summary in output.container_recipe_summary_list() {
                let Some(arn) = summary.arn() else {
                    return Err("ListContainerRecipes returned a recipe without an ARN".into());
                };
                let recipe = client
                    .get_container_recipe()
                    .container_recipe_arn(arn)
                    .send()
                    .await
                    .aws_context("GetContainerRecipe")?;
                if let Some(image) = recipe
                    .container_recipe()
                    .and_then(|recipe| recipe.instance_configuration())
                    .and_then(|instance| instance.image())
                {
                    let source = format!(
                        "Image Builder container recipe {}",
                        summary.name().unwrap_or("<unnamed>")
                    );
                    parents.push((
                        image.to_string(),
                        summary.owner().map(ToString::to_string),
                        source,
                    ));
                }
            }
        }

        for (parent, owner, source) in parents {
            match classify_parent_image(&parent, owner.as_deref()) {
                ParentImage::Reference(reference) => self.record(&reference, &source).await,
                ParentImage::NotOwned => {}
                ParentImage::OwnImage(image) => {
                    for ami_id in own_image_builder_amis(&client, &image).await? {
                        let via = format!("{source} (parent image {parent})");
                        self.record(&ami_id, &via).await;
                    }
                }
                ParentImage::Unresolved(reason) => {
                    warn!("{reason}");
                    self.scan.unresolved_references.insert(reason);
                }
            }
        }

        Ok(())
    }

    /// Stack updates and rollbacks can relaunch any AMI a template or its parameters name.
    /// Instances and launch templates a stack created are already covered by the EC2 sources.
    pub(crate) async fn collect_cloudformation_stacks(&mut self) -> AppResult<()> {
        let Some(client) = self.clients.cloudformation.clone() else {
            return Ok(());
        };

        let mut stack_ids = Vec::new();
        let mut pages = client.describe_stacks().into_paginator().send();
        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeStacks")?;
            for stack in output.stacks() {
                let Some(stack_id) = stack.stack_id() else {
                    return Err("DescribeStacks returned a stack without an id".into());
                };
                let source =
                    format!("CloudFormation stack {}", stack.stack_name().unwrap_or(stack_id));
                for parameter in stack.parameters() {
                    for value in [parameter.parameter_value(), parameter.resolved_value()]
                        .into_iter()
                        .flatten()
                    {
                        for ami_id in find_ami_ids(value) {
                            self.scan.add_reference(ami_id, &source);
                        }
                    }
                }
                stack_ids.push(stack_id.to_string());
            }
        }

        // One GetTemplate per stack. CloudFormation's API rate limits are far lower than EC2's
        // (8 at a time got throttled in delta0), so it gets its own small cap.
        let templates = stream::iter(&stack_ids)
            .map(|stack_id| {
                let request = client.get_template().stack_name(stack_id).send();
                async move { request.await.aws_context(&format!("GetTemplate({stack_id})")) }
            })
            .buffered(self.clients.concurrency.min(CLOUDFORMATION_CONCURRENCY))
            .collect::<Vec<_>>()
            .await;

        for (stack_id, template) in stack_ids.iter().zip(templates) {
            if let Some(body) = template?.template_body() {
                let source = format!("CloudFormation stack {stack_id} template");
                for ami_id in find_ami_ids(body) {
                    self.scan.add_reference(ami_id, &source);
                }
                for parameter in find_ssm_dynamic_references(body) {
                    self.record_ssm_parameter_amis(&parameter, stack_id).await;
                }
            }
        }

        Ok(())
    }

    /// Batch launches compute environment instances from `imageId` or an EC2 configuration's
    /// `imageIdOverride`. Launch templates they name are covered by `collect_launch_templates`.
    pub(crate) async fn collect_batch_compute_environments(&mut self) -> AppResult<()> {
        let Some(client) = self.clients.batch.clone() else {
            return Ok(());
        };

        let mut pages = client.describe_compute_environments().into_paginator().send();
        while let Some(page) = pages.next().await {
            let output = page.aws_context("DescribeComputeEnvironments")?;
            for environment in output.compute_environments() {
                let Some(resources) = environment.compute_resources() else {
                    continue;
                };
                let overrides = resources
                    .ec2_configuration()
                    .iter()
                    .filter_map(|configuration| configuration.image_id_override());
                // Deprecated in favour of imageIdOverride, but older environments still launch
                // from it, so it must still count as a reference.
                #[allow(deprecated)]
                let image_references = resources
                    .image_id()
                    .into_iter()
                    .chain(overrides)
                    .map(ToString::to_string)
                    .collect::<Vec<String>>();
                let source = format!(
                    "Batch compute environment {}",
                    environment.compute_environment_name().unwrap_or("<unnamed>")
                );
                for image_reference in image_references {
                    self.record(&image_reference, &source).await;
                }
                if let Some(template) = resources.launch_template() {
                    self.scan.pin_launch_template(
                        template.launch_template_id(),
                        template.launch_template_name(),
                        template.version(),
                    );
                    for template_override in template.overrides() {
                        self.scan.pin_launch_template(
                            template_override.launch_template_id(),
                            template_override.launch_template_name(),
                            template_override.version(),
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// EKS managed node groups name a launch template version; EKS launches from a copy, but
    /// scaling and upgrades go back to the pinned version, so it must survive pruning.
    pub(crate) async fn collect_eks_nodegroups(&mut self) -> AppResult<()> {
        let Some(client) = self.clients.eks.clone() else {
            return Ok(());
        };

        let mut clusters = Vec::new();
        let mut cluster_pages = client.list_clusters().into_paginator().send();
        while let Some(page) = cluster_pages.next().await {
            clusters.extend(page.aws_context("ListClusters")?.clusters().iter().cloned());
        }

        for cluster in clusters {
            let mut nodegroups = Vec::new();
            let mut nodegroup_pages =
                client.list_nodegroups().cluster_name(&cluster).into_paginator().send();
            while let Some(page) = nodegroup_pages.next().await {
                nodegroups.extend(page.aws_context("ListNodegroups")?.nodegroups().iter().cloned());
            }
            for nodegroup in nodegroups {
                let output = match client
                    .describe_nodegroup()
                    .cluster_name(&cluster)
                    .nodegroup_name(&nodegroup)
                    .send()
                    .await
                {
                    Ok(output) => output,
                    // Deleted since it was listed: it pins nothing any more.
                    Err(error) if error.code() == Some("ResourceNotFoundException") => continue,
                    Err(error) => return Err(error).aws_context("DescribeNodegroup"),
                };
                if let Some(template) =
                    output.nodegroup().and_then(|nodegroup| nodegroup.launch_template())
                {
                    self.scan.pin_launch_template(
                        template.id(),
                        template.name(),
                        template.version(),
                    );
                }
            }
        }

        Ok(())
    }

    /// Active EMR clusters launch (and scale) from a cluster-wide custom AMI or a per instance
    /// group / instance fleet one.
    pub(crate) async fn collect_emr_clusters(&mut self) -> AppResult<()> {
        let Some(client) = self.clients.emr.clone() else {
            return Ok(());
        };

        let mut image_references = Vec::new();
        let mut pages = client
            .list_clusters()
            .cluster_states(emr::types::ClusterState::Starting)
            .cluster_states(emr::types::ClusterState::Bootstrapping)
            .cluster_states(emr::types::ClusterState::Running)
            .cluster_states(emr::types::ClusterState::Waiting)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let output = page.aws_context("ListClusters")?;
            for summary in output.clusters() {
                let Some(cluster_id) = summary.id() else {
                    return Err("ListClusters returned a cluster without an id".into());
                };
                let described = client
                    .describe_cluster()
                    .cluster_id(cluster_id)
                    .send()
                    .await
                    .aws_context(&format!("DescribeCluster({cluster_id})"))?;
                let Some(cluster) = described.cluster() else {
                    return Err(format!("DescribeCluster({cluster_id}) returned no cluster").into());
                };
                let source = format!("EMR cluster {cluster_id}");
                image_references
                    .extend(cluster.custom_ami_id().map(|ami| (ami.to_string(), source.clone())));

                if cluster.instance_collection_type()
                    == Some(&emr::types::InstanceCollectionType::InstanceFleet)
                {
                    let mut fleets = client
                        .list_instance_fleets()
                        .cluster_id(cluster_id)
                        .into_paginator()
                        .send();
                    while let Some(fleet_page) = fleets.next().await {
                        let fleet_output =
                            fleet_page.aws_context(&format!("ListInstanceFleets({cluster_id})"))?;
                        image_references.extend(
                            fleet_output
                                .instance_fleets()
                                .iter()
                                .flat_map(emr::types::InstanceFleet::instance_type_specifications)
                                .filter_map(|specification| specification.custom_ami_id())
                                .map(|ami| (ami.to_string(), source.clone())),
                        );
                    }
                } else {
                    let mut groups = client
                        .list_instance_groups()
                        .cluster_id(cluster_id)
                        .into_paginator()
                        .send();
                    while let Some(group_page) = groups.next().await {
                        let group_output =
                            group_page.aws_context(&format!("ListInstanceGroups({cluster_id})"))?;
                        image_references.extend(
                            group_output
                                .instance_groups()
                                .iter()
                                .filter_map(|group| group.custom_ami_id())
                                .map(|ami| (ami.to_string(), source.clone())),
                        );
                    }
                }
            }
        }

        for (image_reference, source) in image_references {
            self.record(&image_reference, &source).await;
        }

        Ok(())
    }

    /// Records any AMI ids an SSM parameter holds. Unlike `record`, a value that isn't an AMI is
    /// fine here (templates resolve all kinds of parameters); only a failed lookup is unresolved.
    pub(crate) async fn record_ssm_parameter_amis(&mut self, parameter: &str, stack_id: &str) {
        match self.clients.ssm.get_parameter().name(parameter).send().await {
            Ok(output) => {
                if let Some(value) = output.parameter().and_then(|parameter| parameter.value()) {
                    let source =
                        format!("CloudFormation stack {stack_id} via SSM parameter {parameter}");
                    for ami_id in find_ami_ids(value) {
                        self.scan.add_reference(ami_id, &source);
                    }
                }
            }
            Err(error) => {
                let reason = format!(
                    "failed to resolve SSM parameter '{}' used by CloudFormation stack {}: {}",
                    parameter,
                    stack_id,
                    describe_sdk_error(&error)
                );
                warn!("{reason}");
                self.scan.unresolved_references.insert(reason);
            }
        }
    }
}

/// An Image Builder image in this account that a recipe builds on, e.g. from
/// `arn:aws:imagebuilder:us-east-1:123456789012:image/base/1.x.x`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnImageBuilderImage {
    pub(crate) name: String,
    /// Semantic version, possibly with `x` wildcards (`1.x.x`, `x.x.x`).
    pub(crate) version: String,
    pub(crate) build: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ParentImage {
    /// An AMI id or `resolve:ssm:` reference, for `ReferenceCollector::record`.
    Reference(String),
    /// AWS-managed or another account's Image Builder image, or a Marketplace product: its AMIs
    /// aren't owned by this account.
    NotOwned,
    OwnImage(OwnImageBuilderImage),
    Unresolved(String),
}

/// Classifies an Image Builder parent image (or container recipe instance image). The recipe
/// owner tells this account's images apart from shared ones without an STS call.
pub(crate) fn classify_parent_image(parent: &str, recipe_owner: Option<&str>) -> ParentImage {
    let parent = parent.trim();
    if is_ami_id(parent) {
        return ParentImage::Reference(parent.to_string());
    }
    if let Some(parameter) = parent.strip_prefix("ssm:") {
        return ParentImage::Reference(format!("resolve:ssm:{parameter}"));
    }
    if parent.starts_with("prod-") {
        return ParentImage::NotOwned;
    }

    let unresolved =
        || ParentImage::Unresolved(format!("unrecognized Image Builder parent image '{parent}'"));
    let parts = parent.splitn(6, ':').collect::<Vec<&str>>();
    let [_arn, _partition, service, _region, owner, resource] = parts.as_slice() else {
        return unresolved();
    };
    if *service != "imagebuilder" {
        return unresolved();
    }
    if *owner == "aws" || recipe_owner.is_some_and(|recipe_owner| recipe_owner != *owner) {
        return ParentImage::NotOwned;
    }

    let segments = resource.split('/').collect::<Vec<&str>>();
    match segments.as_slice() {
        ["image", name, version] if !name.is_empty() && !version.is_empty() => {
            ParentImage::OwnImage(OwnImageBuilderImage {
                name: (*name).to_string(),
                version: (*version).to_string(),
                build: None,
            })
        }
        ["image", name, version, build]
            if !name.is_empty() && !version.is_empty() && !build.is_empty() =>
        {
            ParentImage::OwnImage(OwnImageBuilderImage {
                name: (*name).to_string(),
                version: (*version).to_string(),
                build: Some((*build).to_string()),
            })
        }
        _ => unresolved(),
    }
}

/// Whether an image version (`1.2.3`, or `1.2.3/4` with a build) matches a version pattern where
/// any component may be `x`. Unparseable versions match, which keeps more AMIs, never fewer.
pub(crate) fn image_version_matches(pattern: &str, version: &str) -> bool {
    let version = version.split('/').next().unwrap_or(version);
    let pattern_parts = pattern.split('.').collect::<Vec<&str>>();
    let version_parts = version.split('.').collect::<Vec<&str>>();
    if pattern_parts.len() != version_parts.len() {
        return true;
    }
    pattern_parts
        .iter()
        .zip(&version_parts)
        .all(|(pattern_part, version_part)| *pattern_part == "x" || pattern_part == version_part)
}

/// Every AMI produced by any build of the matching versions of one of this account's Image
/// Builder images. Image Builder picks the latest match at build time; keeping all matches is the
/// conservative superset.
async fn own_image_builder_amis(
    client: &imagebuilder::Client,
    image: &OwnImageBuilderImage,
) -> AppResult<Vec<String>> {
    let name_filter =
        imagebuilder::types::Filter::builder().name("name").values(image.name.clone()).build();
    let mut version_arns = Vec::new();
    let mut pages = client
        .list_images()
        .owner(imagebuilder::types::Ownership::SelfValue)
        .filters(name_filter)
        .include_deprecated(true)
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let output = page.aws_context("ListImages")?;
        for version in output.image_version_list() {
            let matches = version
                .version()
                .is_none_or(|version| image_version_matches(&image.version, version));
            if !matches {
                continue;
            }
            let Some(arn) = version.arn() else {
                return Err("ListImages returned an image version without an ARN".into());
            };
            version_arns.push(arn.to_string());
        }
    }

    let mut ami_ids = Vec::new();
    for version_arn in version_arns {
        let mut builds = client
            .list_image_build_versions()
            .image_version_arn(&version_arn)
            .into_paginator()
            .send();
        while let Some(page) = builds.next().await {
            let output = page.aws_context("ListImageBuildVersions")?;
            for build in output.image_summary_list() {
                let build_matches = image.build.as_deref().is_none_or(|wanted| {
                    build.arn().is_none_or(|arn| arn.rsplit('/').next() == Some(wanted))
                });
                if !build_matches {
                    continue;
                }
                ami_ids.extend(
                    build
                        .output_resources()
                        .into_iter()
                        .flat_map(imagebuilder::types::OutputResources::amis)
                        .filter_map(|ami| ami.image())
                        .map(ToString::to_string),
                );
            }
        }
    }

    Ok(ami_ids)
}

/// Finds every well-formed AMI id in free text (templates, parameter values). Over-matching only
/// keeps an AMI, so no word-boundary rules; ids are compared lowercased.
pub(crate) fn find_ami_ids(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut search_from = 0;
    while let Some(offset) = text[search_from..].find("ami-") {
        let hex_start = search_from + offset + 4;
        let hex_len = bytes[hex_start..].iter().take_while(|byte| byte.is_ascii_hexdigit()).count();
        let candidate = text[hex_start - 4..hex_start + hex_len].to_ascii_lowercase();
        if is_ami_id(&candidate) {
            found.push(candidate);
        }
        search_from = hex_start;
    }
    found
}

/// Parameter names (with optional `:version`) from `{{resolve:ssm:...}}` dynamic references.
/// `ssm-secure` references hold secrets, not AMI ids, and don't match this prefix.
pub(crate) fn find_ssm_dynamic_references(template: &str) -> Vec<String> {
    const PREFIX: &str = "{{resolve:ssm:";
    let mut found = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find(PREFIX) {
        rest = &rest[start + PREFIX.len()..];
        let Some(end) = rest.find("}}") else {
            break;
        };
        let parameter = rest[..end].trim();
        if !parameter.is_empty() {
            found.push(parameter.to_string());
        }
        rest = &rest[end..];
    }
    found
}
