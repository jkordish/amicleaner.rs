#![cfg_attr(
    feature = "cargo-clippy",
    warn(clippy::all)
)]

use std::{env, error::Error, io::stdin, process::exit};

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_sdk_ec2::{
    operation::describe_images::DescribeImagesOutput,
    types::{Filter, VolumeState},
    Client
};
use aws_types::region::Region as AwsRegion;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use clap::{Arg, Command};
use console::Emoji;
use hashbrown::HashMap;
use indicatif::{ProgressBar, ProgressStyle};
use log::{self, info};
use prettytable::{format, row, Table};
use serde_json::Value;
use tokio_stream::StreamExt;

static SPARKLE: Emoji<'_, '_> = Emoji("\u{2728}", "");
static DEATH: Emoji<'_, '_> = Emoji("\u{2620}", "");
static PARTY: Emoji<'_, '_> = Emoji("\u{1f389}", "");
static DISAPPROVE: Emoji<'_, '_> = Emoji("\u{1f61e}", "");

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // SimpleLogger::new().with_level(log::LevelFilter::Info).init()?;

    log::set_max_level(log::LevelFilter::Info);

    let matches = Command::new("amicleaner")
        .version("2.0.0")
        .author("Joseph Kordish <joseph.kordish@trellix.com>")
        .about("Find and terminate AMIs")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .takes_value(true)
                .help("Input string(s) to match against. Can pass comma separated list.")
        )
        .arg(
            Arg::new("days")
                .short('d')
                .long("older-than")
                .takes_value(true)
                .default_value("7")
                .help("Anything older than days should be terminated")
        )
        .arg(
            Arg::new("keep")
                .short('k')
                .long("keep-previous")
                .takes_value(true)
                .conflicts_with("days")
                .help("Number of previous to keep")
        )
        .arg(
            Arg::new("region")
                .short('r')
                .long("aws-region")
                .takes_value(true)
                .default_value("us-west-2")
                .help("Region(s) to check. Can pass comma separated list.")
        )
        .arg(
            Arg::new("force")
                .short('f')
                .long("force-delete")
                .takes_value(false)
                .help("Skip confirmation")
        )
        .arg(
            Arg::new("orphans")
                .short('o')
                .long("check-orphans")
                .takes_value(false)
                .help("Process orphan EBS volumes/snapshots")
        )
        .arg(
            Arg::new("manifest")
                .short('m')
                .long("manifest")
                .takes_value(true)
                .required(true)
                .default_value("22.04")
                .help("Exclude Manifest(s). Can pass comma separated list")
        )
        .get_matches();

    // handle comma separated list of potential regions
    let regions = matches
        .value_of("region")
        .unwrap()
        .split(',')
        .map(String::from)
        .collect::<Vec<String>>();

    // handle comma separated list of potential inputs
    let inputs = matches
        .value_of("input")
        .unwrap_or("")
        .split(',')
        .map(String::from)
        .collect::<Vec<String>>();

    let helix_manifest_amis = include_helix_manifest(matches.value_of("manifest").unwrap()).await?;

    info!("Image IDs from provided manifests: {}", &helix_manifest_amis.len(),);

    for region in regions {
        info!("working {}", &region);

        let mut ami_image_ids = fetch_instances_image_ids(&region).await?;
        let unattached_lc_image_ids = fetch_unattached_lc_image_ids(&region).await?;

        info!("Image IDs from non-terminated instances: {}", &ami_image_ids.len());
        info!("Image IDs from launch templates: {}", &unattached_lc_image_ids.len());

        ami_image_ids.extend(unattached_lc_image_ids);
        ami_image_ids.extend(helix_manifest_amis.clone());
        ami_image_ids.sort_unstable();
        ami_image_ids.dedup();

        info!("Total of {} utilized Image IDs", &ami_image_ids.len());

        if !inputs.is_empty() {
            for input in &inputs {
                let input = input.trim();
                info!("Searching off of {} in {}", &input, &region);

                // mutable vec to hold all of the found image_ids
                let mut collected: Vec<(String, DateTime<Utc>, String, &str)> = Vec::new();

                // fetch all the AMIs that are available and await the response
                match fetch_available_image_ids(input, &region).await {
                    Ok(amis) => {
                        if let Some(ami) = amis.images {
                            for entry in ami {
                                let image_id = entry.image_id.unwrap();
                                let name = entry.name.unwrap();
                                let creation_date =
                                    entry.creation_date.unwrap().parse::<DateTime<Utc>>()?;
                                if !ami_image_ids.contains(&image_id.to_string()) {
                                    collected.push((name, creation_date, image_id, &region));
                                }
                            }
                        }
                        info!("Total of {} {} Image IDs", collected.len(), &region);
                    }
                    Err(e) => {
                        eprint!("{DEATH} {e:?}");
                        exit(1)
                    }
                }

                // sort it based off of creation_date
                collected.sort_by_key(|key| key.1);

                // run if "days" was provided a value
                if matches.value_of("days").is_some() {
                    collected = collected
                        .into_iter()
                        .filter(|(_, date, ..)| {
                            *date
                                < Utc::now()
                                    .checked_sub_signed(Duration::days(
                                        matches.value_of("days").unwrap().parse::<i64>().unwrap()
                                    ))
                                    .unwrap()
                        })
                        .collect::<Vec<(String, DateTime<Utc>, String, &str)>>();
                }

                // run if "keep" was provided a value
                if matches.value_of("keep").is_some() {
                    // keep what we want
                    let keep =
                        collected.len() - matches.value_of("keep").unwrap().parse::<usize>()?;
                    // truncate it by dropping the excess keeping what we want
                    collected.truncate(keep);
                }

                if collected.is_empty() {
                    info!("No Image IDs Found. Skipping");
                } else {
                    // print a pretty table
                    make_table(&collected);

                    if matches.is_present("force") {
                        // short circuit if "forced" is passed
                        info!("Not even going to look? yolo {}", DISAPPROVE);
                        destroy_image_id(&region, &collected).await?;
                    } else {
                        // otherwise we should prompt for user input
                        let mut confirmation = String::new();
                        println!("Destroy found Image IDs? y/n");
                        stdin().read_line(&mut confirmation).expect("Failed to read line");
                        match confirmation.trim().to_ascii_lowercase().as_str() {
                            "n" | "N" => continue,
                            "y" | "Y" => destroy_image_id(&region, &collected).await?,
                            _ => eprintln!("Please use either y or n")
                        }
                    }
                }

                if matches.is_present("orphans") {
                    let orphans = fetch_orphans(&region).await?;
                    if orphans.is_empty() {
                        info!("No Orphans Found. Skipping");
                    } else if matches.is_present("force") {
                        make_table_orphans(&region, &orphans);
                        // short circuit if "forced" is passed
                        info!("Not even going to look? yolo {}", DISAPPROVE);
                        info!("pew pew");
                        destroy_orphans_id(&region, &orphans).await?;
                    } else {
                        make_table_orphans(&region, &orphans);
                        // otherwise we should prompt for user input
                        let mut confirmation = String::new();
                        println!("Destroy found Orphans? y/n");
                        stdin().read_line(&mut confirmation).expect("Failed to read line");
                        match confirmation.trim().to_ascii_lowercase().as_str() {
                            "n" | "N" => continue,
                            "y" | "Y" => {
                                info!("pew pew");
                                destroy_orphans_id(&region, &orphans).await?;
                            }
                            _ => eprintln!("Please use either y or n")
                        }
                    }
                }
                info!("Finished! {}", PARTY);
            }
        }
    }

    Ok(())
}

/// Finds Image IDs from that we are the owner of
async fn fetch_available_image_ids(
    input: &str,
    region: &str
) -> Result<DescribeImagesOutput, Box<dyn Error>> {
    // Retrieve from your aws account your custom AMIs

    let client = get_aws_client(region).await?;

    // create our filter
    let ami_filter = format!("*{input}*");
    let filter = vec![
        Filter::builder().set_name(Some("name".into())).set_values(Some(vec![ami_filter])).build(),
        Filter::builder()
            .set_name(Some("is-public".into()))
            .set_values(Some(vec!["false".into()]))
            .build(),
    ];

    match client.describe_images().set_filters(Some(filter)).send().await {
        Ok(result) => Ok(result),
        Err(e) => Err(e.into())
    }
}

/// Finds Image IDs from non-terminated EC2 Instances
async fn fetch_instances_image_ids(region: &str) -> Result<Vec<String>, Box<dyn Error>> {

    let client = get_aws_client(region).await?;

    let filter = vec![Filter::builder()
        .set_name(Some("instance-state-name".into()))
        .set_values(Some(vec![
            "pending".to_string(),
            "running".to_string(),
            "shutting-down".to_string(),
            "stopping".to_string(),
            "stopped".to_string(),
        ]))
        .build()];

    let mut image_ids: Vec<String> = vec![];

    let mut request = client
        .describe_instances()
        .set_filters(Some(filter))
        .into_paginator()
        .page_size(100)
        .send();

    info!("Fetching all instances for their image id");
    while let Some(results) = request.next().await {
        if let Some(reservations) = results.unwrap().reservations() {
            for reservation in reservations {
                for instance in reservation.instances().unwrap() {
                    if let Some(id) = instance.image_id() {
                        image_ids.push(id.to_string());
                    }
                }
            }
        }
    }

    Ok(image_ids)
}

/// Finds Image IDs from launch configurations that are unattached to an
/// autoscaling group
async fn fetch_unattached_lc_image_ids(region: &str) -> Result<Vec<String>, Box<dyn Error>> {

    let client = get_aws_client(region).await?;

    let mut image_ids: Vec<String> = vec![];

    let _filter = Filter::builder()
        .set_name(Some("is-default-version".into()))
        .set_values(Some(vec!["true".to_string()]))
        .build();

    let mut request = client
        .describe_launch_template_versions()
        .versions("$Latest".to_string())
        .into_paginator()
        .page_size(100)
        .send();

    info!("Looking up in use launch_templates for their image id");
    while let Some(results) = request.next().await {
        if let Ok(result) = results {
            if let Some(templates) = result.launch_template_versions() {
                for template in templates {
                    if let Some(data) = template.launch_template_data() {
                        if let Some(id) = data.image_id() {
                            image_ids.push(id.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(image_ids)
}

/// Find potential orphaned EBS Volumes and Snapshots
#[allow(clippy::too_many_lines)]
async fn fetch_orphans(
    region: &str
) -> Result<HashMap<String, (String, DateTime<Utc>)>, Box<dyn Error>> {

    let client = get_aws_client(region).await?;

    // Snapshots
    let mut collection = HashMap::new();

    let filter = vec![
        Filter::builder()
            .set_name(Some("status".into()))
            .set_values(Some(vec!["completed".to_string()]))
            .build(),
        Filter::builder()
            .set_name(Some("description".into()))
            .set_values(Some(vec![
                "Created by CreateImage*".into(),
                "Created by EBSSnapshotScheduler*".into(),
                "Created for policy*".into(),
            ]))
            .build(),
    ];

    let mut request = client
        .describe_snapshots()
        .set_filters(Some(filter))
        .into_paginator()
        .page_size(100)
        .send();

    info!("Looking up snapshots");
    while let Some(results) = request.next().await {
        if let Ok(result) = results {
            if let Some(snapshots) = result.snapshots() {
                for snapshot in snapshots {
                    let mut name = String::new();
                    if let Some(tags) = snapshot.tags() {
                        for tag in tags {
                            if tag.key == Some("Name".into()) {
                                name = tag.value().unwrap().into();
                            }
                        }
                    }

                    if let Some(start_time) = snapshot.start_time() {
                        if Duration::seconds(start_time.secs()).num_weeks() > 14 {
                            let creation = DateTime::<Utc>::from_utc(
                                NaiveDateTime::from_timestamp_opt(start_time.secs(), 0).unwrap(),
                                Utc
                            );
                            collection.insert(
                                snapshot.snapshot_id.as_ref().unwrap().to_string(),
                                (name.to_string(), creation)
                            );
                        };
                    }
                }
            }
        }
    }

    // EBS Volumes

    let filter = vec![Filter::builder()
        .set_name(Some("status".into()))
        .set_values(Some(vec!["available".to_string()]))
        .build()];

    let mut in_use_volumes = HashMap::new();
    let mut available_volumes = HashMap::new();

    let mut request =
        client.describe_volumes().set_filters(Some(filter)).into_paginator().page_size(100).send();

    info!("Looking up volumes");
    while let Some(results) = request.next().await {
        if let Ok(result) = results {
            if let Some(volumes) = result.volumes() {
                for volume in volumes {
                    let mut name = String::new();
                    if let Some(tags) = volume.tags() {
                        for tag in tags {
                            if tag.key == Some("Name".into()) {
                                name = tag.value().unwrap().into();
                            }
                        }
                    }
                    let volume_id = volume.volume_id().unwrap();
                    let snapshot_id = volume.snapshot_id().unwrap();
                    let creation = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp_opt(volume.create_time().unwrap().secs(), 0)
                            .unwrap(),
                        Utc
                    );

                    match volume.state().unwrap() {
                        VolumeState::Available => {
                            available_volumes.insert(volume_id.to_string(), (name, creation));
                        }
                        VolumeState::InUse => {
                            in_use_volumes
                                .insert(snapshot_id.to_string(), (name.to_string(), creation));
                        }
                        _ => {}
                    };
                }
            }
        }
    }

    // retain only those that are not in use
    let drain: HashMap<String, (String, DateTime<Utc>)> =
        collection.extract_if(|k, _v| !in_use_volumes.contains_key(k)).collect();

    available_volumes.extend(drain);

    Ok(available_volumes)
}

async fn destroy_image_id(
    region: &str,
    data: &[(String, DateTime<Utc>, String, &str)]
) -> Result<(), Box<dyn Error>> {
    // create a progress bar
    let pb = ProgressBar::new(data.len() as u64);

    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {msg}")?
            .progress_chars("#>-")
    );

    let client = get_aws_client(region).await?;

    for (_name, _data, image_id, _) in data {
        if client.deregister_image().image_id(image_id).send().await.is_ok() {
            pb.set_message(format!("{SPARKLE} {image_id}"));
        }
        pb.inc(1);
    }
    pb.finish_with_message(format!("{PARTY}"));
    Ok(())
}

async fn destroy_orphans_id(
    region: &str,
    orphans: &HashMap<String, (String, DateTime<Utc>)>
) -> Result<(), Box<dyn Error>> {
    // create a progress bar
    let pb = ProgressBar::new(orphans.len() as u64);

    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {msg}")?
            .progress_chars("#>-")
    );

    let client = get_aws_client(region).await?;

    let mut failures = vec![];

    for volume in orphans.keys() {
        if volume.starts_with("vol-") {
            match client.delete_volume().volume_id(volume).send().await {
                Ok(_) => {
                    pb.inc(1);
                    pb.set_message(format!("{SPARKLE} {volume}"));
                }
                Err(e) => {
                    failures.push(format!("{volume}: {e}"));

                    println!("failed {volume}: {:#?}", e);
                }
            }
        } else {
            match client.delete_snapshot().snapshot_id(volume).send().await {
                Ok(_) => {
                    pb.inc(1);
                    pb.set_message(format!("{SPARKLE} {volume}"));
                }
                Err(e) => {
                    failures.push(format!("{volume}: {e}"));
                    println!("failed {volume}: {:#?}", e);
                }
            }
        }
    }
    pb.finish_with_message(format!("{PARTY}"));
    println!("{:?}", failures);
    Ok(())
}

fn make_table_orphans(region: &str, volumes: &HashMap<String, (String, DateTime<Utc>)>) {
    let mut table = Table::new();
    let format = format::FormatBuilder::new()
        .column_separator('|')
        .borders('|')
        .separators(
            &[format::LinePosition::Top, format::LinePosition::Bottom],
            format::LineSeparator::new('-', '+', '+', '+')
        )
        .padding(1, 1)
        .build();
    table.set_format(format);
    table.set_titles(row!["Name", "Volume/Snapshot", "Creation Time", "Region"]);
    for (volume, (name, date)) in volumes {
        table.add_row(row![name, volume, date, region]);
    }
    table.printstd();
    println!("Total: {}", &volumes.len());
}

fn make_table(data: &[(String, DateTime<Utc>, String, &str)]) {
    let mut table = Table::new();
    let format = format::FormatBuilder::new()
        .column_separator('|')
        .borders('|')
        .separators(
            &[format::LinePosition::Top, format::LinePosition::Bottom],
            format::LineSeparator::new('-', '+', '+', '+')
        )
        .padding(1, 1)
        .build();
    table.set_format(format);
    table.set_titles(row!["Name", "ID", "Creation Date", "Region"]);
    for ami in data {
        table.add_row(row![ami.0, ami.2, ami.1, ami.3]);
    }
    table.printstd();
    println!("Total: {}", &data.len());
}

async fn include_helix_manifest(manifests: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let user = env::var("GITHUB_USER").expect("GITHUB_USER is not set");
    let pass = env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN is not set");

    let mut amis = vec![];
    let mut manifests =
        manifests.split(',').map(String::from).collect::<Vec<String>>();
    manifests.push("ci".into());

    info!("Grabbing manifests");
    for manifest in manifests {
        let url = format!(
            "https://git-us-west-2.cloud.apps.fireeye.com/raw/SRE/tap-manifests/{}/manifest.json",
            &manifest
        );

        let client = reqwest::Client::builder().use_rustls_tls().build()?;

        let body = client.get(&url).basic_auth(&user, Some(&pass)).send().await?.text().await?;

        let manifest: Value = serde_json::from_str(body.as_str())?;

        // probably a better way to do this
        manifest["amis"].as_object().unwrap().into_iter().for_each(|(_, v)| {
            v.as_object().unwrap().into_iter().for_each(|(_, v)| amis.push(v.to_string()));
        });
    }

    Ok(amis)
}


async fn get_aws_client(region: &str) -> Result<Client, Box<dyn Error>> {
    let credentials_provider = DefaultCredentialsChain::builder().build().await;

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .region(AwsRegion::new(region.to_string()))
        .load()
        .await;

    Ok(Client::new(&config))
}