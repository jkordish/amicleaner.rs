//! Local cache of launch template versions. A version's data never changes after it's created and
//! version numbers are never reused, so later runs only fetch versions newer than the cache.
//!
//! Safety: the cache can only *add* references. Every cached version is replayed as a reference;
//! nothing in the cache removes one. A cached version that has since been deleted over-protects
//! its AMI until the template's next full rescan (`--cache-max-age-days`), which replaces the entry.
//! Templates that no longer exist are pruned. The raw image reference is stored (not the resolved
//! AMI), so `resolve:ssm:` references are resolved fresh on every run.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use log::warn;
use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;

/// Where the cache lives and how long a template's full scan stays trusted.
#[derive(Clone, Debug)]
pub(crate) struct LtCacheConfig {
    pub(crate) dir: PathBuf,
    pub(crate) account_id: String,
    pub(crate) max_age: Duration,
}

impl LtCacheConfig {
    pub(crate) fn path(&self, region: &str) -> PathBuf {
        let safe = |value: &str| {
            value
                .chars()
                .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
                .collect::<String>()
        };
        self.dir.join(format!("launch-templates-{}-{}.json", safe(&self.account_id), safe(region)))
    }
}

/// What's known about one launch template (keyed by id in [`LaunchTemplateCache`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct CachedTemplate {
    /// Every version up to and including this number has been read.
    pub(crate) scanned_through: i64,
    /// When every version of the template was last read.
    pub(crate) full_scan_at: DateTime<Utc>,
    /// Version number -> raw image reference, for versions that set an image.
    pub(crate) versions: BTreeMap<i64, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct LaunchTemplateCache {
    pub(crate) schema_version: u32,
    pub(crate) account_id: String,
    pub(crate) region: String,
    pub(crate) templates: BTreeMap<String, CachedTemplate>,
}

/// Which versions of a template need fetching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FetchPlan {
    /// Every version (no usable cache entry).
    Full,
    /// Versions from this number on; earlier ones come from the cache.
    From(i64),
    /// Nothing; the cache covers every version.
    UpToDate,
}

/// Decides how much of a template to fetch. Anything surprising falls back to a full scan, which is
/// always correct; only a fresh entry that covers exactly up to `latest` skips the API.
pub(crate) fn fetch_plan(
    cached: Option<&CachedTemplate>,
    latest: Option<i64>,
    now: DateTime<Utc>,
    max_age: Duration,
) -> FetchPlan {
    let (Some(cached), Some(latest)) = (cached, latest) else {
        return FetchPlan::Full;
    };
    let age = now - cached.full_scan_at;
    // A future timestamp (clock skew) or an expired entry gets a full rescan, which is also what
    // drops versions deleted since the entry was built.
    if age < Duration::zero() || age >= max_age {
        return FetchPlan::Full;
    }
    match latest.cmp(&cached.scanned_through) {
        std::cmp::Ordering::Equal => FetchPlan::UpToDate,
        std::cmp::Ordering::Greater => FetchPlan::From(cached.scanned_through + 1),
        // Version numbers only grow, so this means the cache doesn't describe this template.
        std::cmp::Ordering::Less => FetchPlan::Full,
    }
}

impl LaunchTemplateCache {
    pub(crate) fn empty(account_id: &str, region: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            account_id: account_id.to_string(),
            region: region.to_string(),
            templates: BTreeMap::new(),
        }
    }

    /// The cache at `path`, or an empty one when it's missing or unusable (with a warning for
    /// anything other than "missing"). An unusable cache only costs a full scan.
    pub(crate) fn load(path: &Path, account_id: &str, region: &str) -> Self {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::empty(account_id, region);
            }
            Err(error) => {
                warn!("ignoring launch template cache {}: {error}", path.display());
                return Self::empty(account_id, region);
            }
        };
        match serde_json::from_reader::<_, Self>(BufReader::new(file)) {
            Ok(cache)
                if cache.schema_version == SCHEMA_VERSION
                    && cache.account_id == account_id
                    && cache.region == region =>
            {
                cache
            }
            Ok(_) => {
                warn!(
                    "ignoring launch template cache {}: written for another account, region or version",
                    path.display()
                );
                Self::empty(account_id, region)
            }
            Err(error) => {
                warn!("ignoring launch template cache {}: {error}", path.display());
                Self::empty(account_id, region)
            }
        }
    }

    /// Writes to a temporary file and renames it into place, so a crash never leaves a torn cache.
    pub(crate) fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension(format!("json.tmp-{}", std::process::id()));
        let result = (|| {
            let mut writer = BufWriter::new(File::create(&tmp)?);
            serde_json::to_writer(&mut writer, self)?;
            writer.flush()?;
            writer.into_inner().map_err(std::io::IntoInnerError::into_error)?.sync_all()?;
            fs::rename(&tmp, path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    /// Drops templates that weren't listed this run (deleted templates can't launch anything).
    pub(crate) fn prune(&mut self, listed_ids: &HashSet<&str>) {
        self.templates.retain(|id, _| listed_ids.contains(id.as_str()));
    }
}

/// Default cache directory: `$XDG_CACHE_HOME/amicleaner`, else `$HOME/.cache/amicleaner`.
pub(crate) fn default_cache_dir(
    xdg_cache_home: Option<&str>,
    home: Option<&str>,
) -> Option<PathBuf> {
    fn non_empty(value: Option<&str>) -> Option<&str> {
        value.filter(|value| !value.trim().is_empty())
    }
    non_empty(xdg_cache_home)
        .map(PathBuf::from)
        .or_else(|| non_empty(home).map(|home| Path::new(home).join(".cache")))
        .map(|base| base.join("amicleaner"))
}
