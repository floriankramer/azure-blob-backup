/**
    Copyright (C) 2023  Florian Kramer

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
use anyhow::{anyhow, Result};
use azure_storage_blobs::prelude::*;
use futures::stream::StreamExt;
use std::{
    collections::HashMap,
    fmt::Display,
    io::{Read, Seek, Write},
    os::unix::prelude::{MetadataExt, PermissionsExt},
};
use walkdir;

use crate::config::Config;

pub async fn run(conf: &Config) -> Result<()> {
    // Get the config values
    let local_root = conf.get_string("local_root")?;
    let sas_url = conf.get_string("sas_url")?;
    let min_update_age = conf.get_i64("min_update_age")?;
    let num_daily = conf.get_i64("num_daily")?;
    let num_weekly = conf.get_i64("num_weekly")?;
    let num_monthly = conf.get_i64("num_monthly")?;

    if num_daily < 0 || num_daily > 7 {
        return Err(anyhow!(
            "Malformed config: num daily has to be in the interval of [1;7], but is {}",
            num_daily
        ));
    }
    if num_weekly < 0 || num_weekly > 4 {
        return Err(anyhow!(
            "Malformed config: num weekly has to be in the interval of [0;4], but is {}",
            num_weekly
        ));
    }
    if num_monthly < 0 {
        return Err(anyhow!(
            "Malformed config: num num_monthly has to be non-negative, but is {}",
            num_monthly
        ));
    }
    if num_monthly == 0 && num_weekly == 0 && num_daily == 0 {
        return Err(anyhow!(
            "Malformed config: requested for no backups to be kept."
        ));
    }

    log::info!("Uploading {}", local_root);

    // Create the local index
    log::info!("Begin indexing of the local storage");
    let local = create_local_index(&local_root)?;
    log::info!("Indexed the local storage with {} files", local.files.len());

    // Create the remote index
    log::info!("Begin indexing of the remote storage");
    let mut remote = create_remote_index(&sas_url).await?;
    log::info!(
        "Indexed the remote storage with {} files",
        remote.files.len()
    );

    // Run an update
    log::info!("Begin syncronization of the local and remote storage");
    sync_remote_index(
        &local,
        &mut remote,
        &sas_url,
        &local_root,
        min_update_age as u64,
        num_daily as u64,
        num_weekly as u64,
        num_monthly as u64,
    )
    .await?;

    Ok(())
}

fn create_local_index(root: &str) -> Result<Index> {
    let mut index = Index::new();

    let walker = walkdir::WalkDir::new(root);
    let walker = walker.follow_links(false);

    for entry in walker {
        let entry = entry?;
        let file_type = entry.file_type();

        if file_type.is_file() || file_type.is_symlink() || file_type.is_dir() {
            let path = entry.path().to_str();
            match path {
                Some(path) => {
                    let path = path.to_string();
                    // Strip the root path, make sure we have a leading slash
                    let mut path: String = path.chars().skip(root.len()).collect();
                    if path.is_empty() || path.as_bytes()[0] != b'/' {
                        path = "/".to_string() + &path;
                    }

                    index.files.insert(path, vec![entry.try_into()?]);
                }
                None => {
                    return Err(anyhow!(
                        "Found a path with non unicode characters: {:?}",
                        entry.path()
                    ));
                }
            }
        }
    }

    Ok(index)
}

async fn create_remote_index(sas_url: &str) -> Result<Index> {
    let mut index = Index::new();
    let client = ContainerClient::from_sas_url(&url::Url::parse(sas_url)?)?;

    let list_builder = client.list_blobs();
    let mut list_stream = list_builder.into_stream();

    while let Some(page) = list_stream.next().await {
        for blob in page?.blobs.blobs() {
            let path = "/".to_string() + &blob.name;
            let last_delim = path.rfind('/');

            if last_delim.is_none() {
                return Err(anyhow!("Malformed remote path: {}", path));
            }
            let last_delim = last_delim.unwrap();

            if last_delim + 1 >= path.len() {
                return Err(anyhow!("Malformed remote path (trailing slash): {}", path));
            }

            let version =
                Version::try_from(std::str::from_utf8(&path.as_bytes()[last_delim + 1..])?)?;
            let file_path = std::str::from_utf8(&path.as_bytes()[..last_delim])?.to_string();

            let versions = index.files.get_mut(&file_path);
            match versions {
                Some(versions) => {
                    versions.push(version);
                }
                None => {
                    index.files.insert(file_path, vec![version]);
                }
            }
        }
    }

    Ok(index)
}

async fn upload_file(
    version: &Version,
    path: &str,
    local_root: &str,
    client: &mut ContainerClient,
) -> Result<()> {
    let remote_path = path.to_owned() + "/" + &version.serialize();
    let local_path = local_root.to_string() + path;

    let blob = client.blob_client(&remote_path);

    match version.file_type {
        FileType::Symlink => {
            let link = std::fs::read_link(local_path)?;
            let link = link.to_string_lossy().to_string().into_bytes();
            blob.put_block_blob(link).await?;
        }
        FileType::Folder => {
            blob.put_block_blob(vec![]).await?;
        }
        FileType::Regular => {
            // Stream up the file
            let mut file = std::fs::File::open(&local_path)?;
            // Get the file length
            let len = file.seek(std::io::SeekFrom::End(0))?;
            file.seek(std::io::SeekFrom::Start(0))?;

            // Azure block storage expects a whole bunch of blocks to be uploaded and then merged into a blob.
            // We choose at least 4MiB per block, or try to aim for 25000 blocks per blob (half of the max of 50000).
            let block_size = std::cmp::max(4 << 20, len / 25000);

            // If our file size is not a multiple of the block size we need a partially filled block
            let mut num_blocks = len / block_size;
            if len % block_size != 0 {
                num_blocks += 1;
            }

            // Every block needs an id, so we use a combination of the block index in the blob and a hash of the filename
            let id_suffix = sha256::digest(remote_path);

            let mut block_buf: Vec<u8> = Vec::new();
            block_buf.resize(block_size as usize, 0);

            let mut block_list = Vec::<BlobBlockType>::new();

            for i in 0..num_blocks {
                // Generate an id
                let mut block_id = format!("{i:016}{id_suffix}");
                block_id.truncate(64);
                let block_id = BlockId::from(block_id);

                // load the block from disk
                let num_read = file.read(&mut block_buf[..])?;

                // upload the block
                let payload = Vec::from(&mut block_buf[0..num_read]);
                blob.put_block(block_id.clone(), payload).await?;

                // remember its id
                block_list.push(BlobBlockType::Uncommitted(block_id));
            }

            // commit the blocks
            blob.put_block_list(BlockList { blocks: block_list })
                .await?;
        }
        FileType::Deleted => {
            blob.put_block_blob(vec![]).await?;
        }
    }

    Ok(())
}

async fn delete_file_version(
    version: &Version,
    path: &str,
    client: &mut ContainerClient,
) -> Result<()> {
    let remote_path = path.to_owned() + "/" + &version.serialize();

    let blob = client.blob_client(remote_path);
    blob.delete().await?;

    Ok(())
}

async fn sync_remote_index(
    local: &Index,
    remote: &mut Index,
    sas_url: &str,
    local_root: &str,
    min_update_age: u64,
    num_daily: u64,
    num_weekly: u64,
    num_monthly: u64,
) -> Result<()> {
    let mut client = ContainerClient::from_sas_url(&url::Url::parse(sas_url)?)?;

    let mut processed: usize = 0;
    let total_files = local.files.len();

    log::info!("Finding new files to upload");
    for local_entry in &local.files {
        if local_entry.1.len() != 1 {
            return Err(anyhow!(
                "Malformed local index: Path with not exactly one version."
            ));
        }

        let mut update = true;

        let remote_entry = remote.files.get_mut(local_entry.0);
        match remote_entry {
            Some(remote_entry) => {
                for version in remote_entry.iter() {
                    // If we have the exact version, or one that is within the min_update_age period
                    // don't do anything.
                    if version == &local_entry.1[0]
                        || (local_entry.1[0].upload_time > version.upload_time
                            && local_entry.1[0].upload_time - version.upload_time < min_update_age)
                    {
                        update = false;
                    }
                }
                if update {
                    // Add the new version
                    remote_entry.push(local_entry.1[0].clone());
                }
            }
            None => {
                update = true;
                remote
                    .files
                    .insert(local_entry.0.clone(), vec![local_entry.1[0].clone()]);
            }
        }

        if update {
            upload_file(&local_entry.1[0], local_entry.0, local_root, &mut client).await?;
        }

        processed += 1;
        print!("\r{processed} / {total_files}");

        // If the flush fails its not the end of the world.
        #[allow(unused_must_use)]
        {
            std::io::stdout().flush();
        }
    }
    println!("");

    let mut processed: usize = 0;
    let total_files = remote.files.len();
    log::info!("Finding deleted files");
    // Check for remote files that were deleted locally
    for remote_entry in &mut remote.files {
        if remote_entry.1.is_empty() {
            log::error!(
                "Malformed remote index: empty version list for {}",
                &remote_entry.0
            );

            processed += 1;
            print!("\r{processed} / {total_files}");
            continue;
        }

        if !local.files.contains_key(remote_entry.0) {
            // Find the newest remote version
            let mut version = remote_entry.1[0].clone();
            for i in 1..remote_entry.1.len() {
                if remote_entry.1[i].upload_time > version.upload_time {
                    version = remote_entry.1[i].clone();
                }
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();

            if now < version.upload_time || now - version.upload_time < min_update_age {
                // The latest entry is up to date enough, don't do anything
                continue;
            }

            version.size = 0;
            version.mod_time = 0;
            version.upload_time = now;
            version.file_type = FileType::Deleted;

            // Create an entry on the remote for the deleted file. This is needed to gradually remove old files
            upload_file(&version, remote_entry.0, local_root, &mut client).await?;
            remote_entry.1.push(version);
        }

        processed += 1;
        print!("\r{processed} / {total_files}");

        // If the flush fails its not the end of the world.
        #[allow(unused_must_use)]
        {
            std::io::stdout().flush();
        }
    }
    println!("");

    // Remove uneeded remote versions

    struct VersionBucket {
        start: u64,
        end: u64,
        versions: Vec<usize>,
    }

    struct BucketedVersion {
        start: u64,
        end: u64,
        bucket_count: usize,
        version_idx: usize,
    }

    let day = 60 * 60 * 24;
    let week = day * 7;
    let month = week * 4;
    let mut buckets = Vec::<VersionBucket>::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    for i in 0..num_daily {
        buckets.push(VersionBucket {
            start: now - (i + 1) * day,
            end: now - i * day,
            versions: Vec::new(),
        });
    }
    for i in 0..num_weekly {
        buckets.push(VersionBucket {
            start: now - (i + 1) * week,
            end: now - i * week,
            versions: Vec::new(),
        });
    }
    for i in 0..num_monthly {
        buckets.push(VersionBucket {
            start: now - (i + 1) * month,
            end: now - i * month,
            versions: Vec::new(),
        });
    }

    let mut processed: usize = 0;
    let total_files = remote.files.len();
    log::info!("Finding and deleting unneeded versions");
    for remote_entry in &mut remote.files {
        // The num_daily, weekly and monthly values defines buckets starting from now and going backwards in time.
        // Every file version represents an extent in time starting at the upload_time and reaching either to the next upload
        // time or until now.
        // For a version to be kept it has to be unique in one bucket, and has to be in at least one bucket. A version can be
        // in more than one bucket.
        // If a bucket has several versions of which only one should be kept, delete all but the oldest.
        // If all versions of a file are of type Deleted, they should all be deleted.

        // Reset the buckets
        for bucket in &mut buckets {
            bucket.versions.clear();
        }

        // Start by sorting entries by their upload time
        remote_entry
            .1
            .sort_by(|a, b| a.upload_time.partial_cmp(&b.upload_time).unwrap());

        // Wrap the versions in BucketedVersions
        let mut bucketed_versions = Vec::<BucketedVersion>::new();

        for i in 0..remote_entry.1.len() {
            // Turn the version into a bucketed version
            let mut end = now;
            if i + 1 < remote_entry.1.len() {
                end = remote_entry.1[i + 1].upload_time;
            }
            // Avoid entries always reaching into the next day.
            end -= day / 2;
            end = std::cmp::max(end, remote_entry.1[i].upload_time + 1);

            let bucketed = BucketedVersion {
                start: remote_entry.1[i].upload_time,
                end,
                bucket_count: 0,
                version_idx: i,
            };

            bucketed_versions.push(bucketed);
            let bucketed = bucketed_versions.last_mut().unwrap();

            // add it to all buckets it intersects
            for bucket in &mut buckets {
                if bucket.start < bucketed.end && bucket.end >= bucketed.start {
                    bucketed.bucket_count += 1;

                    bucket.versions.push(bucketed.version_idx);
                }
            }
        }

        // make all buckets have only one entry
        for bucket in &mut buckets {
            if bucket.versions.len() <= 1 {
                continue;
            }

            // find the oldest upload time
            let mut oldest = remote_entry.1[bucket.versions[0]].upload_time;
            let mut oldest_idx = 0;
            for i in 1..bucket.versions.len() {
                let upload_time = remote_entry.1[bucket.versions[i]].upload_time;
                if upload_time < oldest {
                    oldest_idx = i;
                    oldest = upload_time;
                }
            }

            // kick everything out of the bucket that is not the oldest
            for idx in bucket.versions.iter() {
                if idx != &oldest_idx {
                    // It's enough to reduce the counter, we don't need the bucket.versions list after this
                    bucketed_versions[*idx].bucket_count -= 1;
                }
            }
        }

        // Delete versions which aren't in a bucket
        for bucketed in &bucketed_versions {
            if bucketed.bucket_count == 0 {
                let version = &remote_entry.1[bucketed.version_idx];
                delete_file_version(version, remote_entry.0, &mut client).await?;
            }
        }

        processed += 1;
        print!("\r{processed} / {total_files}");

        // If the flush fails its not the end of the world.
        #[allow(unused_must_use)]
        {
            std::io::stdout().flush();
        }
    }
    println!("");

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum FileType {
    Regular,
    Symlink,
    Folder,
    Deleted,
}

impl FileType {
    fn parse(raw: &str) -> Result<FileType> {
        if raw == "Regular" {
            Ok(FileType::Regular)
        } else if raw == "Symlink" {
            Ok(FileType::Symlink)
        } else if raw == "Folder" {
            Ok(FileType::Folder)
        } else if raw == "Deleted" {
            Ok(FileType::Deleted)
        } else {
            Err(anyhow!("{} is not a file type", raw))
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Regular => f.write_str("Regular"),
            FileType::Symlink => f.write_str("Symlink"),
            FileType::Folder => f.write_str("Folder"),
            FileType::Deleted => f.write_str("Deleted"),
        }
    }
}

#[derive(Debug, Clone)]
struct Version {
    mod_time: u64,
    upload_time: u64,
    permissions: u32,
    size: u64,
    file_type: FileType,
    owner: u32,
    group: u32,
}

impl Version {
    fn serialize(&self) -> String {
        format!(
            "{}-{}-{:o}-{}-{}-{}-{}",
            self.mod_time,
            self.upload_time,
            self.permissions,
            self.size,
            self.file_type,
            self.owner,
            self.group
        )
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.serialize())?;

        Ok(())
    }
}

impl PartialEq for Version {
    fn eq(&self, other: &Self) -> bool {
        // Upload time is ignored as that is metadata and not part of the version
        self.mod_time == other.mod_time
            && self.permissions == other.permissions
            && self.size == other.size
            && self.file_type == other.file_type
            && self.owner == other.owner
            && self.group == other.group
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Version) -> Option<std::cmp::Ordering> {
        self.upload_time.partial_cmp(&other.upload_time)
    }
}

impl TryFrom<walkdir::DirEntry> for Version {
    type Error = anyhow::Error;

    fn try_from(entry: walkdir::DirEntry) -> Result<Self> {
        let metadata = entry.metadata()?;

        let mut file_type = FileType::Regular;
        if entry.file_type().is_symlink() {
            file_type = FileType::Symlink;
        } else if entry.file_type().is_dir() {
            file_type = FileType::Folder;
        }

        Ok(Version {
            mod_time: metadata
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            upload_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            permissions: metadata.permissions().mode(),
            size: metadata.size(),
            file_type,
            owner: metadata.uid(),
            group: metadata.gid(),
        })
    }
}

impl TryFrom<&str> for Version {
    type Error = anyhow::Error;

    fn try_from(path: &str) -> Result<Self> {
        let parts = path.split('-');
        let collected: Vec<&str> = parts.collect();

        if collected.len() != 7 {
            return Err(anyhow!("Malformed version string {}", path));
        }

        Ok(Version {
            mod_time: collected[0].parse()?,
            upload_time: collected[1].parse()?,
            permissions: u32::from_str_radix(collected[2], 8)?,
            size: collected[3].parse()?,
            file_type: FileType::parse(collected[4])?,
            owner: collected[5].parse()?,
            group: collected[6].parse()?,
        })
    }
}

struct Index {
    files: HashMap<String, Vec<Version>>,
}

impl Index {
    fn new() -> Index {
        Index {
            files: HashMap::new(),
        }
    }
}
