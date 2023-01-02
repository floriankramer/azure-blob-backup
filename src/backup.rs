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
    os::unix::prelude::{MetadataExt, PermissionsExt},
};
use walkdir;

use crate::config::Config;

pub async fn run(conf: &Config) -> Result<()> {
    // Get the config values
    let local_root = conf.get_string("local_root")?;
    let sas_url = conf.get_string("sas_url")?;
    let versions_to_keep = conf.get_i64("num_versions")?;

    // Create the local index
    let local = create_local_index(&local_root)?;

    // Create the remote index
    let remote = create_remote_index(&sas_url).await?;

    // Run an update
    sync_remote_index(&local, &remote, &sas_url, &local_root, versions_to_keep as usize).await?;

    return Ok(());
}

fn create_local_index(root: &str) -> Result<Index> {
    let mut index = Index::new();

    let walker = walkdir::WalkDir::new(root);
    let walker = walker.follow_links(false);

    for entry in walker {
        let entry = entry.unwrap();
        let file_type = entry.file_type();

        if file_type.is_file() || file_type.is_symlink() || file_type.is_dir() {
            let path = entry.path().to_str();
            match path {
                Some(path) => {
                    let path = path.to_string();
                    // Strip the root path, make sure we have a leading slash
                    let mut path: String = path.chars().skip(root.len()).collect();
                    if path.len() == 0 || path.as_bytes()[0] != '/' as u8 {
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

    return Ok(index);
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

            if let None = last_delim {
                return Err(anyhow!("Malformed remote path: {}", path));
            }
            let last_delim = last_delim.unwrap();

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

    return Ok(index);
}

async fn upload_file(
    version: &Version,
    path: &str,
    local_root: &str,
    client: &mut ContainerClient,
) -> Result<()> {
    let remote_path = path.to_owned() + "/" + &version.to_string();
    let local_path = local_root.to_string() + path;

    let blob = client.blob_client(remote_path);

    if version.is_symlink {
        let link = std::fs::read_link(local_path)?;
        let link = link.to_string_lossy().to_string().into_bytes();
        blob.put_block_blob(link).await?;
    } else if version.is_folder {
        blob.put_block_blob(vec![]).await?;
    } else {
        //  For now, just read the whole data into memory. This is not a good idea for big files
        let data = std::fs::read(&local_path)?;
        blob.put_block_blob(data).await?;
    }

    return Ok(());
}

async fn delete_file_version(
    version: &Version,
    path: &str,
    client: &mut ContainerClient,
) -> Result<()> {
    let remote_path = path.to_owned() + "/" + &version.to_string();

    let blob = client.blob_client(remote_path);
    blob.delete().await?;

    return Ok(());
}

async fn sync_remote_index(
    local: &Index,
    remote: &Index,
    sas_url: &str,
    local_root: &str,
    versions_to_keep: usize,
) -> Result<()> {
    let mut client = ContainerClient::from_sas_url(&url::Url::parse(sas_url)?)?;

    for entry in &local.files {
        if entry.1.len() != 1 {
            return Err(anyhow!(
                "Malformed local index: Path with not exactly one version."
            ));
        }

        let mut update = true;

        let remote_entry = remote.files.get(entry.0);
        match remote_entry {
            Some(remote_entry) => {
                for version in remote_entry {
                    if version == &entry.1[0] {
                        update = false;
                    }
                }
            }
            None => {
                update = true;
            }
        }

        if update {
            upload_file(&entry.1[0], entry.0, local_root, &mut client).await?;

            match remote_entry {
                Some(remote_entry) => {
                    if remote_entry.len() >= versions_to_keep {
                        // Delete the oldest
                        let mut oldest = remote_entry[0].clone();
                        for candidate in remote_entry {
                            if candidate < &oldest {
                                oldest = candidate.clone();
                            }
                        }
                        delete_file_version(&oldest, entry.0, &mut client).await?;
                    }
                }
                None => {}
            }
        }
    }

    return Ok(());
}

#[derive(Debug, Clone)]
struct Version {
    mod_time: u64,
    upload_time: u64,
    permissions: u32,
    size: u64,
    is_symlink: bool,
    is_folder: bool,
}

impl Version {
    fn to_string(&self) -> String {
        return format!(
            "{}-{}-{:o}-{}-{}-{}",
            self.mod_time, self.upload_time, self.permissions, self.size, self.is_symlink, self.is_folder
        );
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string())?;
        return Ok(());
    }
}

impl PartialEq for Version {
    fn eq(&self, other: &Self) -> bool {
        // Upload time is ignored as that is metadata and not part of the version
        return self.mod_time == other.mod_time
            && self.permissions == other.permissions
            && self.size == other.size
            && self.is_symlink == other.is_symlink
            && self.is_folder == other.is_folder;
    }

    fn ne(&self, other: &Self) -> bool {
        return !(self == other);
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Version) -> Option<std::cmp::Ordering> {
        return self.upload_time.partial_cmp(&other.upload_time);
    }
}

impl TryFrom<walkdir::DirEntry> for Version {
    type Error = anyhow::Error;

    fn try_from(entry: walkdir::DirEntry) -> Result<Self> {
        let metadata = entry.metadata()?;
        return Ok(Version {
            mod_time: metadata
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            upload_time: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs(),
            permissions: metadata.permissions().mode(),
            size: metadata.size(),
            is_symlink: entry.file_type().is_symlink(),
            is_folder: entry.file_type().is_dir(),
        });
    }
}

impl TryFrom<&str> for Version {
    type Error = anyhow::Error;

    fn try_from(path: &str) -> Result<Self> {
        let parts = path.split('-');
        let collected: Vec<&str> = parts.collect();

        if collected.len() != 6 {
            return Err(anyhow!("Malformed version string {}", path));
        }

        return Ok(Version {
            mod_time: collected[0].parse()?,
            upload_time: collected[1].parse()?,
            permissions: u32::from_str_radix(collected[2], 8)?,
            size: collected[3].parse()?,
            is_symlink: collected[4].parse()?,
            is_folder: collected[5].parse()?,
        });
    }
}

struct Index {
    files: HashMap<String, Vec<Version>>,
}

impl Index {
    fn new() -> Index {
        return Index {
            files: HashMap::new(),
        };
    }
}
