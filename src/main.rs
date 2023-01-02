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

pub mod backup;
pub mod config;

use std::env::args;

use anyhow::Result;
use simple_logger;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info)?;
    
    log::info!("{}", include_str!("../version"));
    
    let mut conf_path = "/etc/azure_blob_backup/config.yaml".to_string();
    if args().len() > 1 {
        conf_path = args().nth(1).unwrap_or(conf_path);
    }
    
    let conf = config::load(&conf_path)?;
    
    backup::run(&conf).await?;
    
    return Ok(())
}
