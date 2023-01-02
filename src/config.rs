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

use anyhow::anyhow;
use anyhow::Result;
use yaml_rust;

pub struct Config(yaml_rust::Yaml);

pub fn load(path: &str) -> Result<Config> {
    let raw = std::fs::read_to_string(path)?;

    let docs = yaml_rust::YamlLoader::load_from_str(&raw)?;

    return Ok(Config(docs[0].clone()));
}

impl Config {
    pub fn get_string(&self, name: &str) -> anyhow::Result<String> {
        let maybe_str = self.0[name].as_str();
        match maybe_str {
            Some(str) => {
                return Ok(str.to_string());
            }
            None => {
                return Err(anyhow!("No such config key: {}", name));
            }
        }
    }

    pub fn get_i64(&self, name: &str) -> anyhow::Result<i64> {
        let maybe_val = self.0[name].as_i64();
        match maybe_val {
            Some(val) => {
                return Ok(val);
            }
            None => {
                return Err(anyhow!("No such config key: {}", name));
            }
        }
    }
}
