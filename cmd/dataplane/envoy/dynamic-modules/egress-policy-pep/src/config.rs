// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration types for the egress-policy-pep HTTP filter.

use std::time::Duration;

use envoy_proxy_dynamic_modules_rust_sdk::{
  EnvoyCounterId, EnvoyHttpFilter, HttpFilter, HttpFilterConfig,
};
use serde::{de, Deserialize, Deserializer};

use crate::EgressPolicyPepFilter;

/// Default value for `cache_enabled`.
pub const DEFAULT_CACHE_ENABLED: bool = true;

/// Default value for `cache_ttl` (5 seconds).
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);

/// Deserialized configuration parameters for egress-policy-pep.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Config {
  pub cache_enabled: bool,
  #[serde(deserialize_with = "deserialize_duration")]
  pub cache_ttl: Duration,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      cache_enabled: DEFAULT_CACHE_ENABLED,
      cache_ttl: DEFAULT_CACHE_TTL,
    }
  }
}

impl Config {
  /// Parses filter configuration from raw config bytes, returning defaults when
  /// the configuration slice is empty.
  pub fn from_config(config: &[u8]) -> Result<Self, serde_json::Error> {
    if config.is_empty() {
      return Ok(Self::default());
    }
    serde_json::from_slice(config)
  }
}

/// The filter configuration for egress-policy-pep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressPolicyPepFilterConfig {
  pub config: Config,
  pub cache_hit_counter: EnvoyCounterId,
  pub cache_miss_counter: EnvoyCounterId,
  pub allowed_counter: EnvoyCounterId,
  pub rejected_counter: EnvoyCounterId,
  pub has_effects_counter: EnvoyCounterId,
}

impl Default for EgressPolicyPepFilterConfig {
  fn default() -> Self {
    Self {
      config: Config::default(),
      cache_hit_counter: EnvoyCounterId(0),
      cache_miss_counter: EnvoyCounterId(0),
      allowed_counter: EnvoyCounterId(0),
      rejected_counter: EnvoyCounterId(0),
      has_effects_counter: EnvoyCounterId(0),
    }
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyPepFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyPepFilter {
      cache_hit_counter: self.cache_hit_counter,
      cache_miss_counter: self.cache_miss_counter,
      allowed_counter: self.allowed_counter,
      rejected_counter: self.rejected_counter,
      has_effects_counter: self.has_effects_counter,
    })
  }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurationInput {
  String(String),
  Seconds(u64),
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
  D: Deserializer<'de>,
{
  match DurationInput::deserialize(deserializer)? {
    DurationInput::String(s) => parse_seconds_duration(&s).map_err(de::Error::custom),
    DurationInput::Seconds(secs) => Ok(Duration::from_secs(secs)),
  }
}

fn parse_seconds_duration(s: &str) -> Result<Duration, String> {
  let s = s.trim();
  let val = s
    .strip_suffix('s')
    .ok_or_else(|| format!("invalid duration (must be in seconds, e.g. \"5s\"): {s}"))?;
  val
    .trim()
    .parse::<u64>()
    .map(Duration::from_secs)
    .map_err(|_| format!("invalid duration: {s}"))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_filter_config_defaults() {
    let cfg = Config::from_config(b"").unwrap();
    assert!(cfg.cache_enabled);
    assert_eq!(cfg.cache_ttl, Duration::from_secs(5));

    let empty_obj = Config::from_config(b"{}").unwrap();
    assert!(empty_obj.cache_enabled);
    assert_eq!(empty_obj.cache_ttl, Duration::from_secs(5));
  }

  #[test]
  fn test_filter_config_custom_values() {
    let cfg = Config::from_config(br#"{"cache_enabled":false,"cache_ttl":"10s"}"#).unwrap();
    assert!(!cfg.cache_enabled);
    assert_eq!(cfg.cache_ttl, Duration::from_secs(10));

    let secs_cfg = Config::from_config(br#"{"cache_ttl":15}"#).unwrap();
    assert!(secs_cfg.cache_enabled);
    assert_eq!(secs_cfg.cache_ttl, Duration::from_secs(15));
  }

  #[test]
  fn test_filter_config_invalid_values() {
    assert!(Config::from_config(b"not-json").is_err());
    assert!(Config::from_config(br#"{"cache_ttl":"invalid"}"#).is_err());
    assert!(Config::from_config(br#"{"cache_ttl":"250ms"}"#).is_err());
    assert!(Config::from_config(br#"{"cache_ttl":"5m"}"#).is_err());
  }
}
