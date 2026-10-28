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

//! Configuration types for the ingress-cache HTTP filter.

use envoy_proxy_dynamic_modules_rust_sdk::{
  EnvoyCounterId, EnvoyHttpFilter, HttpFilter, HttpFilterConfig,
};
use serde::Deserialize;

use crate::filter::CacheFilter;

/// Default TTL in seconds for cached actor -> worker bindings.
pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 5;

/// Default port `atunnel` listens on at the worker.
pub const DEFAULT_ATUNNEL_PORT: u16 = 443;

/// Default request header carrying the actor's target port to `atunnel`.
pub const DEFAULT_TARGET_PORT_HEADER: &str = "x-ate-target-port";

/// Default for the most actors each Envoy worker thread's cache binds.
pub const DEFAULT_MAX_CACHED_ACTORS: usize = 10_000;

/// Filter configuration supplied as JSON in the Envoy HTTP filter config.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(default)]
pub struct Config {
  /// How long an actor -> worker binding may be served from cache.
  pub cache_ttl_seconds: u64,
  /// The port `atunnel` listens on at the worker.
  pub atunnel_port: u16,
  /// Header carrying the actor's target port to `atunnel`.
  pub target_port_header: String,
  /// The most actors cached per Envoy worker thread. Each thread has its own cache.
  pub max_cached_actors: usize,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
      atunnel_port: DEFAULT_ATUNNEL_PORT,
      target_port_header: DEFAULT_TARGET_PORT_HEADER.to_string(),
      max_cached_actors: DEFAULT_MAX_CACHED_ACTORS,
    }
  }
}

impl Config {
  /// Parses filter configuration from raw JSON bytes, returning defaults when
  /// the slice is empty or whitespace-only.
  pub fn from_config(filter_config: &[u8]) -> Result<Self, serde_json::Error> {
    let raw = std::str::from_utf8(filter_config).unwrap_or("");
    let json_str = if raw.trim().is_empty() { "{}" } else { raw };
    serde_json::from_str(json_str)
  }
}

/// Per-filter-chain configuration pairing parsed [`Config`] settings with
/// Envoy counter IDs registered at config creation time.
pub struct FilterConfig {
  pub settings: Config,
  pub cache_hit: Option<EnvoyCounterId>,
  pub cache_miss: Option<EnvoyCounterId>,
  pub cache_bypass: Option<EnvoyCounterId>,
  pub cache_eviction: Option<EnvoyCounterId>,
  pub cache_replay: Option<EnvoyCounterId>,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(CacheFilter {
      config: self.settings.clone(),
      cache_hit: self.cache_hit,
      cache_miss: self.cache_miss,
      cache_bypass: self.cache_bypass,
      cache_eviction: self.cache_eviction,
      cache_replay: self.cache_replay,
      learn_key: None,
      actor_key: None,
      was_cache_hit: false,
      has_request_body: false,
      entry: None,
      learned: None,
    })
  }
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_config_defaults_and_overrides() {
    assert_eq!(Config::from_config(b"").unwrap(), Config::default());
    assert_eq!(Config::from_config(b"  ").unwrap(), Config::default());
    assert_eq!(Config::from_config(b"{}").unwrap(), Config::default());

    let custom = Config::from_config(
      br#"{"cache_ttl_seconds": 10, "atunnel_port": 8443, "target_port_header": "x-custom-port", "max_cached_actors": 500}"#,
    )
    .unwrap();
    assert_eq!(
      custom,
      Config {
        cache_ttl_seconds: 10,
        atunnel_port: 8443,
        target_port_header: "x-custom-port".to_string(),
        max_cached_actors: 500,
      }
    );

    assert!(Config::from_config(b"not-json").is_err());
  }
}
