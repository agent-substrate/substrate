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

//! The egress-policy-cache HTTP filter dynamic module.

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::{
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  declare_init_functions, envoy_log_error, EnvoyHttpFilter,
  EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use serde::Deserialize;
use std::time::Duration;

declare_init_functions!(init, new_http_filter_config_fn);

/// Called when the dynamic module is loaded into Envoy.
fn init() -> bool {
  true
}

/// Called when a new HTTP filter configuration is created.
fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  _envoy_filter_config: &mut EC,
  _filter_name: &str,
  filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let config = parse_config(filter_config)?;
  Some(Box::new(EgressPolicyCacheFilterConfig { config }))
}

fn parse_config(filter_config: &[u8]) -> Option<Config> {
  let raw = std::str::from_utf8(filter_config).ok()?;
  let json_str = if raw.trim().is_empty() { "{}" } else { raw };
  serde_json::from_str(json_str)
    .map_err(|e| {
      envoy_log_error!("egress_policy_cache: invalid filter config: {}", e);
    })
    .ok()
}

/// Configuration for the egress-policy-cache HTTP filter.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Config {
  #[serde(default = "default_cache_ttl", deserialize_with = "deserialize_secs")]
  pub cache_ttl: Duration,
  #[serde(default = "default_cache_enabled")]
  pub cache_enabled: bool,
}

fn default_cache_ttl() -> Duration {
  Duration::from_secs(5)
}

fn default_cache_enabled() -> bool {
  true
}

fn deserialize_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
  D: serde::Deserializer<'de>,
{
  let secs = u64::deserialize(deserializer)?;
  Ok(Duration::from_secs(secs))
}

impl Default for Config {
  fn default() -> Self {
    Self {
      cache_ttl: default_cache_ttl(),
      cache_enabled: default_cache_enabled(),
    }
  }
}

/// Per-filter-chain configuration for the egress policy cache filter.
pub struct EgressPolicyCacheFilterConfig {
  pub config: Config,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyCacheFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyCacheFilter {
      config: self.config.clone(),
    })
  }
}

/// Per-stream HTTP filter instance for caching egress policy decisions.
pub struct EgressPolicyCacheFilter {
  pub config: Config,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyCacheFilter {
  fn on_request_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let _ = (&self.config.cache_ttl, self.config.cache_enabled);
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    let _ = (&self.config.cache_ttl, self.config.cache_enabled);
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::MockEnvoyHttpFilter;

  #[test]
  fn test_config_defaults_and_overrides() {
    assert_eq!(
      parse_config(b""),
      Some(Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
      })
    );
    assert_eq!(
      parse_config(b"{}"),
      Some(Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
      })
    );
    assert_eq!(
      parse_config(br#"{"cache_ttl": 10, "cache_enabled": false}"#),
      Some(Config {
        cache_ttl: Duration::from_secs(10),
        cache_enabled: false,
      })
    );
    assert_eq!(parse_config(b"not-json"), None);
  }

  #[test]
  fn test_headers_continue() {
    let config = EgressPolicyCacheFilterConfig {
      config: Config::default(),
    };
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut filter = config.new_http_filter(&mut mock_filter);

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }
}
