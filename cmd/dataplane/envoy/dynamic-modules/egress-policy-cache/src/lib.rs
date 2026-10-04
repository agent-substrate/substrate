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
    envoy_dynamic_module_type_attribute_id,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  declare_init_functions, envoy_log_error, EnvoyHttpFilter,
  EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use lru::LruCache;
use serde::Deserialize;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use thread_local::ThreadLocal;

/// Key holding the egress policy SNI rules JSON.
pub const ATE_POLICY_EGRESS: &str = "dev.ate.policy.egress";

/// Thread-local store of LRU caches with `String` keys and values.
pub type ThreadLocalCache = ThreadLocal<RefCell<LruCache<String, String>>>;

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
  Some(Box::new(EgressPolicyCacheFilterConfig::new(config)))
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
  #[serde(default = "default_max_cache_items", alias = "max-cache-items")]
  pub max_cache_items: usize,
}

fn default_cache_ttl() -> Duration {
  Duration::from_secs(5)
}

fn default_cache_enabled() -> bool {
  true
}

fn default_max_cache_items() -> usize {
  1000
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
      max_cache_items: default_max_cache_items(),
    }
  }
}

fn new_lru_cache(max_cache_items: usize) -> RefCell<LruCache<String, String>> {
  let cap = NonZeroUsize::new(max_cache_items).unwrap_or(NonZeroUsize::MIN);
  RefCell::new(LruCache::new(cap))
}

/// Per-filter-chain configuration for the egress policy cache filter.
pub struct EgressPolicyCacheFilterConfig {
  pub config: Config,
  pub cache: Arc<ThreadLocalCache>,
}

impl EgressPolicyCacheFilterConfig {
  pub fn new(config: Config) -> Self {
    Self {
      config,
      cache: Arc::new(ThreadLocal::new()),
    }
  }

  /// Returns a reference to the current thread's LRU cache, initializing it
  /// with capacity `config.max_cache_items` on first access.
  pub fn local_cache(&self) -> &RefCell<LruCache<String, String>> {
    let max_items = self.config.max_cache_items;
    self.cache.get_or(|| new_lru_cache(max_items))
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyCacheFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyCacheFilter {
      config: self.config.clone(),
      cache: Arc::clone(&self.cache),
    })
  }
}

/// Per-stream HTTP filter instance for caching egress policy decisions.
pub struct EgressPolicyCacheFilter {
  pub config: Config,
  pub cache: Arc<ThreadLocalCache>,
}

impl EgressPolicyCacheFilter {
  /// Returns a reference to the current thread's LRU cache, initializing it
  /// with capacity `config.max_cache_items` on first access.
  pub fn local_cache(&self) -> &RefCell<LruCache<String, String>> {
    let max_items = self.config.max_cache_items;
    self.cache.get_or(|| new_lru_cache(max_items))
  }
}

fn is_http_200<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> bool {
  if let Some(status) = envoy_filter.get_response_header_value(":status") {
    return status.as_slice() == b"200";
  }
  false
}

fn read_egress_policy<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Option<String> {
  if let Some(buf) = envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS.as_bytes())
    && let Ok(s) = std::str::from_utf8(buf.as_slice())
      && !s.is_empty() {
        return Some(s.to_owned());
      }
  None
}

fn read_peer_cert_digest<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Option<String> {
  let buf = envoy_filter.get_attribute_string(
    envoy_dynamic_module_type_attribute_id::ConnectionSha256PeerCertificateDigest,
  )?;
  let s = std::str::from_utf8(buf.as_slice()).ok()?;
  if s.is_empty() {
    return None;
  }
  Some(s.to_owned())
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyCacheFilter {
  fn on_request_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let _ = (
      &self.config.cache_ttl,
      self.config.cache_enabled,
      self.local_cache(),
    );
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    // Store policy in cache in case it was authorized (200 response)
    // Actor identity comes from cert's subject. Use cert digest as key, since
    // it uniquely identifies the cert and hence actor.
    if is_http_200(envoy_filter) 
      && let Some(policy) = read_egress_policy(envoy_filter) 
      && let Some(cert_digest) = read_peer_cert_digest(envoy_filter) {
      self.local_cache().borrow_mut().put(cert_digest, policy);
    }
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  #[test]
  fn test_config_defaults_and_overrides() {
    assert_eq!(
      parse_config(b""),
      Some(Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
        max_cache_items: 1000,
      })
    );
    assert_eq!(
      parse_config(b"{}"),
      Some(Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
        max_cache_items: 1000,
      })
    );
    assert_eq!(
      parse_config(
        br#"{"cache_ttl": 10, "cache_enabled": false, "max_cache_items": 500}"#
      ),
      Some(Config {
        cache_ttl: Duration::from_secs(10),
        cache_enabled: false,
        max_cache_items: 500,
      })
    );
    assert_eq!(
      parse_config(br#"{"max-cache-items": 250}"#),
      Some(Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
        max_cache_items: 250,
      })
    );
    assert_eq!(parse_config(b"not-json"), None);
  }

  #[test]
  fn test_headers_continue() {
    let config = EgressPolicyCacheFilterConfig::new(Config::default());
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| None);
    mock_filter
      .expect_get_attribute_int()
      .withf(|id| *id == envoy_dynamic_module_type_attribute_id::ResponseCode)
      .return_const(None);
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

  #[test]
  fn test_on_response_headers_caches_on_200_with_policy_and_cert_digest() {
    let config = EgressPolicyCacheFilterConfig::new(Config::default());
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS.as_bytes())
      .returning(|_| {
        Some(EnvoyBuffer::new(
          br#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#,
        ))
      });
    mock_filter
      .expect_get_attribute_string()
      .withf(|id| {
        *id == envoy_dynamic_module_type_attribute_id::ConnectionSha256PeerCertificateDigest
      })
      .returning(|_| Some(EnvoyBuffer::new(b"abc123digest")));

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );

    assert_eq!(
      config
        .local_cache()
        .borrow_mut()
        .get("abc123digest")
        .map(String::as_str),
      Some(r#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#)
    );
  }

  #[test]
  fn test_on_response_headers_skips_cache_on_non_200() {
    let config = EgressPolicyCacheFilterConfig::new(Config::default());
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"403")));

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert_eq!(config.local_cache().borrow().len(), 0);
  }

  #[test]
  fn test_thread_local_lru_cache_shared_across_filters_and_isolated_across_threads() {
    let filter_config = Arc::new(EgressPolicyCacheFilterConfig::new(Config {
      cache_ttl: Duration::from_secs(5),
      cache_enabled: true,
      max_cache_items: 2,
    }));

    let filter1 = EgressPolicyCacheFilter {
      config: filter_config.config.clone(),
      cache: Arc::clone(&filter_config.cache),
    };
    let filter2 = EgressPolicyCacheFilter {
      config: filter_config.config.clone(),
      cache: Arc::clone(&filter_config.cache),
    };

    assert_eq!(filter1.local_cache().borrow().cap().get(), 2);

    filter1
      .local_cache()
      .borrow_mut()
      .put("k1".to_string(), "v1".to_string());
    filter1
      .local_cache()
      .borrow_mut()
      .put("k2".to_string(), "v2".to_string());

    // Second filter created from the same config on the same thread sees the entries.
    assert_eq!(
      filter2.local_cache().borrow_mut().get("k1").map(String::as_str),
      Some("v1")
    );

    // Inserting a 3rd item evicts the least-recently-used item ("k2").
    filter2
      .local_cache()
      .borrow_mut()
      .put("k3".to_string(), "v3".to_string());
    assert_eq!(filter1.local_cache().borrow_mut().get("k2"), None);
    assert_eq!(
      filter1.local_cache().borrow_mut().get("k1").map(String::as_str),
      Some("v1")
    );
    assert_eq!(
      filter1.local_cache().borrow_mut().get("k3").map(String::as_str),
      Some("v3")
    );

    // A filter on another worker thread gets its own empty thread-local cache.
    let filter_config_clone = Arc::clone(&filter_config);
    let other_thread_len = std::thread::spawn(move || {
      let other_filter = EgressPolicyCacheFilter {
        config: filter_config_clone.config.clone(),
        cache: Arc::clone(&filter_config_clone.cache),
      };
      assert_eq!(other_filter.local_cache().borrow().cap().get(), 2);
      other_filter.local_cache().borrow().len()
    })
    .join()
    .unwrap();
    assert_eq!(other_thread_len, 0);
  }
}
