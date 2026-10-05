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
  declare_init_functions, envoy_log_error, EnvoyCounterId, EnvoyHttpFilter,
  EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use lru::LruCache;
use serde::Deserialize;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thread_local::ThreadLocal;

/// Key holding the egress policy SNI rules JSON.
pub const ATE_POLICY_EGRESS: &str = "dev.ate.policy.egress";

/// Key holding the cached egress policy SNI rules JSON before it is copied to
/// upstream-shared filter state.
pub const ATE_POLICY_EGRESS_CACHED: &str = "dev.ate.policy.egress.cached";

/// Counter name for egress policy cache hits on CONNECT.
pub const CONNECT_CACHE_HIT_COUNTER: &str = "ate_egress.connect_cache_hit";

/// Counter name for egress policy cache misses on CONNECT.
pub const CONNECT_CACHE_MISS_COUNTER: &str = "ate_egress.connect_cache_miss";

/// Cached egress policy value and the timestamp when it was stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedPolicy {
  pub policy: String,
  pub stored_at: Instant,
}

impl CachedPolicy {
  pub fn new(policy: String, stored_at: Instant) -> Self {
    Self { policy, stored_at }
  }
}

/// Thread-local store of LRU caches with `String` keys and [`CachedPolicy`] values.
pub type ThreadLocalCache = ThreadLocal<RefCell<LruCache<String, CachedPolicy>>>;

declare_init_functions!(init, new_http_filter_config_fn);

/// Called when the dynamic module is loaded into Envoy.
fn init() -> bool {
  true
}

/// Called when a new HTTP filter configuration is created.
fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  envoy_filter_config: &mut EC,
  _filter_name: &str,
  filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let config = parse_config(filter_config)?;
  let cache_hit_counter = envoy_filter_config
    .define_counter(CONNECT_CACHE_HIT_COUNTER)
    .ok()?;
  let cache_miss_counter = envoy_filter_config
    .define_counter(CONNECT_CACHE_MISS_COUNTER)
    .ok()?;
  Some(Box::new(EgressPolicyCacheFilterConfig::with_counters(
    config,
    cache_hit_counter,
    cache_miss_counter,
  )))
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

fn new_lru_cache(max_cache_items: usize) -> RefCell<LruCache<String, CachedPolicy>> {
  let cap = NonZeroUsize::new(max_cache_items).unwrap_or(NonZeroUsize::MIN);
  RefCell::new(LruCache::new(cap))
}

/// Per-filter-chain configuration for the egress policy cache filter.
pub struct EgressPolicyCacheFilterConfig {
  pub config: Config,
  pub cache: Arc<ThreadLocalCache>,
  pub cache_hit_counter: EnvoyCounterId,
  pub cache_miss_counter: EnvoyCounterId,
}

impl EgressPolicyCacheFilterConfig {
  pub fn new(config: Config) -> Self {
    Self::with_counters(config, EnvoyCounterId(0), EnvoyCounterId(1))
  }

  pub fn with_counters(
    config: Config,
    cache_hit_counter: EnvoyCounterId,
    cache_miss_counter: EnvoyCounterId,
  ) -> Self {
    Self {
      config,
      cache: Arc::new(ThreadLocal::new()),
      cache_hit_counter,
      cache_miss_counter,
    }
  }

  /// Returns a reference to the current thread's LRU cache, initializing it
  /// with capacity `config.max_cache_items` on first access.
  pub fn local_cache(&self) -> &RefCell<LruCache<String, CachedPolicy>> {
    let max_items = self.config.max_cache_items;
    self.cache.get_or(|| new_lru_cache(max_items))
  }

  /// Creates a concrete [`EgressPolicyCacheFilter`] bound to this configuration.
  pub fn create_filter(&self) -> EgressPolicyCacheFilter {
    EgressPolicyCacheFilter {
      config: self.config.clone(),
      cache: Arc::clone(&self.cache),
      cache_hit_counter: self.cache_hit_counter,
      cache_miss_counter: self.cache_miss_counter,
      has_cached_policy: false,
    }
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyCacheFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(self.create_filter())
  }
}

/// Per-stream HTTP filter instance for caching egress policy decisions.
pub struct EgressPolicyCacheFilter {
  pub config: Config,
  pub cache: Arc<ThreadLocalCache>,
  pub cache_hit_counter: EnvoyCounterId,
  pub cache_miss_counter: EnvoyCounterId,
  pub has_cached_policy: bool,
}

impl EgressPolicyCacheFilter {
  /// Returns a reference to the current thread's LRU cache, initializing it
  /// with capacity `config.max_cache_items` on first access.
  pub fn local_cache(&self) -> &RefCell<LruCache<String, CachedPolicy>> {
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
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let cached_policy = if let Some(cert_digest) = read_peer_cert_digest(envoy_filter) {
      let mut cache = self.local_cache().borrow_mut();
      if let Some(entry) = cache.get(&cert_digest) {
        if entry.stored_at.elapsed() > self.config.cache_ttl {
          cache.pop(&cert_digest);
          None
        } else {
          Some(entry.policy.clone())
        }
      } else {
        None
      }
    } else {
      None
    };

    if let Some(policy) = cached_policy {
      self.has_cached_policy = true;
      envoy_filter.set_filter_state_bytes(ATE_POLICY_EGRESS_CACHED.as_bytes(), policy.as_bytes());
      let _ = envoy_filter.increment_counter(self.cache_hit_counter, 1);
    } else {
      let _ = envoy_filter.increment_counter(self.cache_miss_counter, 1);
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    // Store policy in cache in case it was authorized (200 response) on a
    // cache miss. Actor identity comes from cert's subject. Use cert digest as
    // key, since it uniquely identifies the cert and hence actor.
    if self.config.cache_enabled
      && !self.has_cached_policy
      && is_http_200(envoy_filter)
      && let Some(policy) = read_egress_policy(envoy_filter)
      && let Some(cert_digest) = read_peer_cert_digest(envoy_filter)
    {
      self.local_cache().borrow_mut().put(
        cert_digest,
        CachedPolicy::new(policy, Instant::now()),
      );
    }
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{
    abi::{
      envoy_dynamic_module_type_http_callout_init_result,
      envoy_dynamic_module_type_metrics_result,
    },
    EnvoyBuffer, EnvoyCounterVecId, EnvoyGaugeId, EnvoyGaugeVecId,
    EnvoyHistogramId, EnvoyHistogramVecId, EnvoyHttpFilterConfigScheduler,
    MockEnvoyHttpFilter,
  };

  #[derive(Default)]
  struct TestEnvoyHttpFilterConfig {
    defined_counters: Vec<String>,
  }

  impl EnvoyHttpFilterConfig for TestEnvoyHttpFilterConfig {
    fn define_counter(
      &mut self,
      name: &str,
    ) -> Result<EnvoyCounterId, envoy_dynamic_module_type_metrics_result> {
      self.defined_counters.push(name.to_string());
      Ok(EnvoyCounterId(self.defined_counters.len()))
    }
    fn define_counter_vec(
      &mut self,
      _: &str,
      _: &[&str],
    ) -> Result<EnvoyCounterVecId, envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn define_gauge(
      &mut self,
      _: &str,
    ) -> Result<EnvoyGaugeId, envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn define_gauge_vec(
      &mut self,
      _: &str,
      _: &[&str],
    ) -> Result<EnvoyGaugeVecId, envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn define_histogram(
      &mut self,
      _: &str,
    ) -> Result<EnvoyHistogramId, envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn define_histogram_vec(
      &mut self,
      _: &str,
      _: &[&str],
    ) -> Result<EnvoyHistogramVecId, envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn increment_counter(
      &self,
      _: EnvoyCounterId,
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn increment_counter_vec(
      &self,
      _: EnvoyCounterVecId,
      _: &[&str],
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn increase_gauge(
      &self,
      _: EnvoyGaugeId,
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn increase_gauge_vec(
      &self,
      _: EnvoyGaugeVecId,
      _: &[&str],
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn decrease_gauge(
      &self,
      _: EnvoyGaugeId,
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn decrease_gauge_vec(
      &self,
      _: EnvoyGaugeVecId,
      _: &[&str],
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn set_gauge(
      &self,
      _: EnvoyGaugeId,
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn set_gauge_vec(
      &self,
      _: EnvoyGaugeVecId,
      _: &[&str],
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn record_histogram_value(
      &self,
      _: EnvoyHistogramId,
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn record_histogram_value_vec(
      &self,
      _: EnvoyHistogramVecId,
      _: &[&str],
      _: u64,
    ) -> Result<(), envoy_dynamic_module_type_metrics_result> {
      todo!()
    }
    fn new_scheduler(&self) -> Box<dyn EnvoyHttpFilterConfigScheduler> {
      todo!()
    }
    fn send_http_callout<'a>(
      &mut self,
      _: &'a str,
      _: &'a [(&'a str, &'a [u8])],
      _: Option<&'a [u8]>,
      _: u64,
    ) -> (envoy_dynamic_module_type_http_callout_init_result, u64) {
      todo!()
    }
    fn start_http_stream<'a>(
      &mut self,
      _: &'a str,
      _: &'a [(&'a str, &'a [u8])],
      _: Option<&'a [u8]>,
      _: bool,
      _: u64,
    ) -> (envoy_dynamic_module_type_http_callout_init_result, u64) {
      todo!()
    }
    unsafe fn send_http_stream_data(&mut self, _: u64, _: &[u8], _: bool) -> bool {
      todo!()
    }
    unsafe fn send_http_stream_trailers<'a>(
      &mut self,
      _: u64,
      _: &'a [(&'a str, &'a [u8])],
    ) -> bool {
      todo!()
    }
    unsafe fn reset_http_stream(&mut self, _: u64) {
      todo!()
    }
  }

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
  fn test_new_http_filter_config_defines_counters() {
    let mut test_config = TestEnvoyHttpFilterConfig::default();
    let filter_config = new_http_filter_config_fn::<
      TestEnvoyHttpFilterConfig,
      MockEnvoyHttpFilter,
    >(&mut test_config, "egress_policy_cache", b"{}");
    assert!(filter_config.is_some());
    assert_eq!(
      test_config.defined_counters,
      vec![
        CONNECT_CACHE_HIT_COUNTER.to_string(),
        CONNECT_CACHE_MISS_COUNTER.to_string(),
      ]
    );
  }

  #[test]
  fn test_headers_continue() {
    let hit_id = EnvoyCounterId(1);
    let miss_id = EnvoyCounterId(2);
    let config =
      EgressPolicyCacheFilterConfig::with_counters(Config::default(), hit_id, miss_id);
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_attribute_string()
      .withf(|id| {
        *id == envoy_dynamic_module_type_attribute_id::ConnectionSha256PeerCertificateDigest
      })
      .returning(|_| Some(EnvoyBuffer::new(b"unknown_digest")));
    mock_filter
      .expect_increment_counter()
      .withf(move |id, val| *id == miss_id && *val == 1)
      .return_const(Ok(()))
      .once();
    mock_filter
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| None);
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
  fn test_on_request_headers_cache_hit_writes_filter_state_and_increments_hit_counter() {
    let hit_id = EnvoyCounterId(1);
    let miss_id = EnvoyCounterId(2);
    let config =
      EgressPolicyCacheFilterConfig::with_counters(Config::default(), hit_id, miss_id);
    let expected_policy = r#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#;
    config.local_cache().borrow_mut().put(
      "abc123digest".to_string(),
      CachedPolicy::new(expected_policy.to_string(), Instant::now()),
    );

    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_attribute_string()
      .withf(|id| {
        *id == envoy_dynamic_module_type_attribute_id::ConnectionSha256PeerCertificateDigest
      })
      .returning(|_| Some(EnvoyBuffer::new(b"abc123digest")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(move |key, val| {
        key == ATE_POLICY_EGRESS_CACHED.as_bytes() && val == expected_policy.as_bytes()
      })
      .return_const(true)
      .once();
    mock_filter
      .expect_increment_counter()
      .withf(move |id, val| *id == hit_id && *val == 1)
      .return_const(Result::<(), envoy_dynamic_module_type_metrics_result>::Ok(()))
      .once();

    let mut filter = config.create_filter();
    assert!(!filter.has_cached_policy);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(filter.has_cached_policy);
  }

  #[test]
  fn test_on_request_headers_expired_cache_entry_is_removed_and_increments_miss_counter() {
    let hit_id = EnvoyCounterId(1);
    let miss_id = EnvoyCounterId(2);
    let config = EgressPolicyCacheFilterConfig::with_counters(
      Config {
        cache_ttl: Duration::from_secs(5),
        cache_enabled: true,
        max_cache_items: 1000,
      },
      hit_id,
      miss_id,
    );
    config.local_cache().borrow_mut().put(
      "abc123digest".to_string(),
      CachedPolicy::new(
        r#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#.to_string(),
        Instant::now() - Duration::from_secs(10),
      ),
    );

    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_attribute_string()
      .withf(|id| {
        *id == envoy_dynamic_module_type_attribute_id::ConnectionSha256PeerCertificateDigest
      })
      .returning(|_| Some(EnvoyBuffer::new(b"abc123digest")));
    mock_filter.expect_set_filter_state_bytes().never();
    mock_filter
      .expect_increment_counter()
      .withf(move |id, val| *id == miss_id && *val == 1)
      .return_const(Result::<(), envoy_dynamic_module_type_metrics_result>::Ok(()))
      .once();

    let mut filter = config.create_filter();
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(!filter.has_cached_policy);
    assert!(config.local_cache().borrow_mut().get("abc123digest").is_none());
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

    let cached = config
      .local_cache()
      .borrow_mut()
      .get("abc123digest")
      .cloned()
      .expect("expected entry in cache");
    assert_eq!(
      cached.policy,
      r#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#
    );
    assert!(cached.stored_at.elapsed() < Duration::from_secs(5));
  }

  #[test]
  fn test_on_response_headers_skips_cache_when_already_cached() {
    let config = EgressPolicyCacheFilterConfig::new(Config::default());
    let initial_stored_at = Instant::now() - Duration::from_secs(3);
    config.local_cache().borrow_mut().put(
      "abc123digest".to_string(),
      CachedPolicy::new(
        r#"{"rules":[{"pattern":"*.example.com","mode":"mitm"}]}"#.to_string(),
        initial_stored_at,
      ),
    );

    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock_filter.expect_get_filter_state_bytes().never();

    let mut filter = config.create_filter();
    filter.has_cached_policy = true;
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );

    let cached = config
      .local_cache()
      .borrow_mut()
      .get("abc123digest")
      .cloned()
      .expect("expected entry in cache");
    assert_eq!(cached.stored_at, initial_stored_at);
  }

  #[test]
  fn test_on_response_headers_skips_cache_when_disabled() {
    let config = EgressPolicyCacheFilterConfig::new(Config {
      cache_ttl: Duration::from_secs(5),
      cache_enabled: false,
      max_cache_items: 1000,
    });
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter.expect_get_response_header_value().never();

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert_eq!(config.local_cache().borrow().len(), 0);
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

    let filter1 = filter_config.create_filter();
    let filter2 = filter_config.create_filter();

    assert_eq!(filter1.local_cache().borrow().cap().get(), 2);

    let now = Instant::now();
    filter1
      .local_cache()
      .borrow_mut()
      .put("k1".to_string(), CachedPolicy::new("v1".to_string(), now));
    filter1
      .local_cache()
      .borrow_mut()
      .put("k2".to_string(), CachedPolicy::new("v2".to_string(), now));

    // Second filter created from the same config on the same thread sees the entries.
    assert_eq!(
      filter2
        .local_cache()
        .borrow_mut()
        .get("k1")
        .map(|e| e.policy.as_str()),
      Some("v1")
    );

    // Inserting a 3rd item evicts the least-recently-used item ("k2").
    filter2
      .local_cache()
      .borrow_mut()
      .put("k3".to_string(), CachedPolicy::new("v3".to_string(), now));
    assert_eq!(filter1.local_cache().borrow_mut().get("k2"), None);
    assert_eq!(
      filter1
        .local_cache()
        .borrow_mut()
        .get("k1")
        .map(|e| e.policy.as_str()),
      Some("v1")
    );
    assert_eq!(
      filter1
        .local_cache()
        .borrow_mut()
        .get("k3")
        .map(|e| e.policy.as_str()),
      Some("v3")
    );

    // A filter on another worker thread gets its own empty thread-local cache.
    let filter_config_clone = Arc::clone(&filter_config);
    let other_thread_len = std::thread::spawn(move || {
      let other_filter = filter_config_clone.create_filter();
      assert_eq!(other_filter.local_cache().borrow().cap().get(), 2);
      other_filter.local_cache().borrow().len()
    })
    .join()
    .unwrap();
    assert_eq!(other_thread_len, 0);
  }
}
