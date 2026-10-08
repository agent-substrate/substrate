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

//! The egress-policy-pep HTTP filter enforces egress policy on HTTP requests
//! and responses.

use std::{
  ffi::c_void,
  time::{Duration, Instant},
};

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::{
    envoy_dynamic_module_type_filter_state_life_span,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  declare_init_functions, envoy_log_error, envoy_log_trace, EnvoyHttpFilter,
  EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use serde::{de, Deserialize, Deserializer};
pub use substrate_envoy_common::{EgressPolicy, SniRule, ATE_POLICY_EGRESS};

/// Filter state key holding the cached egress policy object on the inner
/// connection.
pub const ATE_POLICY_EGRESS_INNER: &[u8] = b"dev.ate.policy.egress.inner";

/// Default value for `cache_enabled`.
pub const DEFAULT_CACHE_ENABLED: bool = true;

/// Default value for `cache_ttl` (5 seconds).
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);

/// Cached egress policy state stored in Envoy filter state under
/// [`ATE_POLICY_EGRESS_INNER`] with [`envoy_dynamic_module_type_filter_state_life_span::Connection`]
/// lifespan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedEgressPolicy {
  pub created_at: Instant,
  pub policy: EgressPolicy,
}

/// Frees the boxed [`CachedEgressPolicy`] when Envoy destroys the filter state
/// entry.
extern "C" fn drop_cached_egress_policy(object: *mut c_void) {
  if !object.is_null() {
    drop(unsafe { Box::from_raw(object as *mut CachedEgressPolicy) });
  }
}

/// The filter configuration for egress-policy-pep.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct EgressPolicyPepFilterConfig {
  pub cache_enabled: bool,
  #[serde(deserialize_with = "deserialize_duration")]
  pub cache_ttl: Duration,
}

impl Default for EgressPolicyPepFilterConfig {
  fn default() -> Self {
    Self {
      cache_enabled: DEFAULT_CACHE_ENABLED,
      cache_ttl: DEFAULT_CACHE_TTL,
    }
  }
}

impl EgressPolicyPepFilterConfig {
  /// Parses filter configuration from raw config bytes, returning defaults when
  /// the configuration slice is empty.
  pub fn from_config(config: &[u8]) -> Result<Self, serde_json::Error> {
    if config.is_empty() {
      return Ok(Self::default());
    }
    serde_json::from_slice(config)
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

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyPepFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyPepFilter)
  }
}

/// Creates a [`CachedEgressPolicy`] from the [`ATE_POLICY_EGRESS`] filter state
/// and stores it in [`ATE_POLICY_EGRESS_INNER`] with connection lifespan.
/// Returns an immutable reference to the created [`CachedEgressPolicy`], or
/// `None` if [`ATE_POLICY_EGRESS`] is absent, invalid JSON, or storing the
/// filter state fails.
fn create_cached_egress_policy<EHF: EnvoyHttpFilter>(
  envoy_filter: &mut EHF,
) -> Option<&CachedEgressPolicy> {
  let raw_policy = envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS)?;
  let policy = match serde_json::from_slice::<EgressPolicy>(raw_policy.as_slice()) {
    Ok(policy) => policy,
    Err(err) => {
      envoy_log_error!("egress policy pep: invalid egress policy: {}", err);
      return None;
    }
  };
  let state = Box::new(CachedEgressPolicy {
    created_at: Instant::now(),
    policy,
  });
  let ptr = Box::into_raw(state);
  // SAFETY: `ptr` is a freshly boxed `CachedEgressPolicy` and
  // `drop_cached_egress_policy` frees that exact type without unwinding.
  // When `set_filter_state_object` succeeds, Envoy retains ownership of `ptr`
  // for the lifetime of the connection.
  unsafe {
    if !envoy_filter.set_filter_state_object(
      ATE_POLICY_EGRESS_INNER,
      ptr as *mut c_void,
      drop_cached_egress_policy,
      envoy_dynamic_module_type_filter_state_life_span::Connection,
    ) {
      drop(Box::from_raw(ptr));
      return None;
    }
    Some(&*ptr)
  }
}

/// Per-stream HTTP filter instance for egress policy enforcement.
pub struct EgressPolicyPepFilter;

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyPepFilter {
  fn on_request_headers(
    &mut self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    envoy_log_trace!(
      "egress policy pep: on_request_headers end_of_stream={}",
      end_of_stream
    );
    // If the policy cache on the inner connection does not exist, then it is
    // the first request on the inner connection and the policy provided by the
    // CONNECT filter chain is still fresh. Create a new connection level filer state
    // with the policy and creation timestamp, so its freshness can be checked.
    if envoy_filter
      .get_filter_state_object(ATE_POLICY_EGRESS_INNER)
      .is_none()
    {
      create_cached_egress_policy(envoy_filter);
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    envoy_log_trace!(
      "egress policy pep: on_response_headers end_of_stream={}",
      end_of_stream
    );
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }
}

declare_init_functions!(init, new_http_filter_config_fn);

/// Called when the dynamic module is loaded into Envoy.
fn init() -> bool {
  true
}

/// Called when a new HTTP filter configuration is created.
fn new_http_filter_config_fn<
  EC: EnvoyHttpFilterConfig,
  EHF: EnvoyHttpFilter,
>(
  _envoy_filter_config: &mut EC,
  _name: &str,
  config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  match EgressPolicyPepFilterConfig::from_config(config) {
    Ok(cfg) => Some(Box::new(cfg)),
    Err(err) => {
      envoy_log_error!("egress policy pep: invalid filter config: {}", err);
      None
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  };

  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  fn expected_policy() -> EgressPolicy {
    EgressPolicy {
      rules: vec![SniRule {
        pattern: "api.example.com".to_string(),
        mode: "mitm".to_string(),
      }],
    }
  }

  #[test]
  fn test_init() {
    assert!(init());
  }

  #[test]
  fn test_filter_config_defaults() {
    let cfg = EgressPolicyPepFilterConfig::from_config(b"").unwrap();
    assert!(cfg.cache_enabled);
    assert_eq!(cfg.cache_ttl, Duration::from_secs(5));

    let empty_obj = EgressPolicyPepFilterConfig::from_config(b"{}").unwrap();
    assert!(empty_obj.cache_enabled);
    assert_eq!(empty_obj.cache_ttl, Duration::from_secs(5));
  }

  #[test]
  fn test_filter_config_custom_values() {
    let cfg = EgressPolicyPepFilterConfig::from_config(
      br#"{"cache_enabled":false,"cache_ttl":"10s"}"#,
    )
    .unwrap();
    assert!(!cfg.cache_enabled);
    assert_eq!(cfg.cache_ttl, Duration::from_secs(10));

    let secs_cfg = EgressPolicyPepFilterConfig::from_config(br#"{"cache_ttl":15}"#).unwrap();
    assert!(secs_cfg.cache_enabled);
    assert_eq!(secs_cfg.cache_ttl, Duration::from_secs(15));
  }

  #[test]
  fn test_filter_config_invalid_values() {
    assert!(EgressPolicyPepFilterConfig::from_config(b"not-json").is_err());
    assert!(EgressPolicyPepFilterConfig::from_config(br#"{"cache_ttl":"invalid"}"#).is_err());
    assert!(EgressPolicyPepFilterConfig::from_config(br#"{"cache_ttl":"250ms"}"#).is_err());
    assert!(EgressPolicyPepFilterConfig::from_config(br#"{"cache_ttl":"5m"}"#).is_err());
  }

  #[test]
  fn test_create_cached_egress_policy_returns_ref_when_created() {
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let raw_policy = br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"}]}"#;
    let stored_ptr = Arc::new(AtomicUsize::new(0));
    let stored_ptr_clone = Arc::clone(&stored_ptr);

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(move |_| Some(EnvoyBuffer::new(raw_policy)));
    mock_filter
      .expect_set_filter_state_object()
      .withf(|key, object, _dtor, life_span| {
        key == ATE_POLICY_EGRESS_INNER
          && !object.is_null()
          && *life_span == envoy_dynamic_module_type_filter_state_life_span::Connection
      })
      .times(1)
      .returning(move |_key, object, _destructor, _life_span| {
        stored_ptr_clone.store(object as usize, Ordering::SeqCst);
        true
      });

    let cached = create_cached_egress_policy(&mut mock_filter)
      .expect("expected CachedEgressPolicy reference");
    assert_eq!(cached.policy, expected_policy());
    assert!(cached.created_at.elapsed() < Duration::from_secs(5));

    drop_cached_egress_policy(stored_ptr.load(Ordering::SeqCst) as *mut c_void);
  }

  #[test]
  fn test_create_cached_egress_policy_returns_none_when_egress_policy_missing() {
    let mut mock_filter = MockEnvoyHttpFilter::new();

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(|_| None);
    mock_filter.expect_set_filter_state_object().times(0);

    assert!(create_cached_egress_policy(&mut mock_filter).is_none());
  }

  #[test]
  fn test_create_cached_egress_policy_returns_none_when_egress_policy_invalid_json() {
    let mut mock_filter = MockEnvoyHttpFilter::new();

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"not-json")));
    mock_filter.expect_set_filter_state_object().times(0);

    assert!(create_cached_egress_policy(&mut mock_filter).is_none());
  }

  #[test]
  fn test_create_cached_egress_policy_returns_none_when_set_fails() {
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let raw_policy = br#"{"rules":[]}"#;

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(move |_| Some(EnvoyBuffer::new(raw_policy)));
    mock_filter
      .expect_set_filter_state_object()
      .times(1)
      .returning(|_key, _object, _destructor, _life_span| false);

    assert!(create_cached_egress_policy(&mut mock_filter).is_none());
  }

  #[test]
  fn test_on_request_headers_populates_inner_filter_state_when_missing() {
    let config = EgressPolicyPepFilterConfig::default();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let raw_policy = br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"}]}"#;
    let stored_ptr = Arc::new(AtomicUsize::new(0));
    let stored_ptr_clone = Arc::clone(&stored_ptr);

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(|_| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(move |_| Some(EnvoyBuffer::new(raw_policy)));
    mock_filter
      .expect_set_filter_state_object()
      .withf(|key, object, _dtor, life_span| {
        key == ATE_POLICY_EGRESS_INNER
          && !object.is_null()
          && *life_span == envoy_dynamic_module_type_filter_state_life_span::Connection
      })
      .times(1)
      .returning(move |_key, object, _destructor, _life_span| {
        stored_ptr_clone.store(object as usize, Ordering::SeqCst);
        true
      });

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );

    let ptr = stored_ptr.load(Ordering::SeqCst) as *mut c_void;
    assert!(!ptr.is_null());
    let cached = unsafe { &*(ptr as *const CachedEgressPolicy) };
    assert_eq!(cached.policy, expected_policy());
    assert!(cached.created_at.elapsed() < Duration::from_secs(5));
    drop_cached_egress_policy(ptr);
  }

  #[test]
  fn test_on_request_headers_skips_setting_when_egress_policy_missing() {
    let config = EgressPolicyPepFilterConfig::default();
    let mut mock_filter = MockEnvoyHttpFilter::new();

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(|_| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .times(1)
      .returning(|_| None);
    mock_filter.expect_set_filter_state_object().times(0);

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_skips_setting_when_inner_filter_state_exists() {
    let config = EgressPolicyPepFilterConfig::default();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut existing = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy { rules: vec![] },
    };
    let existing_ptr = &mut existing as *mut CachedEgressPolicy as usize;

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(move |_| Some(existing_ptr as *mut c_void));
    mock_filter.expect_get_filter_state_bytes().times(0);
    mock_filter.expect_set_filter_state_object().times(0);

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_response_headers_continues() {
    let config = EgressPolicyPepFilterConfig::default();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }
}
