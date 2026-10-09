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
    envoy_dynamic_module_type_attribute_id,
    envoy_dynamic_module_type_filter_state_life_span,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  declare_init_functions, envoy_log_error, envoy_log_debug, envoy_log_trace, EnvoyCounterId,
  EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use serde::{de, Deserialize, Deserializer};
pub use substrate_envoy_common::{
  pattern_matches, EgressPolicy, EgressRule, ATE_POLICY_EGRESS,
  EGRESS_MODE_CLEARTEXT, EGRESS_MODE_MITM,
};

/// Filter state key holding the cached egress policy object on the inner
/// connection.
pub const ATE_POLICY_EGRESS_INNER: &[u8] = b"dev.ate.policy.egress.inner";

/// Filter state key indicating that ext_proc callout should be skipped for the
/// current request.
pub const ATE_POLICY_EGRESS_SKIP_CALLOUT: &[u8] = b"dev.ate.policy.egress.skip_callout";

/// Dynamic metadata namespace for egress routing decisions.
pub const EGRESS_METADATA_NAMESPACE: &str = "dev.ate.egress";

/// Dynamic metadata key for the egress dial mode.
pub const EGRESS_DIAL_KEY: &str = "dial";

/// Dynamic metadata value for dialing by hostname.
pub const EGRESS_DIAL_NAME: &str = "name";

/// Dynamic metadata key for the hostname to dial.
pub const EGRESS_DIAL_HOST_KEY: &str = "host";

/// Counter name for egress policy cache hits.
pub const CACHE_HIT_COUNTER_NAME: &str = "ate_egress.cache_hit";

/// Counter name for egress policy cache misses.
pub const CACHE_MISS_COUNTER_NAME: &str = "ate_egress.cache_miss";

/// Counter name for requests allowed by egress policy enforcement.
pub const ALLOWED_COUNTER_NAME: &str = "ate_egress.allowed";

/// Counter name for requests rejected by egress policy enforcement.
pub const REJECTED_COUNTER_NAME: &str = "ate_egress.rejected";

/// Counter name for requests matching a rule that has effects.
pub const HAS_EFFECTS_COUNTER_NAME: &str = "ate_egress.has_effects";

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

fn default_counter_id() -> EnvoyCounterId {
  EnvoyCounterId(0)
}

/// The filter configuration for egress-policy-pep.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct EgressPolicyPepFilterConfig {
  pub cache_enabled: bool,
  #[serde(deserialize_with = "deserialize_duration")]
  pub cache_ttl: Duration,
  #[serde(skip, default = "default_counter_id")]
  pub cache_hit_counter: EnvoyCounterId,
  #[serde(skip, default = "default_counter_id")]
  pub cache_miss_counter: EnvoyCounterId,
  #[serde(skip, default = "default_counter_id")]
  pub allowed_counter: EnvoyCounterId,
  #[serde(skip, default = "default_counter_id")]
  pub rejected_counter: EnvoyCounterId,
  #[serde(skip, default = "default_counter_id")]
  pub has_effects_counter: EnvoyCounterId,
}

impl Default for EgressPolicyPepFilterConfig {
  fn default() -> Self {
    Self {
      cache_enabled: DEFAULT_CACHE_ENABLED,
      cache_ttl: DEFAULT_CACHE_TTL,
      cache_hit_counter: default_counter_id(),
      cache_miss_counter: default_counter_id(),
      allowed_counter: default_counter_id(),
      rejected_counter: default_counter_id(),
      has_effects_counter: default_counter_id(),
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
    Box::new(EgressPolicyPepFilter {
      cache_hit_counter: self.cache_hit_counter,
      cache_miss_counter: self.cache_miss_counter,
      allowed_counter: self.allowed_counter,
      rejected_counter: self.rejected_counter,
      has_effects_counter: self.has_effects_counter,
    })
  }
}

/// Per-stream HTTP filter instance for egress policy enforcement.
pub struct EgressPolicyPepFilter {
  cache_hit_counter: EnvoyCounterId,
  cache_miss_counter: EnvoyCounterId,
  allowed_counter: EnvoyCounterId,
  rejected_counter: EnvoyCounterId,
  has_effects_counter: EnvoyCounterId,
}

/// Returns the hostname portion of an `:authority` header value, stripping any
/// `:port` suffix if present.
fn authority_hostname(authority: &[u8]) -> &[u8] {
  match authority.iter().position(|&b| b == b':') {
    Some(idx) => &authority[..idx],
    None => authority,
  }
}

impl EgressPolicyPepFilter {
  /// Returns the [`CachedEgressPolicy`] from [`ATE_POLICY_EGRESS_INNER`] if
  /// present, or creates and stores one from [`ATE_POLICY_EGRESS`] with
  /// connection lifespan. Increments [`CACHE_HIT_COUNTER_NAME`] when a cached
  /// policy is returned and [`CACHE_MISS_COUNTER_NAME`] otherwise.
  fn get_or_create_cached_egress_policy<EHF: EnvoyHttpFilter>(
    &self,
    envoy_filter: &mut EHF,
  ) -> Option<&CachedEgressPolicy> {
    // If the policy cache on the inner connection does not exist, then it is
    // the first request on the inner connection and the policy provided by the
    // CONNECT filter chain is still fresh. Create a new connection-level filter
    // state with the policy and creation timestamp, so its freshness can be
    // checked.
    let cached_ptr = match envoy_filter
      .get_filter_state_object(ATE_POLICY_EGRESS_INNER)
      .filter(|ptr| !ptr.is_null())
    {
      Some(ptr) => Some(ptr as *const CachedEgressPolicy),
      None => envoy_filter
        // If there is no cached policy object yet, use policy passed from CONNECT termination
        // to create inner policy object with the creation timestamp to check freshness.
        .get_filter_state_bytes(ATE_POLICY_EGRESS)
        .and_then(|raw_policy| {
          match serde_json::from_slice::<EgressPolicy>(raw_policy.as_slice()) {
            Ok(policy) => Some(policy),
            Err(err) => {
              envoy_log_debug!("egress policy pep: invalid egress policy: {}", err);
              None
            }
          }
        })
        .and_then(|policy| {
          let state = Box::new(CachedEgressPolicy {
            created_at: Instant::now(),
            policy,
          });
          let ptr = Box::into_raw(state);
          // SAFETY: `ptr` is a freshly boxed `CachedEgressPolicy` and
          // `drop_cached_egress_policy` frees that exact type without unwinding.
          // When `set_filter_state_object` succeeds, Envoy retains ownership of
          // `ptr` for the lifetime of the connection.
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
          }
          Some(ptr as *const CachedEgressPolicy)
        }),
    };

    let counter = if cached_ptr.is_some() {
      self.cache_hit_counter
    } else {
      self.cache_miss_counter
    };
    let _ = envoy_filter.increment_counter(counter, 1);

    // SAFETY: `cached_ptr` points to a `CachedEgressPolicy` stored in
    // `ATE_POLICY_EGRESS_INNER` and owned by Envoy for the connection lifespan.
    cached_ptr.map(|ptr| unsafe { &*ptr })
  }

  fn apply_matched_rule<EHF: EnvoyHttpFilter>(
    &self,
    envoy_filter: &mut EHF,
    rule: &EgressRule,
    hostname: &str,
  ) {
    if rule.has_effects {
      let _ = envoy_filter.increment_counter(self.has_effects_counter, 1);
    } else {
      let _ = envoy_filter.set_filter_state_bytes(ATE_POLICY_EGRESS_SKIP_CALLOUT, b"true");
      envoy_filter.set_dynamic_metadata_string(
        EGRESS_METADATA_NAMESPACE,
        EGRESS_DIAL_KEY,
        EGRESS_DIAL_NAME,
      );
      envoy_filter.set_dynamic_metadata_string(
        EGRESS_METADATA_NAMESPACE,
        EGRESS_DIAL_HOST_KEY,
        hostname,
      );
      envoy_filter.clear_route_cache();
    }
  }

  /// Enforces the egress policy on the current HTTP request.
  fn enforce_egress_policy<EHF: EnvoyHttpFilter>(
    &self,
    envoy_filter: &mut EHF,
    policy: &CachedEgressPolicy,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    if envoy_filter
      .get_attribute_string(envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .is_some()
    {
      let sni = envoy_filter.get_attribute_string(
        envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName,
      );
      let authority = envoy_filter.get_request_header_value(":authority");
      let matches = match (sni.as_ref(), authority.as_ref()) {
        (Some(sni), Some(authority)) => {
          sni.as_slice() == authority_hostname(authority.as_slice())
        }
        _ => false,
      };
      if !matches {
        envoy_filter.send_response(400, &[], None, None);
        return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
      }
      if let Some(sni_host) = sni
        .as_ref()
        .and_then(|s| std::str::from_utf8(s.as_slice()).ok())
      {
        let lower = sni_host.to_ascii_lowercase();
        let hostname = lower.strip_suffix('.').unwrap_or(&lower);
        if let Some(rule) = policy
          .policy
          .rules
          .iter()
          .find(|rule| rule.mode == EGRESS_MODE_MITM && pattern_matches(&rule.pattern, hostname))
        {
          self.apply_matched_rule(envoy_filter, rule, hostname);
        }
      }
    } else {
      let authority = envoy_filter.get_request_header_value(":authority");
      let matched_rule = authority
        .as_ref()
        .and_then(|auth| std::str::from_utf8(authority_hostname(auth.as_slice())).ok())
        .and_then(|host| {
          let lower = host.to_ascii_lowercase();
          let hostname = lower.strip_suffix('.').unwrap_or(&lower).to_owned();
          policy
            .policy
            .rules
            .iter()
            .find(|rule| {
              rule.mode == EGRESS_MODE_CLEARTEXT && pattern_matches(&rule.pattern, &hostname)
            })
            .map(|rule| (rule, hostname))
        });
      let Some((rule, hostname)) = matched_rule else {
        envoy_filter.send_response(403, &[], Some(b"egress denied"), None);
        return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
      };
      self.apply_matched_rule(envoy_filter, rule, &hostname);
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }
}

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
    match self.get_or_create_cached_egress_policy(envoy_filter) {
      Some(policy) => {
        let status = self.enforce_egress_policy(envoy_filter, policy);
        let counter = if status
          == envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        {
          self.allowed_counter
        } else {
          self.rejected_counter
        };
        let _ = envoy_filter.increment_counter(counter, 1);
        status
      }
      None => envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue,
    }
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
  envoy_filter_config: &mut EC,
  _name: &str,
  config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let mut cfg = match EgressPolicyPepFilterConfig::from_config(config) {
    Ok(cfg) => cfg,
    Err(err) => {
      envoy_log_error!("egress policy pep: invalid filter config: {}", err);
      return None;
    }
  };
  cfg.cache_hit_counter = envoy_filter_config.define_counter(CACHE_HIT_COUNTER_NAME).ok()?;
  cfg.cache_miss_counter = envoy_filter_config.define_counter(CACHE_MISS_COUNTER_NAME).ok()?;
  cfg.allowed_counter = envoy_filter_config.define_counter(ALLOWED_COUNTER_NAME).ok()?;
  cfg.rejected_counter = envoy_filter_config.define_counter(REJECTED_COUNTER_NAME).ok()?;
  cfg.has_effects_counter = envoy_filter_config.define_counter(HAS_EFFECTS_COUNTER_NAME).ok()?;
  Some(Box::new(cfg))
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
      rules: vec![EgressRule {
        pattern: "api.example.com".to_string(),
        mode: "mitm".to_string(),
        has_effects: false,
      }],
    }
  }

  #[test]
  fn test_init() {
    assert!(init());
  }

  #[test]
  fn test_counter_names() {
    assert_eq!(CACHE_HIT_COUNTER_NAME, "ate_egress.cache_hit");
    assert_eq!(CACHE_MISS_COUNTER_NAME, "ate_egress.cache_miss");
    assert_eq!(ALLOWED_COUNTER_NAME, "ate_egress.allowed");
    assert_eq!(REJECTED_COUNTER_NAME, "ate_egress.rejected");
    assert_eq!(HAS_EFFECTS_COUNTER_NAME, "ate_egress.has_effects");
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

  fn test_filter() -> EgressPolicyPepFilter {
    EgressPolicyPepFilter {
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      has_effects_counter: EnvoyCounterId(5),
    }
  }

  #[test]
  fn test_get_or_create_cached_egress_policy_returns_existing_when_present() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut existing = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: expected_policy(),
    };
    let existing_ptr = &mut existing as *mut CachedEgressPolicy as usize;

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(move |_| Some(existing_ptr as *mut c_void));
    mock_filter.expect_get_filter_state_bytes().times(0);
    mock_filter.expect_set_filter_state_object().times(0);
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let cached = filter
      .get_or_create_cached_egress_policy(&mut mock_filter)
      .expect("expected existing CachedEgressPolicy reference");
    assert_eq!(cached.policy, expected_policy());
  }

  #[test]
  fn test_get_or_create_cached_egress_policy_returns_ref_when_created() {
    let filter = test_filter();
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let cached = filter
      .get_or_create_cached_egress_policy(&mut mock_filter)
      .expect("expected CachedEgressPolicy reference");
    assert_eq!(cached.policy, expected_policy());
    assert!(cached.created_at.elapsed() < Duration::from_secs(5));

    drop_cached_egress_policy(stored_ptr.load(Ordering::SeqCst) as *mut c_void);
  }

  #[test]
  fn test_get_or_create_cached_egress_policy_returns_none_when_egress_policy_missing() {
    let filter = test_filter();
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    assert!(filter.get_or_create_cached_egress_policy(&mut mock_filter).is_none());
  }

  #[test]
  fn test_get_or_create_cached_egress_policy_returns_none_when_egress_policy_invalid_json() {
    let filter = test_filter();
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
      .returning(|_| Some(EnvoyBuffer::new(b"not-json")));
    mock_filter.expect_set_filter_state_object().times(0);
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    assert!(filter.get_or_create_cached_egress_policy(&mut mock_filter).is_none());
  }

  #[test]
  fn test_get_or_create_cached_egress_policy_returns_none_when_set_fails() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let raw_policy = br#"{"rules":[]}"#;

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
      .times(1)
      .returning(|_key, _object, _destructor, _life_span| false);
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    assert!(filter.get_or_create_cached_egress_policy(&mut mock_filter).is_none());
  }

  fn expect_skip_callout(mock_filter: &mut MockEnvoyHttpFilter, want_host: &'static str) {
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, val| key == ATE_POLICY_EGRESS_SKIP_CALLOUT && val == b"true")
      .times(1)
      .returning(|_, _| true);
    mock_filter
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == EGRESS_METADATA_NAMESPACE && key == EGRESS_DIAL_KEY && val == EGRESS_DIAL_NAME
      })
      .times(1)
      .returning(|_, _, _| ());
    mock_filter
      .expect_set_dynamic_metadata_string()
      .withf(move |ns, key, val| {
        ns == EGRESS_METADATA_NAMESPACE && key == EGRESS_DIAL_HOST_KEY && val == want_host
      })
      .times(1)
      .returning(|_, _, _| ());
    mock_filter
      .expect_clear_route_cache()
      .times(1)
      .returning(|| ());
  }

  #[test]
  fn test_on_request_headers_populates_inner_filter_state_when_missing() {
    let config = EgressPolicyPepFilterConfig {
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      ..Default::default()
    };
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    expect_skip_callout(&mut mock_filter, "api.example.com");
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(3) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

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
    let config = EgressPolicyPepFilterConfig {
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      ..Default::default()
    };
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_skips_setting_when_inner_filter_state_exists() {
    let config = EgressPolicyPepFilterConfig {
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      ..Default::default()
    };
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut existing = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy {
        rules: vec![EgressRule {
          pattern: "api.example.com".to_string(),
          mode: "cleartext".to_string(),
          has_effects: false,
        }],
      },
    };
    let existing_ptr = &mut existing as *mut CachedEgressPolicy as usize;

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(move |_| Some(existing_ptr as *mut c_void));
    mock_filter.expect_get_filter_state_bytes().times(0);
    mock_filter.expect_set_filter_state_object().times(0);
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| None);
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    expect_skip_callout(&mut mock_filter, "api.example.com");
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(3) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_increments_rejected_when_enforce_stops() {
    let config = EgressPolicyPepFilterConfig {
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      ..Default::default()
    };
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let mut existing = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: expected_policy(),
    };
    let existing_ptr = &mut existing as *mut CachedEgressPolicy as usize;

    mock_filter
      .expect_get_filter_state_object()
      .withf(|key| key == ATE_POLICY_EGRESS_INNER)
      .times(1)
      .returning(move |_| Some(existing_ptr as *mut c_void));
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"other.example.com")));
    mock_filter
      .expect_send_response()
      .withf(|status, _headers, _body, _details| *status == 400)
      .times(1)
      .returning(|_, _, _, _| ());
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(4) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    );
  }

  #[test]
  fn test_enforce_egress_policy_tls_matching_sni_and_host_continues() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: expected_policy(),
    };

    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    expect_skip_callout(&mut mock_filter, "api.example.com");
    mock_filter.expect_send_response().times(0);

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &cached),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_enforce_egress_policy_tls_does_not_skip_callout_when_has_effects() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy {
        rules: vec![EgressRule {
          pattern: "*.example.com".to_string(),
          mode: "mitm".to_string(),
          has_effects: true,
        }],
      },
    };

    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(5) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    mock_filter.expect_set_filter_state_bytes().times(0);
    mock_filter.expect_set_dynamic_metadata_string().times(0);
    mock_filter.expect_clear_route_cache().times(0);
    mock_filter.expect_send_response().times(0);

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &cached),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_authority_hostname() {
    assert_eq!(authority_hostname(b"api.example.com"), b"api.example.com");
    assert_eq!(authority_hostname(b"api.example.com:443"), b"api.example.com");
    assert_eq!(authority_hostname(b"api.example.com:1"), b"api.example.com");
    assert_eq!(authority_hostname(b""), b"");
  }

  #[test]
  fn test_enforce_egress_policy_tls_matching_sni_and_host_with_port_continues() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: expected_policy(),
    };

    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com:1")));
    expect_skip_callout(&mut mock_filter, "api.example.com");
    mock_filter.expect_send_response().times(0);

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &cached),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_enforce_egress_policy_tls_mismatched_sni_and_host_sends_400() {
    let filter = test_filter();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: expected_policy(),
    };

    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"TLSv1.3")));
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"other.example.com")));
    mock_filter
      .expect_send_response()
      .withf(|status, _headers, _body, _details| *status == 400)
      .times(1)
      .returning(|_, _, _, _| ());

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &cached),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    );
  }

  #[test]
  fn test_enforce_egress_policy_cleartext_matching_host_continues() {
    let filter = test_filter();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy {
        rules: vec![
          EgressRule {
            pattern: "*.example.com".to_string(),
            mode: "cleartext".to_string(),
            has_effects: false,
          },
          EgressRule {
            pattern: "exact.example.org".to_string(),
            mode: "cleartext".to_string(),
            has_effects: false,
          },
        ],
      },
    };

    for (authority, want_host) in [
      (b"api.example.com".as_slice(), "api.example.com"),
      (b"API.Example.COM.:8080".as_slice(), "api.example.com"),
      (b"exact.example.org".as_slice(), "exact.example.org"),
    ] {
      let mut mock_filter = MockEnvoyHttpFilter::new();
      mock_filter
        .expect_get_attribute_string()
        .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
        .times(1)
        .returning(|_| None);
      mock_filter
        .expect_get_request_header_value()
        .withf(|key| key == ":authority")
        .times(1)
        .returning(move |_| Some(EnvoyBuffer::new(authority)));
      expect_skip_callout(&mut mock_filter, want_host);
      mock_filter.expect_send_response().times(0);

      assert_eq!(
        filter.enforce_egress_policy(&mut mock_filter, &cached),
        envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
      );
    }
  }

  #[test]
  fn test_enforce_egress_policy_cleartext_does_not_skip_callout_when_has_effects() {
    let filter = test_filter();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy {
        rules: vec![EgressRule {
          pattern: "*.example.com".to_string(),
          mode: "cleartext".to_string(),
          has_effects: true,
        }],
      },
    };

    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_attribute_string()
      .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
      .times(1)
      .returning(|_| None);
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":authority")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"api.example.com")));
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(5) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    mock_filter.expect_set_filter_state_bytes().times(0);
    mock_filter.expect_set_dynamic_metadata_string().times(0);
    mock_filter.expect_clear_route_cache().times(0);
    mock_filter.expect_send_response().times(0);

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &cached),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_enforce_egress_policy_cleartext_mismatched_host_sends_403() {
    let filter = test_filter();
    let cached = CachedEgressPolicy {
      created_at: Instant::now(),
      policy: EgressPolicy {
        rules: vec![
          EgressRule {
            pattern: "api.example.com".to_string(),
            mode: "cleartext".to_string(),
            has_effects: false,
          },
          EgressRule {
            pattern: "tls-only.example.com".to_string(),
            mode: "mitm".to_string(),
            has_effects: false,
          },
        ],
      },
    };

    for authority in &[
      Some(b"other.example.com".as_slice()),
      Some(b"tls-only.example.com".as_slice()),
      Some(b"10.96.0.1:80".as_slice()),
      None,
    ] {
      let mut mock_filter = MockEnvoyHttpFilter::new();
      let authority = *authority;
      mock_filter
        .expect_get_attribute_string()
        .withf(|attr| *attr == envoy_dynamic_module_type_attribute_id::ConnectionTlsVersion)
        .times(1)
        .returning(|_| None);
      mock_filter
        .expect_get_request_header_value()
        .withf(|key| key == ":authority")
        .times(1)
        .returning(move |_| authority.map(EnvoyBuffer::new));
      mock_filter
        .expect_send_response()
        .withf(|status, _headers, body, _details| {
          *status == 403 && *body == Some(b"egress denied".as_slice())
        })
        .times(1)
        .returning(|_, _, _, _| ());

      assert_eq!(
        filter.enforce_egress_policy(&mut mock_filter, &cached),
        envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
      );
    }
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
