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

use std::{rc::Rc, time::SystemTime};

use chrono::{DateTime, Utc};
use crate::constants::{ATE_POLICY_EGRESS_HASH, SNI_MODE_MITM};
use crate::pep_config::{Config};
use crate::egress_policy::{EgressPolicy, SniRule};
use crate::egress_policy_store::GlobalEgressPolicyStore;
use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::{
    envoy_dynamic_module_type_attribute_id,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
  },
  envoy_log_trace, EnvoyCounterId,
  EnvoyHttpFilter, HttpFilter,
};

/// Filter state key holding the initial timestamp of the policy retrieval.
pub const ATE_EGRESS_POLICY_CREATED_AT: &[u8] = b"dev.ate.egress.policy.created_at";

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

/// Response body returned when a WebSocket upgrade request is rejected.
const WEBSOCKET_DENIED_BODY: &[u8] = b"WebSocket egress is not supported";

/// Response body returned when a CONNECT request is rejected.
const CONNECT_DENIED_BODY: &[u8] = b"CONNECT egress is not supported";

/// Response body returned when SNI is missing for MITM policy enforcement.
const MISSING_SNI_BODY: &[u8] = b"Missing SNI in TLS connection";

/// Response body and details (for logs) returned when SNI does not match request authority.
const SNI_AUTHORITY_MISMATCH_BODY: &[u8] = b"SNI does not match request authority";
const SNI_AUTHORITY_MISMATCH_DETAILS: &str = "sni_authority_mismatch";


/// Per-stream HTTP filter instance for egress policy enforcement.
pub struct EgressPolicyPepFilter {
  pub(crate) config: Config,
  pub(crate) cache_hit_counter: EnvoyCounterId,
  pub(crate) cache_miss_counter: EnvoyCounterId,
  pub(crate) allowed_counter: EnvoyCounterId,
  pub(crate) rejected_counter: EnvoyCounterId,
  pub(crate) has_effects_counter: EnvoyCounterId,
}

/// Returns the hostname portion of an `:authority` header value, stripping any
/// `:port` suffix if present.
fn authority_hostname(authority: &[u8]) -> &[u8] {
  match authority.iter().position(|&b| b == b':') {
    Some(idx) => &authority[..idx],
    None => authority,
  }
}

/// Reports whether a comma-separated HTTP header value contains `want` as an
/// ASCII case-insensitive token.
fn has_http_token(value: &[u8], want: &[u8]) -> bool {
  value
    .split(|&b| b == b',')
    .any(|token| token.trim_ascii().eq_ignore_ascii_case(want))
}

/// Reports whether the current request is an HTTP/1.1 WebSocket upgrade.
fn is_websocket_upgrade<EHF: EnvoyHttpFilter>(envoy_filter: &mut EHF) -> bool {
  envoy_filter
    .get_request_header_value("upgrade")
    .is_some_and(|v| has_http_token(v.as_slice(), b"websocket"))
    && envoy_filter
      .get_request_header_value("connection")
      .is_some_and(|v| has_http_token(v.as_slice(), b"upgrade"))
    && envoy_filter
      .get_request_header_value(":method")
      .is_some_and(|v| v.as_slice() == b"GET")
}

/// Reports whether the current request is an HTTP CONNECT request.
fn is_connect_request<EHF: EnvoyHttpFilter>(envoy_filter: &mut EHF) -> bool {
  envoy_filter
    .get_request_header_value(":method")
    .is_some_and(|v| v.as_slice().eq_ignore_ascii_case(b"CONNECT"))
}

impl EgressPolicyPepFilter {
  fn get_policy<EHF: EnvoyHttpFilter>(&self, envoy_filter: &mut EHF,) -> Option<Rc<EgressPolicy>> {
    let created_at = envoy_filter
      .get_filter_state_bytes(ATE_EGRESS_POLICY_CREATED_AT)
      .and_then(|value| std::str::from_utf8(value.as_slice()).ok()?.parse::<DateTime<Utc>>().ok())?;
    if DateTime::<Utc>::from(SystemTime::now())
      .signed_duration_since(created_at)
      .to_std()
      .is_ok_and(|age| age > self.config.cache_ttl)
    {
      return None;
    }
    envoy_filter
      .get_filter_state_bytes(ATE_POLICY_EGRESS_HASH)
      .and_then(|value| std::str::from_utf8(value.as_slice()).ok()?.parse::<u64>().ok())
      .and_then(|hash| GlobalEgressPolicyStore::get_local_store().lookup(hash))
  }

  fn apply_matched_rule<EHF: EnvoyHttpFilter>(
    &self,
    envoy_filter: &mut EHF,
    rule: &SniRule,
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
    policy: &EgressPolicy,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    // TODO(yanavlasov): prevent upgrades in the filter chain config, so this additional
    // check is not needed.
    if is_websocket_upgrade(envoy_filter) {
      envoy_filter.send_response(403, &[], Some(WEBSOCKET_DENIED_BODY), None);
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
    }
    if is_connect_request(envoy_filter) {
      envoy_filter.send_response(403, &[], Some(CONNECT_DENIED_BODY), None);
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
    }

    let sni = envoy_filter.get_attribute_string(
      envoy_dynamic_module_type_attribute_id::ConnectionRequestedServerName,
    );
    // Sanity check that SNI is present. Listener filter checks for SNI presence, so if it
    // missing, then something went off the rails in Envoy.
    if sni.is_none() {
      envoy_filter.send_response(400, &[], Some(MISSING_SNI_BODY), None);
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
    }
    // Check that hostname from :authority matches SNI
    let authority = envoy_filter.get_request_header_value(":authority");
    let matches = match (sni.as_ref(), authority.as_ref()) {
      (Some(sni), Some(authority)) => sni
        .as_slice()
        .eq_ignore_ascii_case(authority_hostname(authority.as_slice())),
      _ => false,
    };
    if !matches {
      envoy_filter.send_response(400, &[], Some(SNI_AUTHORITY_MISMATCH_BODY), Some(SNI_AUTHORITY_MISMATCH_DETAILS));
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
    }
    if let Some(sni_host) = sni
      .as_ref()
      .and_then(|s| std::str::from_utf8(s.as_slice()).ok())
    {
      let lower = sni_host.to_ascii_lowercase();
      let hostname = lower.strip_suffix('.').unwrap_or(&lower);
      if let Some(rule) = policy
        .rules
        .iter()
        .find(|rule| rule.mode == SNI_MODE_MITM && rule.pattern_matches(hostname))
      {
        self.apply_matched_rule(envoy_filter, rule, hostname);
      }
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyPepFilter {
  fn on_request_headers(
    &self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    envoy_log_trace!(
      "egress policy pep: on_request_headers end_of_stream={}",
      end_of_stream
    );
    if !self.config.cache_enabled {
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    }
    match self.get_policy(envoy_filter) {
      Some(policy) => {
        let _ = envoy_filter.increment_counter(self.cache_hit_counter, 1);
        let status = self.enforce_egress_policy(envoy_filter, &policy);
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
      None => {
        let _ = envoy_filter.increment_counter(self.cache_miss_counter, 1);
        envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
      }
    }
  }
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  fn test_filter() -> EgressPolicyPepFilter {
    EgressPolicyPepFilter {
      config: Config::default(),
      cache_hit_counter: EnvoyCounterId(1),
      cache_miss_counter: EnvoyCounterId(2),
      allowed_counter: EnvoyCounterId(3),
      rejected_counter: EnvoyCounterId(4),
      has_effects_counter: EnvoyCounterId(5),
    }
  }

  fn expect_no_websocket_upgrade(mock_filter: &mut MockEnvoyHttpFilter) {
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == "upgrade")
      .times(1)
      .returning(|_| None);
  }

  #[test]
  fn test_enforce_egress_policy_rejects_connect() {
    let filter = test_filter();
    let policy = EgressPolicy {
      rules: Vec::new(),
      hash: 1,
    };
    let mut mock_filter = MockEnvoyHttpFilter::new();
    expect_no_websocket_upgrade(&mut mock_filter);
    mock_filter
      .expect_get_request_header_value()
      .withf(|key| key == ":method")
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"CONNECT")));
    mock_filter
      .expect_send_response()
      .withf(|status, headers, body, details| {
        *status == 403
          && headers.is_empty()
          && *body == Some(CONNECT_DENIED_BODY)
          && details.is_none()
      })
      .times(1)
      .returning(|_, _, _, _| {});

    assert_eq!(
      filter.enforce_egress_policy(&mut mock_filter, &policy),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    );
  }

  #[test]
  fn test_get_policy_fresh_and_stale() {
    let filter = test_filter();
    let policy = Rc::new(EgressPolicy {
      rules: Vec::new(),
      hash: 42,
    });
    GlobalEgressPolicyStore::get_local_store().store(42, &policy);

    let fresh_timestamp = DateTime::<Utc>::from(SystemTime::now()).to_rfc3339();
    let mut mock_filter = MockEnvoyHttpFilter::new();
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_EGRESS_POLICY_CREATED_AT)
      .times(1)
      .returning(move |_| Some(EnvoyBuffer::new(fresh_timestamp.as_bytes())));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"42")));
    assert_eq!(filter.get_policy(&mut mock_filter), Some(Rc::clone(&policy)));

    let stale_timestamp = DateTime::<Utc>::from(
      SystemTime::now() - filter.config.cache_ttl - std::time::Duration::from_secs(1),
    )
    .to_rfc3339();
    let mut stale_mock = MockEnvoyHttpFilter::new();
    stale_mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_EGRESS_POLICY_CREATED_AT)
      .times(1)
      .returning(move |_| Some(EnvoyBuffer::new(stale_timestamp.as_bytes())));
    stale_mock.expect_get_filter_state_bytes().times(0);
    assert!(filter.get_policy(&mut stale_mock).is_none());

    GlobalEgressPolicyStore::get_local_store().remove(42);
  }
}
