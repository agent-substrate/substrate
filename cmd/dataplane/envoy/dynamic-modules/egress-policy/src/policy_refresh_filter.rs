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

//! HTTP filter implementation for refreshing the egress policy.

use std::time::SystemTime;

use chrono::{DateTime, Utc};
use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::{
    envoy_dynamic_module_type_filter_state_life_span,
    envoy_dynamic_module_type_filter_state_stream_sharing,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
  },
  EnvoyCounterId, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter,
  HttpFilterConfig,
};

use crate::constants::ATE_POLICY_EGRESS_HASH;
use crate::pep_http_filter::ATE_EGRESS_POLICY_CREATED_AT;
use crate::policy_store_filter::ATE_POLICY_EGRESS_HASH_NOT_SHARED;

/// Counter for policy changes detected on an active connection.
pub const POLICY_CHANGE_FOR_ACTIVE_CONNECTION: &str =
  "ate_egress.policy_change_for_active_connection";

/// Configuration for the egress-policy-refresh HTTP filter.
pub struct EgressPolicyRefreshFilterConfig {
  pub policy_change_for_active_connection: EnvoyCounterId,
}

impl Default for EgressPolicyRefreshFilterConfig {
  fn default() -> Self {
    Self {
      policy_change_for_active_connection: EnvoyCounterId(0),
    }
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyRefreshFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyRefreshFilter {
      policy_change_for_active_connection: self.policy_change_for_active_connection,
    })
  }
}

/// Per-stream HTTP filter instance for egress-policy-refresh.
pub struct EgressPolicyRefreshFilter {
  pub policy_change_for_active_connection: EnvoyCounterId,
}

impl Default for EgressPolicyRefreshFilter {
  fn default() -> Self {
    Self {
      policy_change_for_active_connection: EnvoyCounterId(0),
    }
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyRefreshFilter {
  fn on_request_headers(
    &self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    // Compare hash produced by the previos polisy-store filter in the ATE_POLICY_EGRESS_HASH_NOT_SHARED
    // filter state, with the hash original of the original policy retrieved during establishment of
    // the CONNECT tunnel.
    let same_hash = match (
      envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS_HASH_NOT_SHARED),
      envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS_HASH),
    ) {
      (Some(not_shared), Some(shared)) => not_shared.as_slice() == shared.as_slice(),
      // If either hash is not available, continue to the next filter.
      _ => return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue,
    };
    
    if same_hash {
      // If policy did not change, update the timestamp so it can be used for the next TTL duration.
      let now = DateTime::<Utc>::from(SystemTime::now()).to_rfc3339();
      envoy_filter.set_filter_state_value(
        ATE_EGRESS_POLICY_CREATED_AT,
        now.as_bytes(),
        envoy_dynamic_module_type_filter_state_life_span::Connection,
        envoy_dynamic_module_type_filter_state_stream_sharing::None,
      );
    } else {
      // If policy did change, increment the counter and keep timestamp stale, so all subsequent
      // requests on this connection continue going to the ext_proc.
      // TODO(yanavlasov): initiate GOAWAY (connection: close for H/1) on this connection, so it
      // can be re-established with the new policy. Dynamic modules do not currently have API
      // for this.
      let _ = envoy_filter.increment_counter(self.policy_change_for_active_connection, 1);
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }
}

/// Called when a new HTTP filter configuration is created.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  envoy_filter_config: &mut EC,
  _name: &str,
  _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let policy_change_for_active_connection = envoy_filter_config
    .define_counter(POLICY_CHANGE_FOR_ACTIVE_CONNECTION)
    .ok()?;
  Some(Box::new(EgressPolicyRefreshFilterConfig {
    policy_change_for_active_connection,
  }))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  #[test]
  fn test_on_request_headers_updates_created_at_when_hashes_match() {
    let config = EgressPolicyRefreshFilterConfig {
      policy_change_for_active_connection: EnvoyCounterId(1),
    };
    let mut mock_filter = MockEnvoyHttpFilter::default();
    let filter = config.new_http_filter(&mut mock_filter);

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH_NOT_SHARED)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"12345")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"12345")));
    mock_filter
      .expect_set_filter_state_value()
      .withf(|key, value, life_span, stream_sharing| {
        key == ATE_EGRESS_POLICY_CREATED_AT
          && *life_span == envoy_dynamic_module_type_filter_state_life_span::Connection
          && *stream_sharing == envoy_dynamic_module_type_filter_state_stream_sharing::None
          && std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            .is_some()
      })
      .times(1)
      .returning(|_, _, _, _| true);
    mock_filter.expect_increment_counter().times(0);

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_increments_counter_when_hashes_differ() {
    let config = EgressPolicyRefreshFilterConfig {
      policy_change_for_active_connection: EnvoyCounterId(1),
    };
    let mut mock_filter = MockEnvoyHttpFilter::default();
    let filter = config.new_http_filter(&mut mock_filter);

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH_NOT_SHARED)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"12345")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"67890")));
    mock_filter.expect_set_filter_state_value().times(0);
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(1) && *value == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_noop_when_hash_missing() {
    let config = EgressPolicyRefreshFilterConfig {
      policy_change_for_active_connection: EnvoyCounterId(1),
    };
    let mut mock_filter = MockEnvoyHttpFilter::default();
    let filter = config.new_http_filter(&mut mock_filter);

    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH_NOT_SHARED)
      .times(1)
      .returning(|_| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"12345")));
    mock_filter.expect_set_filter_state_value().times(0);
    mock_filter.expect_increment_counter().times(0);

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }
}
