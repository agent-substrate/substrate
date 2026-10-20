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
  envoy_log_trace, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};

use crate::constants::ATE_POLICY_EGRESS_HASH;
use crate::pep_http_filter::ATE_EGRESS_POLICY_CREATED_AT;
use crate::policy_store_filter::ATE_POLICY_EGRESS_HASH_NOT_SHARED;

/// Configuration for the egress-policy-refresh HTTP filter.
#[derive(Default)]
pub struct EgressPolicyRefreshFilterConfig;

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyRefreshFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyRefreshFilter)
  }
}

/// Per-stream HTTP filter instance for egress-policy-refresh.
#[derive(Default)]
pub struct EgressPolicyRefreshFilter;

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyRefreshFilter {
  fn on_request_headers(
    &self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    envoy_log_trace!(
      "egress policy refresh: on_request_headers end_of_stream={}",
      end_of_stream
    );
    let same_hash = match (
      envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS_HASH_NOT_SHARED),
      envoy_filter.get_filter_state_bytes(ATE_POLICY_EGRESS_HASH),
    ) {
      (Some(not_shared), Some(shared)) => not_shared.as_slice() == shared.as_slice(),
      _ => false,
    };
    if same_hash {
      let now = DateTime::<Utc>::from(SystemTime::now()).to_rfc3339();
      envoy_filter.set_filter_state_value(
        ATE_EGRESS_POLICY_CREATED_AT,
        now.as_bytes(),
        envoy_dynamic_module_type_filter_state_life_span::Connection,
        envoy_dynamic_module_type_filter_state_stream_sharing::None,
      );
    }
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }
}

/// Called when a new HTTP filter configuration is created.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  _envoy_filter_config: &mut EC,
  _name: &str,
  _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  Some(Box::new(EgressPolicyRefreshFilterConfig))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  #[test]
  fn test_on_request_headers_updates_created_at_when_hashes_match() {
    let config = EgressPolicyRefreshFilterConfig;
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

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }

  #[test]
  fn test_on_request_headers_does_not_update_when_hashes_differ_or_missing() {
    let config = EgressPolicyRefreshFilterConfig;
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

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );

    let mut missing_mock = MockEnvoyHttpFilter::default();
    missing_mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH_NOT_SHARED)
      .times(1)
      .returning(|_| None);
    missing_mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS_HASH)
      .times(1)
      .returning(|_| Some(EnvoyBuffer::new(b"12345")));
    missing_mock.expect_set_filter_state_value().times(0);

    assert_eq!(
      filter.on_request_headers(&mut missing_mock, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
  }
}
