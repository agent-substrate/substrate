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

//! HTTP filter implementation for storing the egress policy.

use std::rc::Rc;

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::{
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  envoy_log_trace, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
};
use substrate_envoy_common::egress_policy::EgressPolicy;

/// Configuration for the egress-policy-store HTTP filter.
pub struct EgressPolicyStoreFilterConfig;

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyStoreFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyStoreFilter::default())
  }
}

/// Per-stream HTTP filter instance for egress-policy-store.
#[derive(Default)]
pub struct EgressPolicyStoreFilter {
  pub policy: Option<Rc<EgressPolicy>>,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyStoreFilter {
  fn on_request_headers(
    &mut self,
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let Some(policy) = EgressPolicy::new_from_metadata(envoy_filter) else {
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    };
    envoy_log_trace!("egress policy store: policy={:?}", policy);
    self.policy = Some(policy);
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }
}

/// Called when a new HTTP filter configuration is created.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  _envoy_filter_config: &mut EC,
  _name: &str,
  _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  Some(Box::new(EgressPolicyStoreFilterConfig))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{
    abi::envoy_dynamic_module_type_metadata_source, EnvoyBuffer, MockEnvoyHttpFilter,
  };
  use substrate_envoy_common::egress_policy::{
    SniRule, ATE_POLICY_EGRESS_NAMESPACE, ATE_POLICY_JSON_KEY,
  };

  #[test]
  fn test_on_request_headers_parses_policy() {
    let mut filter = EgressPolicyStoreFilter::default();
    let mut mock_filter = MockEnvoyHttpFilter::default();
    mock_filter
      .expect_get_metadata_string()
      .withf(|source, namespace, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && namespace == ATE_POLICY_EGRESS_NAMESPACE
          && key == ATE_POLICY_JSON_KEY
      })
      .returning(|_, _, _| {
        Some(EnvoyBuffer::new(
          br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"},{"pattern":"*.example.com","mode":"passthrough"}]}"#,
        ))
      })
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(
      filter.policy,
      Some(Rc::new(EgressPolicy {
        rules: vec![
          SniRule {
            pattern: "api.example.com".to_string(),
            mode: "mitm".to_string(),
          },
          SniRule {
            pattern: "*.example.com".to_string(),
            mode: "passthrough".to_string(),
          },
        ],
      }))
    );
  }

  #[test]
  fn test_on_request_headers_missing_or_invalid_policy() {
    let mut filter = EgressPolicyStoreFilter::default();
    let mut mock_filter = MockEnvoyHttpFilter::default();
    mock_filter
      .expect_get_metadata_string()
      .withf(|source, namespace, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && namespace == ATE_POLICY_EGRESS_NAMESPACE
          && key == ATE_POLICY_JSON_KEY
      })
      .returning(|_, _, _| None)
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(filter.policy, None);

    mock_filter
      .expect_get_metadata_string()
      .withf(|source, namespace, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && namespace == ATE_POLICY_EGRESS_NAMESPACE
          && key == ATE_POLICY_JSON_KEY
      })
      .returning(|_, _, _| Some(EnvoyBuffer::new(b"not-json")))
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(filter.policy, None);
  }

  #[test]
  fn test_on_response_headers() {
    let config = EgressPolicyStoreFilterConfig;
    let mut mock_filter = MockEnvoyHttpFilter::default();
    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }
}
