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
  envoy_log_trace, EnvoyGaugeId, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter,
  HttpFilterConfig,
};
use crate::egress_policy::EgressPolicy;
use crate::egress_policy_store::GlobalEgressPolicyStore;

/// Filter state key where the unshared egress policy hash is written.
pub const ATE_POLICY_EGRESS_HASH_NOT_SHARED: &[u8] = b"dev.ate.policy.egress.hash.not_shared";

/// Configuration for the egress-policy-store HTTP filter.
pub struct EgressPolicyStoreFilterConfig {
  pub store_size: EnvoyGaugeId,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyStoreFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyStoreFilter {
      policy: Rc::default(),
      store_size: self.store_size,
    })
  }
}

/// Per-stream HTTP filter instance for egress-policy-store.
pub struct EgressPolicyStoreFilter {
  pub policy: Rc<EgressPolicy>,
  pub store_size: EnvoyGaugeId,
}

impl Default for EgressPolicyStoreFilter {
  fn default() -> Self {
    Self {
      policy: Rc::default(),
      store_size: EnvoyGaugeId(0),
    }
  }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EgressPolicyStoreFilter {
  fn on_request_headers(
    &mut self,
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let Some(policy) = EgressPolicy::new_from_metadata(envoy_filter) else {
      // TODO: error counter
      // TODO: return an error response
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    };
    envoy_log_trace!("egress policy store: policy={:?}", policy);
    let mut local_store = GlobalEgressPolicyStore::get_local_store();
    if let Some(existing_policy) = local_store.lookup(policy.hash) {
      self.policy = existing_policy;
    } else {
      local_store.store(policy.hash, &policy);
      let _ = envoy_filter.increase_gauge(self.store_size, 1);
      self.policy = policy;
    }
    envoy_filter.set_filter_state_bytes(
      ATE_POLICY_EGRESS_HASH_NOT_SHARED,
      self.policy.hash.to_string().as_bytes(),
    );
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    _envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }

  fn on_stream_complete(&mut self, envoy_filter: &mut EHF) {
    if Rc::strong_count(&self.policy) == 2 {
      GlobalEgressPolicyStore::get_local_store().remove(self.policy.hash);
      let _ = envoy_filter.decrease_gauge(self.store_size, 1);
    }
  }
}

/// Called when a new HTTP filter configuration is created.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  envoy_filter_config: &mut EC,
  _name: &str,
  _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let store_size = envoy_filter_config.define_gauge("store_size").ok()?;
  Some(Box::new(EgressPolicyStoreFilterConfig { store_size }))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{
    abi::envoy_dynamic_module_type_metadata_source, EnvoyBuffer, MockEnvoyHttpFilter,
  };
  use crate::egress_policy::{
    SniRule, ATE_POLICY_EGRESS_NAMESPACE, ATE_POLICY_JSON_KEY,
  };

  #[test]
  fn test_on_request_headers_parses_policy() {
    let config = EgressPolicyStoreFilterConfig {
      store_size: EnvoyGaugeId(1),
    };
    let mut filter = EgressPolicyStoreFilter {
      policy: Rc::default(),
      store_size: config.store_size,
    };
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
    mock_filter
      .expect_increase_gauge()
      .withf(|id, value| *id == EnvoyGaugeId(1) && *value == 1)
      .returning(|_, _| Ok(()))
      .once();
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_HASH_NOT_SHARED
          && std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .is_some()
      })
      .returning(|_, _| true)
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(
      filter.policy.rules,
      vec![
        SniRule {
          pattern: "api.example.com".to_string(),
          mode: "mitm".to_string(),
        },
        SniRule {
          pattern: "*.example.com".to_string(),
          mode: "passthrough".to_string(),
        },
      ]
    );
    assert_ne!(filter.policy.hash, 0);

    let hash = filter.policy.hash;
    assert!(Rc::ptr_eq(
      &GlobalEgressPolicyStore::get_local_store()
        .lookup(hash)
        .unwrap(),
      &filter.policy
    ));

    // A second filter receiving the same policy reuses the stored Rc<EgressPolicy> without incrementing store_size.
    let mut second_filter = EgressPolicyStoreFilter {
      policy: Rc::default(),
      store_size: config.store_size,
    };
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
    let hash_bytes_second = hash.to_string().into_bytes();
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(move |key, value| {
        key == ATE_POLICY_EGRESS_HASH_NOT_SHARED && value == hash_bytes_second
      })
      .returning(|_, _| true)
      .once();

    assert_eq!(
      second_filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(Rc::ptr_eq(&second_filter.policy, &filter.policy));

    // Completing and dropping the first filter keeps the policy in the local store while the second filter holds it.
    filter.on_stream_complete(&mut mock_filter);
    drop(filter);
    assert!(GlobalEgressPolicyStore::get_local_store()
      .lookup(hash)
      .is_some());

    // Completing and dropping the last filter holding the policy decrements store_size and removes it from the local store.
    mock_filter
      .expect_decrease_gauge()
      .withf(|id, value| *id == EnvoyGaugeId(1) && *value == 1)
      .returning(|_, _| Ok(()))
      .once();
    second_filter.on_stream_complete(&mut mock_filter);
    drop(second_filter);
    assert!(GlobalEgressPolicyStore::get_local_store()
      .lookup(hash)
      .is_none());
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
    assert_eq!(filter.policy, Rc::default());

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
    assert_eq!(filter.policy, Rc::default());
  }

  #[test]
  fn test_on_response_headers() {
    let config = EgressPolicyStoreFilterConfig {
      store_size: EnvoyGaugeId(0),
    };
    let mut mock_filter = MockEnvoyHttpFilter::default();
    let mut filter = config.new_http_filter(&mut mock_filter);
    assert_eq!(
      filter.on_response_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }
}
