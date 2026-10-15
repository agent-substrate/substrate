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
  },
  envoy_log_trace, EnvoyCounterId, EnvoyGaugeId, EnvoyHttpFilter, EnvoyHttpFilterConfig,
  HttpFilter, HttpFilterConfig,
};
use crate::egress_policy::EgressPolicy;
use crate::egress_policy_store::GlobalEgressPolicyStore;

/// Filter state key where the unshared egress policy hash is written.
pub const ATE_POLICY_EGRESS_HASH_NOT_SHARED: &[u8] = b"dev.ate.policy.egress.hash.not_shared";

/// Gauge for the size of egress policy store.
pub const EGRESS_POLICY_STORE_SIZE: &str = "ate_egress.policy_store_size";

/// Counter for egress policy parse errors.
pub const EGRESS_POLICY_PARSE_ERRORS: &str = "ate_egress.parse_errors";

/// Counter for shared egress policies already present in the local store.
pub const EGRESS_POLICY_SHARED: &str = "ate_egress.policy_shared";

/// Configuration for the egress-policy-store HTTP filter.
pub struct EgressPolicyStoreFilterConfig {
  pub store_size: EnvoyGaugeId,
  pub parse_errors: EnvoyCounterId,
  pub policy_shared: EnvoyCounterId,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EgressPolicyStoreFilterConfig {
  fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
    Box::new(EgressPolicyStoreFilter {
      policy: Rc::default(),
      store_size: self.store_size,
      parse_errors: self.parse_errors,
      policy_shared: self.policy_shared,
    })
  }
}

/// Per-stream HTTP filter instance for egress-policy-store.
pub struct EgressPolicyStoreFilter {
  pub policy: Rc<EgressPolicy>,
  pub store_size: EnvoyGaugeId,
  pub parse_errors: EnvoyCounterId,
  pub policy_shared: EnvoyCounterId,
}

impl Default for EgressPolicyStoreFilter {
  fn default() -> Self {
    Self {
      policy: Rc::default(),
      store_size: EnvoyGaugeId(0),
      parse_errors: EnvoyCounterId(0),
      policy_shared: EnvoyCounterId(0),
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
      let _ = envoy_filter.increment_counter(self.parse_errors, 1);
      // TODO: return an error response
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    };
    envoy_log_trace!("egress policy store: policy={:?}", policy);
    let mut local_store = GlobalEgressPolicyStore::get_local_store();
    if let Some(existing_policy) = local_store.lookup(policy.hash) {
      // Sanity check that the policy is the same and fail closed on hash collisions.
      if existing_policy != policy {
        envoy_filter.send_response(400, &[], Some(b"hash collision"), Some("hash_collision"));
        return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
      }
      let _ = envoy_filter.increment_counter(self.policy_shared, 1);
      // Use the policy that was alredy in the store.
      self.policy = existing_policy;
    } else {
      local_store.store(policy.hash, &policy);
      let _ = envoy_filter.increase_gauge(self.store_size, 1);
      self.policy = policy;
    }
    // Dynamic module API does not yet support writing filter state values
    // that are shared with the upstream (will be available in Envoy v1.40).
    // Write non shared value, and then write shared value in the set_filter_state
    // filter which is next in the filter chain.
    // TODO(yanavlasov): remove this workaround after Envoy v1.40 is released.
    envoy_filter.set_filter_state_bytes(
      ATE_POLICY_EGRESS_HASH_NOT_SHARED,
      self.policy.hash.to_string().as_bytes(),
    );
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  // on_stream_complete is called right before the filter object is dropped
  // This is the last callback that still has valid filter ref.
  fn on_stream_complete(&mut self, envoy_filter: &mut EHF) {
    // Reference count 2 for the policy object means that it is held by the policy
    // store table and this filter that is about to be dropped.
    // To avoid leaking the policy in the store, remove it from the store. When on_stream_complete
    // callback finishes and and filter is dropped, the only remaining reference to the policy is
    // decremented and the policy object will be dropped as well.
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
  let store_size = envoy_filter_config.define_gauge(EGRESS_POLICY_STORE_SIZE).ok()?;
  let parse_errors = envoy_filter_config
    .define_counter(EGRESS_POLICY_PARSE_ERRORS)
    .ok()?;
  let policy_shared = envoy_filter_config
    .define_counter(EGRESS_POLICY_SHARED)
    .ok()?;
  Some(Box::new(EgressPolicyStoreFilterConfig {
    store_size,
    parse_errors,
    policy_shared,
  }))
}

// ==========================================
// Tests
// ==========================================

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
      parse_errors: EnvoyCounterId(2),
      policy_shared: EnvoyCounterId(3),
    };
    let mut filter = EgressPolicyStoreFilter {
      policy: Rc::default(),
      store_size: config.store_size,
      parse_errors: config.parse_errors,
      policy_shared: config.policy_shared,
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
      .expect_set_dynamic_metadata_string()
      .withf(|namespace, key, value| {
        namespace == ATE_POLICY_EGRESS_NAMESPACE && key == ATE_POLICY_JSON_KEY && value.is_empty()
      })
      .return_const(())
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
      parse_errors: config.parse_errors,
      policy_shared: config.policy_shared,
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
    mock_filter
      .expect_set_dynamic_metadata_string()
      .withf(|namespace, key, value| {
        namespace == ATE_POLICY_EGRESS_NAMESPACE && key == ATE_POLICY_JSON_KEY && value.is_empty()
      })
      .return_const(())
      .once();
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(3) && *value == 1)
      .returning(|_, _| Ok(()))
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
    let mut filter = EgressPolicyStoreFilter {
      parse_errors: EnvoyCounterId(2),
      ..Default::default()
    };
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .returning(|_, _| Ok(()))
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
    mock_filter
      .expect_increment_counter()
      .withf(|id, value| *id == EnvoyCounterId(2) && *value == 1)
      .returning(|_, _| Ok(()))
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(filter.policy, Rc::default());
  }

  #[test]
  fn test_on_request_headers_hash_collision() {
    let mut setup_mock = MockEnvoyHttpFilter::default();
    setup_mock
      .expect_get_metadata_string()
      .returning(|_, _, _| {
        Some(EnvoyBuffer::new(
          br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"}]}"#,
        ))
      })
      .once();
    setup_mock
      .expect_set_dynamic_metadata_string()
      .return_const(())
      .once();
    let incoming_policy = EgressPolicy::new_from_metadata(&mut setup_mock).unwrap();
    let colliding_hash = incoming_policy.hash;

    // Create a different policy but with the same hash to test filter behavior.
    let existing_policy = Rc::new(EgressPolicy {
      rules: vec![SniRule {
        pattern: "other.example.com".to_string(),
        mode: "passthrough".to_string(),
      }],
      hash: colliding_hash,
    });
    GlobalEgressPolicyStore::get_local_store().store(colliding_hash, &existing_policy);

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
          br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"}]}"#,
        ))
      })
      .once();
    mock_filter
      .expect_set_dynamic_metadata_string()
      .withf(|namespace, key, value| {
        namespace == ATE_POLICY_EGRESS_NAMESPACE && key == ATE_POLICY_JSON_KEY && value.is_empty()
      })
      .return_const(())
      .once();
    mock_filter
      .expect_send_response()
      .withf(|status_code, headers, body, details| {
        *status_code == 400
          && headers.is_empty()
          && body == &Some(&b"hash collision"[..])
          && details == &Some("hash_collision")
      })
      .return_const(())
      .once();

    assert_eq!(
      filter.on_request_headers(&mut mock_filter, false),
      envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    );
    assert_eq!(filter.policy, Rc::default());

    GlobalEgressPolicyStore::get_local_store().remove(colliding_hash);
  }
}
