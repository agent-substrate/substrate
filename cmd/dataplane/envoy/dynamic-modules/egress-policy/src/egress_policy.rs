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

//! Shared egress policy types for Substrate Envoy dynamic modules.

use std::hash::{Hash, Hasher};
use std::rc::Rc;

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::envoy_dynamic_module_type_metadata_source, EnvoyHttpFilter,
};
// Use quiality flavor of Rapidhash to trade collision resistance over performance.
use rapidhash::quality::RapidHasher;
use serde::Deserialize;

/// Dynamic metadata namespace holding the egress policy JSON.
pub const ATE_POLICY_EGRESS_NAMESPACE: &str = "dev.ate.policy.egress";

/// Dynamic metadata key under [`ATE_POLICY_EGRESS_NAMESPACE`] holding the serialized policy JSON.
pub const ATE_POLICY_JSON_KEY: &str = "policy_json";

/// The SNI rules for a connection.
#[derive(Debug, Default, Deserialize, PartialEq, Eq, Hash)]
pub struct EgressPolicy {
  /// Most specific first; the first match wins.
  pub rules: Vec<SniRule>,
  #[serde(skip)]
  pub hash: u64,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
pub struct SniRule {
  pub pattern: String,
  // TODO(yanavlasov): optimize this to use enum to reduce memory usage.
  // Mode determines which filter chain (MITM or passthrough) is selected if the patter matches.
  pub mode: String,
}

impl EgressPolicy {
  /// Reads the egress policy JSON from dynamic metadata (`dev.ate.policy.egress:policy_json`)
  /// and parses it into an [`Rc<EgressPolicy>`].
  pub fn new_from_metadata<EHF: EnvoyHttpFilter>(envoy_filter: &mut EHF) -> Option<Rc<Self>> {
    let metadata = envoy_filter.get_metadata_string(
      envoy_dynamic_module_type_metadata_source::Dynamic,
      ATE_POLICY_EGRESS_NAMESPACE,
      ATE_POLICY_JSON_KEY,
    )?;
    let mut policy = serde_json::from_slice::<Self>(metadata.as_slice()).ok()?;
    // Once the policy was parsed overwrite JSON string in the metadata with empty value so it does
    // not consume memory.
    envoy_filter.set_dynamic_metadata_string(ATE_POLICY_EGRESS_NAMESPACE, ATE_POLICY_JSON_KEY, "");
    let mut hasher = RapidHasher::default();
    Hash::hash(&policy, &mut hasher);
    policy.hash = hasher.finish();
    Some(Rc::new(policy))
  }
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  #[test]
  fn test_policy_json_shape() {
    let parsed: EgressPolicy = serde_json::from_str(
      r#"{"rules":[{"pattern":"api.example.com","mode":"mitm"},{"pattern":"*","mode":"mitm"}]}"#,
    )
    .unwrap();
    assert_eq!(
      parsed,
      EgressPolicy {
        rules: vec![
          SniRule {
            pattern: "api.example.com".to_string(),
            mode: "mitm".to_string(),
          },
          SniRule {
            pattern: "*".to_string(),
            mode: "mitm".to_string(),
          },
        ],
        hash: 0,
      }
    );
    assert!(
      serde_json::from_str::<EgressPolicy>(r#"{"allowed_snis":["api.example.com"]}"#).is_err()
    );
  }

  #[test]
  fn test_new_from_metadata() {
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
      .times(2);
    mock_filter
      .expect_set_dynamic_metadata_string()
      .withf(|namespace, key, value| {
        namespace == ATE_POLICY_EGRESS_NAMESPACE && key == ATE_POLICY_JSON_KEY && value.is_empty()
      })
      .return_const(())
      .times(2);

    let p1 = EgressPolicy::new_from_metadata(&mut mock_filter).unwrap();
    let p2 = EgressPolicy::new_from_metadata(&mut mock_filter).unwrap();
    assert_eq!(
      p1.rules,
      vec![SniRule {
        pattern: "api.example.com".to_string(),
        mode: "mitm".to_string(),
      }]
    );
    assert_ne!(p1.hash, 0);
    assert_eq!(p1.hash, p2.hash);
    assert_eq!(p1, p2);

    mock_filter
      .expect_get_metadata_string()
      .withf(|source, namespace, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && namespace == ATE_POLICY_EGRESS_NAMESPACE
          && key == ATE_POLICY_JSON_KEY
      })
      .returning(|_, _, _| {
        Some(EnvoyBuffer::new(
          br#"{"rules":[{"pattern":"api.example.com","mode":"passthrough"}]}"#,
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

    let p3 = EgressPolicy::new_from_metadata(&mut mock_filter).unwrap();
    assert_ne!(p1.hash, p3.hash);
  }

  #[test]
  fn test_new_from_metadata_missing_or_invalid() {
    let mut mock_filter = MockEnvoyHttpFilter::default();
    mock_filter
      .expect_get_metadata_string()
      .returning(|_, _, _| None)
      .once();
    assert_eq!(EgressPolicy::new_from_metadata(&mut mock_filter), None);

    mock_filter
      .expect_get_metadata_string()
      .returning(|_, _, _| Some(EnvoyBuffer::new(b"not-json")))
      .once();
    assert_eq!(EgressPolicy::new_from_metadata(&mut mock_filter), None);
  }
}
