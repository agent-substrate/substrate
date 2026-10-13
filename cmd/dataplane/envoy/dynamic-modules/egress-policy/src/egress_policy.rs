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

use std::rc::Rc;

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::envoy_dynamic_module_type_metadata_source, EnvoyHttpFilter,
};
use serde::Deserialize;

/// Dynamic metadata namespace holding the egress policy JSON.
pub const ATE_POLICY_EGRESS_NAMESPACE: &str = "dev.ate.policy.egress";

/// Dynamic metadata key under [`ATE_POLICY_EGRESS_NAMESPACE`] holding the serialized policy JSON.
pub const ATE_POLICY_JSON_KEY: &str = "policy_json";

/// The SNI rules for a connection.
#[derive(Debug, Deserialize, PartialEq)]
pub struct EgressPolicy {
  /// Most specific first; the first match wins.
  pub rules: Vec<SniRule>,
}

/// A pattern and the mode applied when it matches.
#[derive(Debug, Deserialize, PartialEq)]
pub struct SniRule {
  pub pattern: String,
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
    let policy = serde_json::from_slice::<Self>(metadata.as_slice()).ok()?;
    Some(Rc::new(policy))
  }
}

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
      .once();

    let policy = EgressPolicy::new_from_metadata(&mut mock_filter);
    assert_eq!(
      policy,
      Some(Rc::new(EgressPolicy {
        rules: vec![SniRule {
          pattern: "api.example.com".to_string(),
          mode: "mitm".to_string(),
        }],
      }))
    );
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
