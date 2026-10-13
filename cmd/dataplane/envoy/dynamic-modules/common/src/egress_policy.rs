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

use serde::Deserialize;

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

#[cfg(test)]
mod tests {
  use super::*;

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
}
