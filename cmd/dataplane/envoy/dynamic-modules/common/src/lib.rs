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

//! Shared egress policy types and matching logic for Envoy dynamic modules.

use serde::Deserialize;

/// Filter state holding the SNI rules as JSON. See
/// EgressPolicyMetadataNamespace in cmd/atenet/internal/router/extproc.
pub const ATE_POLICY_EGRESS: &[u8] = b"dev.ate.policy.egress";

/// Mode of a rule whose match terminates the connection.
pub const SNI_MODE_MITM: &str = "mitm";

/// Mode of a rule whose match forwards the connection without decryption.
pub const SNI_MODE_PASSTHROUGH: &str = "passthrough";

/// The SNI rules for a connection.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct EgressPolicy {
  /// Most specific first; the first match wins.
  pub rules: Vec<SniRule>,
}

/// A pattern and the mode applied when it matches.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SniRule {
  pub pattern: String,
  pub mode: String,
}

/// Reports whether a normalized `hostname` matches `pattern`. Must agree with
/// HostnamePattern.Matches in internal/egresspolicy.
pub fn pattern_matches(pattern: &str, hostname: &str) -> bool {
  if hostname.is_empty() {
    return false;
  }
  if pattern == "*" {
    return true;
  }
  if let Some(suffix) = pattern.strip_prefix("*.") {
    return match hostname
      .strip_suffix(suffix)
      .and_then(|rest| rest.strip_suffix('.'))
    {
      Some(label) => !label.is_empty() && !label.contains('.'),
      None => false,
    };
  }
  pattern == hostname
}

#[cfg(test)]
mod tests {
  use super::*;

  fn policy(rules: &[(&str, &str)]) -> EgressPolicy {
    EgressPolicy {
      rules: rules
        .iter()
        .map(|(pattern, mode)| SniRule {
          pattern: pattern.to_string(),
          mode: mode.to_string(),
        })
        .collect(),
    }
  }

  #[test]
  fn test_pattern_matches() {
    let cases: &[(&str, &str, bool)] = &[
      // "*" matches every name and only names.
      ("*", "anything.example.com", true),
      ("*", "localhost", true),
      ("*", "", false),
      // "*.suffix" matches exactly one non-empty leftmost label.
      ("*.example.com", "api.example.com", true),
      ("*.example.com", "example.com", false),
      ("*.example.com", "a.b.example.com", false),
      ("*.example.com", ".example.com", false),
      ("*.example.com", "xexample.com", false),
      ("*.example.com", "api.example.co", false),
      ("*.example.com", "api.example.com.evil", false),
      // Anything else matches the whole name.
      ("api.example.com", "api.example.com", true),
      ("api.example.com", "www.api.example.com", false),
      ("api.example.com", "api.example.co", false),
      ("example.com", "api.example.com", false),
      // Normalization is the caller's job.
      ("api.example.com", "API.example.com", false),
      ("api.example.com", "api.example.com.", false),
    ];
    for (pattern, hostname, want) in cases {
      assert_eq!(
        pattern_matches(pattern, hostname),
        *want,
        "pattern_matches({pattern:?}, {hostname:?})"
      );
    }
  }

  #[test]
  fn test_policy_json_shape() {
    let parsed: EgressPolicy = serde_json::from_str(
      r#"{"rules":[{"pattern":"api.example.com","mode":"mitm"},{"pattern":"*","mode":"mitm"}]}"#,
    )
    .unwrap();
    assert_eq!(parsed, policy(&[("api.example.com", "mitm"), ("*", "mitm")]));
    assert!(serde_json::from_str::<EgressPolicy>(r#"{"allowed_snis":["api.example.com"]}"#).is_err());
  }
}
