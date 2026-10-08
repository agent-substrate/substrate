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

//! The egress-policy listener filter picks the filter chain for a tunneled
//! connection. See the README in this directory.

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::envoy_dynamic_module_type_on_listener_filter_status,
  declare_listener_filter_init_functions, envoy_log_trace, EnvoyListenerFilter,
  EnvoyListenerFilterConfig, ListenerFilter, ListenerFilterConfig,
};
pub use substrate_envoy_common::{
  pattern_matches, EgressPolicy, SniRule, ATE_POLICY_EGRESS, SNI_MODE_MITM,
  SNI_MODE_PASSTHROUGH,
};

/// Filter state this filter writes the chosen chain name to.
pub const ATE_EGRESS_FILTER_CHAIN: &[u8] = b"dev.ate.egress.filter_chain";

/// Verdict for TLS an https rule allows.
pub const ATE_EGRESS_FILTER_CHAIN_MITM: &str = "mitm";

/// Verdict for TLS a tls_passthrough rule allows.
pub const ATE_EGRESS_FILTER_CHAIN_PASSTHROUGH: &str = "passthrough";

/// Verdict for anything that is not TLS.
pub const ATE_EGRESS_FILTER_CHAIN_CLEARTEXT: &str = "cleartext";

/// Verdict for a denied connection. No chain has this name.
pub const ATE_EGRESS_FILTER_CHAIN_DENIED: &str = "denied";

/// Transport protocol tls_inspector sets for TLS.
const TRANSPORT_TLS: &str = "tls";

/// Transport protocol Envoy assumes when no inspector detected one.
const TRANSPORT_RAW_BUFFER: &str = "raw_buffer";

/// ASCII-lowercases an SNI and strips one trailing dot, as the gateway does.
fn normalize_sni(sni: &str) -> String {
  let lower = sni.to_ascii_lowercase();
  match lower.strip_suffix('.') {
    Some(stripped) => stripped.to_owned(),
    None => lower,
  }
}

/// Returns the verdict from the transport alone, or None for TLS, which is
/// decided by SNI. Unknown transports are denied.
pub fn transport_verdict(transport: Option<&str>) -> Option<&'static str> {
  match transport {
    Some(TRANSPORT_TLS) => None,
    None | Some(TRANSPORT_RAW_BUFFER) => Some(ATE_EGRESS_FILTER_CHAIN_CLEARTEXT),
    Some(_) => Some(ATE_EGRESS_FILTER_CHAIN_DENIED),
  }
}

/// Returns the mode of the first rule matching the SNI, or denied.
pub fn tls_verdict(policy: Option<&EgressPolicy>, sni: Option<&str>) -> &'static str {
  let (Some(policy), Some(sni)) = (policy, sni) else {
    return ATE_EGRESS_FILTER_CHAIN_DENIED;
  };
  let hostname = normalize_sni(sni);
  match policy
    .rules
    .iter()
    .find(|rule| pattern_matches(&rule.pattern, &hostname))
  {
    Some(rule) if rule.mode == SNI_MODE_MITM => ATE_EGRESS_FILTER_CHAIN_MITM,
    Some(rule) if rule.mode == SNI_MODE_PASSTHROUGH => ATE_EGRESS_FILTER_CHAIN_PASSTHROUGH,
    _ => ATE_EGRESS_FILTER_CHAIN_DENIED,
  }
}

/// The filter takes no configuration.
pub struct EgressPolicyFilterConfig;

impl<ELF: EnvoyListenerFilter> ListenerFilterConfig<ELF> for EgressPolicyFilterConfig {
  fn new_listener_filter(&self, _envoy: &mut ELF) -> Box<dyn ListenerFilter<ELF>> {
    Box::new(EgressPolicyFilter)
  }
}

/// Runs after tls_inspector and http_inspector, once per connection.
pub struct EgressPolicyFilter;

impl<ELF: EnvoyListenerFilter> ListenerFilter<ELF> for EgressPolicyFilter {
  fn on_accept(
    &mut self,
    envoy_filter: &mut ELF,
  ) -> envoy_dynamic_module_type_on_listener_filter_status {
    let transport = envoy_filter
      .get_detected_transport_protocol()
      .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned());

    let verdict = match transport_verdict(transport.as_deref()) {
      Some(verdict) => verdict,
      None => {
        let sni = envoy_filter
          .get_requested_server_name()
          .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned());
        // Unparseable rules deny, like absent ones.
        let policy = envoy_filter
          .get_filter_state_bytes(ATE_POLICY_EGRESS)
          .and_then(|value| serde_json::from_slice::<EgressPolicy>(value.as_slice()).ok());
        let verdict = tls_verdict(policy.as_ref(), sni.as_deref());
        envoy_log_trace!("egress policy: sni={:?} chain={}", sni, verdict);
        verdict
      }
    };

    envoy_filter.set_filter_state_bytes(ATE_EGRESS_FILTER_CHAIN, verdict.as_bytes());
    envoy_log_trace!(
      "egress policy: transport={:?} chain={}",
      transport,
      verdict
    );
    envoy_dynamic_module_type_on_listener_filter_status::Continue
  }
}

declare_listener_filter_init_functions!(init, new_listener_filter_config_fn);

/// Called when the dynamic module is loaded into Envoy.
fn init() -> bool {
  true
}

/// Called when a new listener filter configuration is created.
fn new_listener_filter_config_fn<
  EC: EnvoyListenerFilterConfig,
  ELF: EnvoyListenerFilter,
>(
  _envoy_filter_config: &mut EC,
  _name: &str,
  _config: &[u8],
) -> Option<Box<dyn ListenerFilterConfig<ELF>>> {
  Some(Box::new(EgressPolicyFilterConfig))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyBuffer, MockEnvoyListenerFilter, MockEnvoyListenerFilterConfig,
  };

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
  fn test_normalize_sni() {
    assert_eq!(normalize_sni("API.Example.COM."), "api.example.com");
    assert_eq!(normalize_sni("api.example.com"), "api.example.com");
    // One trailing dot, not every one.
    assert_eq!(normalize_sni("api.example.com.."), "api.example.com.");
    // Only ASCII folds: U+212A KELVIN SIGN stays itself and cannot become "k".
    assert_eq!(normalize_sni("\u{212A}.example.com"), "\u{212A}.example.com");
  }

  #[test]
  fn test_transport_verdict() {
    assert_eq!(transport_verdict(Some("tls")), None);
    assert_eq!(transport_verdict(None), Some(ATE_EGRESS_FILTER_CHAIN_CLEARTEXT));
    assert_eq!(
      transport_verdict(Some("raw_buffer")),
      Some(ATE_EGRESS_FILTER_CHAIN_CLEARTEXT)
    );
    assert_eq!(transport_verdict(Some("quic")), Some(ATE_EGRESS_FILTER_CHAIN_DENIED));
    assert_eq!(transport_verdict(Some("")), Some(ATE_EGRESS_FILTER_CHAIN_DENIED));
  }

  #[test]
  fn test_tls_verdict_denies_without_inputs() {
    let p = policy(&[("*", SNI_MODE_MITM)]);
    assert_eq!(tls_verdict(None, Some("api.example.com")), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_eq!(tls_verdict(Some(&p), None), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_eq!(tls_verdict(Some(&p), Some("")), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_eq!(
      tls_verdict(Some(&policy(&[])), Some("api.example.com")),
      ATE_EGRESS_FILTER_CHAIN_DENIED
    );
  }

  #[test]
  fn test_tls_verdict_matches_wildcards() {
    let p = policy(&[("api.example.com", SNI_MODE_MITM), ("*.example.org", SNI_MODE_MITM)]);
    assert_eq!(tls_verdict(Some(&p), Some("api.example.com")), ATE_EGRESS_FILTER_CHAIN_MITM);
    assert_eq!(tls_verdict(Some(&p), Some("API.EXAMPLE.COM.")), ATE_EGRESS_FILTER_CHAIN_MITM);
    assert_eq!(tls_verdict(Some(&p), Some("www.example.org")), ATE_EGRESS_FILTER_CHAIN_MITM);
    assert_eq!(tls_verdict(Some(&p), Some("example.org")), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_eq!(tls_verdict(Some(&p), Some("a.b.example.org")), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_eq!(tls_verdict(Some(&p), Some("www.example.com")), ATE_EGRESS_FILTER_CHAIN_DENIED);

    let any = policy(&[("*", SNI_MODE_MITM)]);
    assert_eq!(tls_verdict(Some(&any), Some("whatever.test")), ATE_EGRESS_FILTER_CHAIN_MITM);
  }

  #[test]
  fn test_tls_verdict_first_match_decides() {
    // The first match is final, even with an unknown mode.
    let unknown_first = policy(&[("*.example.com", "not-a-mode"), ("api.example.com", SNI_MODE_MITM)]);
    assert_eq!(
      tls_verdict(Some(&unknown_first), Some("api.example.com")),
      ATE_EGRESS_FILTER_CHAIN_DENIED
    );
    let known_first = policy(&[
      ("api.example.com", SNI_MODE_MITM),
      ("pinned.example.com", SNI_MODE_PASSTHROUGH),
      ("*.example.com", "not-a-mode"),
    ]);
    assert_eq!(
      tls_verdict(Some(&known_first), Some("api.example.com")),
      ATE_EGRESS_FILTER_CHAIN_MITM
    );
    assert_eq!(
      tls_verdict(Some(&known_first), Some("pinned.example.com")),
      ATE_EGRESS_FILTER_CHAIN_PASSTHROUGH
    );
    assert_eq!(
      tls_verdict(Some(&known_first), Some("www.example.com")),
      ATE_EGRESS_FILTER_CHAIN_DENIED
    );
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

  fn new_filter_config() -> Box<dyn ListenerFilterConfig<MockEnvoyListenerFilter>> {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    new_listener_filter_config_fn::<MockEnvoyListenerFilterConfig, MockEnvoyListenerFilter>(
      &mut mock_config,
      "envoy_substrate_egress_policy",
      b"",
    )
    .expect("a filter config")
  }

  /// Runs on_accept for a TLS connection with the given SNI and policy filter
  /// state, asserting the verdict written.
  fn assert_tls_verdict(sni: Option<&'static [u8]>, policy: Option<&'static [u8]>, want: &'static str) {
    let config = new_filter_config();
    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(move || sni.map(EnvoyBuffer::new));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(move |_| policy.map(EnvoyBuffer::new));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(move |key, value| key == ATE_EGRESS_FILTER_CHAIN && value == want.as_bytes())
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);
    assert_eq!(
      filter.on_accept(&mut mock_filter),
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  /// Runs on_accept for a connection with the given detected transport,
  /// asserting the verdict written.
  fn assert_transport_verdict(transport: Option<&'static [u8]>, want: &'static str) {
    let config = new_filter_config();
    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(move || transport.map(EnvoyBuffer::new));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(move |key, value| key == ATE_EGRESS_FILTER_CHAIN && value == want.as_bytes())
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);
    assert_eq!(
      filter.on_accept(&mut mock_filter),
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
    assert_eq!(
      filter.on_data(&mut mock_filter, 0),
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_init() {
    assert!(init());
  }

  #[test]
  fn test_on_accept_no_transport_is_cleartext() {
    assert_transport_verdict(None, ATE_EGRESS_FILTER_CHAIN_CLEARTEXT);
  }

  #[test]
  fn test_on_accept_raw_buffer_is_cleartext() {
    assert_transport_verdict(Some(b"raw_buffer"), ATE_EGRESS_FILTER_CHAIN_CLEARTEXT);
  }

  #[test]
  fn test_on_accept_unknown_transport_is_denied() {
    assert_transport_verdict(Some(b"quic"), ATE_EGRESS_FILTER_CHAIN_DENIED);
  }

  const RULES: &[u8] =
    br#"{"rules":[{"pattern":"api.google.com","mode":"mitm"},{"pattern":"*.google.com","mode":"mitm"}]}"#;

  #[test]
  fn test_on_accept_matching_sni() {
    assert_tls_verdict(Some(b"api.google.com"), Some(RULES), ATE_EGRESS_FILTER_CHAIN_MITM);
  }

  #[test]
  fn test_on_accept_wildcard_sni() {
    assert_tls_verdict(Some(b"mail.google.com"), Some(RULES), ATE_EGRESS_FILTER_CHAIN_MITM);
  }

  #[test]
  fn test_on_accept_case_insensitive_sni() {
    assert_tls_verdict(Some(b"API.Google.COM"), Some(RULES), ATE_EGRESS_FILTER_CHAIN_MITM);
  }

  #[test]
  fn test_on_accept_mismatched_sni() {
    assert_tls_verdict(Some(b"google.com"), Some(RULES), ATE_EGRESS_FILTER_CHAIN_DENIED);
    assert_tls_verdict(Some(b"www.example.com"), Some(RULES), ATE_EGRESS_FILTER_CHAIN_DENIED);
  }

  #[test]
  fn test_on_accept_no_rules() {
    assert_tls_verdict(Some(b"api.google.com"), Some(br#"{"rules":[]}"#), ATE_EGRESS_FILTER_CHAIN_DENIED);
  }

  #[test]
  fn test_on_accept_invalid_policy_json() {
    assert_tls_verdict(Some(b"api.google.com"), Some(b"not-json"), ATE_EGRESS_FILTER_CHAIN_DENIED);
  }

  #[test]
  fn test_on_accept_missing_policy() {
    assert_tls_verdict(Some(b"api.google.com"), None, ATE_EGRESS_FILTER_CHAIN_DENIED);
  }

  #[test]
  fn test_on_accept_missing_sni() {
    assert_tls_verdict(None, Some(RULES), ATE_EGRESS_FILTER_CHAIN_DENIED);
  }
}
