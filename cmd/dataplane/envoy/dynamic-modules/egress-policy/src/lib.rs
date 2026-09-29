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

use envoy_proxy_dynamic_modules_rust_sdk::{
  abi::envoy_dynamic_module_type_on_listener_filter_status,
  declare_listener_filter_init_functions, envoy_log_trace, EnvoyListenerFilter,
  EnvoyListenerFilterConfig, ListenerFilter, ListenerFilterConfig,
};
use serde::Deserialize;

/// Key of the filter state object holding the Substrate egress policy.
pub const ATE_POLICY_EGRESS: &[u8] = b"dev.ate.policy.egress";

/// Key of the filter state object holding the SNI passthrough match result.
pub const ATE_EGRESS_FILTER_CHAIN: &[u8] = b"dev.ate.egress.filter_chain";

/// Filter chain name for MITM traffic.
pub const ATE_EGRESS_FILTER_CHAIN_MITM: &str = "mitm";

/// Filter chain name for cleartext traffic.
pub const ATE_EGRESS_FILTER_CHAIN_CLEARTEXT: &str = "cleartext";

/// Filter chain name when request is denied by policy.
/// Since there is no filter chain with this name, TCP connection will be reset.
pub const ATE_EGRESS_FILTER_CHAIN_NONE: &str = "denied";


/// Parsed Substrate egress policy from the `ATE_POLICY_EGRESS` filter state JSON.
#[derive(Debug, Deserialize)]
pub struct EgressPolicy {
  pub allowed_snis: Vec<String>,
}

/// Empty filter configuration for the listener filter.
pub struct EmptyFilterConfig;

impl<ELF: EnvoyListenerFilter> ListenerFilterConfig<ELF> for EmptyFilterConfig {
  fn new_listener_filter(&self, _envoy: &mut ELF) -> Box<dyn ListenerFilter<ELF>> {
    Box::new(EmptyListenerFilter)
  }
}

/// A listener filter that matches requested server name (SNI) against the SNI passthrough policy.
pub struct EmptyListenerFilter;

impl<ELF: EnvoyListenerFilter> ListenerFilter<ELF> for EmptyListenerFilter {
  fn on_accept(
    &mut self,
    envoy_filter: &mut ELF,
  ) -> envoy_dynamic_module_type_on_listener_filter_status {
    let transport_protocol_str = envoy_filter
      .get_detected_transport_protocol()
      .map(|transport_protocol| {
        String::from_utf8_lossy(transport_protocol.as_slice()).into_owned()
      });
    envoy_log_trace!("transport_protocol: {:#?} : {}", transport_protocol_str, transport_protocol_str.as_deref() != Some("tls"));

    if transport_protocol_str.as_deref() != Some("tls")
    {
      // TODO(yanavlasov): allow plaintext traffic only if there are `http` rules in the policy
      envoy_filter.set_filter_state_bytes(
        ATE_EGRESS_FILTER_CHAIN,
        ATE_EGRESS_FILTER_CHAIN_CLEARTEXT.as_bytes(),
      );
      envoy_log_trace!(
        "dev.ate.egress.filter_chain: {}",
        ATE_EGRESS_FILTER_CHAIN_CLEARTEXT
      );
      return envoy_dynamic_module_type_on_listener_filter_status::Continue;
    }

    let server_name_str = envoy_filter
      .get_requested_server_name()
      .map(|server_name| {
        String::from_utf8_lossy(server_name.as_slice()).into_owned()
      });

    let sni_passthrough_policy_str = envoy_filter
      .get_filter_state_bytes(ATE_POLICY_EGRESS)
      .map(|sni_passthrough_policy| {
        String::from_utf8_lossy(sni_passthrough_policy.as_slice()).into_owned()
      });

    let egress_policy = sni_passthrough_policy_str
      .as_deref()
      .and_then(|policy_str| serde_json::from_str::<EgressPolicy>(policy_str).ok());

    let comparison_result = match (&server_name_str, &egress_policy) {
      (Some(server_name), Some(policy))
        if policy
          .allowed_snis
          .iter()
          // TODO(yanavlasov): implement wildcard matching.
          .any(|sni| server_name.eq_ignore_ascii_case(sni)) =>
      {
        ATE_EGRESS_FILTER_CHAIN_MITM
      }
      // TODO(yanavlasov): implement passthrough TLS policy.
      _ => ATE_EGRESS_FILTER_CHAIN_NONE,
    };

    envoy_filter.set_filter_state_bytes(
      ATE_EGRESS_FILTER_CHAIN,
      comparison_result.as_bytes(),
    );
    envoy_log_trace!("dev.ate.egress.filter_chain: {}", comparison_result);

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
  Some(Box::new(EmptyFilterConfig))
}

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyBuffer, MockEnvoyListenerFilter, MockEnvoyListenerFilterConfig,
  };

  #[test]
  fn test_init() {
    assert!(init());
  }

  #[test]
  fn test_empty_listener_filter_lifecycle() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"");
    assert!(config.is_some());
    let config = config.unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| None);
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .returning(|_| None);
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"cleartext"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );

    let status = filter.on_data(&mut mock_filter, 0);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_raw_buffer_transport_protocol() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"raw_buffer")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"cleartext"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_matching_sni_and_policy() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| {
        Some(EnvoyBuffer::new(
          br#"{"allowed_snis":["api.google.com","www.google.com"]}"#,
        ))
      });
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"mitm"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_case_insensitive_matching_sni_and_policy() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"WWW.Google.COM")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| {
        Some(EnvoyBuffer::new(br#"{"allowed_snis":["www.google.com"]}"#))
      });
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"mitm"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_mismatched_sni_and_policy() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| {
        Some(EnvoyBuffer::new(
          br#"{"allowed_snis":["api.google.com","mail.google.com"]}"#,
        ))
      });
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"denied"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_empty_allowed_snis() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(br#"{"allowed_snis":[]}"#)));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"denied"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_invalid_policy_json() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(b"not-json")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"denied"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_missing_policy() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| None);
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"denied"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }

  #[test]
  fn test_on_accept_missing_sni() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_detected_transport_protocol()
      .returning(|| Some(EnvoyBuffer::new(b"tls")));
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| {
        Some(EnvoyBuffer::new(br#"{"allowed_snis":["www.google.com"]}"#))
      });
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_EGRESS_FILTER_CHAIN && value == b"denied"
      })
      .times(1)
      .returning(|_, _| true);

    let mut filter = config.new_listener_filter(&mut mock_filter);

    let status = filter.on_accept(&mut mock_filter);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_listener_filter_status::Continue
    );
  }
}

