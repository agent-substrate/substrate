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
  declare_listener_filter_init_functions, envoy_log_info, EnvoyListenerFilter,
  EnvoyListenerFilterConfig, ListenerFilter, ListenerFilterConfig,
};

/// Key of the filter state object holding the Substrate egress policy.
pub const ATE_POLICY_EGRESS: &[u8] = b"dev.ate.policy.egress";

/// Key of the filter state object holding the SNI passthrough match result.
pub const ATE_POLICY_EGRESS_PASSTHROUGH: &[u8] = b"dev.ate.sni.passthrough.match";

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

    let comparison_result = match (&server_name_str, &sni_passthrough_policy_str) {
      (Some(server_name), Some(policy)) if server_name.eq_ignore_ascii_case(policy) => "true",
      _ => "false",
    };

    envoy_filter.set_filter_state_bytes(
      ATE_POLICY_EGRESS_PASSTHROUGH,
      comparison_result.as_bytes(),
    );
    envoy_log_info!("sni.passthrough.match: {}", comparison_result);

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
      .expect_get_requested_server_name()
      .returning(|| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .returning(|_| None);
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"false"
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
  fn test_on_accept_matching_sni_and_policy() {
    let mut mock_config = MockEnvoyListenerFilterConfig::new();
    let config = new_listener_filter_config_fn::<
      MockEnvoyListenerFilterConfig,
      MockEnvoyListenerFilter,
    >(&mut mock_config, "envoy_substrate_egress_policy", b"")
    .unwrap();

    let mut mock_filter = MockEnvoyListenerFilter::new();
    mock_filter
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"true"
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
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"WWW.Google.COM")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"true"
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
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(b"api.google.com")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"false"
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
      .expect_get_requested_server_name()
      .returning(|| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| None);
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"false"
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
      .expect_get_requested_server_name()
      .returning(|| None);
    mock_filter
      .expect_get_filter_state_bytes()
      .withf(|key| key == ATE_POLICY_EGRESS)
      .returning(|_| Some(EnvoyBuffer::new(b"www.google.com")));
    mock_filter
      .expect_set_filter_state_bytes()
      .withf(|key, value| {
        key == ATE_POLICY_EGRESS_PASSTHROUGH && value == b"false"
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

