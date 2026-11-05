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

//! The `tunnel_watcher` network filter.
//!
//! It runs on the router's `actor_connect` internal listener, ahead of the
//! `tcp_proxy` that opens a CONNECT tunnel to the worker's `atunnel`, and
//! records what happened to each tunnel on the cache entries for its actor and
//! worker:
//!
//! * **stale**: the connection closed with `UF`, so the tunnel was never
//!   established and no request bytes reached the actor;
//! * **spoiled**: the tunnel was established and closed while request bytes were
//!   waiting for a response, so that request may have reached the actor.
//!
//! The HTTP filter replays a bodiless `503 UC` only if its entry is stale and
//! not spoiled. The marks are always in place before the router reports the
//! `503`: the network filter's close callback runs inside `tcp_proxy`'s close,
//! and the router only sees the close on the next event-loop iteration.
//!
//! The filter never parses the bytes it sees; it only notes which direction
//! they move. `tcp_proxy` keeps reads off until the tunnel is up, so request
//! bytes only pass through once a tunnel is established.

use std::net::SocketAddr;

use envoy_proxy_dynamic_modules_rust_sdk::{
  EnvoyCounterId, EnvoyNetworkFilter, EnvoyNetworkFilterConfig, NetworkFilter, NetworkFilterConfig,
  abi::{
    envoy_dynamic_module_type_attribute_id, envoy_dynamic_module_type_network_connection_event,
    envoy_dynamic_module_type_on_network_filter_data_status,
  },
};

use crate::cache;
use crate::filter::RESPONSE_FLAG_UF;

/// Filter state holding the actor (`<atespace>/<actor>`) a tunnel connection is for. It is part
/// of the tunnel pool's key, so every connection in a pool carries the right value.
pub const TUNNEL_ACTOR_FILTER_STATE_KEY: &str = "dev.ate.tunnel.actor";

/// Filter state holding the worker (`ip:port`) a tunnel connection is for.
pub const TUNNEL_WORKER_FILTER_STATE_KEY: &str = "dev.ate.tunnel.worker";

/// Counter incremented when a refused tunnel marks cache entries stale.
pub const MARKED_STALE_COUNTER: &str = "ate_ingress_cache.marked_stale";

/// Counter incremented when a tunnel cut off mid-request marks cache entries spoiled.
pub const MARKED_SPOILED_COUNTER: &str = "ate_ingress_cache.marked_spoiled";

/// Creates the `tunnel_watcher` network filter configuration.
pub fn new_network_filter_config_fn<EC: EnvoyNetworkFilterConfig, ENF: EnvoyNetworkFilter>(
  envoy_filter_config: &mut EC,
  _filter_name: &str,
  _filter_config: &[u8],
) -> Option<Box<dyn NetworkFilterConfig<ENF>>> {
  Some(Box::new(TunnelWatcherConfig {
    marked_stale: envoy_filter_config
      .define_counter(MARKED_STALE_COUNTER)
      .ok(),
    marked_spoiled: envoy_filter_config
      .define_counter(MARKED_SPOILED_COUNTER)
      .ok(),
  }))
}

/// Network filter configuration holding the counter IDs.
pub struct TunnelWatcherConfig {
  pub marked_stale: Option<EnvoyCounterId>,
  pub marked_spoiled: Option<EnvoyCounterId>,
}

impl<ENF: EnvoyNetworkFilter> NetworkFilterConfig<ENF> for TunnelWatcherConfig {
  fn new_network_filter(&self, _envoy: &mut ENF) -> Box<dyn NetworkFilter<ENF>> {
    Box::new(TunnelWatcher::new(self.marked_stale, self.marked_spoiled))
  }
}

/// Per-connection state of the `tunnel_watcher` network filter.
pub struct TunnelWatcher {
  /// Counts tunnel connections that closed with `UF` and marked at least one entry stale.
  marked_stale: Option<EnvoyCounterId>,
  /// Counts established tunnel connections that closed with a request outstanding and marked
  /// at least one entry spoiled.
  marked_spoiled: Option<EnvoyCounterId>,
  /// The `(actor, worker IP)` this connection tunnels to, once known.
  tunnel: Option<(String, String)>,
  /// Whether request bytes went upstream with no response bytes back since.
  request_outstanding: bool,
}

impl TunnelWatcher {
  pub fn new(marked_stale: Option<EnvoyCounterId>, marked_spoiled: Option<EnvoyCounterId>) -> Self {
    Self {
      marked_stale,
      marked_spoiled,
      tunnel: None,
      request_outstanding: false,
    }
  }
}

fn filter_state_str<ENF: EnvoyNetworkFilter>(envoy_filter: &ENF, key: &str) -> Option<String> {
  let buf = envoy_filter.get_filter_state_bytes(key.as_bytes())?;
  std::str::from_utf8(buf.as_slice())
    .ok()
    .map(str::to_string)
}

/// Reads the `(actor, worker IP)` a tunnel connection is for. The worker filter state is an
/// `ip:port` socket address; the cache keys entries by the bare IP in canonical form.
fn read_tunnel<ENF: EnvoyNetworkFilter>(envoy_filter: &ENF) -> Option<(String, String)> {
  let actor = filter_state_str(envoy_filter, TUNNEL_ACTOR_FILTER_STATE_KEY)?;
  let worker = filter_state_str(envoy_filter, TUNNEL_WORKER_FILTER_STATE_KEY)?;
  let worker_ip = worker.parse::<SocketAddr>().ok()?.ip().to_string();
  Some((actor, worker_ip))
}

fn increment<ENF: EnvoyNetworkFilter>(envoy_filter: &ENF, counter: Option<EnvoyCounterId>) {
  if let Some(id) = counter {
    let _ = envoy_filter.increment_counter(id, 1);
  }
}

impl<ENF: EnvoyNetworkFilter> NetworkFilter<ENF> for TunnelWatcher {
  fn on_new_connection(
    &mut self,
    envoy_filter: &mut ENF,
  ) -> envoy_dynamic_module_type_on_network_filter_data_status {
    self.tunnel = read_tunnel(envoy_filter);
    envoy_dynamic_module_type_on_network_filter_data_status::Continue
  }

  fn on_read(
    &mut self,
    _envoy_filter: &mut ENF,
    data_length: usize,
    _end_stream: bool,
  ) -> envoy_dynamic_module_type_on_network_filter_data_status {
    if data_length > 0 {
      self.request_outstanding = true;
    }
    envoy_dynamic_module_type_on_network_filter_data_status::Continue
  }

  fn on_write(
    &mut self,
    _envoy_filter: &mut ENF,
    data_length: usize,
    _end_stream: bool,
  ) -> envoy_dynamic_module_type_on_network_filter_data_status {
    if data_length > 0 {
      self.request_outstanding = false;
    }
    envoy_dynamic_module_type_on_network_filter_data_status::Continue
  }

  fn on_event(
    &mut self,
    envoy_filter: &mut ENF,
    event: envoy_dynamic_module_type_network_connection_event,
  ) {
    let closed = matches!(
      event,
      envoy_dynamic_module_type_network_connection_event::LocalClose
        | envoy_dynamic_module_type_network_connection_event::RemoteClose
    );
    if !closed {
      return;
    }
    let Some((actor, worker_ip)) = self.tunnel.take() else {
      return;
    };
    let flags = envoy_filter
      .get_attribute_int(envoy_dynamic_module_type_attribute_id::ResponseFlags)
      .unwrap_or(0);
    if flags & RESPONSE_FLAG_UF != 0 {
      if cache::mark_stale(&actor, &worker_ip) > 0 {
        increment(envoy_filter, self.marked_stale);
      }
    } else if self.request_outstanding && cache::mark_spoiled(&actor, &worker_ip) > 0 {
      increment(envoy_filter, self.marked_spoiled);
    }
  }
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyNetworkFilter};
  use std::time::Duration;

  use crate::filter::RESPONSE_FLAG_UC;

  const ACTOR: &str = "team-n/actor-1";

  fn mock_tunnel(worker: &'static str, flags: i64) -> MockEnvoyNetworkFilter {
    let mut mock = MockEnvoyNetworkFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TUNNEL_ACTOR_FILTER_STATE_KEY.as_bytes())
      .returning(|_| Some(EnvoyBuffer::new(ACTOR.as_bytes())));
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TUNNEL_WORKER_FILTER_STATE_KEY.as_bytes())
      .returning(move |_| Some(EnvoyBuffer::new(worker.as_bytes())));
    mock
      .expect_get_attribute_int()
      .withf(|id| *id == envoy_dynamic_module_type_attribute_id::ResponseFlags)
      .returning(move |_| Some(flags));
    mock.expect_increment_counter().returning(|_, _| Ok(()));
    mock
  }

  fn close(watcher: &mut TunnelWatcher, mock: &mut MockEnvoyNetworkFilter) {
    watcher.on_event(mock, envoy_dynamic_module_type_network_connection_event::LocalClose);
  }

  fn held_entry(worker_ip: &str) -> std::rc::Rc<cache::Entry> {
    cache::insert(
      ACTOR.to_string(),
      worker_ip.to_string(),
      None,
      Duration::from_secs(5),
      crate::config::DEFAULT_MAX_CACHED_ACTORS,
    )
  }

  #[test]
  fn test_refused_tunnel_marks_entry_stale() {
    let entry = held_entry("10.244.8.1");
    let mut mock = mock_tunnel("10.244.8.1:443", RESPONSE_FLAG_UF);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    // tcp_proxy keeps reads off until the tunnel is up, so a refused tunnel reads nothing.
    close(&mut watcher, &mut mock);
    assert!(entry.is_stale());
    assert!(!entry.is_spoiled());
    assert!(cache::current(ACTOR).is_none());
  }

  #[test]
  fn test_drop_with_request_outstanding_marks_entry_spoiled() {
    let entry = held_entry("10.244.8.2");
    let mut mock = mock_tunnel("10.244.8.2:443", RESPONSE_FLAG_UC);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    watcher.on_read(&mut mock, 120, false);
    close(&mut watcher, &mut mock);
    assert!(entry.is_spoiled());
    assert!(!entry.is_stale());
  }

  #[test]
  fn test_close_after_response_does_not_mark() {
    let entry = held_entry("10.244.8.3");
    let mut mock = mock_tunnel("10.244.8.3:443", 0);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    watcher.on_read(&mut mock, 120, false);
    watcher.on_write(&mut mock, 512, false);
    close(&mut watcher, &mut mock);
    assert!(!entry.is_spoiled());
    assert!(!entry.is_stale());
    assert!(cache::current(ACTOR).is_some());
  }

  #[test]
  fn test_idle_kept_alive_tunnel_closing_does_not_mark() {
    let entry = held_entry("10.244.8.4");
    let mut mock = mock_tunnel("10.244.8.4:443", 0);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    close(&mut watcher, &mut mock);
    assert!(!entry.is_spoiled());
    assert!(!entry.is_stale());
  }

  #[test]
  fn test_ipv6_worker_matches_canonical_entry_ip() {
    let entry = held_entry("2001:db8::1");
    let mut mock = mock_tunnel("[2001:db8::1]:443", RESPONSE_FLAG_UF);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    close(&mut watcher, &mut mock);
    assert!(entry.is_stale());
  }

  #[test]
  fn test_marks_only_matching_worker() {
    let on_b = held_entry("10.244.8.6");
    let mut mock = mock_tunnel("10.244.8.5:443", RESPONSE_FLAG_UF);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    close(&mut watcher, &mut mock);
    assert!(!on_b.is_stale());
    assert!(cache::current(ACTOR).is_some());
  }

  #[test]
  fn test_connection_without_tunnel_filter_state_is_ignored() {
    let entry = held_entry("10.244.8.7");
    let mut mock = MockEnvoyNetworkFilter::default();
    mock.expect_get_filter_state_bytes().returning(|_| None);
    let mut watcher = TunnelWatcher::new(None, None);
    watcher.on_new_connection(&mut mock);
    watcher.on_read(&mut mock, 10, false);
    // get_attribute_int has no expectation: a close must not read anything.
    close(&mut watcher, &mut mock);
    assert!(!entry.is_stale());
    assert!(!entry.is_spoiled());
  }

  #[test]
  fn test_marks_increment_counters() {
    let _entry = held_entry("10.244.8.8");
    let stale_id = EnvoyCounterId(11);
    let mut mock = MockEnvoyNetworkFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TUNNEL_ACTOR_FILTER_STATE_KEY.as_bytes())
      .returning(|_| Some(EnvoyBuffer::new(ACTOR.as_bytes())));
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TUNNEL_WORKER_FILTER_STATE_KEY.as_bytes())
      .returning(|_| Some(EnvoyBuffer::new(b"10.244.8.8:443")));
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UF));
    mock
      .expect_increment_counter()
      .withf(move |id, val| *id == stale_id && *val == 1)
      .times(1)
      .returning(|_, _| Ok(()));
    let mut watcher = TunnelWatcher::new(Some(stale_id), Some(EnvoyCounterId(12)));
    watcher.on_new_connection(&mut mock);
    close(&mut watcher, &mut mock);
  }
}
