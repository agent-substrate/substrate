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

//! Actor identity resolution and routing metadata helpers for ingress requests.

use std::num::NonZeroU16;

use envoy_proxy_dynamic_modules_rust_sdk::EnvoyHttpFilter;

use crate::config::Config;

/// Filter-state key carrying the ingress actor routing target (`<atespace>/<actor>`)
/// across Envoy's `CONNECT` internal-listener hop.
pub const TARGET_ACTOR_FILTER_STATE_KEY: &[u8] = b"dev.ate.target.actor";

/// Filter-state key carrying the outer `CONNECT` `:authority` across the
/// internal-listener hop.
pub const CONNECT_AUTHORITY_FILTER_STATE_KEY: &[u8] = b"dev.ate.connect.authority";

/// Header identifying the actor selected for ingress routing as `<atespace>/<actor>`.
pub const TARGET_ACTOR_HEADER: &str = "ate-target-actor";

/// Default actor port when a request does not specify a custom `CONNECT` port.
pub const DEFAULT_ACTOR_PORT: u16 = 80;

/// Dynamic-metadata namespace and keys read by Envoy's `ORIGINAL_DST` cluster
/// and `actor_connect` upstream tunnel.
pub const ORIGINAL_DST_NAMESPACE: &str = "envoy.filters.listener.original_dst";
pub const ORIGINAL_DST_ADDRESS_KEY: &str = "local";
pub const ORIGINAL_DST_PORT_KEY: &str = "port";
pub const WORKER_SPIFFE_ID_KEY: &str = "worker_spiffe_id";

/// Request header set on a cache hit so the route table selects the fast-path
/// route with `ext_proc` disabled.
pub const ROUTE_RESOLVED_HEADER: &str = "ate-endpoint-cached";

/// Response header set by `atunnel` when a request arrives for an actor not
/// hosted on that worker.
pub const STALE_ASSIGNMENT_HEADER: &str = "x-ate-assignment-stale";

/// An actor reference parsed from a request's routing metadata or headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorRef {
  pub atespace: String,
  pub name: String,
  pub target_port: u16,
}

impl ActorRef {
  /// Returns the canonical `<atespace>/<actor>` cache key for this actor.
  pub fn cache_key(&self) -> String {
    format!("{}/{}", self.atespace, self.name)
  }
}

/// Checks that `name` is a valid RFC-1123 DNS label (`[a-z0-9]([-a-z0-9]*[a-z0-9])?`,
/// 1..=63 characters), matching `resources.IsValidResourceName`.
pub fn is_valid_resource_name(name: &str) -> bool {
  (1..=63).contains(&name.len())
    && !name.starts_with('-')
    && !name.ends_with('-')
    && name
      .bytes()
      .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Reads a UTF-8 filter-state value by `key`, treating empty values and Envoy's
/// `"-"` unset sentinel as `None`.
fn filter_state_str<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, key: &[u8]) -> Option<String> {
  let buf = envoy_filter.get_filter_state_bytes(key)?;
  let s = std::str::from_utf8(buf.as_slice()).ok()?;
  (!s.is_empty() && s != "-").then(|| s.to_string())
}

/// Reads a single request header value as a UTF-8 `String`.
fn request_header_str<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, key: &str) -> Option<String> {
  let buf = envoy_filter.get_request_header_value(key)?;
  std::str::from_utf8(buf.as_slice()).ok().map(str::to_string)
}

/// Reads a single response header value as a UTF-8 `String`.
pub(crate) fn response_header_str<EHF: EnvoyHttpFilter>(
  envoy_filter: &EHF,
  key: &str,
) -> Option<String> {
  let buf = envoy_filter.get_response_header_value(key)?;
  std::str::from_utf8(buf.as_slice()).ok().map(str::to_string)
}

impl Config {
  /// Stamps the resolved worker endpoint and actor target metadata/headers onto
  /// the request so Envoy routes directly to `worker_ip:atunnel_port`.
  pub fn route_to<EHF: EnvoyHttpFilter>(
    &self,
    envoy_filter: &mut EHF,
    actor: &ActorRef,
    worker_ip: &str,
    worker_spiffe_id: Option<&str>,
  ) {
    let target = if worker_ip.contains(':') && !worker_ip.starts_with('[') {
      format!("[{}]:{}", worker_ip, self.atunnel_port)
    } else {
      format!("{}:{}", worker_ip, self.atunnel_port)
    };
    envoy_filter.set_dynamic_metadata_string(
      ORIGINAL_DST_NAMESPACE,
      ORIGINAL_DST_ADDRESS_KEY,
      &target,
    );
    envoy_filter.set_dynamic_metadata_string(
      ORIGINAL_DST_NAMESPACE,
      ORIGINAL_DST_PORT_KEY,
      &actor.target_port.to_string(),
    );
    if let Some(spiffe_id) = worker_spiffe_id {
      envoy_filter.set_dynamic_metadata_string(
        ORIGINAL_DST_NAMESPACE,
        WORKER_SPIFFE_ID_KEY,
        spiffe_id,
      );
    }
    envoy_filter.set_request_header(
      &self.target_port_header,
      actor.target_port.to_string().as_bytes(),
    );
    // Overwrite ate-target-actor so an untrusted inner header on a CONNECT
    // tunnel cannot select a different actor after routing is resolved.
    envoy_filter.set_request_header(TARGET_ACTOR_HEADER, actor.cache_key().as_bytes());
  }

  /// Extracts the raw `<atespace>/<actor>` string from filter state or the
  /// `ate-target-actor` header, ignoring untrusted inner headers on terminated
  /// `CONNECT` tunnels.
  pub fn target_actor_value<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> Option<String> {
    if let Some(actor) = filter_state_str(envoy_filter, TARGET_ACTOR_FILTER_STATE_KEY) {
      return Some(actor);
    }
    // Requests reaching main_internal via a terminated CONNECT tunnel retain
    // the tunnel's authority in CONNECT_AUTHORITY_FILTER_STATE_KEY. For such
    // requests, the actor header must have arrived on the outer CONNECT request;
    // inner tunneled headers are untrusted and must never be used for routing.
    if filter_state_str(envoy_filter, CONNECT_AUTHORITY_FILTER_STATE_KEY).is_some() {
      return None;
    }
    let vals = envoy_filter.get_request_header_values(TARGET_ACTOR_HEADER);
    let [val] = vals.as_slice() else {
      return None;
    };
    std::str::from_utf8(val.as_slice()).ok().map(str::to_string)
  }

  /// Resolves the target actor port from `CONNECT` authority filter state or headers,
  /// defaulting to [`DEFAULT_ACTOR_PORT`] (`80`).
  pub fn resolve_target_port<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> u16 {
    let authority =
      filter_state_str(envoy_filter, CONNECT_AUTHORITY_FILTER_STATE_KEY).or_else(|| {
        if request_header_str(envoy_filter, ":method").as_deref() == Some("CONNECT") {
          request_header_str(envoy_filter, ":authority")
        } else {
          None
        }
      });

    authority
      .as_deref()
      .and_then(Self::parse_authority_port)
      .unwrap_or(DEFAULT_ACTOR_PORT)
  }

  /// Extracts a non-zero port number from an `authority` string (`host:port` or
  /// `[ipv6]:port`).
  pub fn parse_authority_port(authority: &str) -> Option<u16> {
    let (host, port_str) = authority.rsplit_once(':')?;
    // A bare IPv6 literal without a port (`2001:db8::1`) contains `:` without brackets.
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
      return None;
    }
    port_str.parse::<NonZeroU16>().ok().map(NonZeroU16::get)
  }

  /// Parses `<atespace>/<actor>` into an [`ActorRef`] after validating both
  /// segments as RFC-1123 DNS labels.
  pub fn parse_target_actor(value: &str, target_port: u16) -> Option<ActorRef> {
    let (atespace, name) = value.split_once('/')?;
    if name.contains('/') || !is_valid_resource_name(atespace) || !is_valid_resource_name(name) {
      return None;
    }
    Some(ActorRef {
      atespace: atespace.to_string(),
      name: name.to_string(),
      target_port,
    })
  }

  /// Resolves the [`ActorRef`] for the current request from filter state and headers.
  pub fn resolve_actor_ref<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> Option<ActorRef> {
    let raw = self.target_actor_value(envoy_filter)?;
    let target_port = self.resolve_target_port(envoy_filter);
    Self::parse_target_actor(&raw, target_port)
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
  fn test_parse_target_actor_and_port() {
    let actor =
      Config::parse_target_actor("team-a/actor-1", 80).expect("should parse valid actor ref");
    assert_eq!(
      actor,
      ActorRef {
        atespace: "team-a".to_string(),
        name: "actor-1".to_string(),
        target_port: 80,
      }
    );
    assert_eq!(actor.cache_key(), "team-a/actor-1");

    // Invalid actor references (missing slash, extra slash, comma-joined duplicates, uppercase, empty)
    assert!(Config::parse_target_actor("actor-1", 80).is_none());
    assert!(Config::parse_target_actor("team-a/actor-1/extra", 80).is_none());
    assert!(Config::parse_target_actor("team-a/actor-1,team-b/actor-2", 80).is_none());
    assert!(Config::parse_target_actor("Team-A/actor-1", 80).is_none());
    assert!(Config::parse_target_actor("/actor-1", 80).is_none());
    assert!(Config::parse_target_actor("team-a/", 80).is_none());
    assert!(Config::parse_target_actor("-team-a/actor-1", 80).is_none());
    assert!(Config::parse_target_actor("team-a/actor-1-", 80).is_none());

    // Authority port parsing
    assert_eq!(Config::parse_authority_port("example.com:8080"), Some(8080));
    assert_eq!(
      Config::parse_authority_port("[2001:db8::1]:9090"),
      Some(9090)
    );
    assert_eq!(Config::parse_authority_port("example.com"), None);
    assert_eq!(Config::parse_authority_port("example.com:0"), None);
    assert_eq!(Config::parse_authority_port("2001:db8::1"), None);
  }

  #[test]
  fn test_connect_tunnel_ignores_inner_header_without_outer_filter_state() {
    let config = Config::default();
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TARGET_ACTOR_FILTER_STATE_KEY)
      .returning(|_| None);
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == CONNECT_AUTHORITY_FILTER_STATE_KEY)
      .returning(|_| Some(EnvoyBuffer::new(b"atenet-router:8080")));

    // Even if an inner header were present, target_actor_value must return None
    // when dev.ate.connect.authority is set without dev.ate.target.actor.
    assert!(config.resolve_actor_ref(&mock).is_none());
  }
}
