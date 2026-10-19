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

//! HTTP filter implementation for the ingress-cache dynamic module.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use envoy_proxy_dynamic_modules_rust_sdk::{
  EnvoyCounterId, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter, HttpFilterConfig,
  abi::{
    envoy_dynamic_module_type_attribute_id, envoy_dynamic_module_type_metadata_source,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_trailers_status,
  },
  envoy_log_error, envoy_log_trace,
};

use crate::cache::{Binding, cache};
use crate::config::{Config, FilterConfig};
use crate::routing::{
  GRPC_STATUS_HEADER, ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_NAMESPACE, ROUTE_RESOLVED_HEADER,
  STALE_ASSIGNMENT_HEADER, TARGET_ACTOR_HEADER, WORKER_SPIFFE_ID_KEY, is_grpc_unavailable,
  request_header_str, response_header_str, response_trailer_str,
};

/// Counter incremented when an unexpired actor -> worker binding is served from cache.
pub const CACHE_HIT_COUNTER: &str = "ate_ingress_cache.hit";

/// Counter incremented when no valid cached binding exists for the target actor.
pub const CACHE_MISS_COUNTER: &str = "ate_ingress_cache.miss";

/// Counter incremented when an expired or stale cache entry is evicted.
pub const CACHE_EVICTION_COUNTER: &str = "ate_ingress_cache.eviction";

/// Counter incremented when a stale fast-path request is replayed via `recreate_stream`.
pub const RECREATE_STREAM_COUNTER: &str = "ate_ingress_cache.recreate_stream";

/// Envoy core response flag: No healthy upstream (UH = 1 << 1).
pub const RESPONSE_FLAG_UH: i64 = 1 << 1;

/// Envoy core response flag: Upstream remote reset (UR = 1 << 4).
pub const RESPONSE_FLAG_UR: i64 = 1 << 4;

/// Envoy core response flag: Upstream connection failure (UF = 1 << 5).
pub const RESPONSE_FLAG_UF: i64 = 1 << 5;

/// Envoy core response flag: Upstream connection termination (UC = 1 << 6).
pub const RESPONSE_FLAG_UC: i64 = 1 << 6;

/// Mask of upstream flags that indicate a failure communicating with the upstream worker,
/// warranting cache eviction.
pub const UPSTREAM_FAILURE_RESPONSE_FLAGS: i64 =
  RESPONSE_FLAG_UF | RESPONSE_FLAG_UH | RESPONSE_FLAG_UC | RESPONSE_FLAG_UR;

/// Mask of response flags that guarantee zero bytes reached the actor,
/// making replay safe for bodiless requests.
pub const ZERO_BYTE_FAILURE_RESPONSE_FLAGS: i64 = RESPONSE_FLAG_UF | RESPONSE_FLAG_UH;


/// Creates a new HTTP filter configuration for the `ingress-cache` dynamic module.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
  envoy_filter_config: &mut EC,
  _filter_name: &str,
  filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
  let config = Config::from_config(filter_config)
    .map_err(|e| {
      envoy_log_error!("ingress_cache: invalid filter config: {}", e);
    })
    .ok()?;

  envoy_log_trace!("ingress_cache: ttl={}s", config.cache_ttl_seconds);

  Some(Box::new(FilterConfig {
    cache_hit: envoy_filter_config.define_counter(CACHE_HIT_COUNTER).ok(),
    cache_miss: envoy_filter_config.define_counter(CACHE_MISS_COUNTER).ok(),
    cache_eviction: envoy_filter_config
      .define_counter(CACHE_EVICTION_COUNTER)
      .ok(),
    recreate_stream: envoy_filter_config
      .define_counter(RECREATE_STREAM_COUNTER)
      .ok(),
    settings: config,
  }))
}

/// Per-stream HTTP filter sitting ahead of `ext_proc` in the ingress filter chain.
///
/// * **On a hit**: publishes dynamic metadata for `ORIGINAL_DST` / `actor_connect`,
///   sets [`ROUTE_RESOLVED_HEADER`] to select the route with `ext_proc` disabled,
///   and clears the route cache.
/// * **On a miss**: lets `ext_proc` run normally, then learns the resolved worker IP
///   from dynamic metadata in `on_response_headers`.
/// * **On upstream stale rejection (`421` or `503` on a cache hit)**: evicts the
///   stale cache entry, removes [`ROUTE_RESOLVED_HEADER`], and calls
///   `recreate_stream` (if replay-safe) so the request replays through `ext_proc`.
/// * **On gRPC UNAVAILABLE (`grpc-status: 14` on a cache hit)**: evicts the stale
///   cache entry and returns the error to the client without replaying.

pub struct CacheFilter {
  pub config: Config,
  pub cache_hit: Option<EnvoyCounterId>,
  pub cache_miss: Option<EnvoyCounterId>,
  pub cache_eviction: Option<EnvoyCounterId>,
  pub recreate_stream: Option<EnvoyCounterId>,
  pub learn_key: Option<String>,
  pub actor_key: Option<String>,
  pub was_cache_hit: bool,
  pub has_request_body: bool,
}

impl CacheFilter {
  #[cfg(test)]
  pub(crate) fn new_test_filter(config: Config) -> Self {
    Self {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: None,
      actor_key: None,
      was_cache_hit: false,
      has_request_body: false,
    }
  }
}

/// Increments an optional Envoy counter by 1.
fn increment_counter<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, counter: Option<EnvoyCounterId>) {
  if let Some(id) = counter {
    let _ = envoy_filter.increment_counter(id, 1);
  }
}

/// Reports whether the upstream response indicates a stale actor assignment or upstream failure.
fn is_stale_rejection<EHF: EnvoyHttpFilter>(
  envoy_filter: &EHF,
  status: Option<&str>,
  response_flags: i64,
) -> bool {
  match status {
    Some("421") => response_header_str(envoy_filter, STALE_ASSIGNMENT_HEADER)
      .is_some_and(|val| val.eq_ignore_ascii_case("true")),
    // With --upstream-connect enabled, atunnel's 421 CONNECT rejection or
    // deactivated tunnel closure is surfaced by Envoy's internal tcp_proxy
    // as an HTTP 503 response with upstream response flags (UF, UH, UC, or UR).
    // An application 503 emitted directly by the actor has response_flags == 0.
    Some("503") => (response_flags & UPSTREAM_FAILURE_RESPONSE_FLAGS) != 0,
    _ => false,
  }
}

/// Reports whether a request that encountered a stale rejection can be safely replayed.
///
/// Replay is safe if:
/// 1. The request has no body (`!filter.has_request_body`). We do not buffer request
///    bodies in Envoy, so replaying a request with a body would drop or corrupt the payload.
/// 2. For 503 errors, response flags guarantee zero bytes reached the actor (`UF` or `UH`).
///    Mid-stream disconnections (`UC` or `UR`) are never replayed to prevent duplicate execution.
/// 3. For 421 errors (with stale header), atunnel rejected the request before forwarding to
///    the actor, so bodiless requests are safe to replay.
fn is_replay_safe(filter: &CacheFilter, status: Option<&str>, response_flags: i64) -> bool {
  if filter.has_request_body {
    return false;
  }
  match status {
    Some("503") => (response_flags & ZERO_BYTE_FAILURE_RESPONSE_FLAGS) != 0,
    Some("421") => true,
    _ => false,
  }
}

/// Extracts the resolved worker IP and optional SPIFFE ID from the `ORIGINAL_DST`
/// dynamic metadata published by `ext_proc`.
fn read_learned_binding<EHF: EnvoyHttpFilter>(
  envoy_filter: &EHF,
) -> Option<(String, Option<String>)> {
  let buf = envoy_filter.get_metadata_string(
    envoy_dynamic_module_type_metadata_source::Dynamic,
    ORIGINAL_DST_NAMESPACE,
    ORIGINAL_DST_ADDRESS_KEY,
  )?;
  let addr = std::str::from_utf8(buf.as_slice()).ok()?;
  let ip = addr.parse::<SocketAddr>().ok()?.ip().to_string();

  let spiffe_id = envoy_filter
    .get_metadata_string(
      envoy_dynamic_module_type_metadata_source::Dynamic,
      ORIGINAL_DST_NAMESPACE,
      WORKER_SPIFFE_ID_KEY,
    )
    .and_then(|b| {
      let s = std::str::from_utf8(b.as_slice()).ok()?;
      (!s.is_empty()).then(|| s.to_string())
    });

  Some((ip, spiffe_id))
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for CacheFilter {
  fn on_request_headers(
    &mut self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    let content_length = request_header_str(envoy_filter, "content-length");
    let is_zero_content_length = content_length.as_deref().map(str::trim) == Some("0");
    self.has_request_body = !end_of_stream && !is_zero_content_length;

    let Some(actor) = self.config.resolve_actor_ref(envoy_filter) else {
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    };

    let key = actor.cache_key();
    self.actor_key = Some(key.clone());

    if self.config.cache_enabled
      && let Some(entry) = cache().get(&key)
    {
      if entry.expires_at > Instant::now() {
        let worker_ip = entry.worker_ip.clone();
        let worker_spiffe_id = entry.worker_spiffe_id.clone();
        drop(entry);
        increment_counter(envoy_filter, self.cache_hit);
        self.config.route_to(
          envoy_filter,
          &actor,
          &worker_ip,
          worker_spiffe_id.as_deref(),
        );
        envoy_filter.set_request_header(ROUTE_RESOLVED_HEADER, b"1");
        envoy_filter.clear_route_cache();
        self.was_cache_hit = true;
        return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
      }
      drop(entry);
      cache().remove(&key);
      increment_counter(envoy_filter, self.cache_eviction);
    }

    increment_counter(envoy_filter, self.cache_miss);
    // Ensure cache-bypass header is not set on a miss or recreated stream.
    envoy_filter.remove_request_header(ROUTE_RESOLVED_HEADER);
    self.was_cache_hit = false;
    self.learn_key = Some(key);
    envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
  }

  fn on_response_headers(
    &mut self,
    envoy_filter: &mut EHF,
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_response_headers_status {
    let status_str = response_header_str(envoy_filter, ":status");
    let response_flags = envoy_filter
      .get_attribute_int(envoy_dynamic_module_type_attribute_id::ResponseFlags)
      .unwrap_or(0);
    let stale_rejection = is_stale_rejection(envoy_filter, status_str.as_deref(), response_flags);
    let grpc_unavailable = status_str.as_deref() == Some("200")
      && response_header_str(envoy_filter, GRPC_STATUS_HEADER)
        .as_deref()
        .is_some_and(is_grpc_unavailable);

    if stale_rejection && self.was_cache_hit {
      if let Some(key) = &self.actor_key {
        cache().remove(key);
        increment_counter(envoy_filter, self.cache_eviction);
      }
      self.was_cache_hit = false;

      if is_replay_safe(self, status_str.as_deref(), response_flags) {
        if let Some(key) = &self.actor_key {
          // Upstream route mutations strip TARGET_ACTOR_HEADER before forwarding to
          // the actor tunnel. Re-inject it so the recreated stream passes the valid
          // actor reference to ext_proc on replay.
          envoy_filter.set_request_header(TARGET_ACTOR_HEADER, key.as_bytes());
        }
        envoy_filter.remove_request_header(ROUTE_RESOLVED_HEADER);
        increment_counter(envoy_filter, self.recreate_stream);
        if envoy_filter.recreate_stream(None) {
          return envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration;
        }
      }
    } else if grpc_unavailable && self.was_cache_hit {
      // Option A: For gRPC errors (grpc-status: 14 UNAVAILABLE in response headers),
      // evict the stale cache entry but do not replay (standard gRPC clients handle
      // retries automatically).
      if let Some(key) = &self.actor_key {
        cache().remove(key);
        increment_counter(envoy_filter, self.cache_eviction);
      }
      self.was_cache_hit = false;
    }

    // On a cache miss that succeeded upstream, learn from the metadata ext_proc published.
    if self.config.cache_enabled
      && !stale_rejection
      && !grpc_unavailable
      && let Some(key) = self.learn_key.take()
      && let Some((worker_ip, worker_spiffe_id)) = read_learned_binding(envoy_filter)
    {
      cache().insert(
        key,
        Binding {
          worker_ip,
          worker_spiffe_id,
          expires_at: Instant::now() + Duration::from_secs(self.config.cache_ttl_seconds),
        },
      );
    }

    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }

  fn on_response_trailers(
    &mut self,
    envoy_filter: &mut EHF,
  ) -> envoy_dynamic_module_type_on_http_filter_response_trailers_status {
    let grpc_unavailable = response_trailer_str(envoy_filter, GRPC_STATUS_HEADER)
      .as_deref()
      .is_some_and(is_grpc_unavailable);

    if grpc_unavailable {
      if self.was_cache_hit {
        if let Some(key) = &self.actor_key {
          cache().remove(key);
          increment_counter(envoy_filter, self.cache_eviction);
        }
        self.was_cache_hit = false;
      } else if let Some(key) = &self.actor_key {
        cache().remove(key);
      }
    }

    envoy_dynamic_module_type_on_http_filter_response_trailers_status::Continue
  }
}

// ==========================================
// Tests
// ==========================================

#[cfg(test)]
mod tests {
  use super::*;
  use crate::routing::{
    ActorRef, CONNECT_AUTHORITY_FILTER_STATE_KEY, ORIGINAL_DST_PORT_KEY,
    TARGET_ACTOR_FILTER_STATE_KEY, TARGET_ACTOR_HEADER,
  };
  use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

  #[test]
  fn test_cache_miss_and_learn_on_response() {
    let config = Config::default();
    let mut filter = CacheFilter::new_test_filter(config.clone());

    let target_actor = b"team-b/actor-2";
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TARGET_ACTOR_FILTER_STATE_KEY)
      .returning(move |_| Some(EnvoyBuffer::new(target_actor)));
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == CONNECT_AUTHORITY_FILTER_STATE_KEY)
      .returning(|_| None);
    mock
      .expect_get_request_header_value()
      .withf(|key| key == ":method")
      .returning(|_| Some(EnvoyBuffer::new(b"POST")));
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| None);
    mock
      .expect_remove_request_header()
      .withf(|key| key == ROUTE_RESOLVED_HEADER)
      .returning(|_| true);

    let status = filter.on_request_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert_eq!(filter.learn_key.as_deref(), Some("team-b/actor-2"));
    assert!(!filter.was_cache_hit);

    // Response headers arrive: learning worker address from dynamic metadata
    let metadata_local = b"10.244.1.50:443";
    let metadata_spiffe = b"spiffe://cluster.local/ns/default/sa/worker-pool-1";
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| None);
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, key| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == ORIGINAL_DST_ADDRESS_KEY
      })
      .returning(move |_, _, _| Some(EnvoyBuffer::new(metadata_local)));
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, key| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == WORKER_SPIFFE_ID_KEY
      })
      .returning(move |_, _, _| Some(EnvoyBuffer::new(metadata_spiffe)));

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );

    // Verify entry is now in cache
    let entry = cache().get("team-b/actor-2").expect("should be in cache");
    assert_eq!(entry.worker_ip, "10.244.1.50");
    assert_eq!(
      entry.worker_spiffe_id.as_deref(),
      Some("spiffe://cluster.local/ns/default/sa/worker-pool-1")
    );
  }

  #[test]
  fn test_cache_hit_populates_metadata_and_skips_extproc() {
    let config = Config::default();
    cache().insert(
      "team-c/actor-3".to_string(),
      Binding {
        worker_ip: "10.244.2.100".to_string(),
        worker_spiffe_id: Some("spiffe://cluster.local/ns/default/sa/worker-pool-2".to_string()),
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);

    let target_actor = b"team-c/actor-3";
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == TARGET_ACTOR_FILTER_STATE_KEY)
      .returning(|_| None);
    mock
      .expect_get_filter_state_bytes()
      .withf(|key| key == CONNECT_AUTHORITY_FILTER_STATE_KEY)
      .returning(|_| None);
    mock
      .expect_get_request_header_values()
      .withf(|key| key == TARGET_ACTOR_HEADER)
      .returning(move |_| vec![EnvoyBuffer::new(target_actor)]);
    mock
      .expect_get_request_header_value()
      .withf(|key| key == ":method")
      .returning(|_| Some(EnvoyBuffer::new(b"POST")));
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| None);

    mock
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == ORIGINAL_DST_NAMESPACE && key == ORIGINAL_DST_ADDRESS_KEY && val == "10.244.2.100:443"
      })
      .returning(|_, _, _| ());
    mock
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == ORIGINAL_DST_NAMESPACE && key == ORIGINAL_DST_PORT_KEY && val == "80"
      })
      .returning(|_, _, _| ());
    mock
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == ORIGINAL_DST_NAMESPACE
          && key == WORKER_SPIFFE_ID_KEY
          && val == "spiffe://cluster.local/ns/default/sa/worker-pool-2"
      })
      .returning(|_, _, _| ());
    mock
      .expect_set_request_header()
      .withf(|key, val| key == "x-ate-target-port" && val == b"80")
      .returning(|_, _| true);
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-c/actor-3")
      .returning(|_, _| true);
    mock
      .expect_set_request_header()
      .withf(|key, val| key == ROUTE_RESOLVED_HEADER && val == b"1")
      .returning(|_, _| true);
    mock.expect_clear_route_cache().returning(|| ());

    let status = filter.on_request_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_recreates_stream_on_cache_hit() {
    let config = Config::default();
    cache().insert(
      "team-d/actor-4".to_string(),
      Binding {
        worker_ip: "10.244.3.40".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-d/actor-4".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"true")));
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-d/actor-4")
      .returning(|_, _| true);
    mock
      .expect_remove_request_header()
      .withf(|key| key == ROUTE_RESOLVED_HEADER)
      .returning(|_| true);
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );

    assert!(cache().get("team-d/actor-4").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_recreates_stream_on_cache_hit() {
    let config = Config::default();
    cache().insert(
      "team-d/actor-4-503".to_string(),
      Binding {
        worker_ip: "10.244.3.41".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-d/actor-4-503".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UF));
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-d/actor-4-503")
      .returning(|_, _| true);
    mock
      .expect_remove_request_header()
      .withf(|key| key == ROUTE_RESOLVED_HEADER)
      .returning(|_| true);
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );

    assert!(cache().get("team-d/actor-4-503").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_does_not_loop_on_cache_miss() {
    let config = Config::default();
    let mut filter = CacheFilter::new_test_filter(config);
    filter.learn_key = Some("team-e/actor-5".to_string());
    filter.actor_key = Some("team-e/actor-5".to_string());
    filter.was_cache_hit = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"true")));
    mock.expect_get_attribute_int().returning(|_| None);

    // recreate_stream and metadata learning should NOT be called on a 421 cache miss!
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-e/actor-5").is_none());
  }

  #[test]
  fn test_upstream_421_without_stale_header_is_not_treated_as_stale() {
    let config = Config::default();
    cache().insert(
      "team-h/actor-8".to_string(),
      Binding {
        worker_ip: "10.244.5.10".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-8".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    // No STALE_ASSIGNMENT_HEADER on the response (e.g. actor application returned 421).
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| None);
    mock.expect_get_attribute_int().returning(|_| None);

    // recreate_stream must NOT be called, and cache must NOT be evicted.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-h/actor-8").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_false_stale_header_is_not_treated_as_stale() {
    let config = Config::default();
    cache().insert(
      "team-h/actor-9".to_string(),
      Binding {
        worker_ip: "10.244.5.11".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-9".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"false")));
    mock.expect_get_attribute_int().returning(|_| None);

    // recreate_stream must NOT be called, and cache must NOT be evicted.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-h/actor-9").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_case_insensitive_true_stale_header() {
    let config = Config::default();
    cache().insert(
      "team-h/actor-10".to_string(),
      Binding {
        worker_ip: "10.244.5.12".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-10".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"TRUE")));
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-h/actor-10")
      .returning(|_, _| true);
    mock
      .expect_remove_request_header()
      .withf(|key| key == ROUTE_RESOLVED_HEADER)
      .returning(|_| true);
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );
    assert!(cache().get("team-h/actor-10").is_none());
  }

  #[test]
  fn test_upstream_503_from_actor_is_not_treated_as_stale() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-11".to_string(),
      Binding {
        worker_ip: "10.244.6.1".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-11".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // Actor's own 503 has no upstream connection flags set (flags == 0).
    mock.expect_get_attribute_int().returning(|_| Some(0));

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    // Cache entry must remain intact, and hit status must not be cleared.
    assert!(cache().get("team-j/actor-11").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_uh_recreates_stream() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-12".to_string(),
      Binding {
        worker_ip: "10.244.6.2".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-12".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // No healthy upstream (UH) flag set by Envoy.
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UH));
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-j/actor-12")
      .returning(|_, _| true);
    mock
      .expect_remove_request_header()
      .withf(|key| key == ROUTE_RESOLVED_HEADER)
      .returning(|_| true);
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );
    assert!(cache().get("team-j/actor-12").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_uc_does_not_replay_when_request_has_body() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-13".to_string(),
      Binding {
        worker_ip: "10.244.6.3".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-13".to_string());
    filter.was_cache_hit = true;
    filter.has_request_body = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // UpstreamConnectionTermination (UC) flag set by Envoy.
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UC));
    mock
      .expect_get_buffered_request_body_size()
      .returning(|| 128);

    // Because UC indicates mid-flight disconnection, recreate_stream must NOT be called under Option A.
    // However, the dead worker binding must still be evicted!
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-j/actor-13").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_uc_does_not_replay_even_for_bodiless_get() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-14".to_string(),
      Binding {
        worker_ip: "10.244.6.4".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-14".to_string());
    filter.was_cache_hit = true;
    filter.has_request_body = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // UpstreamConnectionTermination (UC) flag set by Envoy.
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UC));

    // Under Option A, mid-flight disconnections (UC) are never replayed out of caution.
    // Cache entry is evicted and 503 is returned to client.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-j/actor-14").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_ur_does_not_replay() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-14-ur".to_string(),
      Binding {
        worker_ip: "10.244.6.41".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-14-ur".to_string());
    filter.was_cache_hit = true;
    filter.has_request_body = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // UpstreamRemoteReset (UR) flag set by Envoy.
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UR));

    // Under Option A, UR resets are never replayed.
    // Cache entry is evicted and 503 is returned to client.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-j/actor-14-ur").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_stale_rejection_does_not_replay_when_request_has_body() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-15".to_string(),
      Binding {
        worker_ip: "10.244.6.5".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-15".to_string());
    filter.was_cache_hit = true;
    filter.has_request_body = true; // Request had body (e.g. POST)

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UF));

    // Because request has body (and we do not buffer bodies), recreate_stream must NOT be called.
    // The dead worker binding must still be evicted.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-j/actor-15").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_body_does_not_replay() {
    let config = Config::default();
    cache().insert(
      "team-j/actor-16-421".to_string(),
      Binding {
        worker_ip: "10.244.6.7".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-16-421".to_string());
    filter.was_cache_hit = true;
    filter.has_request_body = true; // Request has body

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"true")));
    mock.expect_get_attribute_int().returning(|_| None);

    // 421 with body: evict cache, do not replay!
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-j/actor-16-421").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_on_request_headers_tracks_request_body() {
    let config = Config::default();

    // 1. Request with end_of_stream: true (no body)
    let mut filter = CacheFilter::new_test_filter(config.clone());
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| None);
    mock.expect_get_filter_state_bytes().returning(|_| None);
    mock
      .expect_get_request_header_values()
      .returning(|_| vec![]);
    filter.on_request_headers(&mut mock, true);
    assert!(!filter.has_request_body);

    // 2. Request with end_of_stream: false, content-length: 100 (has body)
    let mut filter = CacheFilter::new_test_filter(config.clone());
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| Some(EnvoyBuffer::new(b"100")));
    mock.expect_get_filter_state_bytes().returning(|_| None);
    mock
      .expect_get_request_header_values()
      .returning(|_| vec![]);
    filter.on_request_headers(&mut mock, false);
    assert!(filter.has_request_body);

    // 3. Request with end_of_stream: false, content-length: 0 (empty body)
    let mut filter = CacheFilter::new_test_filter(config);
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| Some(EnvoyBuffer::new(b"0")));
    mock.expect_get_filter_state_bytes().returning(|_| None);
    mock
      .expect_get_request_header_values()
      .returning(|_| vec![]);
    filter.on_request_headers(&mut mock, false);
    assert!(!filter.has_request_body);
  }

  #[test]
  fn test_ipv6_learning_and_routing() {
    let config = Config::default();
    let mut filter = CacheFilter::new_test_filter(config.clone());
    filter.learn_key = Some("team-f/actor-6".to_string());
    filter.was_cache_hit = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_get_metadata_string()
      .withf(|source, ns, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == ORIGINAL_DST_ADDRESS_KEY
      })
      .returning(|_, _, _| Some(EnvoyBuffer::new(b"[2001:db8::1]:443")));
    mock
      .expect_get_metadata_string()
      .withf(|source, ns, key| {
        *source == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == WORKER_SPIFFE_ID_KEY
      })
      .returning(|_, _, _| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );

    let entry = cache()
      .get("team-f/actor-6")
      .expect("entry should be cached");
    assert_eq!(entry.worker_ip, "2001:db8::1");
    assert_eq!(entry.worker_spiffe_id, None);
    drop(entry);

    let actor = ActorRef {
      atespace: "team-f".to_string(),
      name: "actor-6".to_string(),
      target_port: 8080,
    };
    let mut route_mock = MockEnvoyHttpFilter::default();
    route_mock
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == ORIGINAL_DST_NAMESPACE
          && key == ORIGINAL_DST_ADDRESS_KEY
          && val == "[2001:db8::1]:443"
      })
      .returning(|_, _, _| ());
    route_mock
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == ORIGINAL_DST_NAMESPACE && key == ORIGINAL_DST_PORT_KEY && val == "8080"
      })
      .returning(|_, _, _| ());
    route_mock
      .expect_set_request_header()
      .withf(|key, val| key == "x-ate-target-port" && val == b"8080")
      .returning(|_, _| true);
    route_mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-f/actor-6")
      .returning(|_, _| true);

    config.route_to(&mut route_mock, &actor, "2001:db8::1", None);
  }

  #[test]
  fn test_grpc_status_14_in_headers_evicts_cache_and_does_not_replay() {
    let config = Config::default();
    cache().insert(
      "team-grpc/actor-h14".to_string(),
      Binding {
        worker_ip: "10.244.6.1".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-grpc/actor-h14".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"14")));
    mock.expect_get_attribute_int().returning(|_| None);

    // Option A: recreate_stream must NOT be called, but cache must be evicted.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-h14").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_grpc_status_14_in_trailers_evicts_cache_and_does_not_replay() {
    let config = Config::default();
    cache().insert(
      "team-grpc/actor-t14".to_string(),
      Binding {
        worker_ip: "10.244.6.2".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-grpc/actor-t14".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| None);
    mock.expect_get_attribute_int().returning(|_| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(filter.was_cache_hit);
    assert!(cache().get("team-grpc/actor-t14").is_some());

    // Response trailers arrive with grpc-status: 14 (UNAVAILABLE)
    mock
      .expect_get_response_trailer_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"14")));

    let trailer_status = filter.on_response_trailers(&mut mock);
    assert_eq!(
      trailer_status,
      envoy_dynamic_module_type_on_http_filter_response_trailers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-t14").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_grpc_status_14_increments_cache_eviction_counter() {
    let config = Config::default();
    cache().insert(
      "team-grpc/actor-count".to_string(),
      Binding {
        worker_ip: "10.244.6.3".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-grpc/actor-count".to_string());
    filter.was_cache_hit = true;
    let eviction_id = EnvoyCounterId(99);
    filter.cache_eviction = Some(eviction_id);

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"14")));
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_increment_counter()
      .withf(move |id, val| *id == eviction_id && *val == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-count").is_none());
  }

  #[test]
  fn test_grpc_status_0_ok_does_not_evict() {
    let config = Config::default();
    cache().insert(
      "team-grpc/actor-ok".to_string(),
      Binding {
        worker_ip: "10.244.6.4".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-grpc/actor-ok".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"0")));
    mock.expect_get_attribute_int().returning(|_| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-ok").is_some());
    assert!(filter.was_cache_hit);

    // Also verify trailers with grpc-status: 0 does not evict
    mock
      .expect_get_response_trailer_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"0")));
    let trailer_status = filter.on_response_trailers(&mut mock);
    assert_eq!(
      trailer_status,
      envoy_dynamic_module_type_on_http_filter_response_trailers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-ok").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_grpc_other_error_status_does_not_evict() {
    let config = Config::default();
    cache().insert(
      "team-grpc/actor-other-err".to_string(),
      Binding {
        worker_ip: "10.244.6.5".to_string(),
        worker_spiffe_id: None,
        expires_at: Instant::now() + Duration::from_secs(5),
      },
    );

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-grpc/actor-other-err".to_string());
    filter.was_cache_hit = true;

    let mut mock = MockEnvoyHttpFilter::default();
    // Test grpc-status: 3 (INVALID_ARGUMENT) in headers
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"3")));
    mock.expect_get_attribute_int().returning(|_| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-other-err").is_some());
    assert!(filter.was_cache_hit);

    // Test grpc-status: 5 (NOT_FOUND) in trailers
    mock
      .expect_get_response_trailer_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"5")));
    let trailer_status = filter.on_response_trailers(&mut mock);
    assert_eq!(
      trailer_status,
      envoy_dynamic_module_type_on_http_filter_response_trailers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-other-err").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_grpc_status_14_on_cache_miss_in_headers_does_not_learn() {
    let config = Config::default();
    let mut filter = CacheFilter::new_test_filter(config);
    filter.learn_key = Some("team-grpc/actor-miss-h".to_string());
    filter.actor_key = Some("team-grpc/actor-miss-h".to_string());
    filter.was_cache_hit = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"14")));
    mock.expect_get_attribute_int().returning(|_| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    // Entry must NOT have been inserted into cache
    assert!(cache().get("team-grpc/actor-miss-h").is_none());
  }

  #[test]
  fn test_grpc_status_14_on_cache_miss_in_trailers_removes_learned_binding() {
    let config = Config::default();
    let mut filter = CacheFilter::new_test_filter(config);
    filter.learn_key = Some("team-grpc/actor-miss-t".to_string());
    filter.actor_key = Some("team-grpc/actor-miss-t".to_string());
    filter.was_cache_hit = false;

    let metadata_local = b"10.244.6.7:443";
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| None);
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, key| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == ORIGINAL_DST_ADDRESS_KEY
      })
      .returning(move |_, _, _| Some(EnvoyBuffer::new(metadata_local)));
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, key| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && key == WORKER_SPIFFE_ID_KEY
      })
      .returning(|_, _, _| None);

    // On headers, learning succeeds
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-miss-t").is_some());

    // When trailers arrive with grpc-status: 14, the failed binding is removed
    mock
      .expect_get_response_trailer_value()
      .withf(|key| key == GRPC_STATUS_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"14")));

    let trailer_status = filter.on_response_trailers(&mut mock);
    assert_eq!(
      trailer_status,
      envoy_dynamic_module_type_on_http_filter_response_trailers_status::Continue
    );
    assert!(cache().get("team-grpc/actor-miss-t").is_none());
  }
}
