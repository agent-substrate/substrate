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
    envoy_dynamic_module_type_metadata_source,
    envoy_dynamic_module_type_on_http_filter_request_headers_status,
    envoy_dynamic_module_type_on_http_filter_response_headers_status,
  },
  envoy_log_error, envoy_log_trace,
};

use crate::cache::{Binding, cache};
use crate::config::{Config, FilterConfig};
use crate::routing::{
  ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_NAMESPACE, ROUTE_RESOLVED_HEADER, STALE_ASSIGNMENT_HEADER,
  WORKER_SPIFFE_ID_KEY, response_header_str,
};

/// Counter incremented when an unexpired actor -> worker binding is served from cache.
pub const CACHE_HIT_COUNTER: &str = "ate_ingress_cache.hit";

/// Counter incremented when no valid cached binding exists for the target actor.
pub const CACHE_MISS_COUNTER: &str = "ate_ingress_cache.miss";

/// Counter incremented when an expired or stale cache entry is evicted.
pub const CACHE_EVICTION_COUNTER: &str = "ate_ingress_cache.eviction";

/// Counter incremented when a stale fast-path request is replayed via `recreate_stream`.
pub const RECREATE_STREAM_COUNTER: &str = "ate_ingress_cache.recreate_stream";

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
///   `recreate_stream` so the request replays through `ext_proc`.
pub struct CacheFilter {
  pub config: Config,
  pub cache_hit: Option<EnvoyCounterId>,
  pub cache_miss: Option<EnvoyCounterId>,
  pub cache_eviction: Option<EnvoyCounterId>,
  pub recreate_stream: Option<EnvoyCounterId>,
  pub learn_key: Option<String>,
  pub actor_key: Option<String>,
  pub was_cache_hit: bool,
}

/// Increments an optional Envoy counter by 1.
fn increment_counter<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, counter: Option<EnvoyCounterId>) {
  if let Some(id) = counter {
    let _ = envoy_filter.increment_counter(id, 1);
  }
}

/// Reports whether the upstream response indicates a stale actor assignment.
fn is_stale_rejection<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> bool {
  match response_header_str(envoy_filter, ":status").as_deref() {
    Some("421") => response_header_str(envoy_filter, STALE_ASSIGNMENT_HEADER)
      .is_none_or(|val| val.eq_ignore_ascii_case("true")),
    // With --upstream-connect enabled, atunnel's 421 CONNECT rejection or
    // deactivated tunnel closure is surfaced by Envoy's internal tcp_proxy
    // as an HTTP 503 response.
    Some("503") => true,
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
    _end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
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
    let stale_rejection = is_stale_rejection(envoy_filter);

    if stale_rejection && self.was_cache_hit {
      if let Some(key) = &self.actor_key {
        cache().remove(key);
        increment_counter(envoy_filter, self.cache_eviction);
      }
      envoy_filter.remove_request_header(ROUTE_RESOLVED_HEADER);
      self.was_cache_hit = false;

      increment_counter(envoy_filter, self.recreate_stream);
      if envoy_filter.recreate_stream(None) {
        return envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration;
      }
    }

    // On a cache miss that succeeded upstream, learn from the metadata ext_proc published.
    if self.config.cache_enabled
      && !stale_rejection
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
    let mut filter = CacheFilter {
      config: config.clone(),
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: None,
      actor_key: None,
      was_cache_hit: false,
    };

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

    let mut filter = CacheFilter {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: None,
      actor_key: None,
      was_cache_hit: false,
    };

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

    let mut filter = CacheFilter {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: None,
      actor_key: Some("team-d/actor-4".to_string()),
      was_cache_hit: true,
    };

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"true")));
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

    let mut filter = CacheFilter {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: None,
      actor_key: Some("team-d/actor-4-503".to_string()),
      was_cache_hit: true,
    };

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
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
    let mut filter = CacheFilter {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: Some("team-e/actor-5".to_string()),
      actor_key: Some("team-e/actor-5".to_string()),
      was_cache_hit: false,
    };

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"421")));
    mock
      .expect_get_response_header_value()
      .withf(|key| key == STALE_ASSIGNMENT_HEADER)
      .returning(|_| Some(EnvoyBuffer::new(b"true")));

    // recreate_stream and metadata learning should NOT be called on a 421 cache miss!
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache().get("team-e/actor-5").is_none());
  }

  #[test]
  fn test_ipv6_learning_and_routing() {
    let config = Config::default();
    let mut filter = CacheFilter {
      config: config.clone(),
      cache_hit: None,
      cache_miss: None,
      cache_eviction: None,
      recreate_stream: None,
      learn_key: Some("team-f/actor-6".to_string()),
      actor_key: None,
      was_cache_hit: false,
    };

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
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
}
