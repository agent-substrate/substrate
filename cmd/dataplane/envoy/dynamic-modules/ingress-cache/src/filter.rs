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
use std::rc::Rc;
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

use crate::cache::{self, Entry, Lookup};
use crate::config::{Config, FilterConfig};
use crate::routing::{
  CACHE_HIT_METADATA_KEY, CACHE_HIT_METADATA_NAMESPACE, CACHE_HIT_METADATA_VALUE,
  GRPC_STATUS_HEADER, ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_NAMESPACE, STALE_ASSIGNMENT_HEADER,
  TARGET_ACTOR_HEADER, WORKER_SPIFFE_ID_KEY, is_grpc_unavailable, request_header_str,
  response_header_str, response_trailer_str,
};

/// Counter incremented when an unexpired actor -> worker binding is served from cache.
pub const CACHE_HIT_COUNTER: &str = "ate_ingress_cache.hit";

/// Counter incremented when no valid cached binding exists for the target actor.
pub const CACHE_MISS_COUNTER: &str = "ate_ingress_cache.miss";

/// Counter incremented when a request with a body skips the cache lookup and is
/// routed through `ext_proc`.
pub const CACHE_BYPASS_COUNTER: &str = "ate_ingress_cache.bypass";

/// Counter incremented when an expired or stale cache entry is evicted.
pub const CACHE_EVICTION_COUNTER: &str = "ate_ingress_cache.eviction";

/// Counter incremented when a stale fast-path request is replayed via `recreate_stream`.
pub const CACHE_REPLAY_COUNTER: &str = "ate_ingress_cache.replay";

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

/// Prefix of the response code details Envoy's router sets when the upstream reset before
/// any response. After a 1xx it uses `upstream_reset_after_response_started`.
pub const EARLY_UPSTREAM_RESET_DETAILS: &str = "upstream_reset_before_response_started";



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
    cache_bypass: envoy_filter_config.define_counter(CACHE_BYPASS_COUNTER).ok(),
    cache_eviction: envoy_filter_config
      .define_counter(CACHE_EVICTION_COUNTER)
      .ok(),
    cache_replay: envoy_filter_config
      .define_counter(CACHE_REPLAY_COUNTER)
      .ok(),
    settings: config,
  }))
}

/// Per-stream HTTP filter sitting ahead of `ext_proc` in the ingress filter chain.
///
/// * **On a hit**: publishes dynamic metadata for `ORIGINAL_DST` / `actor_connect`,
///   sets the [`CACHE_HIT_METADATA_NAMESPACE`] dynamic metadata to select the route
///   with `ext_proc` disabled, and clears the route cache.
/// * **On a miss**: lets `ext_proc` run normally, then learns the resolved worker IP
///   from dynamic metadata in `on_response_headers`.
/// * **On a request with a body**: skips the cache lookup and routes through `ext_proc`,
///   since a stale hit couldn't be replayed. Learns from the response like a miss.
/// * **On upstream stale rejection (`421` or `503` on a cache hit)**: evicts the
///   stale cache entry and calls `recreate_stream` (if replay-safe). The recreated
///   stream starts without this stream's dynamic metadata, so it replays through
///   `ext_proc`.
///   A `503` from a connection that ended before a response is replay-safe only if
///   the `tunnel_watcher` network filter marked the entry stale and not spoiled.
/// * **On gRPC UNAVAILABLE (`grpc-status: 14`)**: does not learn the binding, and removes
///   it if it was learned from this response's headers. gRPC requests always have a body,
///   so they never use the cache.

pub struct CacheFilter {
  pub config: Config,
  /// Counts bodiless requests served from an unexpired cache entry, skipping ext_proc.
  pub cache_hit: Option<EnvoyCounterId>,
  /// Counts bodiless requests with no unexpired cache entry, resolved through ext_proc.
  pub cache_miss: Option<EnvoyCounterId>,
  /// Counts requests with a body, which skip the cache lookup and go through ext_proc.
  pub cache_bypass: Option<EnvoyCounterId>,
  /// Counts cache entries removed because they expired or a cache hit found them stale.
  pub cache_eviction: Option<EnvoyCounterId>,
  /// Counts stale cache hits replayed through ext_proc with `recreate_stream`.
  pub cache_replay: Option<EnvoyCounterId>,
  pub learn_key: Option<String>,
  pub actor_key: Option<String>,
  pub was_cache_hit: bool,
  pub has_request_body: bool,
  /// The entry this request was routed from on a cache hit. Holding it keeps the
  /// entry, and the network filter's marks on it, alive until the request ends.
  pub entry: Option<Rc<Entry>>,
  /// The entry this request's response added to the cache, if any.
  pub learned: Option<Rc<Entry>>,
}

impl CacheFilter {
  /// Evicts the binding this request used or learned, if it is still current, and if this
  /// stream was a cache hit, increments the eviction counter and clears the cache hit flag.
  fn handle_eviction<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
    if let (Some(key), Some(entry)) = (&self.actor_key, self.entry.as_ref().or(self.learned.as_ref())) {
      cache::remove_if_current(key, entry);
    }
    if self.was_cache_hit {
      increment_counter(envoy_filter, self.cache_eviction);
      self.was_cache_hit = false;
    }
  }

  #[cfg(test)]
  pub(crate) fn new_test_filter(config: Config) -> Self {
    Self {
      config,
      cache_hit: None,
      cache_miss: None,
      cache_bypass: None,
      cache_eviction: None,
      cache_replay: None,
      learn_key: None,
      actor_key: None,
      was_cache_hit: false,
      has_request_body: false,
      entry: None,
      learned: None,
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
///    Requests with a body skip the cache, so this is a defensive check.
/// 2. For 503 errors, response flags guarantee zero bytes reached the upstream worker (`UF` or `UH`),
///    or the connection ended before any response (`UC`, with early-reset details) and the
///    entry the request was routed from is stale and not spoiled. Stale means a tunnel for the
///    binding was refused; not spoiled means no request on it was cut off after reaching the
///    actor. A 1xx counts as a response: the actor got the request.
/// 3. For 421 errors (with stale header), atunnel rejected the request before forwarding to
///    the actor, so bodiless requests are safe to replay.
fn is_replay_safe<EHF: EnvoyHttpFilter>(
  envoy_filter: &EHF,
  filter: &CacheFilter,
  status: Option<&str>,
  response_flags: i64,
) -> bool {
  if filter.has_request_body {
    return false;
  }
  match status {
    Some("503") => {
      (response_flags & ZERO_BYTE_FAILURE_RESPONSE_FLAGS) != 0
        // On the CONNECT path a refused tunnel reaches the router as a connection
        // termination (`UC`), like a tunnel cut off mid-request. The network filter's
        // marks tell them apart.
        || ((response_flags & RESPONSE_FLAG_UC) != 0
          && filter.entry.as_ref().is_some_and(|e| e.refused_and_unspoiled())
          && envoy_filter
            .get_attribute_string(envoy_dynamic_module_type_attribute_id::ResponseCodeDetails)
            .is_some_and(|d| d.as_slice().starts_with(EARLY_UPSTREAM_RESET_DETAILS.as_bytes())))
    }
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

/// Reports whether the request may have a body, which a replay can't resend because Envoy
/// doesn't buffer it. A request is bodiless only if it ends with its headers or has
/// `content-length: 0`, so chunked requests and WS upgrades still count as having a body.
fn request_has_body<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, end_of_stream: bool) -> bool {
  if end_of_stream {
    return false;
  }
  request_header_str(envoy_filter, "content-length")
    .map_or(true, |len| len.trim().parse::<u64>() != Ok(0))
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for CacheFilter {
  fn on_request_headers(
    &mut self,
    envoy_filter: &mut EHF,
    end_of_stream: bool,
  ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
    self.has_request_body = request_has_body(envoy_filter, end_of_stream);

    let Some(actor) = self.config.resolve_actor_ref(envoy_filter) else {
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    };

    let key = actor.cache_key();
    self.actor_key = Some(key.clone());

    // A stale hit can't be replayed for a request with a body, so the cache isn't worth
    // the risk: route it through ext_proc. Its response still refreshes the cache.
    if self.has_request_body {
      increment_counter(envoy_filter, self.cache_bypass);
      self.was_cache_hit = false;
      self.learn_key = Some(key);
      return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
    }

    match cache::lookup(&key, Instant::now()) {
      Lookup::Hit(entry) => {
        increment_counter(envoy_filter, self.cache_hit);
        self.config.route_to(
          envoy_filter,
          &actor,
          &entry.worker_ip,
          entry.worker_spiffe_id.as_deref(),
        );
        envoy_filter.set_dynamic_metadata_string(
          CACHE_HIT_METADATA_NAMESPACE,
          CACHE_HIT_METADATA_KEY,
          CACHE_HIT_METADATA_VALUE,
        );
        envoy_filter.clear_route_cache();
        self.was_cache_hit = true;
        self.entry = Some(entry);
        return envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
      }
      Lookup::Expired => increment_counter(envoy_filter, self.cache_eviction),
      Lookup::Miss => {}
    }

    increment_counter(envoy_filter, self.cache_miss);
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

    if stale_rejection || grpc_unavailable {
      let should_replay = self.was_cache_hit
        && stale_rejection
        && is_replay_safe(envoy_filter, self, status_str.as_deref(), response_flags);

      self.handle_eviction(envoy_filter);

      // Only stale HTTP rejections (421 or 503 UF/UH/UC/UR) are replayed; gRPC UNAVAILABLE
      // is returned directly to the client since gRPC clients handle retries.
      if should_replay {
        if let Some(key) = &self.actor_key {
          // Upstream route mutations strip TARGET_ACTOR_HEADER before forwarding to
          // the actor tunnel. Re-inject it so the recreated stream passes the valid
          // actor reference to ext_proc on replay.
          envoy_filter.set_request_header(TARGET_ACTOR_HEADER, key.as_bytes());
        }
        increment_counter(envoy_filter, self.cache_replay);
        if envoy_filter.recreate_stream(None) {
          return envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration;
        }
      }
    }

    // On a cache miss that succeeded upstream, learn from the metadata ext_proc published.
    if !stale_rejection
      && !grpc_unavailable
      && let Some(key) = self.learn_key.take()
      && let Some((worker_ip, worker_spiffe_id)) = read_learned_binding(envoy_filter)
    {
      self.learned = Some(cache::insert(
        key,
        worker_ip,
        worker_spiffe_id,
        Duration::from_secs(self.config.cache_ttl_seconds),
        self.config.max_cached_actors,
      ));
    }

    envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
  }

  fn on_response_trailers(
    &mut self,
    envoy_filter: &mut EHF,
  ) -> envoy_dynamic_module_type_on_http_filter_response_trailers_status {
    if response_trailer_str(envoy_filter, GRPC_STATUS_HEADER)
      .as_deref()
      .is_some_and(is_grpc_unavailable)
    {
      self.handle_eviction(envoy_filter);
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
  use crate::config::DEFAULT_MAX_CACHED_ACTORS;
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
      .returning(|_| Some(EnvoyBuffer::new(b"GET")));
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| None);

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
    let entry = cache::current("team-b/actor-2").expect("should be in cache");
    assert_eq!(entry.worker_ip, "10.244.1.50");
    assert_eq!(
      entry.worker_spiffe_id.as_deref(),
      Some("spiffe://cluster.local/ns/default/sa/worker-pool-1")
    );
  }

  #[test]
  fn test_request_with_body_bypasses_cache_and_learns() {
    let key = "team-b/actor-body";
    let _entry = cache::insert(key.to_string(), "10.244.9.1".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(Config::default());
    let bypass_id = EnvoyCounterId(7);
    filter.cache_bypass = Some(bypass_id);

    // The mock panics on any unexpected call, so a cache hit (route_to setting metadata
    // and headers) would fail the test.
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_filter_state_bytes()
      .withf(|k| k == TARGET_ACTOR_FILTER_STATE_KEY)
      .returning(move |_| Some(EnvoyBuffer::new(key.as_bytes())));
    mock
      .expect_get_filter_state_bytes()
      .withf(|k| k == CONNECT_AUTHORITY_FILTER_STATE_KEY)
      .returning(|_| None);
    mock
      .expect_get_request_header_value()
      .withf(|k| k == ":method")
      .returning(|_| Some(EnvoyBuffer::new(b"POST")));
    mock
      .expect_get_request_header_value()
      .withf(|k| k == "content-length")
      .returning(|_| Some(EnvoyBuffer::new(b"12")));
    mock
      .expect_increment_counter()
      .withf(move |id, val| *id == bypass_id && *val == 1)
      .times(1)
      .returning(|_, _| Ok(()));

    let status = filter.on_request_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(filter.has_request_body);
    assert!(!filter.was_cache_hit);
    assert_eq!(filter.learn_key.as_deref(), Some(key));

    // ext_proc resolved a different worker: the response refreshes the cached binding.
    mock
      .expect_get_response_header_value()
      .withf(|k| k == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"200")));
    mock
      .expect_get_response_header_value()
      .withf(|k| k == GRPC_STATUS_HEADER)
      .returning(|_| None);
    mock.expect_get_attribute_int().returning(|_| None);
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, k| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && k == ORIGINAL_DST_ADDRESS_KEY
      })
      .returning(|_, _, _| Some(EnvoyBuffer::new(b"10.244.9.2:443")));
    mock
      .expect_get_metadata_string()
      .withf(|src, ns, k| {
        *src == envoy_dynamic_module_type_metadata_source::Dynamic
          && ns == ORIGINAL_DST_NAMESPACE
          && k == WORKER_SPIFFE_ID_KEY
      })
      .returning(|_, _, _| None);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert_eq!(cache::current(key).expect("should be cached").worker_ip, "10.244.9.2");
  }

  #[test]
  fn test_cache_hit_populates_metadata_and_skips_extproc() {
    let config = Config::default();
    let _entry = cache::insert("team-c/actor-3".to_string(), "10.244.2.100".to_string(), Some("spiffe://cluster.local/ns/default/sa/worker-pool-2".to_string()), Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

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
      .returning(|_| Some(EnvoyBuffer::new(b"GET")));
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
      .expect_set_dynamic_metadata_string()
      .withf(|ns, key, val| {
        ns == CACHE_HIT_METADATA_NAMESPACE
          && key == CACHE_HIT_METADATA_KEY
          && val == CACHE_HIT_METADATA_VALUE
      })
      .times(1)
      .returning(|_, _, _| ());
    mock.expect_clear_route_cache().returning(|| ());

    let status = filter.on_request_headers(&mut mock, true);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    );
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_recreates_stream_on_cache_hit() {
    let config = Config::default();
    let entry = cache::insert("team-d/actor-4".to_string(), "10.244.3.40".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-d/actor-4".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );

    assert!(cache::current("team-d/actor-4").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_recreates_stream_on_cache_hit() {
    let config = Config::default();
    let entry = cache::insert("team-d/actor-4-503".to_string(), "10.244.3.41".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-d/actor-4-503".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );

    assert!(cache::current("team-d/actor-4-503").is_none());
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
    assert!(cache::current("team-e/actor-5").is_none());
  }

  #[test]
  fn test_upstream_421_without_stale_header_is_not_treated_as_stale() {
    let config = Config::default();
    let entry = cache::insert("team-h/actor-8".to_string(), "10.244.5.10".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-8".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    assert!(cache::current("team-h/actor-8").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_false_stale_header_is_not_treated_as_stale() {
    let config = Config::default();
    let entry = cache::insert("team-h/actor-9".to_string(), "10.244.5.11".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-9".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    assert!(cache::current("team-h/actor-9").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_case_insensitive_true_stale_header() {
    let config = Config::default();
    let entry = cache::insert("team-h/actor-10".to_string(), "10.244.5.12".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-h/actor-10".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );
    assert!(cache::current("team-h/actor-10").is_none());
  }

  #[test]
  fn test_upstream_503_from_actor_is_not_treated_as_stale() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-11".to_string(), "10.244.6.1".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-11".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());

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
    assert!(cache::current("team-j/actor-11").is_some());
    assert!(filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_uh_recreates_stream_for_bodiless_get() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-12".to_string(), "10.244.6.2".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-12".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
    filter.has_request_body = false;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    // No healthy upstream (UH) flag set by Envoy. Zero bytes were sent.
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UH));
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-j/actor-12")
      .returning(|_, _| true);
    mock.expect_recreate_stream().returning(|_| true);

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );
    assert!(cache::current("team-j/actor-12").is_none());
    assert!(!filter.was_cache_hit);
  }

  /// Sets up a bodiless cache hit on `key` -> `ip` and returns the filter and the entry it was
  /// routed from.
  fn hit_with_503(key: &str, ip: &str) -> (CacheFilter, Rc<Entry>) {
    let entry = cache::insert(key.to_string(), ip.to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);
    let mut filter = CacheFilter::new_test_filter(Config::default());
    filter.actor_key = Some(key.to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
    (filter, entry)
  }

  fn mock_503(flags: i64) -> MockEnvoyHttpFilter {
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    mock.expect_get_attribute_int().returning(move |_| Some(flags));
    mock
  }

  fn expect_code_details(mock: &mut MockEnvoyHttpFilter, details: &'static [u8]) {
    mock
      .expect_get_attribute_string()
      .withf(|id| *id == envoy_dynamic_module_type_attribute_id::ResponseCodeDetails)
      .returning(move |_| Some(EnvoyBuffer::new(details)));
  }

  #[test]
  fn test_upstream_503_uc_replays_when_entry_is_stale_and_unspoiled() {
    let (mut filter, entry) = hit_with_503("team-k/actor-1", "10.244.7.1");
    // The network filter saw a tunnel for this binding refused.
    assert_eq!(cache::mark_stale("team-k/actor-1", "10.244.7.1"), 1);

    let mut mock = mock_503(RESPONSE_FLAG_UC);
    expect_code_details(
      &mut mock,
      b"upstream_reset_before_response_started{connection_termination}",
    );
    mock
      .expect_set_request_header()
      .withf(|key, val| key == TARGET_ACTOR_HEADER && val == b"team-k/actor-1")
      .returning(|_, _| true);
    mock.expect_recreate_stream().times(1).returning(|_| true);

    let status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
    );
    assert!(entry.is_stale());
    assert!(cache::current("team-k/actor-1").is_none());
  }

  #[test]
  fn test_upstream_503_uc_does_not_replay_after_a_1xx() {
    // The actor sent a 1xx, so it got the request, but its connection doesn't spoil the entry.
    let (mut filter, _entry) = hit_with_503("team-k/actor-6", "10.244.7.6");
    cache::mark_stale("team-k/actor-6", "10.244.7.6");

    let mut mock = mock_503(RESPONSE_FLAG_UC);
    expect_code_details(
      &mut mock,
      b"upstream_reset_after_response_started{connection_termination}",
    );
    let status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }

  #[test]
  fn test_upstream_503_ur_does_not_replay_even_when_entry_is_stale() {
    // UR can follow response bytes the network filter counted as an answer, so only UC
    // replays on the network filter's marks.
    let (mut filter, _entry) = hit_with_503("team-k/actor-2", "10.244.7.2");
    cache::mark_stale("team-k/actor-2", "10.244.7.2");

    let mut mock = mock_503(RESPONSE_FLAG_UR);
    let status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }

  #[test]
  fn test_upstream_503_uc_does_not_replay_when_entry_is_spoiled() {
    let (mut filter, _entry) = hit_with_503("team-k/actor-3", "10.244.7.3");
    // A refusal and a mid-flight drop on the same binding: some request may have run.
    cache::mark_stale("team-k/actor-3", "10.244.7.3");
    cache::mark_spoiled("team-k/actor-3", "10.244.7.3");

    // recreate_stream has no expectation, so calling it fails the test.
    let mut mock = mock_503(RESPONSE_FLAG_UC);
    let status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
  }

  #[test]
  fn test_upstream_503_uc_does_not_replay_when_entry_is_spoiled_but_not_stale() {
    let (mut filter, _entry) = hit_with_503("team-k/actor-4", "10.244.7.4");
    cache::mark_spoiled("team-k/actor-4", "10.244.7.4");

    let mut mock = mock_503(RESPONSE_FLAG_UC);
    let status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache::current("team-k/actor-4").is_none());
  }

  #[test]
  fn test_eviction_leaves_binding_learned_by_another_request() {
    let (mut filter, _old) = hit_with_503("team-k/actor-5", "10.244.7.5");
    // Another request already learned the actor's new worker.
    let newer = cache::insert(
      "team-k/actor-5".to_string(),
      "10.244.7.50".to_string(),
      None,
      Duration::from_secs(5),
      DEFAULT_MAX_CACHED_ACTORS,
    );

    let mut mock = mock_503(RESPONSE_FLAG_UC);
    filter.on_response_headers(&mut mock, false);
    assert!(Rc::ptr_eq(&cache::current("team-k/actor-5").unwrap(), &newer));
  }

  #[test]
  fn test_upstream_503_with_uh_does_not_replay_when_request_has_body() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-12-body".to_string(), "10.244.6.22".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-12-body".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
    filter.has_request_body = true;

    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_response_header_value()
      .withf(|key| key == ":status")
      .returning(|_| Some(EnvoyBuffer::new(b"503")));
    mock
      .expect_get_attribute_int()
      .returning(|_| Some(RESPONSE_FLAG_UH));

    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache::current("team-j/actor-12-body").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_uc_does_not_replay_when_request_has_body() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-13".to_string(), "10.244.6.3".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-13".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
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

    // Because the request has a body (and bodies are not buffered in memory), recreate_stream must NOT be called.
    // However, the dead worker binding must still be evicted!
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache::current("team-j/actor-13").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_uc_does_not_replay_when_entry_is_not_marked() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-14".to_string(), "10.244.6.4".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-14".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
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

    // No refusal marked the entry stale, so the UC may be a tunnel cut off after the actor
    // got the request. It is evicted, but not replayed.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache::current("team-j/actor-14").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_503_with_ur_does_not_replay() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-14-ur".to_string(), "10.244.6.41".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-14-ur".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
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

    // UR is evicted but not replayed.
    let resp_status = filter.on_response_headers(&mut mock, false);
    assert_eq!(
      resp_status,
      envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    );
    assert!(cache::current("team-j/actor-14-ur").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_stale_rejection_does_not_replay_when_request_has_body() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-15".to_string(), "10.244.6.5".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-15".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
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
    assert!(cache::current("team-j/actor-15").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_upstream_421_with_body_does_not_replay() {
    let config = Config::default();
    let entry = cache::insert("team-j/actor-16-421".to_string(), "10.244.6.7".to_string(), None, Duration::from_secs(5), DEFAULT_MAX_CACHED_ACTORS);

    let mut filter = CacheFilter::new_test_filter(config);
    filter.actor_key = Some("team-j/actor-16-421".to_string());
    filter.was_cache_hit = true;
    filter.entry = Some(entry.clone());
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
    assert!(cache::current("team-j/actor-16-421").is_none());
    assert!(!filter.was_cache_hit);
  }

  #[test]
  fn test_on_request_headers_tracks_request_body() {
    let config = Config::default();

    // 1. Request with end_of_stream: true (no body)
    let mut filter = CacheFilter::new_test_filter(config.clone());
    let mut mock = MockEnvoyHttpFilter::default();
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
    let mut filter = CacheFilter::new_test_filter(config.clone());
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

    // 4. A GET or HEAD without end_of_stream or content-length may carry a body (a chunked
    // or HTTP/2 GET, or an upgrade), so it counts as having one.
    for method in [b"GET".as_slice(), b"HEAD".as_slice()] {
      let mut filter = CacheFilter::new_test_filter(config.clone());
      let mut mock = MockEnvoyHttpFilter::default();
      mock
        .expect_get_request_header_value()
        .withf(|key| key == "content-length")
        .returning(|_| None);
      mock
        .expect_get_request_header_value()
        .withf(|key| key == ":method")
        .returning(move |_| Some(EnvoyBuffer::new(method)));
      mock.expect_get_filter_state_bytes().returning(|_| None);
      mock
        .expect_get_request_header_values()
        .returning(|_| vec![]);
      filter.on_request_headers(&mut mock, false);
      assert!(filter.has_request_body);
    }

    // 5. An unparsable content-length counts as a body.
    let mut filter = CacheFilter::new_test_filter(config);
    let mut mock = MockEnvoyHttpFilter::default();
    mock
      .expect_get_request_header_value()
      .withf(|key| key == "content-length")
      .returning(|_| Some(EnvoyBuffer::new(b"abc")));
    mock.expect_get_filter_state_bytes().returning(|_| None);
    mock
      .expect_get_request_header_values()
      .returning(|_| vec![]);
    filter.on_request_headers(&mut mock, false);
    assert!(filter.has_request_body);
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

    let entry = cache::current("team-f/actor-6").expect("entry should be cached");
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
    assert!(cache::current("team-grpc/actor-miss-h").is_none());
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
    assert!(cache::current("team-grpc/actor-miss-t").is_some());

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
    assert!(cache::current("team-grpc/actor-miss-t").is_none());
  }
}
